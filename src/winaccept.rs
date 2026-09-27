//! Accepting on Windows from a listening socket other workers share.
//!
//! When several processes or threads accept from one listener, Windows can
//! block a non-blocking `accept()` indefinitely: all of them see the socket
//! readable, one takes the connection, and the others wait inside the call
//! for the next one. A worker would sit there holding the GIL. `AcceptEx`
//! has no such race -- the kernel completes each outstanding request with a
//! connection of its own -- and can be cancelled.
//!
//! Outstanding `AcceptEx` requests are served most recent first, so a worker
//! that keeps one posted takes every connection from workers that re-post
//! a moment later. Instead a request is posted only once a connection is
//! waiting, and withdrawn if another worker got there first: every worker
//! competes for every connection, as with readiness-based accepting.
//!
//! This runs on a small thread per worker, which hands connections to the
//! runtime: one cross-thread handoff per connection, none per request.

use std::io;
use std::mem::{size_of, zeroed};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::os::windows::io::AsRawSocket;
use std::ptr;
use std::sync::Arc;
use std::thread::JoinHandle;

use socket2::{Domain, Socket, Type};
use tokio::sync::mpsc::UnboundedSender;
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0};
use windows_sys::Win32::Networking::WinSock::{
    AcceptEx, POLLRDNORM, SO_UPDATE_ACCEPT_CONTEXT, SOCKADDR_STORAGE, SOCKET, SOL_SOCKET,
    WSA_IO_PENDING, WSAGetLastError, WSAPOLLFD, WSAPoll, setsockopt,
};
use windows_sys::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::Threading::{
    CreateEventW, ResetEvent, SetEvent, WaitForMultipleObjects,
};

pub type Accepted = (TcpStream, SocketAddr);

/// How long a posted request waits for the connection that was waiting
/// when it was posted, before concluding another worker took it.
const GRAB_MS: u32 = 5;

/// A manual-reset event.
struct Event(HANDLE);

unsafe impl Send for Event {}
unsafe impl Sync for Event {}

impl Event {
    fn new() -> io::Result<Event> {
        let h = unsafe { CreateEventW(ptr::null(), 1, 0, ptr::null()) };
        if h.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Event(h))
        }
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.0) };
    }
}

/// Stops the accept thread: the event ends a wait on a posted request, a
/// datagram to the thread's own socket ends a wait for readiness.
struct Stop {
    event: Event,
    wake: UdpSocket,
}

impl Stop {
    fn set(&self) {
        unsafe { SetEvent(self.event.0) };
        if let Ok(addr) = self.wake.local_addr() {
            let _ = self.wake.send_to(&[0], addr);
        }
    }

    fn is_set(&self) -> bool {
        unsafe {
            windows_sys::Win32::System::Threading::WaitForSingleObject(self.event.0, 0)
                == WAIT_OBJECT_0
        }
    }
}

pub struct Acceptor {
    stop: Arc<Stop>,
    thread: Option<JoinHandle<()>>,
}

impl Acceptor {
    /// Accepts from `listener` (a handle of its own) until dropped, or until
    /// the receiving side of `tx` goes away.
    pub fn spawn(listener: Socket, tx: UnboundedSender<Accepted>) -> io::Result<Acceptor> {
        let domain = listener.local_addr()?.domain();
        let stop = Arc::new(Stop {
            event: Event::new()?,
            wake: UdpSocket::bind("127.0.0.1:0")?,
        });
        let ready = Event::new()?;
        let stop2 = stop.clone();
        let thread = std::thread::Builder::new()
            .name("weft-accept".into())
            .spawn(move || run(&listener, domain, &ready, &stop2, &tx))?;
        Ok(Acceptor {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Acceptor {
    fn drop(&mut self) {
        self.stop.set();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

const ADDR_LEN: u32 = (size_of::<SOCKADDR_STORAGE>() + 16) as u32;

/// Waits until a connection is queued on `lsock`; false when asked to stop.
fn wait_queued(lsock: SOCKET, stop: &Stop) -> bool {
    let mut fds = [
        WSAPOLLFD {
            fd: lsock,
            events: POLLRDNORM,
            revents: 0,
        },
        WSAPOLLFD {
            fd: stop.wake.as_raw_socket() as SOCKET,
            events: POLLRDNORM,
            revents: 0,
        },
    ];
    loop {
        if stop.is_set() {
            return false;
        }
        let n = unsafe { WSAPoll(fds.as_mut_ptr(), 2, -1) };
        if n < 0 || fds[1].revents != 0 {
            return false;
        }
        if fds[0].revents != 0 {
            return true;
        }
    }
}

fn run(
    listener: &Socket,
    domain: Domain,
    ready: &Event,
    stop: &Stop,
    tx: &UnboundedSender<Accepted>,
) {
    let lsock = listener.as_raw_socket() as SOCKET;
    let mut buf = [0u8; 2 * ADDR_LEN as usize];
    while !tx.is_closed() {
        if !wait_queued(lsock, stop) {
            return;
        }
        let Ok(acc) = Socket::new(domain, Type::STREAM, None) else {
            // Out of sockets: back off.
            std::thread::sleep(std::time::Duration::from_millis(10));
            continue;
        };
        // No handle to this listener is associated with a completion port,
        // so the event alone reports completion.
        let mut ov: OVERLAPPED = unsafe { zeroed() };
        ov.hEvent = ready.0;
        unsafe { ResetEvent(ready.0) };
        let mut n = 0u32;
        let ok = unsafe {
            AcceptEx(
                lsock,
                acc.as_raw_socket() as SOCKET,
                buf.as_mut_ptr().cast(),
                0,
                ADDR_LEN,
                ADDR_LEN,
                &mut n,
                &mut ov,
            )
        };
        if ok == 0 {
            if unsafe { WSAGetLastError() } != WSA_IO_PENDING {
                std::thread::sleep(std::time::Duration::from_millis(10));
                continue;
            }
            let handles = [ready.0, stop.event.0];
            let w = unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, GRAB_MS) };
            if w != WAIT_OBJECT_0 {
                // Another worker took it, or asked to stop. The request must
                // be finished before `ov` and `buf` go away, and it may have
                // completed meanwhile.
                unsafe { CancelIoEx(lsock as HANDLE, &ov) };
            }
            if unsafe { GetOverlappedResult(lsock as HANDLE, &ov, &mut n, 1) } == 0 {
                continue;
            }
        }
        let updated = unsafe {
            setsockopt(
                acc.as_raw_socket() as SOCKET,
                SOL_SOCKET,
                SO_UPDATE_ACCEPT_CONTEXT,
                (&raw const lsock).cast(),
                size_of::<SOCKET>() as i32,
            )
        } == 0;
        if !updated {
            continue;
        }
        let Some(peer) = acc.peer_addr().ok().and_then(|a| a.as_socket()) else {
            continue;
        };
        if acc.set_nonblocking(true).is_err() {
            continue;
        }
        if tx.send((acc.into(), peer)).is_err() {
            return;
        }
    }
}
