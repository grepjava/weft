//! Readiness for descriptors the application registers with the event loop:
//! the loop's own wakeup socket, database drivers, outgoing HTTP clients.
//!
//! They are registered with the same Tokio reactor that serves connections,
//! so one wait covers both. Tokio's readiness is edge-triggered and cached,
//! while a selector is level-triggered, so Tokio's answer is treated as a
//! hint: a descriptor it reports ready is confirmed with a zero-timeout OS
//! poll before it is handed to asyncio, and cleared (re-arming Tokio) when
//! the OS disagrees.

use std::io;
use std::task::Context;
#[cfg(unix)]
use std::task::Poll;

pub const READ: u32 = 1;
pub const WRITE: u32 = 2;

pub struct Reg {
    pub events: u32,
    src: imp::Src,
}

impl Reg {
    /// Must be called inside the runtime's context.
    pub fn new(fd: i64, events: u32) -> io::Result<Reg> {
        Ok(Reg {
            events,
            src: imp::Src::open(fd, events)?,
        })
    }

    /// Events Tokio believes are ready. Registers the waker for the rest.
    pub fn hint(&self, cx: &mut Context<'_>) -> u32 {
        let mut ready = 0;
        if self.events & READ != 0 && self.src.poll(cx, true) {
            ready |= READ;
        }
        if self.events & WRITE != 0 && self.src.poll(cx, false) {
            ready |= WRITE;
        }
        ready
    }

    /// Forgets readiness the OS did not confirm and re-arms the waker.
    /// Returns true when readiness is (again) reported straight away.
    pub fn rearm(&self, cx: &mut Context<'_>, bits: u32) -> bool {
        let mut again = false;
        if bits & READ != 0 {
            again |= self.src.clear(cx, true);
        }
        if bits & WRITE != 0 {
            again |= self.src.clear(cx, false);
        }
        again
    }
}

/// Zero-timeout check of `(fd, events)` pairs; writes confirmed events.
pub fn confirm(fds: &[(i64, u32)], out: &mut Vec<u32>) {
    out.clear();
    out.resize(fds.len(), 0);
    imp::confirm(fds, out);
}

/// Blocks until `io` can take more bytes, for at most `ms`. For code that has
/// to write from inside a synchronous call, where there is no task to park.
pub fn wait_writable(io: &tokio::net::TcpStream, ms: i32) -> bool {
    #[cfg(unix)]
    let fd = std::os::fd::AsRawFd::as_raw_fd(io) as i64;
    #[cfg(windows)]
    let fd = std::os::windows::io::AsRawSocket::as_raw_socket(io) as i64;
    imp::wait_writable(fd, ms)
}

#[cfg(unix)]
mod imp {
    use super::*;
    use std::os::fd::{AsRawFd, RawFd};
    use tokio::io::Interest;
    use tokio::io::unix::AsyncFd;

    struct Fd(RawFd);

    impl AsRawFd for Fd {
        fn as_raw_fd(&self) -> RawFd {
            self.0
        }
    }

    pub struct Src(AsyncFd<Fd>);

    impl Src {
        pub fn open(fd: i64, events: u32) -> io::Result<Src> {
            let interest = match (events & READ != 0, events & WRITE != 0) {
                (true, true) => Interest::READABLE | Interest::WRITABLE,
                (false, true) => Interest::WRITABLE,
                _ => Interest::READABLE,
            };
            AsyncFd::with_interest(Fd(fd as RawFd), interest).map(Src)
        }

        pub fn poll(&self, cx: &mut Context<'_>, read: bool) -> bool {
            let r = if read {
                self.0.poll_read_ready(cx).map(|r| r.map(drop))
            } else {
                self.0.poll_write_ready(cx).map(|r| r.map(drop))
            };
            r.is_ready()
        }

        pub fn clear(&self, cx: &mut Context<'_>, read: bool) -> bool {
            if read {
                match self.0.poll_read_ready(cx) {
                    Poll::Ready(Ok(mut g)) => g.clear_ready(),
                    Poll::Ready(Err(_)) => return true,
                    Poll::Pending => return false,
                }
                self.0.poll_read_ready(cx).is_ready()
            } else {
                match self.0.poll_write_ready(cx) {
                    Poll::Ready(Ok(mut g)) => g.clear_ready(),
                    Poll::Ready(Err(_)) => return true,
                    Poll::Pending => return false,
                }
                self.0.poll_write_ready(cx).is_ready()
            }
        }
    }

    pub fn confirm(fds: &[(i64, u32)], out: &mut [u32]) {
        let mut pfds: Vec<libc::pollfd> = fds
            .iter()
            .map(|&(fd, ev)| libc::pollfd {
                fd: fd as RawFd,
                events: (if ev & READ != 0 { libc::POLLIN } else { 0 })
                    | (if ev & WRITE != 0 { libc::POLLOUT } else { 0 }),
                revents: 0,
            })
            .collect();
        let n = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, 0) };
        if n <= 0 {
            return;
        }
        for (i, p) in pfds.iter().enumerate() {
            let r = p.revents;
            if r & libc::POLLNVAL != 0 {
                continue;
            }
            let mut ev = 0;
            if r & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                ev |= READ;
            }
            if r & (libc::POLLOUT | libc::POLLHUP | libc::POLLERR) != 0 {
                ev |= WRITE;
            }
            out[i] = ev & fds[i].1;
        }
    }

    pub fn wait_writable(fd: i64, ms: i32) -> bool {
        let mut p = libc::pollfd {
            fd: fd as RawFd,
            events: libc::POLLOUT,
            revents: 0,
        };
        unsafe { libc::poll(&mut p, 1, ms) > 0 }
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::mem::ManuallyDrop;
    use std::os::windows::io::{FromRawSocket, RawSocket};
    use tokio::io::Interest;
    use windows_sys::Win32::Networking::WinSock::{
        POLLERR, POLLHUP, POLLNVAL, POLLRDNORM, POLLWRNORM, SOCKET, WSAPOLLFD, WSAPoll,
    };

    /// Tokio on Windows cannot watch a socket it does not own, so it watches
    /// a duplicate handle to the same socket. Readiness is a property of the
    /// socket, not the handle, and dropping the duplicate leaves the
    /// application's handle open.
    pub struct Src(tokio::net::TcpStream);

    impl Src {
        pub fn open(fd: i64, _events: u32) -> io::Result<Src> {
            let orig =
                ManuallyDrop::new(unsafe { socket2::Socket::from_raw_socket(fd as RawSocket) });
            let dup = orig.try_clone()?;
            let std: std::net::TcpStream = dup.into();
            tokio::net::TcpStream::from_std(std).map(Src)
        }

        pub fn poll(&self, cx: &mut Context<'_>, read: bool) -> bool {
            if read {
                self.0.poll_read_ready(cx).is_ready()
            } else {
                self.0.poll_write_ready(cx).is_ready()
            }
        }

        pub fn clear(&self, cx: &mut Context<'_>, read: bool) -> bool {
            let interest = if read {
                Interest::READABLE
            } else {
                Interest::WRITABLE
            };
            let _ = self
                .0
                .try_io(interest, || Err::<(), _>(io::ErrorKind::WouldBlock.into()));
            self.poll(cx, read)
        }
    }

    pub fn confirm(fds: &[(i64, u32)], out: &mut [u32]) {
        let mut pfds: Vec<WSAPOLLFD> = fds
            .iter()
            .map(|&(fd, ev)| WSAPOLLFD {
                fd: fd as SOCKET,
                events: (if ev & READ != 0 { POLLRDNORM } else { 0 })
                    | (if ev & WRITE != 0 { POLLWRNORM } else { 0 }),
                revents: 0,
            })
            .collect();
        let n = unsafe { WSAPoll(pfds.as_mut_ptr(), pfds.len() as u32, 0) };
        if n <= 0 {
            return;
        }
        for (i, p) in pfds.iter().enumerate() {
            let r = p.revents;
            if r & POLLNVAL != 0 {
                continue;
            }
            let mut ev = 0;
            if r & (POLLRDNORM | POLLHUP | POLLERR) != 0 {
                ev |= READ;
            }
            if r & (POLLWRNORM | POLLHUP | POLLERR) != 0 {
                ev |= WRITE;
            }
            out[i] = ev & fds[i].1;
        }
    }

    pub fn wait_writable(fd: i64, ms: i32) -> bool {
        let mut p = WSAPOLLFD {
            fd: fd as SOCKET,
            events: POLLWRNORM,
            revents: 0,
        };
        unsafe { WSAPoll(&mut p, 1, ms) > 0 }
    }
}
