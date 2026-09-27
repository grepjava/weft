//! One worker: a Tokio runtime on the asyncio thread, driven as the event
//! loop's selector.
//!
//! asyncio's `_run_once` calls `selector.select(timeout)` once per iteration.
//! Here that call runs the worker's current-thread Tokio runtime until one of
//!
//! * a server task scheduled Python work (created a request task, resolved a
//!   future) -- asyncio has callbacks to run;
//! * a descriptor the application registered became ready;
//! * another thread called `call_soon_threadsafe`;
//! * the timeout asyncio asked for elapsed.
//!
//! Accepting, parsing and writing happen inside that call, as Tokio tasks on
//! the same thread. There is no second thread, no queue between the server
//! and the loop, and no `call_soon_threadsafe` on the request path. The GIL is
//! held while tasks run -- they call into Python directly -- and released only
//! while Tokio parks in the OS wait, so other Python threads (a thread pool
//! running sync endpoints, say) run whenever the server has nothing to do.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::net::SocketAddr;
use std::pin::pin;
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Poll, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pyo3_ffi::*;
use slab::Slab;
use tokio::net::TcpListener;
use tokio::runtime::{LocalOptions, LocalRuntime};
use tokio::sync::Notify;

use crate::fdpoll::{self, Reg};
use crate::http::{self, Conn};
use crate::py::PyRef;

thread_local! {
    /// The worker serving on this thread. `send` and `receive` arrive from
    /// Python carrying only a token, and find their connection through it.
    static CURRENT: Cell<*const Shared> = const { Cell::new(ptr::null()) };
    /// Inside `select`, which is the only place the runtime is driven.
    static IN_SELECT: Cell<bool> = const { Cell::new(false) };
    /// A task scheduled Python work during this `select`.
    static PY_PENDING: Cell<bool> = const { Cell::new(false) };
    static SAVED: Cell<*mut PyThreadState> = const { Cell::new(ptr::null_mut()) };
}

/// Tokio is about to wait in the OS. Let other Python threads run, unless a
/// task has already scheduled Python work, in which case the park is a
/// zero-timeout poll and releasing the GIL would only churn it.
fn before_park() {
    if IN_SELECT.get() && !PY_PENDING.get() {
        SAVED.set(unsafe { PyEval_SaveThread() });
    }
}

fn after_unpark() {
    let ts = SAVED.replace(ptr::null_mut());
    if !ts.is_null() {
        unsafe { PyEval_RestoreThread(ts) };
    }
}

/// Resolves the current thread's worker.
#[inline]
pub fn current<'a>() -> Option<&'a Shared> {
    let p = CURRENT.get();
    if p.is_null() {
        None
    } else {
        Some(unsafe { &*p })
    }
}

/// A key identifying the calling thread without touching `thread::current()`.
#[inline]
pub fn thread_key() -> usize {
    CURRENT.with(|c| c as *const _ as usize)
}

/// Everything a request needs from the application side, fixed at `serve`.
pub struct AppCtx {
    pub app: PyRef,
    pub loop_: PyRef,
    pub task_type: PyRef,
    pub future_type: PyRef,
    /// `("loop",)`, the kwnames for `Task(coro, loop=...)` and `Future(loop=...)`.
    pub kw_loop: PyRef,
    /// Every constant scope entry, shallow-copied per request.
    pub scope_proto: PyRef,
    /// The same for the `websocket` scope.
    pub ws_scope_proto: PyRef,
    pub ws: crate::ws::WsCfg,
    pub state: Option<PyRef>,
    pub report: Option<PyRef>,
    /// The awaitable `send` returns when it did not have to wait.
    pub done: PyRef,
    pub keep_alive: Duration,
    pub header_timeout: Duration,
    pub max_head: usize,
    pub max_body: u64,
    /// How long a request may go without its bytes moving; `None` for ever.
    pub request_timeout: Option<Duration>,
    pub server_header: bool,
    pub date_header: bool,
    /// Set when the application is WSGI rather than ASGI.
    pub wsgi: Option<crate::wsgi::WsgiCtx>,
    pub trusted: Option<crate::ops::Trusted>,
    pub request_id: bool,
    pub trace_context: bool,
    pub request_start: bool,
    /// `--health-check-path`, answered without the application.
    pub health: Option<Vec<u8>>,
    pub max_conns: usize,
    /// Set when `--rate-limit` is on; the shared table lives in `limit`.
    pub rate: Option<crate::limit::RateLimit>,
    pub static_dirs: Vec<crate::staticf::Route>,
    pub compress: bool,
    pub compress_static: bool,
    pub compress_min: usize,
    pub cache: Option<crate::cache::Cfg>,
    /// Scheme reported to the application and used as the cache key.
    pub https: bool,
    pub tls: Option<std::sync::Arc<rustls::ServerConfig>>,
    /// `Strict-Transport-Security` value, when `--hsts` is set.
    pub hsts: Option<Vec<u8>>,
    pub http2: bool,
    pub http2_only: bool,
    pub metrics_bind: Option<(String, u16)>,
    pub quic: Option<QuicListen>,
    /// UDP port advertised as `alt-svc: h3=":port"`.
    pub alt_svc: Option<u16>,
    pub h3_extensions: Option<PyRef>,
}

#[derive(Clone)]
pub struct QuicListen {
    pub host: String,
    pub port: u16,
    pub server: quinn::ServerConfig,
}

#[derive(Default)]
pub struct Fnv(u64);

impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        let mut h = if self.0 == 0 {
            0xcbf29ce484222325
        } else {
            self.0
        };
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        self.0 = h;
    }
}

pub struct Shared {
    regs: RefCell<HashMap<i64, Reg>>,
    /// Wakers queued by Python-side calls, run inside the runtime's context
    /// at the start of the next `select`, where waking a task is a push onto
    /// the local run queue rather than a syscall to unpark the driver.
    deferred: RefCell<Vec<Waker>>,
    main_waker: RefCell<Option<Waker>>,
    app: RefCell<Option<Rc<AppCtx>>>,
    pub conns: RefCell<Slab<Rc<Conn>>>,
    generation: Cell<u32>,
    pub stopping: Cell<bool>,
    /// `--drain-delay` is running: still serving, but the health check fails
    /// and responses close their connections.
    pub draining: Cell<bool>,
    max_conns: Cell<usize>,
    pub stop: Notify,
    /// Raw header name as received -> lowercased `bytes`, bounded.
    hcache: RefCell<HashMap<Box<[u8]>, PyRef, BuildHasherDefault<Fnv>>>,
    date: Cell<(u64, [u8; 29])>,
    /// Sends completed without waiting since the last `select`.
    sends: Cell<u32>,
    #[cfg(windows)]
    acceptor: RefCell<Option<crate::winaccept::Acceptor>>,
}

/// Sends that may complete without waiting between two `select` calls. A
/// task past it yields once, so that a loop of sends to a fast socket cannot
/// keep the event loop from accepting, reading or running anything else.
const SEND_BUDGET: u32 = 256;

impl Shared {
    /// Counts one send; `true` once this loop iteration has had its share.
    #[inline]
    pub fn over_send_budget(&self) -> bool {
        let n = self.sends.get() + 1;
        self.sends.set(n);
        n > SEND_BUDGET
    }

    /// Responses should close their connections.
    #[inline]
    pub fn closing(&self) -> bool {
        self.stopping.get() || self.draining.get()
    }

    pub fn max_conns(&self) -> usize {
        self.max_conns.get()
    }

    /// Takes a new connection, or answers 503 and hangs up when the worker
    /// already has `--max-connections`.
    fn admit(self: &Rc<Self>, io: tokio::net::TcpStream, peer: SocketAddr, unix: bool) {
        let _ = io.set_nodelay(true);
        let tls = self.app().and_then(|c| c.tls.clone());
        let stream = match tls {
            Some(cfg) => match crate::tls::Io::server(io, cfg) {
                Ok(s) => s,
                Err(_) => return,
            },
            None => crate::tls::Io::plain(io),
        };
        let tcp = self
            .conns
            .borrow()
            .iter()
            .filter(|(_, c)| !c.stream)
            .count();
        if tcp >= self.max_conns.get() {
            // Closing with the request unread would reset the connection and
            // could lose the answer, so what arrives is read and dropped for
            // a moment first.
            crate::metrics::rejected();
            tokio::task::spawn_local(async move {
                let _ = tokio::time::timeout(Duration::from_secs(1), stream.handshake()).await;
                if tokio::time::timeout(Duration::from_secs(1), stream.writable())
                    .await
                    .is_ok()
                {
                    let _ = stream.try_write(b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                    let _ = stream.flush_tls_only();
                }
                stream.shutdown_write();
                let mut sink = [0u8; 4096];
                let _ = tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if stream.readable().await.is_err() {
                            return;
                        }
                        match stream.try_read(&mut sink) {
                            Ok(0) => return,
                            Ok(_) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                            Err(_) => return,
                        }
                    }
                })
                .await;
            });
            return;
        }
        http::spawn(self, stream, peer, unix);
    }

    #[inline]
    pub fn app(&self) -> Option<Rc<AppCtx>> {
        self.app.borrow().clone()
    }

    #[inline]
    pub fn conn(&self, slot: u32, generation: u32) -> Option<Rc<Conn>> {
        self.conns
            .borrow()
            .get(slot as usize)
            .filter(|c| c.generation == generation)
            .cloned()
    }

    #[inline]
    pub fn next_generation(&self) -> u32 {
        let g = self.generation.get().wrapping_add(1);
        self.generation.set(g);
        g
    }

    /// A server task has given asyncio callbacks to run: end this `select`.
    #[inline]
    pub fn py_scheduled(&self) {
        PY_PENDING.set(true);
        if let Some(w) = self.main_waker.borrow().as_ref() {
            w.wake_by_ref();
        }
    }

    #[inline]
    pub fn defer_wake(&self, w: Waker) {
        self.deferred.borrow_mut().push(w);
    }

    fn run_deferred(&self) {
        let wakers = std::mem::take(&mut *self.deferred.borrow_mut());
        for w in wakers {
            w.wake();
        }
    }

    pub unsafe fn header_name(&self, raw: &[u8]) -> crate::py::PResult<PyRef> {
        if let Some(v) = self.hcache.borrow().get(raw) {
            return Ok(v.clone());
        }
        let mut lower = [0u8; 128];
        let name = if raw.len() <= lower.len() {
            for (d, s) in lower.iter_mut().zip(raw) {
                *d = s.to_ascii_lowercase();
            }
            unsafe { crate::py::bytes(&lower[..raw.len()])? }
        } else {
            return unsafe { crate::py::bytes(&raw.to_ascii_lowercase()) };
        };
        let mut cache = self.hcache.borrow_mut();
        // Stop growing at a bound: a flood of unique names must not become
        // a way to exhaust memory.
        if cache.len() < 512 {
            cache.insert(raw.into(), name.clone());
        }
        Ok(name)
    }

    /// The `Date` header value, reformatted at most once a second.
    pub fn date(&self) -> [u8; 29] {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let (secs, buf) = self.date.get();
        if secs == now {
            return buf;
        }
        let buf = http::format_date(now);
        self.date.set((now, buf));
        buf
    }

    fn poll_regs(
        &self,
        cx: &mut std::task::Context<'_>,
        events: &mut Vec<(i64, u32)>,
        spins: &mut u32,
    ) {
        let regs = self.regs.borrow();
        if regs.is_empty() {
            return;
        }
        let mut hinted: Vec<(i64, u32)> = Vec::new();
        for (&fd, reg) in regs.iter() {
            let h = reg.hint(cx);
            if h != 0 {
                hinted.push((fd, h));
            }
        }
        if hinted.is_empty() {
            return;
        }
        let mut confirmed = Vec::new();
        fdpoll::confirm(&hinted, &mut confirmed);
        for (&(fd, hint), &ok) in hinted.iter().zip(&confirmed) {
            if ok != 0 {
                events.push((fd, ok));
            }
            let stale = hint & !ok;
            if stale != 0 && regs[&fd].rearm(cx, stale) && *spins < 8 {
                // Readiness arrived between the check and the re-arm, so no
                // waker is registered for it: look again.
                *spins += 1;
                cx.waker().wake_by_ref();
            }
        }
    }
}

pub struct Remote {
    notify: Notify,
}

impl Remote {
    pub fn wake(&self) {
        self.notify.notify_one();
    }
}

pub struct Core {
    // Declared first so that it is dropped first: its tasks own connections,
    // and connections own Python references.
    rt: LocalRuntime,
    pub sh: Rc<Shared>,
    pub remote: Arc<Remote>,
    pub owner: usize,
}

impl Core {
    pub fn new() -> std::io::Result<Core> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .on_thread_park(before_park)
            .on_thread_unpark(after_unpark)
            .build_local(LocalOptions::default())?;
        Ok(Core {
            rt,
            sh: Rc::new(Shared {
                regs: RefCell::new(HashMap::new()),
                deferred: RefCell::new(Vec::new()),
                main_waker: RefCell::new(None),
                app: RefCell::new(None),
                conns: RefCell::new(Slab::with_capacity(256)),
                generation: Cell::new(0),
                stopping: Cell::new(false),
                draining: Cell::new(false),
                max_conns: Cell::new(usize::MAX),
                stop: Notify::new(),
                hcache: RefCell::new(HashMap::default()),
                date: Cell::new((0, [0; 29])),
                sends: Cell::new(0),
                #[cfg(windows)]
                acceptor: RefCell::new(None),
            }),
            remote: Arc::new(Remote {
                notify: Notify::new(),
            }),
            owner: thread_key(),
        })
    }

    fn make_current(&self) {
        CURRENT.set(Rc::as_ptr(&self.sh));
    }

    /// One selector call. `None` waits indefinitely, `<= 0` only polls.
    pub fn select(&self, timeout: Option<f64>) -> Vec<(i64, u32)> {
        self.make_current();
        PY_PENDING.set(false);
        IN_SELECT.set(true);
        self.sh.sends.set(0);
        let zero = matches!(timeout, Some(t) if t <= 0.0);
        let deadline = match timeout {
            Some(t) if t > 0.0 => Some(Instant::now() + Duration::from_secs_f64(t.min(86_400.0))),
            _ => None,
        };
        let sh = self.sh.clone();
        let remote = self.remote.clone();
        let events = self.rt.block_on(async move {
            let mut events = Vec::new();
            let mut notified = pin!(remote.notify.notified());
            let mut sleep = deadline.map(|d| Box::pin(tokio::time::sleep_until(d.into())));
            let mut yielded = pin!(tokio::task::yield_now());
            let mut spins = 0u32;
            std::future::poll_fn(|cx| {
                sh.run_deferred();
                {
                    let mut w = sh.main_waker.borrow_mut();
                    if !w.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                        *w = Some(cx.waker().clone());
                    }
                }
                sh.poll_regs(cx, &mut events, &mut spins);
                if !events.is_empty() || PY_PENDING.get() {
                    return Poll::Ready(());
                }
                if notified.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(());
                }
                if zero {
                    // One tick: run what is runnable, poll the driver without
                    // blocking (a deferred yield parks with a zero timeout and
                    // skips the park hooks), and return.
                    return yielded.as_mut().poll(cx);
                }
                if let Some(s) = sleep.as_mut()
                    && s.as_mut().poll(cx).is_ready()
                {
                    return Poll::Ready(());
                }
                Poll::Pending
            })
            .await;
            events
        });
        IN_SELECT.set(false);
        crate::ops::flush_log();
        events
    }

    pub fn register(&self, fd: i64, events: u32) -> std::io::Result<()> {
        let _g = self.rt.enter();
        let reg = Reg::new(fd, events)?;
        // Dropping the old registration first: epoll refuses a second ADD.
        self.sh.regs.borrow_mut().remove(&fd);
        self.sh.regs.borrow_mut().insert(fd, reg);
        Ok(())
    }

    pub fn unregister(&self, fd: i64) {
        let _g = self.rt.enter();
        self.sh.regs.borrow_mut().remove(&fd);
    }

    pub fn modify(&self, fd: i64, events: u32) -> std::io::Result<()> {
        if let Some(r) = self.sh.regs.borrow_mut().get_mut(&fd)
            && r.events == events
        {
            return Ok(());
        }
        self.register(fd, events)
    }

    /// Starts accepting on a duplicate of the listening socket `fd`.
    /// `exclusive`: no other worker accepts from it.
    pub fn serve(&self, fd: i64, ctx: AppCtx, exclusive: bool) -> std::io::Result<()> {
        let _g = self.rt.enter();
        let unix = is_unix(fd);
        #[cfg(windows)]
        if !exclusive && !unix {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let acceptor = crate::winaccept::Acceptor::spawn(borrow_socket(fd).try_clone()?, tx)?;
            *self.sh.acceptor.borrow_mut() = Some(acceptor);
            self.start(ctx);
            self.rt.spawn_local(accept_handoff(self.sh.clone(), rx));
            return Ok(());
        }
        #[cfg(unix)]
        let _ = exclusive;
        let listener = TcpListener::from_std(dup_listener(fd)?)?;
        self.start(ctx);
        self.rt
            .spawn_local(accept_loop(self.sh.clone(), listener, unix));
        Ok(())
    }

    fn start(&self, ctx: AppCtx) {
        if crate::ops::log_enabled() {
            self.rt.spawn_local(crate::ops::flusher());
        }
        if let Some((host, port)) = &ctx.metrics_bind {
            let cache = ctx.cache.is_some();
            self.rt
                .spawn_local(crate::metrics::serve_scrape(host.clone(), *port, cache));
        }
        self.sh.max_conns.set(ctx.max_conns);
        let quic = ctx.quic.clone();
        *self.sh.app.borrow_mut() = Some(Rc::new(ctx));
        if let Some(q) = quic {
            self.rt.spawn_local(crate::h3c::serve(self.sh.clone(), q));
        }
        self.sh.stopping.set(false);
        self.sh.draining.set(false);
        self.make_current();
    }

    /// Stops accepting and closes idle keep-alive connections. Requests in
    /// flight finish, and their connections close after the response.
    pub fn shutdown(&self) {
        self.sh.stopping.set(true);
        self.sh.stop.notify_waiters();
        #[cfg(windows)]
        self.sh.acceptor.borrow_mut().take();
        let conns: Vec<Rc<Conn>> = self
            .sh
            .conns
            .borrow()
            .iter()
            .map(|(_, c)| c.clone())
            .collect();
        for c in conns {
            c.notify(&self.sh);
        }
    }

    /// Starts `--drain-delay`: keep serving, fail the health check, and close
    /// connections after their responses so clients go elsewhere.
    pub fn drain(&self) {
        self.sh.draining.set(true);
    }

    pub fn connections(&self) -> usize {
        self.sh.conns.borrow().len()
    }

    /// Releases every Python reference the worker holds. The GIL must be held.
    pub fn close(self) {
        if CURRENT.get() == Rc::as_ptr(&self.sh) {
            CURRENT.set(ptr::null());
        }
        crate::ops::flush_log();
        let Core { rt, sh, .. } = self;
        #[cfg(windows)]
        sh.acceptor.borrow_mut().take();
        drop(rt);
        sh.regs.borrow_mut().clear();
        sh.conns.borrow_mut().clear();
        sh.deferred.borrow_mut().clear();
        sh.main_waker.borrow_mut().take();
        sh.hcache.borrow_mut().clear();
        sh.app.borrow_mut().take();
    }
}

/// Host, port, and whether the listener is a unix socket. A unix path is
/// reported as the host with port 0, which is what the ASGI `server` tuple
/// and WSGI `SERVER_NAME` / `SERVER_PORT` get.
pub fn bind_name(fd: i64) -> std::io::Result<(String, u16, bool)> {
    let s = borrow_socket(fd);
    let addr = s.local_addr()?;
    if let Some(ip) = addr.as_socket() {
        return Ok((ip.ip().to_canonical().to_string(), ip.port(), false));
    }
    let path = unix_path(&addr).unwrap_or_default();
    Ok((path, 0, true))
}

fn is_unix(fd: i64) -> bool {
    borrow_socket(fd)
        .local_addr()
        .ok()
        .is_some_and(|a| a.as_socket().is_none())
}

fn unix_path(addr: &socket2::SockAddr) -> Option<String> {
    #[cfg(unix)]
    {
        use std::os::unix::net::SocketAddr as UnixAddr;
        let u: UnixAddr = addr.as_unix()?.into();
        return Some(u.as_pathname()?.to_string_lossy().into_owned());
    }
    #[cfg(windows)]
    {
        let _ = addr;
        None
    }
}

fn borrow_socket(fd: i64) -> std::mem::ManuallyDrop<socket2::Socket> {
    #[cfg(unix)]
    let s = unsafe { <socket2::Socket as std::os::fd::FromRawFd>::from_raw_fd(fd as i32) };
    #[cfg(windows)]
    let s = unsafe {
        <socket2::Socket as std::os::windows::io::FromRawSocket>::from_raw_socket(fd as u64)
    };
    std::mem::ManuallyDrop::new(s)
}

fn dup_listener(fd: i64) -> std::io::Result<std::net::TcpListener> {
    let s = borrow_socket(fd).try_clone()?;
    s.set_nonblocking(true)?;
    Ok(s.into())
}

async fn accept_loop(sh: Rc<Shared>, listener: TcpListener, unix: bool) {
    loop {
        let mut stop = pin!(sh.stop.notified());
        stop.as_mut().enable();
        if sh.stopping.get() {
            return;
        }
        tokio::select! {
            biased;
            _ = &mut stop => return,
            r = listener.accept() => match r {
                Ok((io, peer)) => {
                    let peer = if unix { SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, 0)) } else { peer };
                    sh.admit(io, peer, unix);
                }
                // Out of descriptors, most likely: back off rather than spin.
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            },
        }
    }
}

/// Takes connections from the accept thread (see `winaccept`).
#[cfg(windows)]
async fn accept_handoff(
    sh: Rc<Shared>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<crate::winaccept::Accepted>,
) {
    loop {
        let mut stop = pin!(sh.stop.notified());
        stop.as_mut().enable();
        if sh.stopping.get() {
            return;
        }
        tokio::select! {
            biased;
            _ = &mut stop => return,
            r = rx.recv() => match r {
                Some((std, peer)) => {
                    if let Ok(io) = tokio::net::TcpStream::from_std(std) {
                        sh.admit(io, peer, false);
                    }
                }
                None => return,
            },
        }
    }
}
