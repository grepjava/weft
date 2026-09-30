//! HTTP/1.1 connections.
//!
//! A connection is one Tokio task and one `Conn`, shared (on this thread
//! only) between that task and the Python-side `send`/`receive` callables.
//! The task reads and parses; `send` writes response bytes straight onto the
//! socket from inside the application's `await`, and the task only takes over
//! writing when the socket pushes back.

use std::cell::RefCell;
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use tokio::time::{Instant, Sleep};

use crate::asgi;
use crate::core::{AppCtx, Shared};
use crate::py::PyRef;
use crate::tls::Io;

/// Request body bytes buffered ahead of the application before reading stops.
pub const BODY_HWM: usize = 256 * 1024;
/// Response bytes queued before `send` makes the application wait.
pub const WRITE_HWM: usize = 512 * 1024;
/// Below this, bytes `send` queues wait for the connection task, which writes
/// once per event-loop iteration: every small message sent in one iteration
/// shares a single system call. At or above it, `send` writes at once.
pub const CORK_LIMIT: usize = 64 * 1024;
/// ...and the level they must drain to before it resumes.
pub const WRITE_LWM: usize = 64 * 1024;
/// Bytes read past the current request (pipelining, or watching for the
/// client going away) before reading stops.
const PEEK_LIMIT: usize = 64 * 1024;
/// Unread request body the server will discard to keep a connection alive.
const DRAIN_LIMIT: usize = 256 * 1024;
const LINGER_LIMIT: usize = 1024 * 1024;

pub struct Conn {
    pub slot: u32,
    pub generation: u32,
    pub io: RefCell<Option<Io>>,
    pub peer: SocketAddr,
    /// Accepted on a unix socket; `peer` is then a placeholder.
    pub unix: bool,
    /// An HTTP/2 stream: not a TCP connection for `--max-connections`.
    pub stream: bool,
    pub client: RefCell<Option<PyRef>>,
    /// REMOTE_ADDR and REMOTE_PORT, for WSGI.
    pub remote: RefCell<Option<(PyRef, PyRef)>>,
    pub st: RefCell<State>,
    pub h2: RefCell<Option<crate::h2c::H2Tx>>,
    pub h3: RefCell<Option<crate::h3c::H3Tx>>,
    pub(crate) waker: RefCell<Option<Waker>>,
}

impl Conn {
    pub fn io(&self) -> std::cell::Ref<'_, Io> {
        std::cell::Ref::map(self.io.borrow(), |o| o.as_ref().expect("HTTP/1 connection"))
    }

    pub fn take_io(&self) -> Option<Io> {
        self.io.borrow_mut().take()
    }
}

impl Conn {
    /// Asks the connection task to look at its state again. Called from the
    /// Python side, so the wake is deferred into the next `select`.
    pub fn notify(&self, sh: &Shared) {
        if let Some(w) = self.waker.borrow_mut().take() {
            sh.defer_wake(w);
        }
    }

    /// Registers the connection task to be woken by `notify`.
    pub fn set_waker(&self, cx: &Context<'_>) {
        let mut w = self.waker.borrow_mut();
        if !w.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
            *w = Some(cx.waker().clone());
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub enum Phase {
    /// Nothing sent yet.
    #[default]
    Idle,
    /// `http.response.start` received; the head is held until the first body
    /// message so that framing can be decided knowing whether more follows.
    Head,
    Body,
    Done,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    NoBody,
    Length(u64),
    Chunked,
    Eof,
}

#[derive(Default)]
pub struct HeadInfo {
    pub length: Option<u64>,
    pub chunked: bool,
    pub close: bool,
    pub has_date: bool,
    pub has_server: bool,
    pub has_request_id: bool,
}

pub struct Resp {
    pub phase: Phase,
    pub status: u16,
    /// Status line and the application's headers, without the blank line.
    pub head: Vec<u8>,
    pub info: HeadInfo,
    pub framing: Framing,
    /// The connection closes after this response.
    pub close: bool,
}

impl Default for Resp {
    fn default() -> Self {
        Resp {
            phase: Phase::Idle,
            status: 0,
            head: Vec::new(),
            info: HeadInfo::default(),
            framing: Framing::NoBody,
            close: false,
        }
    }
}

pub enum BodyKind {
    Empty,
    Length(u64),
    Chunked(ChunkDecoder),
}

pub struct Body {
    pub kind: BodyKind,
    pub buf: BytesMut,
    pub done: bool,
}

impl Body {
    /// Moves request body bytes out of the read buffer.
    pub fn feed(&mut self, rbuf: &mut BytesMut) -> Result<(), BodyError> {
        match &mut self.kind {
            BodyKind::Empty => self.done = true,
            BodyKind::Length(rem) => {
                let n = (*rem).min(rbuf.len() as u64) as usize;
                if n > 0 {
                    if self.buf.is_empty() {
                        self.buf = rbuf.split_to(n);
                    } else {
                        self.buf.extend_from_slice(&rbuf[..n]);
                        rbuf.advance(n);
                    }
                    *rem -= n as u64;
                }
                if *rem == 0 {
                    self.done = true;
                }
            }
            BodyKind::Chunked(d) => {
                d.decode(rbuf, &mut self.buf)?;
                if d.done() {
                    self.done = true;
                }
            }
        }
        Ok(())
    }
}

pub struct State {
    /// Requests dispatched on this connection; `send`/`receive` carry the
    /// value current when they were made.
    pub seq: u32,
    pub http11: bool,
    pub head_req: bool,
    pub keep_alive: bool,
    pub expect_continue: bool,
    pub body: Body,
    /// `more_body: False` has been handed to the application.
    pub final_delivered: bool,
    pub recv_waiter: Option<PyRef>,
    pub resp: Resp,
    pub out: BytesMut,
    pub drain_waiters: Vec<PyRef>,
    pub disconnected: bool,
    /// The server answered this request itself, part way through its body;
    /// what the application still sends goes nowhere.
    pub rejected: bool,
    /// Set by a WebSocket handshake, for the rest of the connection.
    pub ws: Option<Box<crate::ws::Ws>>,
    /// A WebTransport session on this HTTP/3 CONNECT stream.
    pub wt: Option<std::rc::Rc<crate::wt::Session>>,
    /// `--request-id`: sent back with the response.
    pub request_id: Vec<u8>,
    /// The access line, written once the response head is settled.
    pub log: Option<Box<crate::ops::LogReq>>,
    pub send_file: Option<Box<crate::staticf::SendFile>>,
    pub encoder: Option<Box<crate::compress::Encoder>>,
    pub cache_cap: Option<Box<crate::cache::Capture>>,
    pub cache_hit: Option<(u32, u32)>,
    pub accepted: crate::compress::Coding,
    pub cache_key: Option<Vec<u8>>,
    pub cache_target: u64,
    pub cache_variant: u64,
    pub cache_ok: bool,
    pub mutating: bool,
    pub h2: bool,
    pub h3: bool,
    pub started: tokio::time::Instant,
}

impl State {
    pub(crate) fn new() -> State {
        State {
            seq: 0,
            http11: true,
            head_req: false,
            keep_alive: true,
            expect_continue: false,
            body: Body {
                kind: BodyKind::Empty,
                buf: BytesMut::new(),
                done: true,
            },
            final_delivered: false,
            recv_waiter: None,
            resp: Resp::default(),
            out: BytesMut::with_capacity(4096),
            drain_waiters: Vec::new(),
            disconnected: false,
            rejected: false,
            ws: None,
            wt: None,
            request_id: Vec::new(),
            log: None,
            send_file: None,
            encoder: None,
            cache_cap: None,
            cache_hit: None,
            accepted: crate::compress::Coding::Identity,
            cache_key: None,
            cache_target: 0,
            cache_variant: 0,
            cache_ok: false,
            mutating: false,
            h2: false,
            h3: false,
            started: tokio::time::Instant::now(),
        }
    }

    pub fn begin(
        &mut self,
        http11: bool,
        head_req: bool,
        keep_alive: bool,
        expect: bool,
        kind: BodyKind,
    ) {
        self.seq = self.seq.wrapping_add(1);
        self.http11 = http11;
        self.head_req = head_req;
        self.keep_alive = keep_alive;
        self.body.done = matches!(kind, BodyKind::Empty);
        self.expect_continue = expect && !self.body.done;
        self.body.kind = kind;
        self.body.buf.clear();
        self.final_delivered = false;
        self.rejected = false;
        self.resp = Resp::default();
        self.request_id.clear();
        self.log = None;
        self.send_file = None;
        self.encoder = None;
        self.cache_cap = None;
        self.cache_hit = None;
        self.accepted = crate::compress::Coding::Identity;
        self.cache_key = None;
        self.cache_target = 0;
        self.cache_variant = 0;
        self.cache_ok = false;
        self.mutating = false;
        self.started = tokio::time::Instant::now();
    }

    pub(crate) fn log_response(&mut self, status: u16) {
        if crate::metrics::enabled() {
            crate::metrics::request_finished(status, self.started.elapsed().as_micros() as u64);
        }
        if let Some(l) = self.log.take() {
            crate::ops::record(&l, status, &self.request_id);
        }
    }

    /// The request cannot go on (its body broke a rule, or stalled). With
    /// nothing written yet the server answers `status` itself; otherwise
    /// half a response is on the wire and closing is all that is left.
    pub fn reject(&mut self, status: u16) {
        if matches!(self.resp.phase, Phase::Idle | Phase::Head) {
            // After any interim responses still queued.
            self.write_error(status);
            self.rejected = true;
        } else {
            self.disconnected = true;
            self.out.clear();
        }
    }

    pub fn start(&mut self, status: u16, head: Vec<u8>, info: HeadInfo) {
        self.resp.phase = Phase::Head;
        self.resp.status = status;
        self.resp.head = head;
        self.resp.info = info;
    }

    /// Frames and queues one body message. The head goes out with the first.
    pub fn write_body(
        &mut self,
        data: &[u8],
        more: bool,
        ctx: &AppCtx,
        date: &[u8; 29],
        stopping: bool,
    ) {
        if self.resp.phase == Phase::Head {
            self.write_head(data.len(), more, ctx, date, stopping);
        }
        if let Some(cap) = self.cache_cap.as_mut()
            && !cap.push(data)
        {
            self.cache_cap = None;
        }
        let encoded;
        let payload: &[u8] = if let Some(enc) = self.encoder.as_mut() {
            encoded = enc.push(data, !more);
            &encoded
        } else {
            data
        };
        match &mut self.resp.framing {
            Framing::NoBody => {}
            Framing::Eof => self.out.extend_from_slice(payload),
            Framing::Length(rem) => {
                // More than was promised would be read as the start of the
                // next response on a keep-alive connection: drop it.
                let n = (*rem).min(payload.len() as u64) as usize;
                self.out.extend_from_slice(&payload[..n]);
                *rem -= n as u64;
            }
            Framing::Chunked => {
                if !payload.is_empty() {
                    let mut hex = [0u8; 18];
                    let n = fmt_hex(payload.len(), &mut hex);
                    self.out.reserve(n + payload.len() + 4);
                    self.out.extend_from_slice(&hex[..n]);
                    self.out.extend_from_slice(b"\r\n");
                    self.out.extend_from_slice(payload);
                    self.out.extend_from_slice(b"\r\n");
                }
            }
        }
        if !more {
            match self.resp.framing {
                Framing::Chunked => self.out.extend_from_slice(b"0\r\n\r\n"),
                // Less than was promised: the client would wait for bytes
                // that are not coming, so the connection closes instead.
                Framing::Length(rem) if rem > 0 => self.resp.close = true,
                _ => {}
            }
            self.resp.phase = Phase::Done;
            if let Some(cap) = self.cache_cap.take() {
                cap.finish();
            }
            if self.mutating && (200..400).contains(&self.resp.status) {
                crate::cache::invalidate(self.cache_target);
            }
        }
    }

    fn write_head(
        &mut self,
        first_len: usize,
        more: bool,
        ctx: &AppCtx,
        date: &[u8; 29],
        stopping: bool,
    ) {
        let status = self.resp.status;
        if self.cache_ok
            && !self.head_req
            && let Some(cfg) = ctx.cache
            && let Some(key) = self.cache_key.take()
        {
            self.cache_cap = Some(Box::new(crate::cache::Capture {
                key,
                target: self.cache_target,
                variant: self.cache_variant,
                status,
                head: self.resp.head.clone(),
                body: Vec::new(),
                max_object: cfg.max_object,
                ttl_max: cfg.ttl_max,
            }));
        }
        let elig = crate::compress::Eligibility::from_head(&self.resp.head);
        let no_body = self.head_req || status < 200 || status == 204 || status == 304;
        let offered = if ctx.compress {
            self.accepted
        } else {
            crate::compress::Coding::Identity
        };
        let coding = elig.choose(
            offered,
            status,
            !no_body,
            self.resp.info.length,
            ctx.compress_min,
        );
        if coding != crate::compress::Coding::Identity {
            self.resp.head = crate::compress::rewrite_head(&self.resp.head, coding);
            self.resp.info.length = None;
            self.resp.info.chunked = true;
            self.encoder = crate::compress::Encoder::new(coding).map(Box::new);
        }
        let info = &self.resp.info;
        let mut close = !self.keep_alive || info.close || stopping;
        let mut extra: Vec<u8> = Vec::with_capacity(160);
        if coding != crate::compress::Coding::Identity {
            extra.extend_from_slice(b"content-encoding: ");
            extra.extend_from_slice(coding.token());
            extra.extend_from_slice(b"\r\n");
        }
        if elig.may_vary(status) && !elig.vary_covered {
            extra.extend_from_slice(b"vary: accept-encoding\r\n");
        }
        if let Some((age, ttl)) = self.cache_hit {
            extra.extend_from_slice(b"age: ");
            push_int(&mut extra, age as u64);
            extra.extend_from_slice(b"\r\ncache-status: weft; hit; ttl=");
            push_int(&mut extra, ttl as u64);
            extra.extend_from_slice(b"\r\n");
        }
        let framing = if no_body {
            Framing::NoBody
        } else if self.h2 || self.h3 {
            if let Some(n) = info.length {
                if !header_lines(&self.resp.head)
                    .iter()
                    .any(|(k, _)| crate::asgi::eq_ci(k, b"content-length"))
                {
                    extra.extend_from_slice(b"content-length: ");
                    push_int(&mut extra, n);
                    extra.extend_from_slice(b"\r\n");
                }
                Framing::Length(n)
            } else if !more {
                extra.extend_from_slice(b"content-length: ");
                push_int(&mut extra, first_len as u64);
                extra.extend_from_slice(b"\r\n");
                Framing::Length(first_len as u64)
            } else {
                Framing::Eof
            }
        } else if info.chunked && self.http11 {
            extra.extend_from_slice(b"transfer-encoding: chunked\r\n");
            Framing::Chunked
        } else if let Some(n) = info.length {
            if !header_lines(&self.resp.head)
                .iter()
                .any(|(k, _)| crate::asgi::eq_ci(k, b"content-length"))
            {
                extra.extend_from_slice(b"content-length: ");
                push_int(&mut extra, n);
                extra.extend_from_slice(b"\r\n");
            }
            Framing::Length(n)
        } else if !more {
            extra.extend_from_slice(b"content-length: ");
            push_int(&mut extra, first_len as u64);
            extra.extend_from_slice(b"\r\n");
            Framing::Length(first_len as u64)
        } else if self.http11 {
            extra.extend_from_slice(b"transfer-encoding: chunked\r\n");
            Framing::Chunked
        } else {
            close = true;
            Framing::Eof
        };
        if ctx.date_header && !info.has_date {
            extra.extend_from_slice(b"date: ");
            extra.extend_from_slice(date);
            extra.extend_from_slice(b"\r\n");
        }
        if ctx.server_header && !info.has_server {
            extra.extend_from_slice(b"server: weft\r\n");
        }
        if !self.request_id.is_empty() && !info.has_request_id {
            extra.extend_from_slice(b"x-request-id: ");
            extra.extend_from_slice(&self.request_id);
            extra.extend_from_slice(b"\r\n");
        }
        if let Some(hsts) = &ctx.hsts
            && !header_lines(&self.resp.head)
                .iter()
                .any(|(k, _)| crate::asgi::eq_ci(k, b"strict-transport-security"))
        {
            extra.extend_from_slice(b"strict-transport-security: ");
            extra.extend_from_slice(hsts);
            extra.extend_from_slice(b"\r\n");
        }
        if !(self.h2 || self.h3) {
            if close {
                if !info.close {
                    extra.extend_from_slice(b"connection: close\r\n");
                }
            } else if !self.http11 {
                extra.extend_from_slice(b"connection: keep-alive\r\n");
            }
        } else {
            close = false;
        }
        if let Some(port) = ctx.alt_svc
            && !header_lines(&self.resp.head)
                .iter()
                .any(|(k, _)| crate::asgi::eq_ci(k, b"alt-svc"))
            && !extra
                .windows(8)
                .any(|w| w.eq_ignore_ascii_case(b"alt-svc:"))
        {
            extra.extend_from_slice(b"alt-svc: h3=\":");
            push_int(&mut extra, port as u64);
            extra.extend_from_slice(b"\"; ma=86400\r\n");
        }
        self.out.reserve(self.resp.head.len() + extra.len() + 2);
        self.out.extend_from_slice(&self.resp.head);
        self.out.extend_from_slice(&extra);
        self.out.extend_from_slice(b"\r\n");
        self.resp.head = Vec::new();
        self.resp.framing = framing;
        self.resp.close = close;
        self.resp.phase = Phase::Body;
        self.log_response(status);
    }

    /// A complete response the server writes itself, then closes.
    pub fn write_error(&mut self, status: u16) {
        let reason = reason(status);
        self.out.extend_from_slice(b"HTTP/1.1 ");
        push_int(&mut self.out, status as u64);
        self.out.extend_from_slice(b" ");
        self.out.extend_from_slice(reason);
        self.out.extend_from_slice(
            b"\r\ncontent-type: text/plain; charset=utf-8\r\nconnection: close\r\ncontent-length: ",
        );
        push_int(&mut self.out, reason.len() as u64);
        self.out.extend_from_slice(b"\r\n\r\n");
        self.out.extend_from_slice(reason);
        self.resp.phase = Phase::Done;
        self.resp.close = true;
        self.log_response(status);
    }

    /// 429 for `--rate-limit`. The connection is kept when `keep` is set: the
    /// client is being asked to slow down, not hung up on.
    pub fn write_limited(&mut self, retry_after: u64, keep: bool, head_only: bool) {
        self.out.extend_from_slice(b"HTTP/1.1 429 Too Many Requests\r\ncontent-type: text/plain; charset=utf-8\r\nretry-after: ");
        push_int(&mut self.out, retry_after);
        self.out
            .extend_from_slice(b"\r\ncontent-length: 18\r\nconnection: ");
        self.out.extend_from_slice(if keep {
            &b"keep-alive\r\n\r\n"[..]
        } else {
            &b"close\r\n\r\n"[..]
        });
        if !head_only {
            self.out.extend_from_slice(b"Too Many Requests\n");
        }
        self.resp.phase = Phase::Done;
        self.resp.close = !keep;
        crate::metrics::rate_limited();
        self.log_response(429);
    }
}

/// Writes the connection's output, parking on the socket as needed. `false`
/// once the client is gone or stops reading for the request timeout.
#[allow(clippy::await_holding_refcell_ref)]
pub async fn flush_all(ctx: &AppCtx, conn: &Conn) -> bool {
    let limit = ctx.request_timeout.unwrap_or(Duration::from_secs(86_400));
    loop {
        {
            let mut st = conn.st.borrow_mut();
            if st.disconnected {
                return false;
            }
            let more_file = crate::staticf::pump(&mut st);
            if conn.h3.borrow().is_some() {
                drop(st);
                return match crate::h3c::flush_h3(conn).await {
                    Ok(_) => true,
                    Err(_) => {
                        conn.st.borrow_mut().disconnected = true;
                        false
                    }
                };
            }
            if conn.h2.borrow().is_some() {
                drop(st);
                loop {
                    match flush_conn(conn) {
                        Ok(true) => return true,
                        Ok(false) => {
                            let wait = poll_fn(|cx| match conn.h2.borrow_mut().as_mut() {
                                Some(tx) => tx.poll_capacity(cx),
                                None => Poll::Ready(Ok(())),
                            });
                            match tokio::time::timeout(limit, wait).await {
                                Ok(Ok(())) => {}
                                _ => {
                                    conn.st.borrow_mut().disconnected = true;
                                    return false;
                                }
                            }
                        }
                        Err(_) => {
                            conn.st.borrow_mut().disconnected = true;
                            return false;
                        }
                    }
                }
            }
            match flush(&conn.io(), &mut st.out) {
                Ok(true)
                    if !more_file && st.send_file.is_none() && !conn.io().tls_wants_write() =>
                {
                    return true;
                }
                Ok(true) | Ok(false) => {}
                Err(_) => {
                    st.disconnected = true;
                    st.out.clear();
                    st.send_file = None;
                    return false;
                }
            }
        }
        if !matches!(
            tokio::time::timeout(limit, conn.io().writable()).await,
            Ok(Ok(()))
        ) {
            let mut st = conn.st.borrow_mut();
            st.disconnected = true;
            st.out.clear();
            return false;
        }
    }
}

/// Status line skipped; each remaining `name: value` pair.
pub fn header_lines(head: &[u8]) -> Vec<(&[u8], &[u8])> {
    let mut lines = Vec::new();
    let mut s = head;
    if let Some(i) = s.windows(2).position(|w| w == b"\r\n") {
        let first = &s[..i];
        s = &s[i + 2..];
        // A stored copy starts with `200\n` and has no HTTP status line.
        if !first.starts_with(b"HTTP/")
            && first.contains(&b':')
            && let Some(col) = first.iter().position(|&c| c == b':')
        {
            lines.push((
                first[..col].trim_ascii_end(),
                crate::asgi::trim(&first[col + 1..]),
            ));
        }
    }
    while let Some(i) = s.windows(2).position(|w| w == b"\r\n") {
        let line = &s[..i];
        s = &s[i + 2..];
        if line.is_empty() {
            break;
        }
        if let Some(col) = line.iter().position(|&c| c == b':') {
            lines.push((
                line[..col].trim_ascii_end(),
                crate::asgi::trim(&line[col + 1..]),
            ));
        }
    }
    if !s.is_empty()
        && let Some(col) = s.iter().position(|&c| c == b':')
    {
        lines.push((s[..col].trim_ascii_end(), crate::asgi::trim(&s[col + 1..])));
    }
    lines
}

/// Writes as much of `out` as the socket takes. `Ok(true)` when all of it.
pub fn flush(io: &Io, out: &mut BytesMut) -> io::Result<bool> {
    if !io.flush_tls_only()? && out.is_empty() {
        return Ok(false);
    }
    while !out.is_empty() {
        match io.try_write(out) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => out.advance(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(e) => return Err(e),
        }
    }
    io.flush_tls_only()
}

pub fn flush_conn(conn: &Conn) -> io::Result<bool> {
    flush_st(conn, &mut conn.st.borrow_mut())
}

pub fn flush_st(conn: &Conn, st: &mut State) -> io::Result<bool> {
    if conn.h3.borrow().is_some() {
        return Ok(st.out.is_empty());
    }
    if let Some(tx) = conn.h2.borrow_mut().as_mut() {
        return tx.flush(st);
    }
    flush(&conn.io(), &mut st.out)
}

#[inline]
pub fn reserve(rbuf: &mut BytesMut) {
    if rbuf.capacity() - rbuf.len() < 4096 {
        rbuf.reserve(16 * 1024);
    }
}

pub fn spawn(sh: &Rc<Shared>, io: Io, peer: SocketAddr, unix: bool) {
    let generation = sh.next_generation();
    let conn = {
        let mut conns = sh.conns.borrow_mut();
        let entry = conns.vacant_entry();
        let conn = Rc::new(Conn {
            slot: entry.key() as u32,
            generation,
            io: RefCell::new(Some(io)),
            unix,
            peer,
            stream: false,
            client: RefCell::new(None),
            remote: RefCell::new(None),
            st: RefCell::new(State::new()),
            h2: RefCell::new(None),
            h3: RefCell::new(None),
            waker: RefCell::new(None),
        });
        entry.insert(conn.clone());
        conn
    };
    crate::metrics::accepted();
    tokio::task::spawn_local(run(sh.clone(), conn));
}

#[allow(clippy::await_holding_refcell_ref)]
async fn run(sh: Rc<Shared>, conn: Rc<Conn>) {
    let mut rbuf = BytesMut::with_capacity(16 * 1024);
    if let Some(ctx) = sh.app() {
        if conn.io.borrow().as_ref().is_some_and(|io| io.is_tls()) {
            let limit = ctx.header_timeout;
            if !matches!(
                tokio::time::timeout(limit, conn.io().handshake()).await,
                Ok(Ok(()))
            ) {
                teardown(&sh, &conn, &mut rbuf).await;
                return;
            }
        }
        let alpn = conn.io.borrow().as_ref().and_then(|io| io.alpn());
        if ctx.http2 && alpn.as_deref() == Some(&b"h2"[..]) {
            let io = conn.take_io().unwrap();
            crate::h2c::serve(sh.clone(), ctx, io, BytesMut::new(), conn.peer, conn.unix).await;
            teardown(&sh, &conn, &mut rbuf).await;
            return;
        }
        if ctx.http2 && alpn.is_none() {
            match read_some(&sh, &conn, &mut rbuf, ctx.header_timeout, false).await {
                Some(n) if n > 0 => {}
                _ => {
                    teardown(&sh, &conn, &mut rbuf).await;
                    return;
                }
            }
            if rbuf.starts_with(crate::h2c::PREFACE) {
                let io = conn.take_io().unwrap();
                crate::h2c::serve(
                    sh.clone(),
                    ctx,
                    io,
                    std::mem::take(&mut rbuf),
                    conn.peer,
                    conn.unix,
                )
                .await;
                teardown(&sh, &conn, &mut rbuf).await;
                return;
            }
            if ctx.http2_only {
                teardown(&sh, &conn, &mut rbuf).await;
                return;
            }
        } else if ctx.http2_only && alpn.as_deref() != Some(&b"h2"[..]) {
            teardown(&sh, &conn, &mut rbuf).await;
            return;
        }
        serve(&sh, &ctx, &conn, &mut rbuf).await;
    }
    teardown(&sh, &conn, &mut rbuf).await;
}

pub enum Next {
    KeepAlive,
    /// The response is complete but the application left request body
    /// unread; discarding it keeps the connection usable.
    Drain,
    Close,
}

async fn serve(sh: &Shared, ctx: &Rc<AppCtx>, conn: &Rc<Conn>, rbuf: &mut BytesMut) {
    let mut timer = pin!(tokio::time::sleep(Duration::from_secs(86_400)));
    loop {
        loop {
            if !rbuf.is_empty() {
                match asgi::dispatch(sh, ctx, conn, rbuf) {
                    asgi::Parsed::Dispatched => break,
                    asgi::Parsed::Wsgi(environ) => {
                        match crate::wsgi::run(sh, ctx, conn, rbuf, environ).await {
                            Next::KeepAlive => continue,
                            _ => return,
                        }
                    }
                    asgi::Parsed::Answered(keep) => {
                        if keep && flush_all(ctx, conn).await {
                            continue;
                        }
                        return;
                    }
                    asgi::Parsed::Upgraded => {
                        crate::ws::run(sh, ctx, conn, rbuf).await;
                        return;
                    }
                    asgi::Parsed::Partial if rbuf.len() > ctx.max_head => {
                        conn.st.borrow_mut().write_error(431);
                        return;
                    }
                    asgi::Parsed::Partial => {}
                    asgi::Parsed::Reject(status) => {
                        conn.st.borrow_mut().write_error(status);
                        return;
                    }
                }
            }
            let idle = rbuf.is_empty();
            if idle && sh.stopping.get() {
                return;
            }
            let limit = if idle {
                ctx.keep_alive
            } else {
                ctx.header_timeout
            };
            match read_some(sh, conn, rbuf, limit, idle).await {
                Some(n) if n > 0 => {}
                _ => return,
            }
        }
        let mut p = Pump {
            eof: false,
            stall: Stall {
                since: None,
                armed: None,
            },
            stream_body: None,
        };
        match poll_fn(|cx| pump(sh, ctx, conn, rbuf, &mut p, timer.as_mut(), cx)).await {
            Next::KeepAlive => {}
            Next::Drain if drain(sh, ctx, conn, rbuf).await => {}
            Next::Drain | Next::Close => return,
        }
    }
}

/// Discards the rest of an unread request body, up to `DRAIN_LIMIT` bytes
/// and the header timeout. `true` when the connection can serve another
/// request.
async fn drain(sh: &Shared, ctx: &AppCtx, conn: &Conn, rbuf: &mut BytesMut) -> bool {
    let deadline = tokio::time::Instant::now() + ctx.header_timeout;
    let mut discarded = 0usize;
    loop {
        {
            let mut st = conn.st.borrow_mut();
            if st.body.feed(rbuf).is_err() {
                return false;
            }
            discarded += st.body.buf.len();
            st.body.buf.clear();
            if st.body.done {
                return true;
            }
        }
        if discarded > DRAIN_LIMIT || sh.stopping.get() {
            return false;
        }
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match read_some(sh, conn, rbuf, left, true).await {
            Some(n) if n > 0 => {}
            _ => return false,
        }
    }
}

/// Reads whatever is available, within `limit`. An idle connection also
/// gives up as soon as the worker starts shutting down.
#[allow(clippy::await_holding_refcell_ref)]
pub async fn read_some(
    sh: &Shared,
    conn: &Conn,
    rbuf: &mut BytesMut,
    limit: Duration,
    idle: bool,
) -> Option<usize> {
    let read = async {
        loop {
            conn.io().readable().await.ok()?;
            reserve(rbuf);
            match conn.io().try_read_buf(rbuf) {
                Ok(n) => return Some(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(_) => return None,
            }
        }
    };
    if idle {
        let mut stop = pin!(sh.stop.notified());
        stop.as_mut().enable();
        if sh.stopping.get() {
            return None;
        }
        tokio::select! {
            biased;
            _ = &mut stop => None,
            r = tokio::time::timeout(limit, read) => r.ok().flatten(),
        }
    } else {
        tokio::time::timeout(limit, read).await.ok().flatten()
    }
}

/// Drives one request after dispatch: feeds the body to the application as
/// it asks, flushes what `send` could not write, watches for the client
/// going away, and decides what happens to the connection once the
/// response is complete.
pub(crate) struct Pump {
    pub eof: bool,
    pub stall: Stall,
    /// An HTTP/2 stream's body, which arrives in DATA frames, not from `io`.
    pub stream_body: Option<StreamBody<h2::RecvStream>>,
}

/// Where an HTTP/2 or HTTP/3 request body comes from.
pub(crate) trait BodySource {
    /// The next piece of the body, `None` once it has ended, `Err` when the
    /// client reset the stream.
    fn poll_piece(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>, ()>>;
}

/// An HTTP/2 or HTTP/3 request body, framed into the read buffer the way
/// `synthesize` announced it, so it is read like an HTTP/1.1 body: after
/// dispatch, as the application asks for it.
pub(crate) struct StreamBody<S> {
    /// `None` once the stream has ended.
    src: Option<S>,
    chunked: bool,
}

impl<S: BodySource> StreamBody<S> {
    pub fn new(src: S, chunked: bool) -> Self {
        StreamBody {
            src: Some(src),
            chunked,
        }
    }

    pub fn ended(&self) -> bool {
        self.src.is_none()
    }

    pub fn push(&self, rbuf: &mut BytesMut, data: &[u8]) {
        if !self.chunked {
            rbuf.extend_from_slice(data);
        } else if !data.is_empty() {
            use std::fmt::Write as _;
            let _ = write!(rbuf, "{:x}\r\n", data.len());
            rbuf.extend_from_slice(data);
            rbuf.extend_from_slice(b"\r\n");
        }
    }

    pub fn end(&mut self, rbuf: &mut BytesMut) {
        if self.chunked && self.src.is_some() {
            rbuf.extend_from_slice(b"0\r\n\r\n");
        }
        self.src = None;
    }

    /// Frames the next piece into `rbuf`. Ready once there was progress,
    /// including the end; `Err` when the client reset the stream.
    pub fn poll_into(&mut self, rbuf: &mut BytesMut, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        let Some(src) = self.src.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        match src.poll_piece(cx) {
            Poll::Ready(Ok(Some(data))) => {
                self.push(rbuf, &data);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(None)) => {
                self.end(rbuf);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(())) => {
                self.src = None;
                Poll::Ready(Err(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// WSGI hands the application the whole body, so an HTTP/2 or HTTP/3
/// stream's is read before it runs. `false` when the request cannot go on;
/// any answer the server owes is already queued.
pub(crate) async fn read_stream_body<S: BodySource>(
    ctx: &AppCtx,
    conn: &Conn,
    rbuf: &mut BytesMut,
    body: &mut StreamBody<S>,
) -> bool {
    let limit = ctx.request_timeout.unwrap_or(Duration::from_secs(86_400));
    loop {
        {
            let mut st = conn.st.borrow_mut();
            let st = &mut *st;
            if let Err(status) = st.body.feed(rbuf) {
                st.reject(status);
                return false;
            }
            if st.body.done {
                return true;
            }
            if body.ended() {
                // Shorter than its Content-Length.
                st.disconnected = true;
                return false;
            }
            if st.expect_continue {
                st.expect_continue = false;
                st.out.extend_from_slice(b"HTTP/1.1 100 Continue\r\n\r\n");
            }
        }
        if !flush_all(ctx, conn).await {
            return false;
        }
        match tokio::time::timeout(limit, poll_fn(|cx| body.poll_into(rbuf, cx))).await {
            Ok(Ok(())) => {}
            Ok(Err(())) => {
                conn.st.borrow_mut().disconnected = true;
                return false;
            }
            Err(_) => {
                conn.st.borrow_mut().reject(408);
                return false;
            }
        }
    }
}

/// The HTTP/1.1 head an HTTP/2 or HTTP/3 request is dispatched as. A body
/// without a Content-Length is framed as chunked unless the stream has
/// already ended; the flag says which.
pub(crate) fn synthesize(parts: &::http::request::Parts, ended: bool) -> (BytesMut, bool) {
    let method = parts.method.as_str();
    let path = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let path = if path.is_empty() { "/" } else { path };
    let authority = parts
        .uri
        .authority()
        .map(|a| a.as_str().to_string())
        .or_else(|| {
            parts
                .headers
                .get(::http::header::HOST)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "localhost".into());
    let mut out = BytesMut::new();
    out.extend_from_slice(method.as_bytes());
    out.extend_from_slice(b" ");
    out.extend_from_slice(path.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\n");
    out.extend_from_slice(b"host: ");
    out.extend_from_slice(authority.as_bytes());
    out.extend_from_slice(b"\r\n");
    let mut has_cl = false;
    for (name, value) in &parts.headers {
        let n = name.as_str();
        if n == "host" || n == "transfer-encoding" {
            continue;
        }
        if n == "content-length" {
            has_cl = true;
        }
        out.extend_from_slice(n.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(value.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    let chunked = !has_cl && !ended;
    if chunked {
        out.extend_from_slice(b"transfer-encoding: chunked\r\n");
    } else if !has_cl {
        out.extend_from_slice(b"content-length: 0\r\n");
    }
    out.extend_from_slice(b"\r\n");
    (out, chunked)
}

/// `--request-timeout`: how long a request may go without its bytes moving
/// while the server is waiting on the client, to read body or to write.
pub(crate) struct Stall {
    /// When the last progress was seen, while waiting; `None` when not.
    pub since: Option<Instant>,
    /// What the timer is set to.
    pub armed: Option<Instant>,
}

pub(crate) fn pump(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Conn,
    rbuf: &mut BytesMut,
    p: &mut Pump,
    mut timer: Pin<&mut Sleep>,
    cx: &mut Context<'_>,
) -> Poll<Next> {
    let eof = &mut p.eof;
    loop {
        let mut st = conn.st.borrow_mut();

        if !st.body.done
            && !st.rejected
            && !rbuf.is_empty()
            && let Err(status) = st.body.feed(rbuf)
        {
            // Nothing after a body that broke the rules can be trusted.
            st.reject(status);
        }

        let mut wrote = false;
        while (!st.out.is_empty()
            || conn
                .io
                .borrow()
                .as_ref()
                .is_some_and(|io| io.tls_wants_write()))
            && !st.disconnected
        {
            if conn.h2.borrow().is_some() {
                drop(st);
                match flush_conn(conn) {
                    Ok(true) => {
                        st = conn.st.borrow_mut();
                        break;
                    }
                    Ok(false) => match conn.h2.borrow_mut().as_mut() {
                        Some(tx) => match tx.poll_capacity(cx) {
                            Poll::Ready(Ok(())) => {
                                st = conn.st.borrow_mut();
                                continue;
                            }
                            Poll::Ready(Err(_)) => {
                                st = conn.st.borrow_mut();
                                st.disconnected = true;
                                st.out.clear();
                            }
                            Poll::Pending => {
                                st = conn.st.borrow_mut();
                                break;
                            }
                        },
                        None => {
                            st = conn.st.borrow_mut();
                            break;
                        }
                    },
                    Err(_) => {
                        st = conn.st.borrow_mut();
                        st.disconnected = true;
                        st.out.clear();
                    }
                }
                continue;
            }
            match conn.io().poll_write_ready(cx) {
                Poll::Ready(Ok(())) => {
                    let before = st.out.len();
                    match flush(&conn.io(), &mut st.out) {
                        Ok(_) => wrote |= st.out.len() < before,
                        Err(_) => {
                            st.disconnected = true;
                            st.out.clear();
                        }
                    }
                }
                Poll::Ready(Err(_)) => {
                    st.disconnected = true;
                    st.out.clear();
                }
                Poll::Pending => break,
            }
        }

        let want_read = conn.io.borrow().is_some()
            && !st.disconnected
            && !st.rejected
            && !*eof
            && if !st.body.done {
                st.body.buf.len() < BODY_HWM
            } else {
                st.resp.phase != Phase::Done && rbuf.len() < PEEK_LIMIT
            };
        let mut got = false;
        if want_read {
            loop {
                match conn.io().poll_read_ready(cx) {
                    Poll::Ready(Ok(())) => {
                        reserve(rbuf);
                        match conn.io().try_read_buf(rbuf) {
                            Ok(0) => *eof = true,
                            Ok(_) => got = true,
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                            Err(_) => *eof = true,
                        }
                    }
                    Poll::Ready(Err(_)) => *eof = true,
                    Poll::Pending => {}
                }
                break;
            }
        }
        // An HTTP/2 stream's body comes in DATA frames, under the same bound;
        // flow control holds the client back meanwhile.
        let want_frames = p.stream_body.as_ref().is_some_and(|b| !b.ended())
            && !st.disconnected
            && !st.rejected
            && !st.body.done
            && st.body.buf.len() < BODY_HWM;
        if want_frames && let Some(b) = p.stream_body.as_mut() {
            match b.poll_into(rbuf, cx) {
                Poll::Ready(Ok(())) => got = true,
                Poll::Ready(Err(())) => *eof = true,
                Poll::Pending => {}
            }
        } else if !st.body.done
            && !st.rejected
            && p.stream_body.as_ref().is_some_and(StreamBody::ended)
        {
            // The stream ended short of its Content-Length.
            *eof = true;
        }
        // Read until the socket would block, so that Tokio's readiness is
        // cleared and a waker registered; `want_read` bounds the buffering.
        if got || wrote {
            p.stall.since = None;
        }
        if got {
            drop(st);
            continue;
        }
        if *eof {
            st.disconnected = true;
        }

        // Hand the application what it is waiting for. Python is called with
        // no borrow held: resolving a future runs loop code.
        let mut wake_recv: Option<(PyRef, Option<(BytesMut, bool)>)> = None;
        if st.recv_waiter.is_some() {
            if st.rejected {
                wake_recv = st.recv_waiter.take().map(|w| (w, None));
            } else if !st.disconnected
                && !st.final_delivered
                && (!st.body.buf.is_empty() || st.body.done)
            {
                let data = st.body.buf.split();
                let more = !st.body.done;
                st.final_delivered = !more;
                wake_recv = st.recv_waiter.take().map(|w| (w, Some((data, more))));
            } else if st.disconnected || st.resp.phase == Phase::Done {
                wake_recv = st.recv_waiter.take().map(|w| (w, None));
            }
        }
        let drained =
            if !st.drain_waiters.is_empty() && (st.out.len() <= WRITE_LWM || st.disconnected) {
                std::mem::take(&mut st.drain_waiters)
            } else {
                Vec::new()
            };
        drop(st);
        if wake_recv.is_some() || !drained.is_empty() {
            unsafe {
                if let Some((fut, msg)) = wake_recv {
                    asgi::resolve_receive(fut, msg);
                }
                for fut in drained {
                    asgi::resolve_none(fut);
                }
            }
            sh.py_scheduled();
            continue;
        }

        let mut st = conn.st.borrow_mut();
        if st.disconnected || (st.rejected && st.out.is_empty()) {
            return Poll::Ready(Next::Close);
        }
        if st.resp.phase == Phase::Done && st.out.is_empty() {
            let reusable = st.keep_alive && !st.resp.close && !sh.closing();
            return Poll::Ready(match (reusable, st.body.done) {
                (true, true) => Next::KeepAlive,
                (true, false) => Next::Drain,
                _ => Next::Close,
            });
        }

        if let Some(limit) = ctx.request_timeout {
            let waiting = !st.out.is_empty() || ((want_read || want_frames) && !st.body.done);
            if waiting {
                let now = Instant::now();
                let at = *p.stall.since.get_or_insert(now) + limit;
                if now >= at {
                    if st.rejected {
                        st.disconnected = true;
                    } else {
                        st.reject(408);
                    }
                    p.stall.since = None;
                    drop(st);
                    continue;
                }
                // Progress only moves the deadline later, so a timer set
                // early is left to fire and be set again.
                if p.stall.armed.is_none() {
                    timer.as_mut().reset(at);
                    p.stall.armed = Some(at);
                }
                if timer.as_mut().poll(cx).is_ready() {
                    p.stall.armed = None;
                    drop(st);
                    continue;
                }
            } else {
                p.stall.since = None;
            }
        }
        drop(st);
        conn.set_waker(cx);
        return Poll::Pending;
    }
}

#[allow(clippy::await_holding_refcell_ref)]
async fn teardown(sh: &Shared, conn: &Rc<Conn>, rbuf: &mut BytesMut) {
    // Let a response the server wrote itself (an error, a 500) reach the
    // client, briefly.
    let (pending, linger) = {
        let st = conn.st.borrow();
        (!st.disconnected && !st.out.is_empty(), !st.disconnected)
    };
    if pending {
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if conn.io.borrow().is_none() {
                    return;
                }
                if conn.io().writable().await.is_err() {
                    return;
                }
                match flush_st(conn, &mut conn.st.borrow_mut()) {
                    Ok(true) | Err(_) => return,
                    Ok(false) => {}
                }
            }
        })
        .await;
    }
    let (waiter, drained, ws) = {
        let mut st = conn.st.borrow_mut();
        st.disconnected = true;
        st.out.clear();
        let ws = st.ws.as_mut().map(|w| w.take_disconnect());
        (
            st.recv_waiter.take(),
            std::mem::take(&mut st.drain_waiters),
            ws,
        )
    };
    if waiter.is_some() || !drained.is_empty() {
        unsafe {
            if let Some(w) = waiter {
                match ws {
                    Some(end) => crate::ws::resolve(w, crate::ws::Deliver::Disconnect(end)),
                    None => asgi::resolve_receive(w, None),
                }
            }
            for fut in drained {
                asgi::resolve_none(fut);
            }
        }
        sh.py_scheduled();
    }
    if let Some(io) = conn.io.borrow().as_ref() {
        io.shutdown_write();
    }
    conn.waker.borrow_mut().take();
    if !conn.stream {
        crate::metrics::closed();
    }
    sh.conns.borrow_mut().try_remove(conn.slot as usize);
    // Closing with unread input makes the OS reset the connection, which
    // can destroy the response before the client reads it: wait for the
    // client to close its side first, briefly.
    if linger {
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            let mut total = 0usize;
            while total < LINGER_LIMIT {
                if conn.io.borrow().is_none() {
                    return;
                }
                if conn.io().readable().await.is_err() {
                    return;
                }
                rbuf.clear();
                reserve(rbuf);
                match conn.io().try_read_buf(rbuf) {
                    Ok(0) => return,
                    Ok(n) => total += n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(_) => return,
                }
            }
        })
        .await;
    }
}

// --- chunked request bodies --------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Chunk {
    Size,
    Ext,
    SizeLf,
    Data,
    DataCr,
    DataLf,
    TrailerStart,
    TrailerLine,
    TrailerLf,
    EndLf,
    Done,
}

pub struct ChunkDecoder {
    state: Chunk,
    remaining: u64,
    digits: u8,
    /// Body bytes still allowed.
    body_left: u64,
    /// Extension and trailer bytes still allowed: they decode to no body, so
    /// without a bound of their own a peer could stream them forever.
    meta_left: usize,
}

/// A chunked body the server will not accept, as the status that says why.
pub type BodyError = u16;
const MALFORMED: BodyError = 400;
const TOO_LARGE: BodyError = 413;
const META_TOO_LARGE: BodyError = 431;

impl ChunkDecoder {
    pub fn new(max_body: u64, max_meta: usize) -> ChunkDecoder {
        ChunkDecoder {
            state: Chunk::Size,
            remaining: 0,
            digits: 0,
            body_left: max_body,
            meta_left: max_meta,
        }
    }

    fn done(&self) -> bool {
        self.state == Chunk::Done
    }

    fn end_size(&mut self) -> Result<(), BodyError> {
        if self.remaining > self.body_left {
            return Err(TOO_LARGE);
        }
        self.body_left -= self.remaining;
        self.state = if self.remaining == 0 {
            Chunk::TrailerStart
        } else {
            Chunk::Data
        };
        self.digits = 0;
        Ok(())
    }

    fn meta(&mut self, n: usize) -> Result<(), BodyError> {
        self.meta_left = self.meta_left.checked_sub(n).ok_or(META_TOO_LARGE)?;
        Ok(())
    }

    fn decode(&mut self, src: &mut BytesMut, dst: &mut BytesMut) -> Result<(), BodyError> {
        loop {
            if self.state == Chunk::Done {
                return Ok(());
            }
            if self.state == Chunk::Data {
                let n = self.remaining.min(src.len() as u64) as usize;
                if n == 0 {
                    return Ok(());
                }
                dst.extend_from_slice(&src[..n]);
                src.advance(n);
                self.remaining -= n as u64;
                if self.remaining == 0 {
                    self.state = Chunk::DataCr;
                }
                continue;
            }
            let Some(&b) = src.first() else { return Ok(()) };
            match self.state {
                Chunk::Size => match b {
                    b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F' => {
                        if self.digits >= 16 {
                            return Err(TOO_LARGE);
                        }
                        let v = (b as char).to_digit(16).unwrap_or(0) as u64;
                        self.remaining = self.remaining * 16 + v;
                        self.digits += 1;
                        src.advance(1);
                    }
                    b';' | b' ' | b'\t' if self.digits > 0 => {
                        self.meta(1)?;
                        self.state = Chunk::Ext;
                        src.advance(1);
                    }
                    b'\r' if self.digits > 0 => {
                        self.state = Chunk::SizeLf;
                        src.advance(1);
                    }
                    _ => return Err(MALFORMED),
                },
                // Every line ends in CRLF: a bare CR or LF is where two
                // parsers stop agreeing on where the body ends.
                Chunk::Ext | Chunk::TrailerLine => {
                    let end = src.iter().position(|&c| c == b'\r' || c == b'\n');
                    let n = end.unwrap_or(src.len());
                    self.meta(n)?;
                    src.advance(n);
                    match end {
                        Some(_) if src[0] == b'\r' => {
                            src.advance(1);
                            self.state = if self.state == Chunk::Ext {
                                Chunk::SizeLf
                            } else {
                                Chunk::TrailerLf
                            };
                        }
                        Some(_) => return Err(MALFORMED),
                        None => return Ok(()),
                    }
                }
                Chunk::SizeLf => {
                    if b != b'\n' {
                        return Err(MALFORMED);
                    }
                    src.advance(1);
                    self.end_size()?;
                }
                Chunk::DataCr => {
                    if b != b'\r' {
                        return Err(MALFORMED);
                    }
                    src.advance(1);
                    self.state = Chunk::DataLf;
                }
                Chunk::DataLf => {
                    if b != b'\n' {
                        return Err(MALFORMED);
                    }
                    src.advance(1);
                    self.state = Chunk::Size;
                    self.remaining = 0;
                }
                Chunk::TrailerStart => {
                    if b == b'\r' {
                        src.advance(1);
                        self.state = Chunk::EndLf;
                    } else if b == b'\n' {
                        return Err(MALFORMED);
                    } else {
                        self.state = Chunk::TrailerLine;
                    }
                }
                Chunk::TrailerLf => {
                    if b != b'\n' {
                        return Err(MALFORMED);
                    }
                    src.advance(1);
                    self.state = Chunk::TrailerStart;
                }
                Chunk::EndLf => {
                    if b != b'\n' {
                        return Err(MALFORMED);
                    }
                    src.advance(1);
                    self.state = Chunk::Done;
                }
                Chunk::Data | Chunk::Done => unreachable!(),
            }
        }
    }
}

// --- formatting ----------------------------------------------------------------

pub fn push_int(out: &mut impl Extend<u8>, mut v: u64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    out.extend(buf[i..].iter().copied());
}

fn fmt_hex(mut v: usize, buf: &mut [u8; 18]) -> usize {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut tmp = [0u8; 18];
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = HEX[v & 15];
        v >>= 4;
        if v == 0 {
            break;
        }
    }
    let n = tmp.len() - i;
    buf[..n].copy_from_slice(&tmp[i..]);
    n
}

/// `Sun, 06 Nov 1994 08:49:37 GMT`, without locale or allocation.
pub fn format_date(secs: u64) -> [u8; 29] {
    const DAYS: [&[u8; 3]; 7] = [b"Thu", b"Fri", b"Sat", b"Sun", b"Mon", b"Tue", b"Wed"];
    const MONTHS: [&[u8; 3]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem / 60) % 60, rem % 60);
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(mo <= 2);

    let mut b = *b"Thu, 01 Jan 1970 00:00:00 GMT";
    b[..3].copy_from_slice(DAYS[(days.rem_euclid(7)) as usize]);
    b[5] = b'0' + (d / 10) as u8;
    b[6] = b'0' + (d % 10) as u8;
    b[8..11].copy_from_slice(MONTHS[(mo - 1) as usize]);
    let y = y as u64;
    b[12] = b'0' + (y / 1000 % 10) as u8;
    b[13] = b'0' + (y / 100 % 10) as u8;
    b[14] = b'0' + (y / 10 % 10) as u8;
    b[15] = b'0' + (y % 10) as u8;
    b[17] = b'0' + (h / 10) as u8;
    b[18] = b'0' + (h % 10) as u8;
    b[20] = b'0' + (m / 10) as u8;
    b[21] = b'0' + (m % 10) as u8;
    b[23] = b'0' + (s / 10) as u8;
    b[24] = b'0' + (s % 10) as u8;
    b
}

pub fn reason(status: u16) -> &'static [u8] {
    match status {
        100 => b"Continue",
        101 => b"Switching Protocols",
        102 => b"Processing",
        104 => b"Upload Resumption Supported",
        103 => b"Early Hints",
        200 => b"OK",
        201 => b"Created",
        202 => b"Accepted",
        203 => b"Non-Authoritative Information",
        204 => b"No Content",
        205 => b"Reset Content",
        206 => b"Partial Content",
        207 => b"Multi-Status",
        300 => b"Multiple Choices",
        301 => b"Moved Permanently",
        302 => b"Found",
        303 => b"See Other",
        304 => b"Not Modified",
        307 => b"Temporary Redirect",
        308 => b"Permanent Redirect",
        400 => b"Bad Request",
        401 => b"Unauthorized",
        402 => b"Payment Required",
        403 => b"Forbidden",
        404 => b"Not Found",
        405 => b"Method Not Allowed",
        406 => b"Not Acceptable",
        408 => b"Request Timeout",
        409 => b"Conflict",
        410 => b"Gone",
        411 => b"Length Required",
        412 => b"Precondition Failed",
        413 => b"Content Too Large",
        414 => b"URI Too Long",
        415 => b"Unsupported Media Type",
        416 => b"Range Not Satisfiable",
        417 => b"Expectation Failed",
        418 => b"I'm a Teapot",
        421 => b"Misdirected Request",
        422 => b"Unprocessable Content",
        423 => b"Locked",
        424 => b"Failed Dependency",
        425 => b"Too Early",
        426 => b"Upgrade Required",
        428 => b"Precondition Required",
        429 => b"Too Many Requests",
        431 => b"Request Header Fields Too Large",
        451 => b"Unavailable For Legal Reasons",
        500 => b"Internal Server Error",
        501 => b"Not Implemented",
        502 => b"Bad Gateway",
        503 => b"Service Unavailable",
        504 => b"Gateway Timeout",
        505 => b"HTTP Version Not Supported",
        507 => b"Insufficient Storage",
        511 => b"Network Authentication Required",
        _ => b"Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date() {
        assert_eq!(&format_date(784_111_777), b"Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(&format_date(0), b"Thu, 01 Jan 1970 00:00:00 GMT");
    }

    fn decode(
        input: &[u8],
        max_body: u64,
        max_meta: usize,
    ) -> Result<(BytesMut, BytesMut, bool), BodyError> {
        let mut d = ChunkDecoder::new(max_body, max_meta);
        let mut out = BytesMut::new();
        let mut src = BytesMut::from(input);
        d.decode(&mut src, &mut out)?;
        Ok((out, src, d.done()))
    }

    #[test]
    fn chunked() {
        let (out, rest, done) = decode(
            b"4\r\nWiki\r\n5;ext=1\r\npedia\r\n0\r\nX-T: 1\r\n\r\nNEXT",
            1 << 20,
            1024,
        )
        .unwrap();
        assert!(done);
        assert_eq!(&out[..], b"Wikipedia");
        assert_eq!(&rest[..], b"NEXT");
    }

    #[test]
    fn chunked_split() {
        let input = b"a;x=\"y\"\r\n0123456789\r\n0\r\nA: b\r\nC: d\r\n\r\n";
        let mut d = ChunkDecoder::new(1 << 20, 1024);
        let mut out = BytesMut::new();
        for b in input {
            let mut src = BytesMut::from(&[*b][..]);
            d.decode(&mut src, &mut out).unwrap();
        }
        assert!(d.done());
        assert_eq!(&out[..], b"0123456789");
    }

    #[test]
    fn chunked_bare_line_endings() {
        for bad in [
            &b"4\nWiki\r\n0\r\n\r\n"[..],
            b"4\r\nWiki\n0\r\n\r\n",
            b"4\r\nWiki\r\n0\r\n\n",
            b"4;x\nWiki\r\n0\r\n\r\n",
            b"4\r\nWiki\r\n0\r\nA: b\n\r\n",
            b"4\r\nWikiX\r\n0\r\n\r\n",
            b"\r\n",
            b"-1\r\n",
        ] {
            assert_eq!(
                decode(bad, 1 << 20, 1024).err(),
                Some(MALFORMED),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn chunked_limits() {
        assert_eq!(
            decode(b"a\r\n0123456789\r\n0\r\n\r\n", 9, 1024).err(),
            Some(TOO_LARGE)
        );
        assert!(decode(b"a\r\n0123456789\r\n0\r\n\r\n", 10, 1024).unwrap().2);
        assert_eq!(
            decode(b"5\r\nhello\r\n6\r\n", 10, 1024).err(),
            Some(TOO_LARGE)
        );
        assert_eq!(
            decode(b"ffffffffffffffffff\r\n", u64::MAX, 1024).err(),
            Some(TOO_LARGE)
        );
        // Extensions and trailers share the header allowance.
        let ext = format!("1;{}\r\nx\r\n0\r\n\r\n", "e".repeat(100));
        assert_eq!(decode(ext.as_bytes(), 1024, 64).err(), Some(META_TOO_LARGE));
        let trailers = format!("0\r\n{}", "X-T: 1\r\n".repeat(20));
        assert_eq!(
            decode(trailers.as_bytes(), 1024, 64).err(),
            Some(META_TOO_LARGE)
        );
        assert!(decode(trailers.as_bytes(), 1024, 1024).is_ok());
    }
}
