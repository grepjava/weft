//! WebTransport over HTTP/3 (draft-ietf-webtrans-http3) as Peregrine's ASGI
//! `webtransport` extension.
//!
//! A session is an extended CONNECT that never finishes. Its traffic arrives
//! on other QUIC streams (0x41 / 0x54 + session id) and in datagrams that
//! name the session by the CONNECT stream's quarter identifier.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::poll_fn;
use std::rc::Rc;

use bytes::{Buf, Bytes, BytesMut};
use h3::proto::coding::Encode;
use h3::quic::{BidiStream as BidiStreamTrait, RecvStream, SendStream, SendStreamUnframed};
use h3::stream::BufRecvStream;
use h3::webtransport::SessionId;
use pyo3_ffi::*;
use tokio::sync::Notify;

use crate::asgi;
use crate::core::{AppCtx, Shared};
use crate::http::{BodyKind, Conn, Phase, State};
use crate::interned::{empty_bytes, s};
use crate::ops::Meta;
use crate::py::{self, PResult, PyRef};
use crate::types::{self, ChanObj};

pub const MAX_SESSIONS: usize = 16;
const MAX_ORPHANS: usize = 32;
const MAX_DATAGRAMS: usize = 64;
const MAX_DATAGRAM_BYTES: usize = 256 * 1024;
const MAX_CHUNK: usize = 64 * 1024;
const MAX_STREAM_BUF: usize = 256 * 1024;
const CLOSE_SESSION: u64 = 0x2843;

pub type H3Bidi = BufRecvStream<h3_quinn::BidiStream<Bytes>, Bytes>;
pub type H3Uni = BufRecvStream<h3_quinn::RecvStream, Bytes>;

pub struct Hub {
    pub quic: quinn::Connection,
    pub opener: RefCell<h3_quinn::OpenStreams>,
    sessions: RefCell<HashMap<u64, SessionRef>>,
    orphans: RefCell<Vec<Orphan>>,
}

struct SessionRef {
    session: Rc<Session>,
    slot: u32,
    generation: u32,
}

enum Orphan {
    Bidi {
        session: u64,
        stream: u64,
        io: Box<H3Bidi>,
    },
    Uni {
        session: u64,
        stream: u64,
        io: H3Uni,
    },
}

pub struct Session {
    pub id: u64,
    pub accepted: Cell<bool>,
    connect_delivered: Cell<bool>,
    disconnect_delivered: Cell<bool>,
    pub gone: Cell<bool>,
    close_code: Cell<u64>,
    close_reason: RefCell<Vec<u8>>,
    streams: RefCell<HashMap<u64, Rc<Stream>>>,
    readable: RefCell<VecDeque<u64>>,
    readable_set: RefCell<HashSet<u64>>,
    paused_readable: RefCell<HashSet<u64>>,
    opened: RefCell<VecDeque<u64>>,
    datagrams: RefCell<VecDeque<Bytes>>,
    datagram_bytes: Cell<usize>,
    datagram_turn: Cell<bool>,
    out_datagrams: RefCell<VecDeque<Bytes>>,
    pending_open: RefCell<VecDeque<bool>>,
    pub kick: Notify,
}

struct Stream {
    id: u64,
    bidirectional: bool,
    writable: Cell<bool>,
    paused: Cell<bool>,
    recv_closed: Cell<bool>,
    end_delivered: Cell<bool>,
    fin_sent: Cell<bool>,
    buf: RefCell<BytesMut>,
    out: RefCell<BytesMut>,
    end_queued: Cell<bool>,
    kick: Rc<Notify>,
}

impl Hub {
    pub fn new(quic: quinn::Connection, opener: h3_quinn::OpenStreams) -> Rc<Hub> {
        Rc::new(Hub {
            quic,
            opener: RefCell::new(opener),
            sessions: RefCell::new(HashMap::new()),
            orphans: RefCell::new(Vec::new()),
        })
    }

    pub fn session_count(&self) -> usize {
        self.sessions.borrow().len()
    }

    pub fn register(&self, session: &Rc<Session>, slot: u32, generation: u32) {
        self.sessions.borrow_mut().insert(
            session.id,
            SessionRef {
                session: session.clone(),
                slot,
                generation,
            },
        );
    }

    pub fn forget(&self, id: u64) {
        self.sessions.borrow_mut().remove(&id);
    }

    pub fn adopt(&self, sh: &Shared, session: &Rc<Session>) {
        let mut kept = Vec::new();
        for o in self.orphans.borrow_mut().drain(..) {
            match o {
                Orphan::Bidi {
                    session: sid,
                    stream,
                    io,
                } if sid == session.id => {
                    attach_bidi(sh, session, stream, *io);
                }
                Orphan::Uni {
                    session: sid,
                    stream,
                    io,
                } if sid == session.id => {
                    attach_uni(sh, session, stream, io);
                }
                other => kept.push(other),
            }
        }
        *self.orphans.borrow_mut() = kept;
    }

    fn lookup(&self, session_id: u64) -> Option<SessionRef> {
        self.sessions.borrow().get(&session_id).cloned()
    }

    pub fn incoming_bidi(&self, sh: &Shared, session_id: u64, stream_id: u64, io: H3Bidi) {
        if let Some(r) = self.lookup(session_id) {
            attach_bidi(sh, &r.session, stream_id, io);
            if let Some(conn) = sh.conn(r.slot, r.generation) {
                wake_recv(&conn);
                conn.notify(sh);
            }
            return;
        }
        push_orphan(
            &self.orphans,
            Orphan::Bidi {
                session: session_id,
                stream: stream_id,
                io: Box::new(io),
            },
        );
    }

    pub fn incoming_uni(&self, sh: &Shared, session_id: u64, stream_id: u64, io: H3Uni) {
        if let Some(r) = self.lookup(session_id) {
            attach_uni(sh, &r.session, stream_id, io);
            if let Some(conn) = sh.conn(r.slot, r.generation) {
                wake_recv(&conn);
                conn.notify(sh);
            }
            return;
        }
        push_orphan(
            &self.orphans,
            Orphan::Uni {
                session: session_id,
                stream: stream_id,
                io,
            },
        );
    }

    pub fn datagram(&self, sh: &Shared, payload: Bytes) {
        let Some((qid, n)) = decode_varint(&payload) else {
            return;
        };
        let data = payload.slice(n..);
        // RFC 9297 / draft-ietf-webtrans-http3: datagrams name the session by
        // the CONNECT stream's quarter identifier. aioquic and Peregrine both
        // send that; the session itself is keyed by the full stream id.
        let Some(r) = self
            .lookup(session_id_from_quarter(qid))
            .or_else(|| self.lookup(qid))
        else {
            return;
        };
        r.session.push_datagram(data);
        if let Some(conn) = sh.conn(r.slot, r.generation) {
            wake_recv(&conn);
            conn.notify(sh);
        }
    }

    pub fn close_peer(&self, sh: &Shared, session_id: u64, code: u64, reason: Vec<u8>) {
        let Some(r) = self.sessions.borrow().get(&session_id).cloned() else {
            return;
        };
        r.session.mark_gone(code, reason);
        if let Some(conn) = sh.conn(r.slot, r.generation) {
            wake_recv(&conn);
            conn.notify(sh);
        }
    }
}

impl Clone for SessionRef {
    fn clone(&self) -> Self {
        SessionRef {
            session: self.session.clone(),
            slot: self.slot,
            generation: self.generation,
        }
    }
}

fn push_orphan(orphans: &RefCell<Vec<Orphan>>, o: Orphan) {
    let mut q = orphans.borrow_mut();
    if q.len() < MAX_ORPHANS {
        q.push(o);
    }
}

fn new_stream(id: u64, bidirectional: bool, writable: bool) -> Rc<Stream> {
    Rc::new(Stream {
        id,
        bidirectional,
        writable: Cell::new(writable),
        paused: Cell::new(false),
        recv_closed: Cell::new(false),
        end_delivered: Cell::new(false),
        fin_sent: Cell::new(false),
        buf: RefCell::new(BytesMut::new()),
        out: RefCell::new(BytesMut::new()),
        end_queued: Cell::new(false),
        kick: Rc::new(Notify::new()),
    })
}

fn attach_bidi(sh: &Shared, session: &Rc<Session>, stream_id: u64, io: H3Bidi) {
    let st = new_stream(stream_id, true, true);
    session.streams.borrow_mut().insert(stream_id, st.clone());
    let session = session.clone();
    tokio::task::spawn_local(async move {
        let (send, recv) = io.split();
        tokio::task::spawn_local(run_h3send(st.clone(), send));
        run_h3recv(session.clone(), st, recv).await;
        session.kick.notify_waiters();
    });
    let _ = sh;
}

fn attach_uni(sh: &Shared, session: &Rc<Session>, stream_id: u64, io: H3Uni) {
    let st = new_stream(stream_id, false, false);
    session.streams.borrow_mut().insert(stream_id, st.clone());
    let session = session.clone();
    tokio::task::spawn_local(async move {
        run_h3recv(session, st, io).await;
    });
    let _ = sh;
}

async fn run_h3send<S>(st: Rc<Stream>, mut send: S)
where
    S: SendStream<Bytes> + SendStreamUnframed<Bytes>,
{
    loop {
        let wait = st.kick.notified();
        let mut data = st.out.borrow_mut().split();
        while !data.is_empty() {
            match poll_fn(|cx| SendStreamUnframed::poll_send(&mut send, cx, &mut data)).await {
                Ok(0) => tokio::task::yield_now().await,
                Ok(_) => {}
                Err(_) => return,
            }
        }
        if st.end_queued.get() && !st.fin_sent.get() {
            if poll_fn(|cx| SendStream::poll_finish(&mut send, cx))
                .await
                .is_ok()
            {
                st.fin_sent.set(true);
            }
            return;
        }
        if st.fin_sent.get() {
            return;
        }
        wait.await;
    }
}

async fn run_h3recv<S>(session: Rc<Session>, st: Rc<Stream>, mut io: S)
where
    S: RecvStream,
    S::Buf: Buf,
{
    loop {
        if session.gone.get() {
            return;
        }
        let wait = st.kick.notified();
        if st.paused.get() || st.buf.borrow().len() >= MAX_STREAM_BUF {
            wait.await;
            continue;
        }
        drop(wait);
        match poll_fn(|cx| RecvStream::poll_data(&mut io, cx)).await {
            Ok(Some(mut buf)) => {
                let b = buf.copy_to_bytes(buf.remaining());
                if !b.is_empty() {
                    st.buf.borrow_mut().extend_from_slice(&b);
                    session.mark_readable(st.id);
                    session.kick.notify_waiters();
                }
            }
            Ok(None) | Err(_) => {
                st.recv_closed.set(true);
                session.mark_readable(st.id);
                session.kick.notify_waiters();
                return;
            }
        }
    }
}

impl Session {
    pub fn new(id: u64) -> Rc<Session> {
        Rc::new(Session {
            id,
            accepted: Cell::new(false),
            connect_delivered: Cell::new(false),
            disconnect_delivered: Cell::new(false),
            gone: Cell::new(false),
            close_code: Cell::new(0),
            close_reason: RefCell::new(Vec::new()),
            streams: RefCell::new(HashMap::new()),
            readable: RefCell::new(VecDeque::new()),
            readable_set: RefCell::new(HashSet::new()),
            paused_readable: RefCell::new(HashSet::new()),
            opened: RefCell::new(VecDeque::new()),
            datagrams: RefCell::new(VecDeque::new()),
            datagram_bytes: Cell::new(0),
            datagram_turn: Cell::new(false),
            out_datagrams: RefCell::new(VecDeque::new()),
            pending_open: RefCell::new(VecDeque::new()),
            kick: Notify::new(),
        })
    }

    fn mark_readable(&self, id: u64) {
        if let Some(s) = self.streams.borrow().get(&id)
            && s.paused.get()
        {
            self.paused_readable.borrow_mut().insert(id);
            return;
        }
        if self.readable_set.borrow_mut().insert(id) {
            self.readable.borrow_mut().push_back(id);
        }
    }

    fn pause(&self, id: u64) {
        let Some(s) = self.streams.borrow().get(&id).cloned() else {
            return;
        };
        if s.paused.get() {
            return;
        }
        s.paused.set(true);
        if self.readable_set.borrow_mut().remove(&id) {
            let mut q = self.readable.borrow_mut();
            if let Some(i) = q.iter().position(|&x| x == id) {
                q.remove(i);
            }
            self.paused_readable.borrow_mut().insert(id);
        }
    }

    fn resume(&self, id: u64) {
        let Some(s) = self.streams.borrow().get(&id).cloned() else {
            return;
        };
        if !s.paused.get() {
            return;
        }
        s.paused.set(false);
        s.kick.notify_waiters();
        if self.paused_readable.borrow_mut().remove(&id) {
            self.mark_readable(id);
        }
        self.kick.notify_waiters();
    }

    fn push_datagram(&self, data: Bytes) {
        let mut q = self.datagrams.borrow_mut();
        while q.len() >= MAX_DATAGRAMS
            || self.datagram_bytes.get() + data.len() > MAX_DATAGRAM_BYTES
        {
            if let Some(old) = q.pop_front() {
                self.datagram_bytes
                    .set(self.datagram_bytes.get().saturating_sub(old.len()));
            } else {
                break;
            }
        }
        self.datagram_bytes
            .set(self.datagram_bytes.get() + data.len());
        q.push_back(data);
    }

    pub fn push_out_datagram(&self, data: Bytes) {
        self.out_datagrams.borrow_mut().push_back(data);
    }

    pub fn take_out_datagrams(&self) -> Vec<Bytes> {
        self.out_datagrams.borrow_mut().drain(..).collect()
    }

    pub fn take_pending_open(&self) -> Option<bool> {
        self.pending_open.borrow_mut().pop_front()
    }

    pub fn close_info(&self) -> (u64, Vec<u8>) {
        (self.close_code.get(), self.close_reason.borrow().clone())
    }

    pub fn spawn_outgoing_h3(
        self: &Rc<Self>,
        id: u64,
        send: h3_quinn::SendStream<Bytes>,
        recv: Option<h3_quinn::RecvStream>,
    ) {
        let bidirectional = recv.is_some();
        let st = self.register_opened(id, bidirectional, true);
        if !bidirectional {
            st.recv_closed.set(true);
            st.end_delivered.set(true);
        }
        tokio::task::spawn_local(run_h3send(st.clone(), send));
        if let Some(recv) = recv {
            tokio::task::spawn_local(run_h3recv(self.clone(), st, recv));
        }
        self.kick.notify_waiters();
    }

    pub fn mark_gone(&self, code: u64, reason: Vec<u8>) {
        if self.gone.get() {
            return;
        }
        self.gone.set(true);
        self.close_code.set(code);
        *self.close_reason.borrow_mut() = reason;
        self.kick.notify_waiters();
    }

    fn register_opened(&self, id: u64, bidirectional: bool, writable: bool) -> Rc<Stream> {
        let st = new_stream(id, bidirectional, writable);
        self.streams.borrow_mut().insert(id, st.clone());
        self.opened.borrow_mut().push_back(id);
        st
    }

    fn take_event(&self) -> Option<Event> {
        if !self.connect_delivered.get() {
            self.connect_delivered.set(true);
            return Some(Event::Connect);
        }
        if self.disconnect_delivered.get() {
            return None;
        }
        if !self.accepted.get() {
            if self.gone.get() {
                self.disconnect_delivered.set(true);
                return Some(Event::Disconnect(
                    self.close_code.get(),
                    self.close_reason.borrow().clone(),
                ));
            }
            return None;
        }
        if let Some(id) = self.opened.borrow_mut().pop_front() {
            let bidi = self
                .streams
                .borrow()
                .get(&id)
                .map(|s| s.bidirectional)
                .unwrap_or(false);
            return Some(Event::Opened(id, bidi));
        }
        if self.datagram_turn.get()
            && let Some(d) = self.take_datagram()
        {
            self.datagram_turn.set(false);
            return Some(Event::Datagram(d));
        }
        if let Some(e) = self.take_chunk() {
            self.datagram_turn.set(true);
            return Some(e);
        }
        if let Some(d) = self.take_datagram() {
            self.datagram_turn.set(false);
            return Some(Event::Datagram(d));
        }
        if self.gone.get() {
            self.disconnect_delivered.set(true);
            return Some(Event::Disconnect(
                self.close_code.get(),
                self.close_reason.borrow().clone(),
            ));
        }
        None
    }

    fn has_event(&self) -> bool {
        if !self.connect_delivered.get() {
            return true;
        }
        if self.disconnect_delivered.get() {
            return false;
        }
        if !self.accepted.get() {
            return self.gone.get();
        }
        !self.opened.borrow().is_empty()
            || !self.readable.borrow().is_empty()
            || !self.datagrams.borrow().is_empty()
            || self.gone.get()
    }

    fn take_datagram(&self) -> Option<Bytes> {
        let d = self.datagrams.borrow_mut().pop_front()?;
        self.datagram_bytes
            .set(self.datagram_bytes.get().saturating_sub(d.len()));
        Some(d)
    }

    fn take_chunk(&self) -> Option<Event> {
        loop {
            let id = self.readable.borrow_mut().pop_front()?;
            self.readable_set.borrow_mut().remove(&id);
            let Some(st) = self.streams.borrow().get(&id).cloned() else {
                continue;
            };
            if st.paused.get() {
                self.paused_readable.borrow_mut().insert(id);
                continue;
            }
            let mut buf = st.buf.borrow_mut();
            if !buf.is_empty() {
                let n = buf.len().min(MAX_CHUNK);
                let data = buf.split_to(n).freeze();
                let leftover = !buf.is_empty();
                let more = leftover || !st.recv_closed.get();
                drop(buf);
                st.kick.notify_waiters();
                if leftover {
                    self.mark_readable(id);
                } else if st.recv_closed.get() {
                    st.end_delivered.set(true);
                }
                let more = more && !st.end_delivered.get();
                return Some(Event::Receive(id, data, more));
            }
            if st.recv_closed.get() && !st.end_delivered.get() {
                st.end_delivered.set(true);
                return Some(Event::Receive(id, Bytes::new(), false));
            }
        }
    }
}

enum Event {
    Connect,
    Opened(u64, bool),
    Receive(u64, Bytes, bool),
    Datagram(Bytes),
    Disconnect(u64, Vec<u8>),
}

pub fn session_id_from_connect(stream_id: u64) -> u64 {
    stream_id
}

pub fn session_id_from_quarter(quarter: u64) -> u64 {
    quarter << 2
}

pub fn session_quarter(session_id: u64) -> u64 {
    session_id >> 2
}

pub fn session_u64(id: SessionId) -> u64 {
    let mut buf = BytesMut::with_capacity(8);
    id.encode(&mut buf);
    decode_varint(&buf).map(|(v, _)| v).unwrap_or(0)
}

pub fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
    if buf.is_empty() {
        return None;
    }
    let n = 1usize << (buf[0] >> 6);
    if buf.len() < n {
        return None;
    }
    let mut v = (buf[0] & 0x3f) as u64;
    for &b in &buf[1..n] {
        v = (v << 8) | b as u64;
    }
    Some((v, n))
}

pub fn encode_varint(out: &mut Vec<u8>, v: u64) {
    if v < 64 {
        out.push(v as u8);
    } else if v < 16384 {
        out.push(0x40 | ((v >> 8) as u8));
        out.push(v as u8);
    } else if v < (1 << 30) {
        out.push(0x80 | ((v >> 24) as u8));
        out.push((v >> 16) as u8);
        out.push((v >> 8) as u8);
        out.push(v as u8);
    } else {
        out.push(0xc0 | ((v >> 56) as u8));
        out.extend_from_slice(&v.to_be_bytes()[1..]);
    }
}

pub fn parse_capsules(buf: &[u8], mut on_close: impl FnMut(u64, Vec<u8>)) {
    let mut i = 0;
    while i < buf.len() {
        let Some((ty, n)) = decode_varint(&buf[i..]) else {
            break;
        };
        i += n;
        let Some((len, n)) = decode_varint(&buf[i..]) else {
            break;
        };
        i += n;
        let len = len as usize;
        if i + len > buf.len() {
            break;
        }
        let payload = &buf[i..i + len];
        i += len;
        if ty == CLOSE_SESSION {
            let code = if payload.len() >= 4 {
                u32::from_be_bytes(payload[..4].try_into().unwrap()) as u64
            } else {
                0
            };
            let reason = if payload.len() > 4 {
                payload[4..].to_vec()
            } else {
                Vec::new()
            };
            on_close(code, reason);
        }
    }
}

pub fn close_capsule(code: u64, reason: &[u8]) -> Bytes {
    let mut payload = Vec::with_capacity(4 + reason.len());
    payload.extend_from_slice(&(code as u32).to_be_bytes());
    payload.extend_from_slice(reason);
    let mut out = Vec::new();
    encode_varint(&mut out, CLOSE_SESSION);
    encode_varint(&mut out, payload.len() as u64);
    out.extend_from_slice(&payload);
    Bytes::from(out)
}

pub fn wt_uni_prefix(session_id: u64) -> Vec<u8> {
    let mut p = Vec::new();
    encode_varint(&mut p, 0x54);
    encode_varint(&mut p, session_id);
    p
}

pub fn wt_bidi_prefix(session_id: u64) -> Vec<u8> {
    let mut p = Vec::new();
    encode_varint(&mut p, 0x41);
    encode_varint(&mut p, session_id);
    p
}

/// Builds the `webtransport` scope and starts the application task.
pub unsafe fn start(
    sh: &Shared,
    ctx: &Rc<AppCtx>,
    conn: &Rc<Conn>,
    req: &httparse::Request<'_, '_>,
    session: Rc<Session>,
    meta: &Meta,
) -> PResult<()> {
    let seq = {
        let mut st = conn.st.borrow_mut();
        st.begin(true, false, false, false, BodyKind::Empty);
        st.h3 = true;
        st.wt = Some(session);
        st.seq
    };
    let i = s();
    unsafe {
        let scope = PyRef::own(PyDict_Copy(ctx.scope_proto.ptr()))?;
        let d = scope.ptr();
        py::dict_set(d, i.type_, PyRef::borrow(i.webtransport))?;
        py::dict_set(d, i.http_version, PyRef::borrow(i.v3))?;
        py::dict_set(d, i.scheme, PyRef::borrow(i.https))?;
        if let Some(ext) = &ctx.h3_extensions {
            py::dict_set(d, i.extensions, ext.clone())?;
        }
        asgi::fill_request(sh, ctx, conn, d, req, meta)?;
        // Keep send-side interned names reachable; receive() builds the rest.
        let _ = (
            i.webtransport_accept,
            i.webtransport_close,
            i.webtransport_stream_open,
            i.webtransport_stream_send,
            i.webtransport_stream_pause,
            i.webtransport_stream_resume,
            i.webtransport_datagram_send,
        );
        asgi::launch(sh, ctx, conn, d, receive_vc, send_vc, seq)
    }
}

pub fn task_finished(st: &mut State, failed: bool) {
    let Some(wt) = st.wt.clone() else { return };
    if !wt.accepted.get() && !wt.gone.get() {
        wt.mark_gone(if failed { 500 } else { 403 }, Vec::new());
    } else if !wt.gone.get() {
        wt.mark_gone(0, Vec::new());
    }
}

pub fn wake_recv(conn: &Conn) {
    let Some(wt) = conn.st.borrow().wt.clone() else {
        return;
    };
    if !wt.has_event() {
        return;
    }
    let Some(fut) = conn.st.borrow_mut().recv_waiter.take() else {
        return;
    };
    let event = wt.take_event();
    unsafe {
        match event_msg(event) {
            Ok(m) => asgi::set_result(&fut, m.ptr()),
            Err(_) => PyErr_WriteUnraisable(fut.ptr()),
        }
    }
}

unsafe extern "C" fn receive_vc(
    callable: *mut PyObject,
    _args: *const *mut PyObject,
    nargsf: usize,
    kwnames: *mut PyObject,
) -> *mut PyObject {
    unsafe {
        if PyVectorcall_NARGS(nargsf) != 0 || (!kwnames.is_null() && PyTuple_GET_SIZE(kwnames) != 0)
        {
            py::type_error("receive() takes no arguments");
            return std::ptr::null_mut();
        }
        receive(&*(callable as *const ChanObj)).map_or(std::ptr::null_mut(), PyRef::into_ptr)
    }
}

unsafe extern "C" fn send_vc(
    callable: *mut PyObject,
    args: *const *mut PyObject,
    nargsf: usize,
    kwnames: *mut PyObject,
) -> *mut PyObject {
    unsafe {
        let r = asgi::one_arg("send", args, nargsf, kwnames)
            .and_then(|msg| send(&*(callable as *const ChanObj), msg));
        r.map_or(std::ptr::null_mut(), PyRef::into_ptr)
    }
}

unsafe fn receive(c: &ChanObj) -> PResult<PyRef> {
    unsafe {
        let (sh, ctx) = asgi::worker()?;
        let gone = || event_msg(Some(Event::Disconnect(0, Vec::new())));
        let Some(conn) = sh.conn(c.slot, c.generation) else {
            return types::ready(Some(gone()?));
        };
        let st = conn.st.borrow_mut();
        if st.seq != c.seq {
            drop(st);
            return types::ready(Some(gone()?));
        }
        if st.recv_waiter.is_some() {
            drop(st);
            return Err(py::runtime_error("receive() is already being awaited"));
        }
        let Some(wt) = st.wt.clone() else {
            drop(st);
            return types::ready(Some(gone()?));
        };
        if let Some(e) = wt.take_event() {
            drop(st);
            return types::ready(Some(event_msg(Some(e))?));
        }
        drop(st);
        let fut = asgi::new_future(&ctx)?;
        conn.st.borrow_mut().recv_waiter = Some(fut.clone());
        conn.notify(sh);
        Ok(fut)
    }
}

unsafe fn send(c: &ChanObj, msg: *mut PyObject) -> PResult<PyRef> {
    unsafe {
        let (sh, ctx) = asgi::worker()?;
        let ty = asgi::message_type(msg)?;
        let conn = sh.conn(c.slot, c.generation);
        let conn = conn.as_deref().filter(|conn| conn.st.borrow().seq == c.seq);
        match ty {
            b"webtransport.accept" => accept(&ctx, msg, conn),
            b"webtransport.close" => close(&ctx, msg, conn),
            b"webtransport.stream.open" => stream_open(sh, &ctx, msg, conn),
            b"webtransport.stream.send" => stream_send(&ctx, msg, conn),
            b"webtransport.stream.pause" => stream_ctrl(&ctx, msg, conn, true),
            b"webtransport.stream.resume" => stream_ctrl(&ctx, msg, conn, false),
            b"webtransport.datagram.send" => datagram_send(&ctx, msg, conn),
            other => Err(py::runtime_error(&format!(
                "Unexpected ASGI message type '{}'",
                String::from_utf8_lossy(other)
            ))),
        }
    }
}

unsafe fn event_msg(event: Option<Event>) -> PResult<PyRef> {
    let i = s();
    unsafe {
        let d = PyRef::own(PyDict_New())?;
        match event {
            Some(Event::Connect) | None => {
                py::dict_set(d.ptr(), i.type_, PyRef::borrow(i.webtransport_connect))?;
            }
            Some(Event::Opened(id, bidi)) => {
                py::dict_set(
                    d.ptr(),
                    i.type_,
                    PyRef::borrow(i.webtransport_stream_opened),
                )?;
                py::dict_set(d.ptr(), i.stream, py::int(id as i64)?)?;
                py::dict_set(
                    d.ptr(),
                    i.bidirectional,
                    PyRef::borrow(if bidi { Py_True() } else { Py_False() }),
                )?;
            }
            Some(Event::Receive(id, data, more)) => {
                py::dict_set(
                    d.ptr(),
                    i.type_,
                    PyRef::borrow(i.webtransport_stream_receive),
                )?;
                py::dict_set(d.ptr(), i.stream, py::int(id as i64)?)?;
                let body = if data.is_empty() {
                    PyRef::borrow(empty_bytes())
                } else {
                    py::bytes(&data)?
                };
                py::dict_set(d.ptr(), i.data, body)?;
                py::dict_set(
                    d.ptr(),
                    i.more_data,
                    PyRef::borrow(if more { Py_True() } else { Py_False() }),
                )?;
            }
            Some(Event::Datagram(data)) => {
                py::dict_set(
                    d.ptr(),
                    i.type_,
                    PyRef::borrow(i.webtransport_datagram_receive),
                )?;
                let body = if data.is_empty() {
                    PyRef::borrow(empty_bytes())
                } else {
                    py::bytes(&data)?
                };
                py::dict_set(d.ptr(), i.data, body)?;
            }
            Some(Event::Disconnect(code, reason)) => {
                py::dict_set(d.ptr(), i.type_, PyRef::borrow(i.webtransport_disconnect))?;
                py::dict_set(d.ptr(), i.code, py::int(code as i64)?)?;
                py::dict_set(d.ptr(), i.reason, py::str_utf8_replace(&reason)?)?;
            }
        }
        Ok(d)
    }
}

unsafe fn accept(ctx: &AppCtx, msg: *mut PyObject, conn: Option<&Conn>) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let extra = match py::dict_get(msg, i.headers)? {
            Some(h) if !py::is_none(h.ptr()) => {
                let mut extra = Vec::new();
                let mut info = crate::http::HeadInfo::default();
                asgi::encode_headers(h.ptr(), &mut extra, &mut info, None)?;
                extra
            }
            _ => Vec::new(),
        };
        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let Some(wt) = conn.st.borrow().wt.clone() else {
            return Err(py::runtime_error("not a webtransport session"));
        };
        if wt.accepted.get() {
            return Err(py::runtime_error("'webtransport.accept' sent twice"));
        }
        if wt.gone.get() {
            return Ok(ctx.done.clone());
        }
        wt.accepted.set(true);
        conn.st.borrow_mut().out.extend_from_slice(&extra);
        conn.st.borrow_mut().resp.status = 200;
        conn.st.borrow_mut().resp.phase = Phase::Head;
        if let Some(sh) = crate::core::current() {
            conn.notify(sh);
        }
        wt.kick.notify_waiters();
        Ok(ctx.done.clone())
    }
}

unsafe fn close(ctx: &AppCtx, msg: *mut PyObject, conn: Option<&Conn>) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let code = match py::dict_get(msg, i.code)? {
            Some(v) if !py::is_none(v.ptr()) => py::i64_arg(v.ptr())? as u64,
            _ => 0,
        };
        let reason = match py::dict_get(msg, i.reason)? {
            Some(v) if !py::is_none(v.ptr()) => {
                if PyUnicode_Check(v.ptr()) != 0 {
                    py::str_view(v.ptr())?.to_vec()
                } else {
                    py::with_bytes(v.ptr(), |b| b.to_vec())?
                }
            }
            _ => Vec::new(),
        };
        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let Some(wt) = conn.st.borrow().wt.clone() else {
            return Err(py::runtime_error("not a webtransport session"));
        };
        if !wt.accepted.get() {
            let status = if (400..600).contains(&code) {
                code as u16
            } else {
                403
            };
            conn.st.borrow_mut().resp.status = status;
            conn.st.borrow_mut().resp.phase = Phase::Done;
        } else {
            conn.st.borrow_mut().resp.phase = Phase::Done;
        }
        wt.mark_gone(code, reason);
        if let Some(sh) = crate::core::current() {
            conn.notify(sh);
            wake_recv(conn);
        }
        Ok(ctx.done.clone())
    }
}

unsafe fn stream_id_of(msg: *mut PyObject) -> PResult<u64> {
    unsafe {
        let i = s();
        let v = py::dict_get(msg, i.stream)?
            .ok_or_else(|| py::raise(PyExc_KeyError, "missing 'stream'"))?;
        Ok(py::i64_arg(v.ptr())? as u64)
    }
}

unsafe fn stream_ctrl(
    ctx: &AppCtx,
    msg: *mut PyObject,
    conn: Option<&Conn>,
    pause: bool,
) -> PResult<PyRef> {
    unsafe {
        let id = stream_id_of(msg)?;
        if let Some(conn) = conn
            && let Some(wt) = conn.st.borrow().wt.clone()
        {
            if pause {
                wt.pause(id);
            } else {
                wt.resume(id);
                wake_recv(conn);
            }
        }
        Ok(ctx.done.clone())
    }
}

unsafe fn stream_send(ctx: &AppCtx, msg: *mut PyObject, conn: Option<&Conn>) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let id = stream_id_of(msg)?;
        let data = match py::dict_get(msg, i.data)? {
            Some(b) if PyBytes_Check(b.ptr()) != 0 => std::slice::from_raw_parts(
                PyBytes_AsString(b.ptr()) as *const u8,
                PyBytes_Size(b.ptr()) as usize,
            ),
            Some(b) if !py::is_none(b.ptr()) => {
                return py::with_bytes(b.ptr(), |x| x.to_vec())
                    .and_then(|v| send_on(ctx, conn, id, &v, end_stream(msg)?));
            }
            _ => &[][..],
        };
        let end = end_stream(msg)?;
        send_on(ctx, conn, id, data, end)
    }
}

unsafe fn end_stream(msg: *mut PyObject) -> PResult<bool> {
    unsafe {
        match py::dict_get(msg, s().end_stream)? {
            Some(v) if !py::is_none(v.ptr()) => py::truthy(v.ptr()),
            _ => Ok(false),
        }
    }
}

fn send_on(ctx: &AppCtx, conn: Option<&Conn>, id: u64, data: &[u8], end: bool) -> PResult<PyRef> {
    unsafe {
        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let Some(wt) = conn.st.borrow().wt.clone() else {
            return Err(py::runtime_error("not a webtransport session"));
        };
        if !wt.accepted.get() {
            return Err(py::runtime_error(
                "'webtransport.stream.send' before 'webtransport.accept'",
            ));
        }
        let Some(st) = wt.streams.borrow().get(&id).cloned() else {
            return Err(py::runtime_error("unknown webtransport stream"));
        };
        if !st.writable.get() {
            return Err(py::runtime_error("this stream cannot be written to"));
        }
        if !data.is_empty() {
            st.out.borrow_mut().extend_from_slice(data);
        }
        if end {
            st.end_queued.set(true);
        }
        st.kick.notify_waiters();
        Ok(ctx.done.clone())
    }
}

unsafe fn datagram_send(ctx: &AppCtx, msg: *mut PyObject, conn: Option<&Conn>) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let data = match py::dict_get(msg, i.data)? {
            Some(b) if PyBytes_Check(b.ptr()) != 0 => std::slice::from_raw_parts(
                PyBytes_AsString(b.ptr()) as *const u8,
                PyBytes_Size(b.ptr()) as usize,
            )
            .to_vec(),
            Some(b) if !py::is_none(b.ptr()) => py::with_bytes(b.ptr(), |x| x.to_vec())?,
            _ => {
                return Err(py::raise(
                    PyExc_ValueError,
                    "'webtransport.datagram.send' needs 'data'",
                ));
            }
        };
        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let Some(wt) = conn.st.borrow().wt.clone() else {
            return Err(py::runtime_error("not a webtransport session"));
        };
        if !wt.accepted.get() {
            return Err(py::runtime_error(
                "'webtransport.datagram.send' before 'webtransport.accept'",
            ));
        }
        wt.push_out_datagram(Bytes::from(data));
        wt.kick.notify_waiters();
        if let Some(sh) = crate::core::current() {
            conn.notify(sh);
        }
        Ok(ctx.done.clone())
    }
}

unsafe fn stream_open(
    sh: &Shared,
    ctx: &AppCtx,
    msg: *mut PyObject,
    conn: Option<&Conn>,
) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let bidi = match py::dict_get(msg, i.bidirectional)? {
            Some(v) if !py::is_none(v.ptr()) => py::truthy(v.ptr())?,
            _ => true,
        };
        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let Some(wt) = conn.st.borrow().wt.clone() else {
            return Err(py::runtime_error("not a webtransport session"));
        };
        if !wt.accepted.get() {
            return Err(py::runtime_error(
                "'webtransport.stream.open' before 'webtransport.accept'",
            ));
        }
        // The connection task opens the QUIC stream; we record the request.
        wt.pending_open.borrow_mut().push_back(bidi);
        wt.kick.notify_waiters();
        conn.notify(sh);
        Ok(ctx.done.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_stream_id_is_the_session_id() {
        assert_eq!(session_id_from_connect(0), 0);
        assert_eq!(session_id_from_connect(4), 4);
        assert_eq!(session_id_from_connect(16), 16);
    }

    #[test]
    fn unknown_session_id_does_not_shift_into_another() {
        assert_ne!(session_id_from_connect(16), session_id_from_connect(4));
        assert_eq!(session_id_from_quarter(4), 16);
        assert_ne!(session_id_from_quarter(4), session_id_from_connect(4));
    }

    #[test]
    fn datagram_quarter_round_trips_to_connect_stream() {
        assert_eq!(session_id_from_quarter(0), 0);
        assert_eq!(session_id_from_quarter(1), 4);
        assert_eq!(session_quarter(4), 1);
    }
}
