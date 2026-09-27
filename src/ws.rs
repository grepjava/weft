//! WebSockets (RFC 6455, with RFC 7692 compression) as the ASGI `websocket`
//! scope.
//!
//! A WebSocket starts as an HTTP/1.1 request head, recognised in
//! `asgi::dispatch`. From the 101 on the connection carries frames, and its
//! task runs `run` here in place of the HTTP pump.
//!
//! Frames are decoded as they arrive, not when the application next calls
//! `receive()`: a ping has to be answered, and the pong for the server's own
//! keepalive ping seen, whether or not anyone is listening. Complete data
//! messages therefore queue, bounded; when the queue is full reading stops,
//! which turns a slow application into TCP backpressure rather than memory.

use std::collections::VecDeque;
use std::future::poll_fn;
use std::io;
use std::pin::{Pin, pin};
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine;
use bytes::{Buf, Bytes, BytesMut};
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
use pyo3_ffi::*;
use sha1::{Digest, Sha1};
use tokio::time::{Instant, Sleep};

use crate::asgi::{self, eq_ci, tokens, trim};
use crate::core::{AppCtx, Shared};
use crate::http::{BodyKind, CORK_LIMIT, Conn, HeadInfo, Phase, State, WRITE_LWM, flush, reserve};
use crate::interned::{empty_bytes, s};
use crate::py::{self, PResult, PyRef};
use crate::types::{self, ChanObj, types};

pub struct WsCfg {
    pub enabled: bool,
    pub max_message: usize,
    /// `None`: no keepalive pings.
    pub ping_interval: Option<Duration>,
    pub ping_timeout: Duration,
    pub max_queue: usize,
    pub max_queue_bytes: usize,
    pub compress: bool,
}

const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const OP_CONT: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;

const NORMAL: u16 = 1000;
const GOING_AWAY: u16 = 1001;
const PROTOCOL_ERROR: u16 = 1002;
const NO_STATUS: u16 = 1005;
const ABNORMAL: u16 = 1006;
const INVALID_PAYLOAD: u16 = 1007;
const TOO_BIG: u16 = 1009;
const INTERNAL_ERROR: u16 = 1011;

/// Messages shorter than this go out uncompressed: the deflate framing would
/// cost more than it saves.
const COMPRESS_MIN: usize = 64;
/// The window the server compresses with, and asks the client to use: 4 KiB
/// holds a run of similar small messages, at a fraction of the memory of the
/// full 32 KiB.
const WINDOW_BITS: u8 = 12;
const DEFLATE_LEVEL: u32 = 6;
/// How long the server waits for the peer to answer its close frame.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// Bookkeeping charged to each queued message on top of its payload.
const MSG_OVERHEAD: usize = 64;

/// The permessage-deflate parameters agreed in the handshake.
pub struct Deflate {
    server_no_context: bool,
    client_no_context: bool,
    server_bits: u8,
    /// `Some` when the client offered `client_max_window_bits`.
    client_bits: Option<u8>,
}

impl Deflate {
    fn response(&self, out: &mut BytesMut) {
        out.extend_from_slice(
            b"sec-websocket-extensions: permessage-deflate; server_max_window_bits=",
        );
        crate::http::push_int(out, self.server_bits as u64);
        if let Some(b) = self.client_bits {
            out.extend_from_slice(b"; client_max_window_bits=");
            crate::http::push_int(out, b as u64);
        }
        if self.server_no_context {
            out.extend_from_slice(b"; server_no_context_takeover");
        }
        if self.client_no_context {
            out.extend_from_slice(b"; client_no_context_takeover");
        }
        out.extend_from_slice(b"\r\n");
    }
}

/// The first permessage-deflate offer this server can accept.
fn negotiate(headers: &[httparse::Header<'_>]) -> Option<Deflate> {
    for h in headers {
        if !eq_ci(h.name.as_bytes(), b"sec-websocket-extensions") {
            continue;
        }
        'offer: for ext in tokens(h.value) {
            let mut parts = ext.split(|&c| c == b';').map(trim);
            if !parts
                .next()
                .is_some_and(|n| eq_ci(n, b"permessage-deflate"))
            {
                continue;
            }
            let (mut snc, mut cnc) = (false, false);
            let mut sbits: Option<u8> = None;
            let mut cbits: Option<Option<u8>> = None;
            for p in parts.filter(|p| !p.is_empty()) {
                let (k, v) = match p.iter().position(|&c| c == b'=') {
                    Some(e) => (trim(&p[..e]), Some(trim_quotes(trim(&p[e + 1..])))),
                    None => (p, None),
                };
                let bits = |v: Option<&[u8]>| -> Option<u8> {
                    let v = std::str::from_utf8(v?).ok()?;
                    if v.len() > 2 || !v.bytes().all(|c| c.is_ascii_digit()) {
                        return None;
                    }
                    v.parse::<u8>().ok().filter(|b| (8..=15).contains(b))
                };
                match k {
                    _ if eq_ci(k, b"server_no_context_takeover") && v.is_none() && !snc => {
                        snc = true
                    }
                    _ if eq_ci(k, b"client_no_context_takeover") && v.is_none() && !cnc => {
                        cnc = true
                    }
                    _ if eq_ci(k, b"server_max_window_bits") && sbits.is_none() => match bits(v) {
                        Some(b) => sbits = Some(b),
                        None => continue 'offer,
                    },
                    _ if eq_ci(k, b"client_max_window_bits") && cbits.is_none() => match v {
                        None => cbits = Some(None),
                        Some(_) => match bits(v) {
                            Some(b) => cbits = Some(Some(b)),
                            None => continue 'offer,
                        },
                    },
                    _ => continue 'offer,
                }
            }
            // zlib cannot produce a raw deflate stream with a 256-byte window.
            if sbits == Some(8) {
                continue;
            }
            return Some(Deflate {
                server_no_context: snc,
                client_no_context: cnc,
                server_bits: sbits.map_or(WINDOW_BITS, |b| b.min(WINDOW_BITS)),
                client_bits: cbits.map(|b| b.map_or(WINDOW_BITS, |b| b.min(WINDOW_BITS))),
            });
        }
    }
    None
}

fn trim_quotes(v: &[u8]) -> &[u8] {
    match v {
        [b'"', inner @ .., b'"'] => inner,
        _ => v,
    }
}

fn accept_key(key: &[u8]) -> [u8; 28] {
    let mut h = Sha1::new();
    h.update(key);
    h.update(GUID);
    let digest = h.finalize();
    let mut out = [0u8; 28];
    let _ = base64::engine::general_purpose::STANDARD.encode_slice(digest.as_slice(), &mut out);
    out
}

/// A complete data message, payload already validated.
pub struct Msg {
    text: bool,
    data: Bytes,
}

/// How the connection ended, as the application is told.
pub struct End {
    code: u16,
    reason: Bytes,
}

pub enum Deliver {
    Msg(Msg),
    Disconnect(End),
}

pub struct Ws {
    accept: [u8; 28],
    deflate: Option<Deflate>,
    // zlib contexts, made on first use: a connection that never sends a
    // compressed message never pays for one.
    deflater: Option<Box<Compress>>,
    inflater: Option<Box<Decompress>>,
    /// The handshake has been answered with 101.
    accepted: bool,
    /// The handshake was answered with an HTTP response instead.
    rejected: bool,
    close_sent: bool,
    close_sent_at: Option<Instant>,
    close_received: bool,
    /// The peer broke the protocol: nothing more is read from it.
    failed: bool,
    /// The write side has been shut down after the closing handshake.
    shut: bool,
    /// 0 until known.
    close_code: u16,
    close_reason: Bytes,
    connect_delivered: bool,
    disconnect_delivered: bool,
    task_done: bool,
    // The message being assembled from fragments.
    assembling: bool,
    msg_text: bool,
    msg_compressed: bool,
    msg: BytesMut,
    queue: VecDeque<Msg>,
    queued_bytes: usize,
    ping_sent: Option<Instant>,
    last_activity: Instant,
}

impl Ws {
    fn new(accept: [u8; 28], deflate: Option<Deflate>) -> Ws {
        Ws {
            accept,
            deflate,
            deflater: None,
            inflater: None,
            accepted: false,
            rejected: false,
            close_sent: false,
            close_sent_at: None,
            close_received: false,
            failed: false,
            shut: false,
            close_code: 0,
            close_reason: Bytes::new(),
            connect_delivered: false,
            disconnect_delivered: false,
            task_done: false,
            assembling: false,
            msg_text: false,
            msg_compressed: false,
            msg: BytesMut::new(),
            queue: VecDeque::new(),
            queued_bytes: 0,
            ping_sent: None,
            last_activity: Instant::now(),
        }
    }

    /// Nothing more will arrive: once the queue is empty, `receive()` says
    /// how it ended.
    fn ended(&self, disconnected: bool) -> bool {
        self.rejected || self.close_received || self.failed || disconnected
    }

    pub fn take_disconnect(&mut self) -> End {
        self.disconnect_delivered = true;
        End {
            code: if self.close_code == 0 {
                ABNORMAL
            } else {
                self.close_code
            },
            reason: self.close_reason.clone(),
        }
    }

    fn queue_full(&self, cfg: &WsCfg) -> bool {
        // One message always fits, so a single large one cannot deadlock
        // against its own budget.
        self.queue.len() >= cfg.max_queue
            || (!self.queue.is_empty() && self.queued_bytes >= cfg.max_queue_bytes)
    }

    fn pop(&mut self) -> Option<Msg> {
        let m = self.queue.pop_front()?;
        self.queued_bytes = self
            .queued_bytes
            .saturating_sub(m.data.len() + MSG_OVERHEAD);
        Some(m)
    }

    fn send_close(&mut self, out: &mut BytesMut, code: u16, reason: &[u8]) {
        if self.close_sent {
            return;
        }
        let mut p = [0u8; 125];
        p[..2].copy_from_slice(&code.to_be_bytes());
        let r = reason.len().min(123);
        p[2..2 + r].copy_from_slice(&reason[..r]);
        write_frame(out, OP_CLOSE, false, &p[..2 + r]);
        self.close_sent = true;
        self.close_sent_at = Some(Instant::now());
    }

    /// Fails the connection: the peer broke the protocol.
    fn fail(&mut self, out: &mut BytesMut, rbuf: &mut BytesMut, code: u16) {
        self.close_code = code;
        if self.accepted {
            self.send_close(out, code, b"");
        }
        self.failed = true;
        self.assembling = false;
        self.msg = BytesMut::new();
        rbuf.clear();
    }

    /// Decodes every complete frame in `rbuf`: control frames are answered
    /// here, data messages queued.
    fn decode(&mut self, rbuf: &mut BytesMut, out: &mut BytesMut, cfg: &WsCfg) {
        while !self.close_received && !self.failed && !self.queue_full(cfg) {
            let h = match parse_header(rbuf, cfg.max_message, self.deflate.is_some()) {
                Ok(Some(h)) => h,
                Ok(None) => return,
                Err(code) => return self.fail(out, rbuf, code),
            };
            if rbuf.len() < h.total {
                rbuf.reserve(h.total - rbuf.len());
                return;
            }
            let mut frame = rbuf.split_to(h.total);
            frame.advance(h.header_len);
            unmask(&mut frame, h.mask);
            if h.opcode >= OP_CLOSE {
                if let Err(code) = self.control(h.opcode, &frame, out) {
                    return self.fail(out, rbuf, code);
                }
            } else if let Err(code) = self.data(&h, frame, cfg) {
                return self.fail(out, rbuf, code);
            }
        }
    }

    fn control(&mut self, opcode: u8, payload: &[u8], out: &mut BytesMut) -> Result<(), u16> {
        match opcode {
            OP_PING => {
                if !self.close_sent {
                    write_frame(out, OP_PONG, false, payload);
                }
            }
            OP_PONG => self.ping_sent = None,
            _ => {
                let code = match payload.len() {
                    0 => NO_STATUS,
                    1 => return Err(PROTOCOL_ERROR),
                    _ => {
                        let code = u16::from_be_bytes([payload[0], payload[1]]);
                        if !valid_close_code(code) {
                            return Err(PROTOCOL_ERROR);
                        }
                        if simdutf8::basic::from_utf8(&payload[2..]).is_err() {
                            return Err(INVALID_PAYLOAD);
                        }
                        self.close_reason = Bytes::copy_from_slice(&payload[2..]);
                        code
                    }
                };
                self.close_received = true;
                self.close_code = code;
                // Echo it; the disconnect reaches the application once it
                // has taken whatever was queued ahead of it.
                self.send_close(out, if code == NO_STATUS { NORMAL } else { code }, b"");
            }
        }
        Ok(())
    }

    fn data(&mut self, h: &Header, frame: BytesMut, cfg: &WsCfg) -> Result<(), u16> {
        if h.opcode == OP_CONT {
            if !self.assembling {
                return Err(PROTOCOL_ERROR);
            }
        } else {
            if self.assembling {
                return Err(PROTOCOL_ERROR);
            }
            self.msg_text = h.opcode == OP_TEXT;
            self.msg_compressed = h.rsv1;
        }
        let payload = if h.fin && !self.assembling {
            frame
        } else {
            // For a compressed message this bounds the compressed bytes;
            // inflating is bounded again, against the same limit.
            if self.msg.len() + frame.len() > cfg.max_message {
                return Err(TOO_BIG);
            }
            if self.msg.is_empty() {
                self.msg = frame;
            } else {
                self.msg.extend_from_slice(&frame);
            }
            if !h.fin {
                self.assembling = true;
                return Ok(());
            }
            self.assembling = false;
            std::mem::take(&mut self.msg)
        };
        // After our close frame the application will not read any more.
        if self.close_sent {
            return Ok(());
        }
        let data: Bytes = if self.msg_compressed {
            self.inflate(payload, cfg.max_message)?.into()
        } else {
            payload.freeze()
        };
        if self.msg_text && simdutf8::basic::from_utf8(&data).is_err() {
            return Err(INVALID_PAYLOAD);
        }
        self.queued_bytes += data.len() + MSG_OVERHEAD;
        self.queue.push_back(Msg {
            text: self.msg_text,
            data,
        });
        Ok(())
    }

    /// Inflates one message. 1009 when it would inflate past the message
    /// limit (a few kilobytes of zeros can claim gigabytes), 1007 when it is
    /// not deflate data at all.
    fn inflate(&mut self, mut input: BytesMut, max: usize) -> Result<Vec<u8>, u16> {
        let client_no_context = self.deflate.as_ref().is_some_and(|d| d.client_no_context);
        let z = self
            .inflater
            .get_or_insert_with(|| Box::new(Decompress::new_with_window_bits(false, 15)));
        // The sender strips the empty block that ends a sync flush.
        input.extend_from_slice(&[0, 0, 0xff, 0xff]);
        let mut out: Vec<u8> = Vec::with_capacity((input.len() * 3).max(256).min(max + 1));
        let mut rest = &input[..];
        loop {
            if out.len() == out.capacity() {
                if out.len() > max {
                    return Err(TOO_BIG);
                }
                let room = (max + 1 - out.len()).min(out.len().max(4096));
                out.reserve_exact(room);
            }
            let before = z.total_in();
            let status = z
                .decompress_vec(rest, &mut out, FlushDecompress::Sync)
                .map_err(|_| INVALID_PAYLOAD)?;
            let consumed = (z.total_in() - before) as usize;
            rest = &rest[consumed..];
            if out.len() > max {
                return Err(TOO_BIG);
            }
            if status == Status::StreamEnd {
                // The client ended its stream: the next message starts a
                // fresh one.
                z.reset(false);
                break;
            }
            // Output space left over means zlib took all it could.
            if out.len() < out.capacity() {
                if rest.is_empty() {
                    break;
                }
                if consumed == 0 {
                    return Err(INVALID_PAYLOAD);
                }
            }
        }
        if client_no_context {
            z.reset(false);
        }
        Ok(out)
    }

    /// Frames one outgoing data message, compressed when that was agreed
    /// and worthwhile. `false` when compression failed, which leaves the
    /// shared context unusable.
    fn send_message(&mut self, out: &mut BytesMut, text: bool, data: &[u8]) -> bool {
        let opcode = if text { OP_TEXT } else { OP_BINARY };
        let Some(d) = self.deflate.as_ref().filter(|_| data.len() >= COMPRESS_MIN) else {
            write_frame(out, opcode, false, data);
            return true;
        };
        let (bits, reset) = (d.server_bits, d.server_no_context);
        let z = self.deflater.get_or_insert_with(|| {
            Box::new(Compress::new_with_window_bits(
                Compression::new(DEFLATE_LEVEL),
                false,
                bits,
            ))
        });
        let mut buf: Vec<u8> = Vec::with_capacity(data.len() / 2 + 64);
        let mut rest = data;
        loop {
            if buf.capacity() - buf.len() < 64 {
                buf.reserve(buf.capacity().max(1024));
            }
            let before = z.total_in();
            if z.compress_vec(rest, &mut buf, FlushCompress::Sync).is_err() {
                return false;
            }
            rest = &rest[(z.total_in() - before) as usize..];
            // A sync flush is complete once it leaves output space unused.
            if rest.is_empty() && buf.len() < buf.capacity() {
                break;
            }
        }
        if !buf.ends_with(&[0, 0, 0xff, 0xff]) {
            return false;
        }
        buf.truncate(buf.len() - 4);
        if reset {
            z.reset();
        }
        write_frame(out, opcode, true, &buf);
        true
    }
}

fn valid_close_code(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
}

struct Header {
    fin: bool,
    rsv1: bool,
    opcode: u8,
    mask: [u8; 4],
    header_len: usize,
    total: usize,
}

/// Reads one client frame header. `Err` carries the close code.
fn parse_header(b: &[u8], max: usize, deflate: bool) -> Result<Option<Header>, u16> {
    if b.len() < 2 {
        return Ok(None);
    }
    let (b0, b1) = (b[0], b[1]);
    let fin = b0 & 0x80 != 0;
    let rsv1 = b0 & 0x40 != 0;
    let opcode = b0 & 0x0f;
    if b0 & 0x30 != 0
        || !matches!(
            opcode,
            OP_CONT | OP_TEXT | OP_BINARY | OP_CLOSE | OP_PING | OP_PONG
        )
    {
        return Err(PROTOCOL_ERROR);
    }
    let control = opcode >= OP_CLOSE;
    // RSV1 marks a compressed message, set on its first frame only.
    if rsv1 && (!deflate || control || opcode == OP_CONT) {
        return Err(PROTOCOL_ERROR);
    }
    // Every client frame is masked (5.1): an unmasked one is a broken
    // client, or an attempt to get an intermediary to read the payload.
    if b1 & 0x80 == 0 {
        return Err(PROTOCOL_ERROR);
    }
    let len7 = (b1 & 0x7f) as usize;
    if control && (!fin || len7 > 125) {
        return Err(PROTOCOL_ERROR);
    }
    let (len, off) = match len7 {
        126 => {
            if b.len() < 4 {
                return Ok(None);
            }
            (u16::from_be_bytes([b[2], b[3]]) as u64, 4)
        }
        127 => {
            if b.len() < 10 {
                return Ok(None);
            }
            let v = u64::from_be_bytes(b[2..10].try_into().unwrap_or_default());
            if v >> 63 != 0 {
                return Err(PROTOCOL_ERROR);
            }
            (v, 10)
        }
        n => (n as u64, 2),
    };
    if len > max as u64 {
        return Err(TOO_BIG);
    }
    if b.len() < off + 4 {
        return Ok(None);
    }
    Ok(Some(Header {
        fin,
        rsv1,
        opcode,
        mask: [b[off], b[off + 1], b[off + 2], b[off + 3]],
        header_len: off + 4,
        total: off + 4 + len as usize,
    }))
}

/// XORs the payload with the mask, eight bytes at a time.
fn unmask(buf: &mut [u8], mask: [u8; 4]) {
    let (head, mid, tail) = unsafe { buf.align_to_mut::<u64>() };
    let mut i = 0usize;
    for b in head.iter_mut() {
        *b ^= mask[i & 3];
        i += 1;
    }
    let r = |k: usize| mask[(i + k) & 3];
    let m = u64::from_ne_bytes([r(0), r(1), r(2), r(3), r(0), r(1), r(2), r(3)]);
    for w in mid.iter_mut() {
        *w ^= m;
    }
    for b in tail.iter_mut() {
        *b ^= mask[i & 3];
        i += 1;
    }
}

/// One unmasked, unfragmented server frame.
fn write_frame(out: &mut BytesMut, opcode: u8, rsv1: bool, payload: &[u8]) {
    let mut h = [0u8; 10];
    h[0] = 0x80 | if rsv1 { 0x40 } else { 0 } | opcode;
    let n = payload.len();
    let hl = if n < 126 {
        h[1] = n as u8;
        2
    } else if n <= 0xffff {
        h[1] = 126;
        h[2..4].copy_from_slice(&(n as u16).to_be_bytes());
        4
    } else {
        h[1] = 127;
        h[2..10].copy_from_slice(&(n as u64).to_be_bytes());
        10
    };
    out.reserve(hl + n);
    out.extend_from_slice(&h[..hl]);
    out.extend_from_slice(payload);
}

// --- dispatch ------------------------------------------------------------------

/// Starts the application on a WebSocket handshake.
pub unsafe fn start(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Conn,
    req: &httparse::Request<'_, '_>,
    key: &[u8],
    meta: &crate::ops::Meta,
) -> PResult<()> {
    let deflate = if ctx.ws.compress {
        negotiate(&req.headers[..])
    } else {
        None
    };
    let seq = {
        let mut st = conn.st.borrow_mut();
        // A WebSocket is never keep-alive: the connection becomes one or ends.
        st.begin(true, false, false, false, BodyKind::Empty);
        st.ws = Some(Box::new(Ws::new(accept_key(key), deflate)));
        st.seq
    };
    let i = s();
    unsafe {
        let scope = PyRef::own(PyDict_Copy(ctx.ws_scope_proto.ptr()))?;
        let d = scope.ptr();
        py::dict_set(d, i.http_version, PyRef::borrow(i.v1_1))?;
        asgi::fill_request(sh, ctx, conn, d, req, meta)?;
        if let Some(secure) = meta.secure {
            py::dict_set(
                d,
                i.scheme,
                PyRef::borrow(if secure { i.wss } else { i.ws }),
            )?;
        }
        py::dict_set(d, i.subprotocols, subprotocols(req)?)?;
        asgi::launch(sh, ctx, conn, d, receive_vc, send_vc, seq)
    }
}

unsafe fn subprotocols(req: &httparse::Request<'_, '_>) -> PResult<PyRef> {
    unsafe {
        let list = PyRef::own(PyList_New(0))?;
        for h in req.headers.iter() {
            if eq_ci(h.name.as_bytes(), b"sec-websocket-protocol") {
                for t in tokens(h.value) {
                    let v = py::str_utf8_replace(t)?;
                    if PyList_Append(list.ptr(), v.ptr()) < 0 {
                        return Err(py::PyErr);
                    }
                }
            }
        }
        Ok(list)
    }
}

/// The application task ended: whatever it sent or did not, the connection
/// has to end properly.
pub fn task_finished(st: &mut State, failed: bool, stopping: bool) {
    let Some(ws) = st.ws.as_deref_mut() else {
        return;
    };
    ws.task_done = true;
    if ws.accepted {
        if !ws.close_sent && !st.disconnected {
            let code = if stopping {
                GOING_AWAY
            } else if failed {
                INTERNAL_ERROR
            } else {
                NORMAL
            };
            ws.send_close(&mut st.out, code, b"");
        }
        return;
    }
    if ws.rejected {
        return;
    }
    // Returning without accepting is a rejection.
    ws.rejected = true;
    match st.resp.phase {
        Phase::Idle => st.write_error(if failed { 500 } else { 403 }),
        Phase::Head | Phase::Body => {
            st.resp.close = true;
            st.resp.phase = Phase::Done;
        }
        Phase::Done => {}
    }
}

// --- the connection task -------------------------------------------------------

pub async fn run(sh: &Shared, ctx: &AppCtx, conn: &Conn, rbuf: &mut BytesMut) {
    let mut timer = pin!(tokio::time::sleep(Duration::from_secs(86_400)));
    let mut armed: Option<Instant> = None;
    let mut eof = false;
    poll_fn(|cx| {
        pump(
            sh,
            ctx,
            conn,
            rbuf,
            &mut eof,
            timer.as_mut(),
            &mut armed,
            cx,
        )
    })
    .await
}

#[allow(clippy::too_many_arguments)]
fn pump(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Conn,
    rbuf: &mut BytesMut,
    eof: &mut bool,
    mut timer: Pin<&mut Sleep>,
    armed: &mut Option<Instant>,
    cx: &mut Context<'_>,
) -> Poll<()> {
    let cfg = &ctx.ws;
    loop {
        let mut guard = conn.st.borrow_mut();
        let st: &mut State = &mut guard;
        let Some(ws) = st.ws.as_deref_mut() else {
            return Poll::Ready(());
        };

        if sh.stopping.get() && ws.accepted && !st.disconnected {
            ws.send_close(&mut st.out, GOING_AWAY, b"");
        }
        if ws.accepted && !st.disconnected && !rbuf.is_empty() {
            ws.decode(rbuf, &mut st.out, cfg);
        }

        while !st.out.is_empty() && !st.disconnected {
            match conn.io().poll_write_ready(cx) {
                Poll::Ready(Ok(())) => {
                    if flush(&conn.io(), &mut st.out).is_err() {
                        st.disconnected = true;
                        st.out.clear();
                    }
                }
                Poll::Ready(Err(_)) => {
                    st.disconnected = true;
                    st.out.clear();
                }
                Poll::Pending => break,
            }
        }
        // The closing handshake is complete: the TCP connection closes from
        // this side first, as 7.1.1 asks.
        if ws.close_sent
            && (ws.close_received || ws.failed)
            && st.out.is_empty()
            && !ws.shut
            && !st.disconnected
        {
            ws.shut = true;
            conn.io().shutdown_write();
        }

        let want_read = ws.accepted
            && !ws.close_received
            && !ws.failed
            && !st.disconnected
            && !*eof
            && !ws.queue_full(cfg)
            && rbuf.len() < cfg.max_message + 14;
        if want_read {
            let mut got = false;
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
            if got {
                ws.last_activity = Instant::now();
                continue;
            }
        }
        if *eof && !st.disconnected {
            st.disconnected = true;
        }

        // Keepalive, and a peer that never answers our close.
        let mut deadline = None;
        if ws.accepted && !st.disconnected && !ws.failed {
            let now = Instant::now();
            if ws.close_sent {
                if !ws.close_received
                    && let Some(at) = ws.close_sent_at.map(|t| t + CLOSE_TIMEOUT)
                {
                    if now >= at {
                        st.disconnected = true;
                        continue;
                    }
                    deadline = Some(at);
                }
            } else if let Some(interval) = cfg.ping_interval {
                if let Some(sent) = ws.ping_sent {
                    let at = sent + cfg.ping_timeout;
                    if now >= at {
                        // No pong: the peer is gone even if the socket has
                        // not noticed, which is what the ping is for.
                        st.disconnected = true;
                        continue;
                    }
                    deadline = Some(at);
                } else {
                    let at = ws.last_activity + interval;
                    if now >= at {
                        write_frame(&mut st.out, OP_PING, false, b"");
                        ws.ping_sent = Some(now);
                        continue;
                    }
                    deadline = Some(at);
                }
            }
        }

        // Hand the application what it is waiting for, with no borrow held.
        let mut wake: Option<(PyRef, Deliver)> = None;
        if st.recv_waiter.is_some() {
            if let Some(m) = ws.pop() {
                wake = st.recv_waiter.take().map(|w| (w, Deliver::Msg(m)));
            } else if ws.ended(st.disconnected) {
                let end = ws.take_disconnect();
                wake = st.recv_waiter.take().map(|w| (w, Deliver::Disconnect(end)));
            }
        }
        let drained =
            if !st.drain_waiters.is_empty() && (st.out.len() <= WRITE_LWM || st.disconnected) {
                std::mem::take(&mut st.drain_waiters)
            } else {
                Vec::new()
            };
        if wake.is_some() || !drained.is_empty() {
            drop(guard);
            unsafe {
                if let Some((fut, d)) = wake {
                    resolve(fut, d);
                }
                for fut in drained {
                    asgi::resolve_none(fut);
                }
            }
            sh.py_scheduled();
            continue;
        }

        let flushed = st.out.is_empty() || st.disconnected;
        if ws.rejected {
            if st.resp.phase == Phase::Done && flushed {
                return Poll::Ready(());
            }
        } else if ws.accepted && flushed {
            let over = ws.task_done || ws.disconnect_delivered;
            let closed = st.disconnected || ws.shut;
            if (over && closed) || (ws.task_done && ws.close_sent) {
                return Poll::Ready(());
            }
        }

        if deadline != *armed {
            if let Some(at) = deadline {
                timer.as_mut().reset(at);
            }
            *armed = deadline;
        }
        if armed.is_some() && timer.as_mut().poll(cx).is_ready() {
            *armed = None;
            drop(guard);
            continue;
        }
        drop(guard);
        conn.set_waker(cx);
        return Poll::Pending;
    }
}

// --- messages ------------------------------------------------------------------

unsafe fn connect_msg() -> PResult<PyRef> {
    let i = s();
    unsafe {
        let d = PyRef::own(PyDict_New())?;
        py::dict_set(d.ptr(), i.type_, PyRef::borrow(i.websocket_connect))?;
        Ok(d)
    }
}

/// `{"type": "websocket.receive", "bytes": ..., "text": ...}`, the unused key
/// present and None: the specification says so, and some frameworks index
/// rather than `.get`.
unsafe fn receive_msg(m: &Msg) -> PResult<PyRef> {
    let i = s();
    unsafe {
        let d = PyRef::own(PyDict_New())?;
        py::dict_set(d.ptr(), i.type_, PyRef::borrow(i.websocket_receive))?;
        if m.text {
            let t = PyRef::own(PyUnicode_DecodeUTF8(
                m.data.as_ptr().cast(),
                m.data.len() as Py_ssize_t,
                std::ptr::null(),
            ))?;
            py::dict_set(d.ptr(), i.text, t)?;
            py::dict_set(d.ptr(), i.bytes_, py::none())?;
        } else {
            let b = if m.data.is_empty() {
                PyRef::borrow(empty_bytes())
            } else {
                py::bytes(&m.data)?
            };
            py::dict_set(d.ptr(), i.bytes_, b)?;
            py::dict_set(d.ptr(), i.text, py::none())?;
        }
        Ok(d)
    }
}

unsafe fn disconnect_msg(end: &End) -> PResult<PyRef> {
    let i = s();
    unsafe {
        let d = PyRef::own(PyDict_New())?;
        py::dict_set(d.ptr(), i.type_, PyRef::borrow(i.websocket_disconnect))?;
        py::dict_set(d.ptr(), i.code, py::int(end.code as i64)?)?;
        py::dict_set(d.ptr(), i.reason, py::str_utf8_replace(&end.reason)?)?;
        Ok(d)
    }
}

pub unsafe fn resolve(fut: PyRef, d: Deliver) {
    unsafe {
        let m = match &d {
            Deliver::Msg(m) => receive_msg(m),
            Deliver::Disconnect(e) => disconnect_msg(e),
        };
        match m {
            Ok(m) => asgi::set_result(&fut, m.ptr()),
            Err(_) => PyErr_WriteUnraisable(fut.ptr()),
        }
    }
}

fn gone() -> End {
    End {
        code: ABNORMAL,
        reason: Bytes::new(),
    }
}

unsafe fn client_disconnected() -> py::PyErr {
    unsafe {
        py::raise(
            types().client_disconnected.0,
            "the WebSocket connection is closed",
        )
    }
}

// --- the callables -------------------------------------------------------------

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

unsafe fn receive(c: &ChanObj) -> PResult<PyRef> {
    unsafe {
        let (sh, ctx) = asgi::worker()?;
        let Some(conn) = sh.conn(c.slot, c.generation) else {
            return types::ready(Some(disconnect_msg(&gone())?));
        };
        let mut guard = conn.st.borrow_mut();
        let st: &mut State = &mut guard;
        if st.seq != c.seq {
            drop(guard);
            return types::ready(Some(disconnect_msg(&gone())?));
        }
        if st.recv_waiter.is_some() {
            drop(guard);
            return Err(py::runtime_error("receive() is already being awaited"));
        }
        let Some(ws) = st.ws.as_deref_mut() else {
            drop(guard);
            return types::ready(Some(disconnect_msg(&gone())?));
        };
        if !ws.connect_delivered {
            ws.connect_delivered = true;
            drop(guard);
            return types::ready(Some(connect_msg()?));
        }
        if let Some(m) = ws.pop() {
            drop(guard);
            // Room in the queue: reading may resume.
            conn.notify(sh);
            return types::ready(Some(receive_msg(&m)?));
        }
        if ws.ended(st.disconnected) {
            let end = ws.take_disconnect();
            drop(guard);
            conn.notify(sh);
            return types::ready(Some(disconnect_msg(&end)?));
        }
        drop(guard);
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
            b"websocket.send" => send_data(sh, &ctx, msg, conn),
            b"websocket.accept" => accept(sh, &ctx, msg, conn),
            b"websocket.close" => close(sh, &ctx, msg, conn),
            b"websocket.http.response.start" => {
                if let Some(conn) = conn {
                    let st = conn.st.borrow();
                    if st.ws.as_ref().is_some_and(|w| w.accepted || w.rejected) {
                        drop(st);
                        return Err(py::runtime_error(
                            "'websocket.http.response.start' after the handshake was answered",
                        ));
                    }
                }
                asgi::http_start(&ctx, c, msg, conn)
            }
            b"websocket.http.response.body" => {
                let r = asgi::http_body(sh, &ctx, c, msg, conn)?;
                if let Some(conn) = conn {
                    let mut st = conn.st.borrow_mut();
                    if st.resp.phase == Phase::Done
                        && let Some(ws) = st.ws.as_deref_mut()
                    {
                        ws.rejected = true;
                    }
                }
                Ok(r)
            }
            other => Err(py::runtime_error(&format!(
                "Unexpected ASGI message type '{}'",
                String::from_utf8_lossy(other)
            ))),
        }
    }
}

fn handshake_header(n: &[u8]) -> bool {
    [
        &b"connection"[..],
        b"upgrade",
        b"content-length",
        b"transfer-encoding",
        b"sec-websocket-accept",
        b"sec-websocket-extensions",
    ]
    .iter()
    .any(|h| eq_ci(n, h))
}

fn handshake_header_or_protocol(n: &[u8]) -> bool {
    handshake_header(n) || eq_ci(n, b"sec-websocket-protocol")
}

unsafe fn accept(
    sh: &Shared,
    ctx: &AppCtx,
    msg: *mut PyObject,
    conn: Option<&Conn>,
) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let sub = match py::dict_get(msg, i.subprotocol)? {
            Some(v) if !py::is_none(v.ptr()) => {
                let b = py::str_view(v.ptr())?;
                if !asgi::valid_name(b) {
                    return Err(py::raise(PyExc_ValueError, "invalid WebSocket subprotocol"));
                }
                Some(b)
            }
            _ => None,
        };
        // The server owns the handshake headers, the extensions it agreed to
        // among them; anything else, a cookie say, the application may add.
        let mut extra = Vec::new();
        let mut info = HeadInfo::default();
        if let Some(h) = py::dict_get(msg, i.headers)?
            && !py::is_none(h.ptr())
        {
            let skip: fn(&[u8]) -> bool = if sub.is_some() {
                handshake_header_or_protocol
            } else {
                handshake_header
            };
            asgi::encode_headers(h.ptr(), &mut extra, &mut info, Some(skip))?;
        }
        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let date = sh.date();
        let mut guard = conn.st.borrow_mut();
        let st: &mut State = &mut guard;
        let Some(ws) = st.ws.as_deref_mut() else {
            drop(guard);
            return Err(py::runtime_error("not a WebSocket connection"));
        };
        if ws.accepted {
            drop(guard);
            return Err(py::runtime_error("'websocket.accept' sent twice"));
        }
        if ws.rejected || st.resp.phase != Phase::Idle {
            drop(guard);
            return Err(py::runtime_error(
                "'websocket.accept' after the handshake was refused",
            ));
        }
        if st.disconnected {
            return Ok(ctx.done.clone());
        }
        let out = &mut st.out;
        out.extend_from_slice(
            b"HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: ",
        );
        out.extend_from_slice(&ws.accept);
        out.extend_from_slice(b"\r\n");
        if let Some(sub) = sub {
            out.extend_from_slice(b"sec-websocket-protocol: ");
            out.extend_from_slice(sub);
            out.extend_from_slice(b"\r\n");
        }
        if let Some(d) = &ws.deflate {
            d.response(out);
        }
        out.extend_from_slice(&extra);
        if ctx.date_header && !info.has_date {
            out.extend_from_slice(b"date: ");
            out.extend_from_slice(&date);
            out.extend_from_slice(b"\r\n");
        }
        if ctx.server_header && !info.has_server {
            out.extend_from_slice(b"server: weft\r\n");
        }
        out.extend_from_slice(b"\r\n");
        ws.accepted = true;
        ws.last_activity = Instant::now();
        if flush(&conn.io(), &mut st.out).is_err() {
            st.disconnected = true;
            st.out.clear();
        }
        drop(guard);
        // Frames may be waiting behind the handshake already.
        conn.notify(sh);
        Ok(ctx.done.clone())
    }
}

unsafe fn send_data(
    sh: &Shared,
    ctx: &AppCtx,
    msg: *mut PyObject,
    conn: Option<&Conn>,
) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let (text, payload) = match py::dict_get(msg, i.text)? {
            Some(t) if !py::is_none(t.ptr()) => {
                if PyUnicode_Check(t.ptr()) == 0 {
                    return Err(py::type_error("'websocket.send' text must be str"));
                }
                (true, PyRef::borrow(t.ptr()))
            }
            _ => match py::dict_get(msg, i.bytes_)? {
                Some(b) if PyBytes_Check(b.ptr()) != 0 => (false, PyRef::borrow(b.ptr())),
                Some(b) if !py::is_none(b.ptr()) => {
                    (false, PyRef::own(PyBytes_FromObject(b.ptr()))?)
                }
                _ => {
                    return Err(py::raise(
                        PyExc_ValueError,
                        "'websocket.send' needs 'bytes' or 'text'",
                    ));
                }
            },
        };
        let data: &[u8] = if text {
            py::str_view(payload.ptr())?
        } else {
            std::slice::from_raw_parts(
                PyBytes_AsString(payload.ptr()) as *const u8,
                PyBytes_Size(payload.ptr()) as usize,
            )
        };
        let Some(conn) = conn else {
            return Err(client_disconnected());
        };
        let mut guard = conn.st.borrow_mut();
        let st: &mut State = &mut guard;
        let Some(ws) = st.ws.as_deref_mut() else {
            drop(guard);
            return Err(py::runtime_error("not a WebSocket connection"));
        };
        if !ws.accepted && !ws.rejected {
            drop(guard);
            return Err(py::runtime_error(
                "'websocket.send' before 'websocket.accept'",
            ));
        }
        if ws.close_sent || ws.ended(st.disconnected) {
            drop(guard);
            return Err(client_disconnected());
        }
        if !ws.send_message(&mut st.out, text, data) {
            // The compression context is in an unknown state, and the
            // client's copy with it: nothing more can be sent it would read.
            ws.close_code = INTERNAL_ERROR;
            ws.failed = true;
            ws.send_close(&mut st.out, INTERNAL_ERROR, b"");
        }
        if st.out.len() >= CORK_LIMIT && flush(&conn.io(), &mut st.out).is_err() {
            st.disconnected = true;
            st.out.clear();
        }
        let queued = st.out.len();
        drop(guard);
        if queued > 0 {
            conn.notify(sh);
        }
        asgi::backpressure(sh, ctx, conn, queued)
    }
}

unsafe fn close(
    sh: &Shared,
    ctx: &AppCtx,
    msg: *mut PyObject,
    conn: Option<&Conn>,
) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let code = match py::dict_get(msg, i.code)? {
            Some(v) if !py::is_none(v.ptr()) => py::i64_arg(v.ptr())?,
            _ => NORMAL as i64,
        };
        let code = u16::try_from(code)
            .ok()
            .filter(|&c| valid_close_code(c))
            .unwrap_or(NORMAL);
        let reason = match py::dict_get(msg, i.reason)? {
            Some(v) if !py::is_none(v.ptr()) => {
                let r = py::str_view(v.ptr())?;
                // At most 123 bytes, cut on a character boundary.
                let mut n = r.len().min(123);
                while n > 0 && n < r.len() && (r[n] & 0xc0) == 0x80 {
                    n -= 1;
                }
                &r[..n]
            }
            _ => &b""[..],
        };
        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let mut guard = conn.st.borrow_mut();
        let st: &mut State = &mut guard;
        let Some(ws) = st.ws.as_deref_mut() else {
            drop(guard);
            return Err(py::runtime_error("not a WebSocket connection"));
        };
        if !ws.accepted {
            // Closing before accepting refuses the handshake: ASGI has the
            // client see an HTTP 403, not a WebSocket close.
            if !ws.rejected && st.resp.phase == Phase::Idle {
                ws.rejected = true;
                st.write_error(403);
            }
        } else if !st.disconnected {
            ws.send_close(&mut st.out, code, reason);
        }
        if !st.disconnected && flush(&conn.io(), &mut st.out).is_err() {
            st.disconnected = true;
            st.out.clear();
        }
        drop(guard);
        conn.notify(sh);
        Ok(ctx.done.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_key_rfc_example() {
        assert_eq!(
            &accept_key(b"dGhlIHNhbXBsZSBub25jZQ=="),
            b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn unmask_matches_bytewise() {
        let mask = [0x12, 0x34, 0x56, 0x78];
        for len in 0..40 {
            for off in 0..8 {
                let mut buf: Vec<u8> = (0..len + off).map(|i| i as u8).collect();
                let want: Vec<u8> = buf[off..]
                    .iter()
                    .enumerate()
                    .map(|(i, b)| b ^ mask[i & 3])
                    .collect();
                unmask(&mut buf[off..], mask);
                assert_eq!(&buf[off..], &want[..]);
            }
        }
    }

    #[test]
    fn header_rules() {
        // masked text "Hi"
        let f = [0x81, 0x82, 1, 2, 3, 4, b'H' ^ 1, b'i' ^ 2];
        let h = parse_header(&f, 1 << 20, false).unwrap().unwrap();
        assert!(h.fin && h.opcode == OP_TEXT && h.total == 8);
        // unmasked
        assert_eq!(
            parse_header(&[0x81, 0x02, 0, 0], 1 << 20, false).err(),
            Some(PROTOCOL_ERROR)
        );
        // RSV1 without deflate
        assert_eq!(
            parse_header(&[0xC1, 0x80, 0, 0, 0, 0], 1 << 20, false).err(),
            Some(PROTOCOL_ERROR)
        );
        // fragmented ping
        assert_eq!(
            parse_header(&[0x09, 0x80, 0, 0, 0, 0], 1 << 20, false).err(),
            Some(PROTOCOL_ERROR)
        );
        // too big
        assert_eq!(
            parse_header(&[0x82, 0xFE, 0x01, 0x00], 100, false).err(),
            Some(TOO_BIG)
        );
        // reserved opcode
        assert_eq!(
            parse_header(&[0x83, 0x80], 100, false).err(),
            Some(PROTOCOL_ERROR)
        );
    }

    #[test]
    fn negotiation() {
        let hs = |v: &'static str| {
            vec![httparse::Header {
                name: "Sec-WebSocket-Extensions",
                value: v.as_bytes(),
            }]
        };
        let d = negotiate(&hs("permessage-deflate; client_max_window_bits")).unwrap();
        assert_eq!((d.server_bits, d.client_bits), (12, Some(12)));
        let d = negotiate(&hs(
            "permessage-deflate; server_max_window_bits=10; server_no_context_takeover",
        ))
        .unwrap();
        assert!(d.server_no_context && d.server_bits == 10 && d.client_bits.is_none());
        assert!(negotiate(&hs("permessage-deflate; server_max_window_bits=8")).is_none());
        assert!(negotiate(&hs("permessage-deflate; bogus")).is_none());
        let d = negotiate(&hs("permessage-deflate; bogus, permessage-deflate")).unwrap();
        assert_eq!(d.server_bits, 12);
    }

    #[test]
    fn deflate_round_trip() {
        let agreed = || Deflate {
            server_no_context: false,
            client_no_context: false,
            server_bits: 12,
            client_bits: None,
        };
        let mut tx = Ws::new([0; 28], Some(agreed()));
        let mut rx = Ws::new([0; 28], Some(agreed()));
        for round in 0..3 {
            let msg = format!(
                "{{\"round\": {round}, \"payload\": \"{}\"}}",
                "abc".repeat(50)
            );
            let mut out = BytesMut::new();
            assert!(tx.send_message(&mut out, true, msg.as_bytes()));
            assert_eq!(out[0], 0x80 | 0x40 | OP_TEXT);
            let (n, off) = match out[1] {
                126 => (u16::from_be_bytes([out[2], out[3]]) as usize, 4),
                n => (n as usize, 2),
            };
            let inflated = rx
                .inflate(BytesMut::from(&out[off..off + n]), 1 << 20)
                .unwrap();
            assert_eq!(inflated, msg.as_bytes());
        }
    }

    #[test]
    fn inflate_bomb_is_bounded() {
        let mut z = Compress::new_with_window_bits(Compression::best(), false, 15);
        let zeros = vec![0u8; 1 << 20];
        let mut buf = Vec::with_capacity(1 << 16);
        z.compress_vec(&zeros, &mut buf, FlushCompress::Sync)
            .unwrap();
        buf.truncate(buf.len() - 4);
        let agreed = Deflate {
            server_no_context: false,
            client_no_context: false,
            server_bits: 12,
            client_bits: None,
        };
        let mut rx = Ws::new([0; 28], Some(agreed));
        assert_eq!(
            rx.inflate(BytesMut::from(&buf[..]), 64 * 1024).err(),
            Some(TOO_BIG)
        );
    }
}
