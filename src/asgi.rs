//! ASGI 3 over HTTP/1.1: the scope, `send`, `receive`, and the request task.

use std::ptr;
use std::rc::Rc;

use bytes::{Buf, BytesMut};
use pyo3_ffi::*;

use crate::core::{AppCtx, Shared, current};
use crate::http::{
    BodyKind, CORK_LIMIT, ChunkDecoder, Conn, HeadInfo, Phase, WRITE_HWM, push_int, reason,
};
use crate::interned::{empty_bytes, s};
use crate::ops::Meta;
use crate::py::{self, PResult, PyErr, PyRef};
use crate::types::{self, ChanObj, types};

const MAX_HEADERS: usize = 100;

pub enum Parsed {
    Dispatched,
    /// A WebSocket handshake was dispatched; the connection is no longer HTTP.
    Upgraded,
    /// A WSGI request with its environ; the body has not been read yet.
    Wsgi(PyRef),
    /// The server answered the request itself; `true` if the connection
    /// stays open.
    Answered(bool),
    Partial,
    Reject(u16),
}

#[inline]
pub fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_ascii_lowercase() == *y)
}

pub fn trim(v: &[u8]) -> &[u8] {
    let start = v
        .iter()
        .position(|c| !c.is_ascii_whitespace())
        .unwrap_or(v.len());
    let end = v
        .iter()
        .rposition(|c| !c.is_ascii_whitespace())
        .map_or(start, |i| i + 1);
    &v[start..end]
}

pub fn tokens(v: &[u8]) -> impl Iterator<Item = &[u8]> {
    v.split(|&c| c == b',').map(trim).filter(|t| !t.is_empty())
}

/// A `Transfer-Encoding` coding list, as it is read.
#[derive(Default)]
struct Codings {
    /// `chunked` has been seen.
    chunked: bool,
    /// Something came after `chunked`, or `chunked` came twice.
    misplaced: bool,
    /// A coding other than `chunked`.
    other: bool,
    any: bool,
}

impl Codings {
    fn push(&mut self, t: &[u8]) {
        self.any = true;
        if self.chunked {
            self.misplaced = true;
        }
        // Only a bare `chunked`: neither `xchunked` nor `chunked;x=1` is it.
        if eq_ci(t, b"chunked") {
            self.chunked = true;
        } else {
            self.other = true;
        }
    }

    fn framable(&self) -> bool {
        self.any && self.chunked && !self.misplaced
    }
}

fn parse_len(v: &[u8]) -> Option<u64> {
    let v = trim(v);
    if v.is_empty() || v.len() > 19 || !v.iter().all(u8::is_ascii_digit) {
        return None;
    }
    v.iter().try_fold(0u64, |acc, &d| {
        acc.checked_mul(10)?.checked_add((d - b'0') as u64)
    })
}

/// Parses one request head from `rbuf` and starts the application on it.
pub fn dispatch(sh: &Shared, ctx: &Rc<AppCtx>, conn: &Rc<Conn>, rbuf: &mut BytesMut) -> Parsed {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut req = httparse::Request::new(&mut headers);
    let n = match req.parse(rbuf) {
        Ok(httparse::Status::Complete(n)) if n > ctx.max_head => return Parsed::Reject(431),
        Ok(httparse::Status::Complete(n)) => n,
        Ok(httparse::Status::Partial) => return Parsed::Partial,
        Err(httparse::Error::TooManyHeaders) => return Parsed::Reject(431),
        Err(_) => return Parsed::Reject(400),
    };

    let mut length: Option<u64> = None;
    let mut te = false;
    let mut te_codings = Codings::default();
    let mut hosts = 0u32;
    let mut close = false;
    let mut keep = false;
    let mut expect = false;
    let mut upgrade = false;
    let mut to_websocket = false;
    let mut ws_key: Option<&[u8]> = None;
    let mut ws_version = false;
    for h in req.headers.iter() {
        let name = h.name.as_bytes();
        match name.len() {
            14 if eq_ci(name, b"content-length") => match parse_len(h.value) {
                Some(v) if length.is_none_or(|l| l == v) => length = Some(v),
                _ => return Parsed::Reject(400),
            },
            17 if eq_ci(name, b"transfer-encoding") => {
                te = true;
                // A second field continues the same list.
                for t in tokens(h.value) {
                    te_codings.push(t);
                }
            }
            4 if eq_ci(name, b"host") => hosts += 1,
            10 if eq_ci(name, b"connection") => {
                for t in tokens(h.value) {
                    close |= eq_ci(t, b"close");
                    keep |= eq_ci(t, b"keep-alive");
                    upgrade |= eq_ci(t, b"upgrade");
                }
            }
            6 if eq_ci(name, b"expect") => expect = eq_ci(trim(h.value), b"100-continue"),
            7 if eq_ci(name, b"upgrade") => {
                to_websocket = tokens(h.value).any(|t| eq_ci(t, b"websocket"))
            }
            17 if eq_ci(name, b"sec-websocket-key") => ws_key = Some(trim(h.value)),
            21 if eq_ci(name, b"sec-websocket-version") => ws_version = trim(h.value) == b"13",
            _ => {}
        }
    }
    let http11 = req.version == Some(1);
    // Either framing may be trusted, but not both at once, and only a list
    // that ends in one `chunked` frames a body at all: this is where request
    // smuggling starts (RFC 9112 6.1, 6.3).
    let chunked = te;
    if te {
        if length.is_some() || !te_codings.framable() {
            return Parsed::Reject(400);
        }
        if te_codings.other {
            return Parsed::Reject(501);
        }
    }
    // HTTP/1.1 names exactly one host (RFC 9112 3.2).
    if (http11 && hosts == 0) || hosts > 1 {
        return Parsed::Reject(400);
    }
    let method_owned = req.method.unwrap_or("GET").to_ascii_uppercase();
    let method = method_owned.as_str();
    if let Some(path) = &ctx.health
        && matches!(method, "GET" | "HEAD")
        && !te
        && length.unwrap_or(0) == 0
        && req
            .path
            .is_some_and(|p| p.split('?').next().unwrap_or(p).as_bytes() == path.as_slice())
    {
        rbuf.advance(n);
        let draining = sh.draining.get();
        let keep = !draining && if http11 { !close } else { false };
        let mut st = conn.st.borrow_mut();
        st.out.extend_from_slice(match (draining, keep) {
            (true, _) => {
                b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\nconnection: close\r\n"
            }
            (false, true) => b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n",
            (false, false) => b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n",
        });
        if let Some(hsts) = &ctx.hsts {
            st.out.extend_from_slice(b"strict-transport-security: ");
            st.out.extend_from_slice(hsts);
            st.out.extend_from_slice(b"\r\n");
        }
        st.out.extend_from_slice(b"\r\n");
        st.started = tokio::time::Instant::now();
        st.log_response(if draining { 503 } else { 200 });
        return Parsed::Answered(keep);
    }
    if length.is_some_and(|l| l > ctx.max_body) {
        return Parsed::Reject(413);
    }
    let meta = request_meta(ctx, conn, &req);
    // After the probe, which must never be refused, and before anything that
    // costs work. A unix peer is not counted unless a trusted proxy named it.
    if let Some(rate) = ctx.rate {
        let key = meta
            .client
            .as_deref()
            .map(|c| crate::limit::name_key(c.as_bytes()))
            .or_else(|| (!conn.unix).then(|| crate::limit::ip_key(conn.peer.ip())));
        if let Some(wait) = crate::limit::wait(rate, key)
            && wait > 0
        {
            let seconds = wait.div_ceil(1_000_000).max(1);
            let keep = !te && length.unwrap_or(0) == 0 && if http11 { !close } else { keep };
            let rid = if ctx.request_id {
                match &meta.inject_id {
                    Some(id) => id.clone(),
                    None => req
                        .headers
                        .iter()
                        .find(|h| eq_ci(h.name.as_bytes(), b"x-request-id"))
                        .map_or(Vec::new(), |h| trim(h.value).to_vec()),
                }
            } else {
                Vec::new()
            };
            let log = crate::ops::log_enabled().then(|| log_req(ctx, &req, method, http11));
            let head_only = method == "HEAD";
            rbuf.advance(n);
            let mut st = conn.st.borrow_mut();
            st.request_id = rid;
            st.log = log;
            st.write_limited(seconds, keep, head_only);
            return Parsed::Answered(keep);
        }
    }

    let accepted = if ctx.compress || ctx.compress_static || ctx.cache.is_some() {
        crate::compress::negotiate(req.headers)
    } else {
        crate::compress::Coding::Identity
    };
    let target = req.path.unwrap_or("/").as_bytes();
    if !ctx.static_dirs.is_empty()
        && matches!(method, "GET" | "HEAD")
        && !te
        && length.unwrap_or(0) == 0
        && let Some(opened) =
            crate::staticf::open(&ctx.static_dirs, target, req.headers, ctx.compress_static)
    {
        let (status, _) = opened.precondition(req.headers);
        let keep = if http11 { !close } else { keep };
        let rid = if ctx.request_id {
            match &meta.inject_id {
                Some(id) => id.clone(),
                None => req
                    .headers
                    .iter()
                    .find(|h| eq_ci(h.name.as_bytes(), b"x-request-id"))
                    .map_or(Vec::new(), |h| trim(h.value).to_vec()),
            }
        } else {
            Vec::new()
        };
        let log = crate::ops::log_enabled().then(|| log_req(ctx, &req, method, http11));
        let head_only = method == "HEAD";
        rbuf.advance(n);
        let mut st = conn.st.borrow_mut();
        st.begin(http11, head_only, keep, false, BodyKind::Empty);
        st.request_id = rid;
        st.log = log;
        st.resp.status = status;
        if crate::staticf::write(
            &mut st,
            opened,
            head_only,
            keep,
            &sh.date(),
            ctx.server_header,
            ctx.hsts.as_deref(),
            sh.closing(),
        ) {
            st.log_response(status);
        }
        return Parsed::Answered(keep && !st.resp.close);
    }
    let cache_prep = ctx.cache.map(|_| {
        let host = req
            .headers
            .iter()
            .find(|h| eq_ci(h.name.as_bytes(), b"host"))
            .map(|h| trim(h.value))
            .unwrap_or(b"");
        let trusted = ctx
            .trusted
            .as_ref()
            .filter(|t| t.allows(conn.unix, conn.peer.ip()))
            .is_some();
        let forwarded = if trusted {
            let xfp = req
                .headers
                .iter()
                .find(|h| eq_ci(h.name.as_bytes(), b"x-forwarded-proto"))
                .map(|h| trim(h.value))
                .unwrap_or(b"");
            let xfh = req
                .headers
                .iter()
                .find(|h| eq_ci(h.name.as_bytes(), b"x-forwarded-host"))
                .map(|h| trim(h.value))
                .unwrap_or(b"");
            let fwd = req
                .headers
                .iter()
                .find(|h| eq_ci(h.name.as_bytes(), b"forwarded"))
                .map(|h| trim(h.value))
                .unwrap_or(b"");
            Some((xfp, xfh, fwd))
        } else {
            None
        };
        let https = meta.secure.unwrap_or(ctx.https);
        let key = crate::cache::key(https, host, target, forwarded);
        let ae = crate::compress::raw_accept(req.headers);
        let variant = if ae.is_empty() {
            0
        } else {
            crate::cache::variant_hash(&ae)
        };
        (
            key,
            crate::cache::target_hash(target),
            variant,
            crate::cache::request_ok(method, req.headers),
        )
    });
    // A hit answers from the head alone, so a request that frames a body is
    // left to the application: its body must not be read as the next request.
    if let Some((Some(key), _, variant, true)) = cache_prep.as_ref()
        && !chunked
        && length.unwrap_or(0) == 0
        && let Some(mut hit) =
            crate::cache::lookup(key, 0, None).or_else(|| crate::cache::lookup(key, *variant, None))
    {
        let etag = crate::cache::stored_etag(&hit.head);
        if !etag.is_empty() && crate::cache::inm_matches(req.headers, &etag) {
            hit.status = 304;
        }
        let keep_conn = if http11 { !close } else { keep };
        let rid = if ctx.request_id {
            match &meta.inject_id {
                Some(id) => id.clone(),
                None => req
                    .headers
                    .iter()
                    .find(|h| eq_ci(h.name.as_bytes(), b"x-request-id"))
                    .map_or(Vec::new(), |h| trim(h.value).to_vec()),
            }
        } else {
            Vec::new()
        };
        let log = crate::ops::log_enabled().then(|| log_req(ctx, &req, method, http11));
        let head_only = method == "HEAD" || hit.status == 304;
        rbuf.advance(n);
        let mut st = conn.st.borrow_mut();
        st.begin(http11, head_only, keep_conn, false, BodyKind::Empty);
        st.request_id = rid;
        st.log = log;
        st.accepted = accepted;
        crate::metrics::cache_hit();
        crate::cache::write_hit(&mut st, ctx, &hit, head_only, &sh.date(), sh.closing());
        return Parsed::Answered(keep_conn && !st.resp.close);
    }
    if let Some((Some(_), _, _, true)) = cache_prep.as_ref() {
        crate::metrics::cache_miss();
    }

    // PEP 3333 has no way to hand an application a stream that outlives its
    // response, so an upgrade is refused rather than half served.
    if ctx.wsgi.is_some() && upgrade && to_websocket {
        return Parsed::Reject(501);
    }
    if upgrade
        && to_websocket
        && http11
        && method == "GET"
        && ws_version
        && let Some(key) = ws_key.filter(|k| !k.is_empty())
    {
        if !ctx.ws.enabled {
            return Parsed::Reject(501);
        }
        let started = unsafe { crate::ws::start(sh, ctx, conn, &req, key, &meta) };
        rbuf.advance(n);
        if started.is_err() {
            unsafe { py::report_exception(ctx.report.as_ref().map(PyRef::ptr)) };
            return Parsed::Reject(500);
        }
        return Parsed::Upgraded;
    }
    let kind = if chunked {
        BodyKind::Chunked(ChunkDecoder::new(ctx.max_body, ctx.max_head))
    } else {
        match length {
            Some(n) if n > 0 => BodyKind::Length(n),
            _ => BodyKind::Empty,
        }
    };
    let seq = {
        let mut st = conn.st.borrow_mut();
        // An HTTP/1.0 message with a transfer coding may have been framed
        // differently by whoever forwarded it: it is the last on this
        // connection (RFC 9112 6.1).
        let keep_alive = if http11 { !close } else { keep && !te };
        st.begin(http11, method == "HEAD", keep_alive, expect, kind);
        st.accepted = accepted;
        st.mutating = crate::cache::mutating(method);
        if let Some((key, target, variant, ok)) = cache_prep {
            st.cache_key = key;
            st.cache_target = target;
            st.cache_variant = variant;
            st.cache_ok = ok && matches!(method, "GET");
        }
        if ctx.request_id {
            st.request_id = match &meta.inject_id {
                Some(id) => id.clone(),
                None => req
                    .headers
                    .iter()
                    .find(|h| eq_ci(h.name.as_bytes(), b"x-request-id"))
                    .map_or(Vec::new(), |h| trim(h.value).to_vec()),
            };
        }
        if crate::ops::log_enabled() {
            st.log = Some(log_req(ctx, &req, method, http11));
        }
        st.seq
    };

    if let Some(w) = &ctx.wsgi {
        let environ = unsafe { crate::wsgi::environ(w, conn, &req, http11, &meta) };
        rbuf.advance(n);
        return match environ {
            Ok(e) => Parsed::Wsgi(e),
            Err(_) => {
                unsafe { py::report_exception(ctx.report.as_ref().map(PyRef::ptr)) };
                Parsed::Reject(500)
            }
        };
    }
    let started = unsafe { start(sh, ctx, conn, &req, http11, seq, &meta) };
    rbuf.advance(n);
    if started.is_err() {
        unsafe { py::report_exception(ctx.report.as_ref().map(PyRef::ptr)) };
        conn.st.borrow_mut().write_error(500);
    }
    Parsed::Dispatched
}

/// The headers and scope values the server adds to this request.
fn request_meta(ctx: &AppCtx, conn: &Conn, req: &httparse::Request<'_, '_>) -> Meta {
    let mut meta = Meta::default();
    let trusted = ctx
        .trusted
        .as_ref()
        .filter(|t| t.allows(conn.unix, conn.peer.ip()));
    if let Some(t) = trusted {
        let via = crate::ops::forwarded(t, req.headers);
        meta.client = via.client;
        meta.secure = via.secure;
    }
    if ctx.request_id {
        let mut ids = req
            .headers
            .iter()
            .filter(|h| eq_ci(h.name.as_bytes(), b"x-request-id"));
        // A proxy that saw the request first may have logged its ID already;
        // anyone else could have sent any value at all.
        let kept = match (trusted, ids.next(), ids.next()) {
            (Some(_), Some(h), None) => crate::ops::valid_request_id(trim(h.value)),
            _ => false,
        };
        if !kept {
            meta.inject_id = Some(crate::ops::new_request_id());
        }
    }
    if ctx.request_start
        && !req
            .headers
            .iter()
            .any(|h| eq_ci(h.name.as_bytes(), b"x-request-start"))
    {
        let micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64);
        let mut v = b"t=".to_vec();
        push_int(&mut v, micros);
        meta.request_start = Some(v);
    }
    meta
}

fn log_req(
    ctx: &AppCtx,
    req: &httparse::Request<'_, '_>,
    method: &str,
    http11: bool,
) -> Box<crate::ops::LogReq> {
    let target = req.path.unwrap_or("/").as_bytes();
    let mut line = Vec::with_capacity(method.len() + 1 + target.len());
    line.extend_from_slice(method.as_bytes());
    line.push(b' ');
    line.extend_from_slice(target);
    let trace = if ctx.trace_context {
        let mut tps = req
            .headers
            .iter()
            .filter(|h| eq_ci(h.name.as_bytes(), b"traceparent"));
        match (tps.next(), tps.next()) {
            (Some(h), None) => crate::ops::traceparent(h.value),
            _ => None,
        }
    } else {
        None
    };
    Box::new(crate::ops::LogReq {
        started: std::time::Instant::now(),
        line,
        method_len: method.len(),
        http11,
        trace,
    })
}

pub unsafe fn method_str(m: &str) -> PResult<PyRef> {
    let i = s();
    let known = match m {
        "GET" => i.m_get,
        "POST" => i.m_post,
        "HEAD" => i.m_head,
        "PUT" => i.m_put,
        "DELETE" => i.m_delete,
        "PATCH" => i.m_patch,
        "OPTIONS" => i.m_options,
        _ => return unsafe { py::str_ascii(m.as_bytes()) },
    };
    Ok(unsafe { PyRef::borrow(known) })
}

pub fn unhex(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|v| v as u8)
}

unsafe fn decode_path(raw: &[u8]) -> PResult<PyRef> {
    if raw.is_ascii() && !raw.contains(&b'%') {
        return unsafe { py::str_ascii(raw) };
    }
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%'
            && i + 2 < raw.len()
            && let (Some(h), Some(l)) = (unhex(raw[i + 1]), unhex(raw[i + 2]))
        {
            out.push(h << 4 | l);
            i += 3;
            continue;
        }
        out.push(raw[i]);
        i += 1;
    }
    unsafe { py::str_utf8_replace(&out) }
}

impl Conn {
    unsafe fn client_tuple(&self) -> PResult<PyRef> {
        if let Some(c) = self.client.borrow().as_ref() {
            return Ok(c.clone());
        }
        let t = if self.unix {
            unsafe { py::tuple2(py::str_ascii(b"unix")?, py::int(0)?)? }
        } else {
            let ip = self.peer.ip().to_canonical().to_string();
            unsafe {
                py::tuple2(
                    py::str_ascii(ip.as_bytes())?,
                    py::int(self.peer.port() as i64)?,
                )?
            }
        };
        *self.client.borrow_mut() = Some(t.clone());
        Ok(t)
    }
}

/// Builds the scope, calls the application and wraps the coroutine in a task.
unsafe fn start(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Conn,
    req: &httparse::Request<'_, '_>,
    http11: bool,
    seq: u32,
    meta: &Meta,
) -> PResult<()> {
    let i = s();
    unsafe {
        let scope = PyRef::own(PyDict_Copy(ctx.scope_proto.ptr()))?;
        let d = scope.ptr();
        let ver = if conn.st.borrow().h3 {
            i.v3
        } else if conn.st.borrow().h2 {
            i.v2
        } else if http11 {
            i.v1_1
        } else {
            i.v1_0
        };
        py::dict_set(d, i.http_version, PyRef::borrow(ver))?;
        if conn.st.borrow().h3
            && let Some(ext) = &ctx.h3_extensions
        {
            py::dict_set(d, i.extensions, ext.clone())?;
        }
        py::dict_set(d, i.method, method_str(req.method.unwrap_or("GET"))?)?;
        fill_request(sh, ctx, conn, d, req, meta)?;
        if let Some(secure) = meta.secure {
            py::dict_set(
                d,
                i.scheme,
                if secure {
                    py::str_ascii(b"https")?
                } else {
                    PyRef::borrow(i.http)
                },
            )?;
        }
        launch(sh, ctx, conn, d, receive_vc, send_vc, seq)
    }
}

/// Whether a request header reaches the application. A name with `_` becomes
/// indistinguishable from its `-` twin once a framework maps it to a CGI-style
/// variable, and `Proxy` becomes `HTTP_PROXY` (httpoxy): both are dropped.
#[inline]
pub fn scope_header(name: &[u8]) -> bool {
    !name.contains(&b'_') && !(name.len() == 5 && eq_ci(name, b"proxy"))
}

/// The scope entries taken from the request head: path, query, headers,
/// client, and the lifespan state.
pub unsafe fn fill_request(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Conn,
    d: *mut PyObject,
    req: &httparse::Request<'_, '_>,
    meta: &Meta,
) -> PResult<()> {
    let i = s();
    unsafe {
        let target = req.path.unwrap_or("/").as_bytes();
        let (mut raw_path, query) = match target.iter().position(|&c| c == b'?') {
            Some(q) => (&target[..q], &target[q + 1..]),
            None => (target, &b""[..]),
        };
        // absolute-form: http://host/path
        if raw_path.first() != Some(&b'/')
            && let Some(p) = raw_path.windows(3).position(|w| w == b"://")
        {
            let rest = &raw_path[p + 3..];
            raw_path = rest
                .iter()
                .position(|&c| c == b'/')
                .map_or(&b"/"[..], |s| &rest[s..]);
        }
        py::dict_set(d, i.path, decode_path(raw_path)?)?;
        py::dict_set(d, i.raw_path, py::bytes(raw_path)?)?;
        py::dict_set(
            d,
            i.query_string,
            if query.is_empty() {
                PyRef::borrow(empty_bytes())
            } else {
                py::bytes(query)?
            },
        )?;

        let shown = |h: &&httparse::Header<'_>| {
            scope_header(h.name.as_bytes()) && !meta.hides(h.name.as_bytes())
        };
        let kept = req.headers.iter().filter(shown).count() + meta.added().count();
        let list = PyRef::own(PyList_New(kept as Py_ssize_t))?;
        let mut n = 0;
        for h in req.headers.iter().filter(shown) {
            let pair = py::tuple2(sh.header_name(h.name.as_bytes())?, py::bytes(h.value)?)?;
            PyList_SET_ITEM(list.ptr(), n, pair.into_ptr());
            n += 1;
        }
        for (name, value) in meta.added() {
            let pair = py::tuple2(py::bytes(name)?, py::bytes(value)?)?;
            PyList_SET_ITEM(list.ptr(), n, pair.into_ptr());
            n += 1;
        }
        py::dict_set(d, i.headers, list)?;
        let client = match &meta.client {
            Some(c) => py::tuple2(py::str_utf8_replace(c.as_bytes())?, py::int(0)?)?,
            None => conn.client_tuple()?,
        };
        py::dict_set(d, i.client, client)?;
        if let Some(state) = &ctx.state {
            py::dict_set(d, i.state, PyRef::own(PyDict_Copy(state.ptr()))?)?;
        }
        Ok(())
    }
}

/// Calls the application with `scope` and wraps the coroutine in a task.
pub unsafe fn launch(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Conn,
    d: *mut PyObject,
    receive_vc: vectorcallfunc,
    send_vc: vectorcallfunc,
    seq: u32,
) -> PResult<()> {
    let i = s();
    unsafe {
        let t = types();
        let receive = types::chan(&t.receive, receive_vc, conn.slot, conn.generation, seq)?;
        let send = types::chan(&t.send, send_vc, conn.slot, conn.generation, seq)?;
        let coro = py::call(ctx.app.ptr(), &[d, receive.ptr(), send.ptr()])?;
        let task = py::call_kw(
            ctx.task_type.ptr(),
            &[coro.ptr(), ctx.loop_.ptr()],
            1,
            ctx.kw_loop.ptr(),
        )?;
        let done = types::chan(&t.done, done_vc, conn.slot, conn.generation, seq)?;
        py::call_method(i.add_done_callback, &[task.ptr(), done.ptr()])?;
    }
    sh.py_scheduled();
    Ok(())
}

// --- messages ------------------------------------------------------------------

unsafe fn request_msg(data: &[u8], more: bool) -> PResult<PyRef> {
    let i = s();
    unsafe {
        let d = PyRef::own(PyDict_New())?;
        py::dict_set(d.ptr(), i.type_, PyRef::borrow(i.http_request))?;
        let body = if data.is_empty() {
            PyRef::borrow(empty_bytes())
        } else {
            py::bytes(data)?
        };
        py::dict_set(d.ptr(), i.body, body)?;
        py::dict_set(
            d.ptr(),
            i.more_body,
            PyRef::borrow(if more { Py_True() } else { Py_False() }),
        )?;
        Ok(d)
    }
}

unsafe fn disconnect_msg() -> PResult<PyRef> {
    let i = s();
    unsafe {
        let d = PyRef::own(PyDict_New())?;
        py::dict_set(d.ptr(), i.type_, PyRef::borrow(i.http_disconnect))?;
        Ok(d)
    }
}

pub unsafe fn new_future(ctx: &AppCtx) -> PResult<PyRef> {
    unsafe {
        py::call_kw(
            ctx.future_type.ptr(),
            &[ctx.loop_.ptr()],
            0,
            ctx.kw_loop.ptr(),
        )
    }
}

/// Resolves a future unless the application has cancelled it meanwhile.
pub unsafe fn set_result(fut: &PyRef, value: *mut PyObject) {
    unsafe {
        if py::call_method(s().set_result, &[fut.ptr(), value]).is_err() {
            PyErr_Clear();
        }
        // Completing a future from a Tokio task queues asyncio callbacks.
        // Select must return so `_run_once` can run them; otherwise the app
        // stays parked in the selector and never sees the event.
        if let Some(sh) = crate::core::current() {
            sh.py_scheduled();
        }
    }
}

/// Completes a pending `receive()` with body bytes, or with a disconnect.
pub unsafe fn resolve_receive(fut: PyRef, msg: Option<(BytesMut, bool)>) {
    unsafe {
        let m = match msg {
            Some((data, more)) => request_msg(&data, more),
            None => disconnect_msg(),
        };
        match m {
            Ok(m) => set_result(&fut, m.ptr()),
            Err(_) => PyErr_WriteUnraisable(fut.ptr()),
        }
    }
}

pub unsafe fn resolve_none(fut: PyRef) {
    unsafe { set_result(&fut, Py_None()) }
}

// --- the callables -------------------------------------------------------------

pub unsafe fn one_arg(
    name: &str,
    args: *const *mut PyObject,
    nargsf: usize,
    kwnames: *mut PyObject,
) -> PResult<*mut PyObject> {
    unsafe {
        let n = PyVectorcall_NARGS(nargsf);
        let kw = if kwnames.is_null() {
            0
        } else {
            PyTuple_GET_SIZE(kwnames)
        };
        if n != 1 || kw != 0 {
            return Err(py::type_error(&format!(
                "{name}() takes exactly one argument"
            )));
        }
        Ok(*args)
    }
}

unsafe extern "C" fn send_vc(
    callable: *mut PyObject,
    args: *const *mut PyObject,
    nargsf: usize,
    kwnames: *mut PyObject,
) -> *mut PyObject {
    unsafe {
        let r = one_arg("send", args, nargsf, kwnames)
            .and_then(|msg| send(&*(callable as *const ChanObj), msg));
        r.map_or(ptr::null_mut(), PyRef::into_ptr)
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
            return ptr::null_mut();
        }
        receive(&*(callable as *const ChanObj)).map_or(ptr::null_mut(), PyRef::into_ptr)
    }
}

unsafe extern "C" fn done_vc(
    callable: *mut PyObject,
    args: *const *mut PyObject,
    nargsf: usize,
    kwnames: *mut PyObject,
) -> *mut PyObject {
    unsafe {
        let r = one_arg("done", args, nargsf, kwnames)
            .and_then(|task| task_done(&*(callable as *const ChanObj), task));
        r.map_or(ptr::null_mut(), PyRef::into_ptr)
    }
}

pub unsafe fn worker<'a>() -> PResult<(&'a Shared, Rc<AppCtx>)> {
    let sh = current().ok_or_else(|| unsafe {
        py::runtime_error("ASGI callable used outside the worker thread that owns it")
    })?;
    let ctx = sh
        .app()
        .ok_or_else(|| unsafe { py::runtime_error("the worker is closed") })?;
    Ok((sh, ctx))
}

pub fn valid_name(n: &[u8]) -> bool {
    !n.is_empty()
        && n.iter()
            .all(|&c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&c))
}

pub fn valid_value(v: &[u8]) -> bool {
    !v.iter().any(|&c| c == b'\r' || c == b'\n' || c == 0)
}

/// Appends `name: value\r\n` lines for an ASGI header iterable, noting the
/// ones that decide framing. Runs Python (the iterable may be a generator),
/// so it is called before any connection state is borrowed. Names `skip`
/// accepts are validated and left out.
pub unsafe fn encode_headers(
    headers: *mut PyObject,
    out: &mut Vec<u8>,
    info: &mut HeadInfo,
    skip: Option<fn(&[u8]) -> bool>,
) -> PResult<()> {
    unsafe {
        let it = PyRef::own(PyObject_GetIter(headers))?;
        loop {
            let item = PyIter_Next(it.ptr());
            if item.is_null() {
                return if PyErr_Occurred().is_null() {
                    Ok(())
                } else {
                    Err(PyErr)
                };
            }
            let item = PyRef::own(item)?;
            let (name, value) =
                if PyTuple_Check(item.ptr()) != 0 && PyTuple_GET_SIZE(item.ptr()) == 2 {
                    (
                        PyRef::borrow(PyTuple_GET_ITEM(item.ptr(), 0)),
                        PyRef::borrow(PyTuple_GET_ITEM(item.ptr(), 1)),
                    )
                } else if PySequence_Check(item.ptr()) != 0 && PySequence_Size(item.ptr()) == 2 {
                    (
                        PyRef::own(PySequence_GetItem(item.ptr(), 0))?,
                        PyRef::own(PySequence_GetItem(item.ptr(), 1))?,
                    )
                } else {
                    return Err(py::type_error("ASGI headers must be (name, value) pairs"));
                };
            let start = out.len();
            let ok = py::with_bytes(name.ptr(), |n| {
                if !valid_name(n) {
                    return false;
                }
                out.extend_from_slice(n);
                true
            })?;
            if !ok {
                return Err(py::raise(PyExc_ValueError, "invalid header name"));
            }
            let name_end = out.len();
            out.extend_from_slice(b": ");
            let ok = py::with_bytes(value.ptr(), |v| {
                if !valid_value(v) {
                    return false;
                }
                out.extend_from_slice(v);
                true
            })?;
            if !ok {
                return Err(py::raise(PyExc_ValueError, "invalid header value"));
            }
            let n = &out[start..name_end];
            let v = &out[name_end + 2..];
            if skip.is_some_and(|f| f(n)) {
                out.truncate(start);
                continue;
            }
            match n.len() {
                14 if eq_ci(n, b"content-length") => match parse_len(v) {
                    Some(l) => info.length = Some(l),
                    None => {
                        return Err(py::raise(PyExc_ValueError, "invalid content-length header"));
                    }
                },
                17 if eq_ci(n, b"transfer-encoding") => {
                    info.chunked = tokens(v).any(|t| eq_ci(t, b"chunked"));
                    if info.chunked {
                        // Framing is ours to write, including this header.
                        out.truncate(start);
                        out.extend_from_slice(b"transfer-encoding: chunked\r\n");
                        continue;
                    }
                }
                10 if eq_ci(n, b"connection") => {
                    info.close |= tokens(v).any(|t| eq_ci(t, b"close"))
                }
                4 if eq_ci(n, b"date") => info.has_date = true,
                12 if eq_ci(n, b"x-request-id") => info.has_request_id = true,
                6 if eq_ci(n, b"server") => info.has_server = true,
                _ => {}
            }
            out.extend_from_slice(b"\r\n");
        }
    }
}

unsafe fn send(c: &ChanObj, msg: *mut PyObject) -> PResult<PyRef> {
    unsafe {
        let (sh, ctx) = worker()?;
        let ty = message_type(msg)?;
        let conn = sh.conn(c.slot, c.generation);
        match ty {
            b"http.response.start" => http_start(&ctx, c, msg, conn.as_deref()),
            b"http.response.body" => http_body(sh, &ctx, c, msg, conn.as_deref()),
            b"http.response.informational" => {
                informational(sh, &ctx, c, msg, conn.as_deref(), false)
            }
            b"http.response.early_hint" => informational(sh, &ctx, c, msg, conn.as_deref(), true),
            other => Err(py::runtime_error(&format!(
                "Unexpected ASGI message type '{}'",
                String::from_utf8_lossy(other)
            ))),
        }
    }
}

/// The message's `type`, as UTF-8 that lives as long as the message.
pub unsafe fn message_type<'a>(msg: *mut PyObject) -> PResult<&'a [u8]> {
    unsafe {
        let ty = py::dict_get(msg, s().type_)?
            .ok_or_else(|| py::raise(PyExc_KeyError, "ASGI message has no 'type'"))?;
        py::str_view(ty.ptr())
    }
}

/// `http.response.start`, also the start of a WebSocket denial response.
pub unsafe fn http_start(
    ctx: &AppCtx,
    c: &ChanObj,
    msg: *mut PyObject,
    conn: Option<&Conn>,
) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let status = py::dict_get(msg, i.status)?
            .ok_or_else(|| py::raise(PyExc_KeyError, "'http.response.start' has no 'status'"))?;
        let status = py::i64_arg(status.ptr())?;
        if !(100..=999).contains(&status) {
            return Err(py::raise(
                PyExc_ValueError,
                &format!("invalid HTTP status {status}"),
            ));
        }
        let status = status as u16;
        let mut head = Vec::with_capacity(256);
        head.extend_from_slice(b"HTTP/1.1 ");
        push_int(&mut head, status as u64);
        head.push(b' ');
        head.extend_from_slice(reason(status));
        head.extend_from_slice(b"\r\n");
        let mut info = HeadInfo::default();
        if let Some(h) = py::dict_get(msg, i.headers)? {
            encode_headers(h.ptr(), &mut head, &mut info, None)?;
        }
        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let mut st = conn.st.borrow_mut();
        if st.disconnected || (st.rejected && st.seq == c.seq) {
            return Ok(ctx.done.clone());
        }
        if st.seq != c.seq || st.resp.phase != Phase::Idle {
            drop(st);
            return Err(py::runtime_error(
                "Unexpected ASGI message 'http.response.start': the response has already started",
            ));
        }
        st.start(status, head, info);
        Ok(ctx.done.clone())
    }
}

/// `http.response.body`, also the body of a WebSocket denial response.
pub unsafe fn http_body(
    sh: &Shared,
    ctx: &AppCtx,
    c: &ChanObj,
    msg: *mut PyObject,
    conn: Option<&Conn>,
) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let more = match py::dict_get(msg, i.more_body)? {
            Some(v) => py::truthy(v.ptr())?,
            None => false,
        };
        // Anything but bytes becomes bytes now, while no state is
        // borrowed: a buffer can run Python code to hand over its data.
        let body = match py::dict_get(msg, i.body)? {
            Some(b) if PyBytes_Check(b.ptr()) != 0 => PyRef::borrow(b.ptr()),
            Some(b) => PyRef::own(PyBytes_FromObject(b.ptr()))?,
            None => PyRef::borrow(empty_bytes()),
        };
        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let date = sh.date();
        let mut st = conn.st.borrow_mut();
        if st.disconnected || (st.rejected && st.seq == c.seq) {
            return Ok(ctx.done.clone());
        }
        if st.seq != c.seq || st.resp.phase == Phase::Done {
            drop(st);
            return Err(py::runtime_error(
                "Unexpected ASGI message 'http.response.body': the response has already completed",
            ));
        }
        if st.resp.phase == Phase::Idle {
            drop(st);
            return Err(py::runtime_error(
                "Expected ASGI message 'http.response.start', but got 'http.response.body'",
            ));
        }
        let stopping = sh.closing();
        py::with_bytes(body.ptr(), |data| {
            st.write_body(data, more, ctx, &date, stopping)
        })?;
        if st.out.len() >= CORK_LIMIT && crate::http::flush_st(conn, &mut st).is_err() {
            st.disconnected = true;
            st.out.clear();
        }
        let complete = st.resp.phase == Phase::Done;
        let queued = st.out.len();
        drop(st);
        if complete || queued > 0 {
            conn.notify(sh);
        }
        backpressure(sh, ctx, conn, queued)
    }
}

/// Fields that describe a connection or a body, which a 1xx has neither of.
fn interim_forbidden(n: &[u8]) -> bool {
    [
        &b"connection"[..],
        b"keep-alive",
        b"proxy-connection",
        b"transfer-encoding",
        b"upgrade",
        b"te",
        b"content-length",
    ]
    .iter()
    .any(|k| eq_ci(n, k))
}

/// `http.response.informational`, or with `early` `http.response.early_hint`:
/// a 1xx before `http.response.start`, as many as the application likes.
/// 100 and 101 are the server's own; HTTP/1.0 has no interim responses, so
/// there both do nothing.
unsafe fn informational(
    sh: &Shared,
    ctx: &AppCtx,
    c: &ChanObj,
    msg: *mut PyObject,
    conn: Option<&Conn>,
    early: bool,
) -> PResult<PyRef> {
    unsafe {
        let i = s();
        let status = if early {
            103
        } else {
            let status = py::dict_get(msg, i.status)?.ok_or_else(|| {
                py::raise(
                    PyExc_ValueError,
                    "http.response.informational needs a status",
                )
            })?;
            let status = py::i64_arg(status.ptr())?;
            if !(102..=199).contains(&status) {
                return Err(py::raise(
                    PyExc_ValueError,
                    "an informational status is 102 to 199; the server sends 100 and 101",
                ));
            }
            status as u16
        };
        let mut head = Vec::with_capacity(128);
        head.extend_from_slice(b"HTTP/1.1 ");
        push_int(&mut head, status as u64);
        head.push(b' ');
        head.extend_from_slice(reason(status));
        head.extend_from_slice(b"\r\n");
        if early {
            if let Some(links) = py::dict_get(msg, i.links)?.filter(|l| !py::is_none(l.ptr())) {
                let it = PyRef::own(PyObject_GetIter(links.ptr())).map_err(|_| {
                    py::raise(
                        PyExc_ValueError,
                        "http.response.early_hint links must be an iterable",
                    )
                })?;
                loop {
                    let item = PyIter_Next(it.ptr());
                    if item.is_null() {
                        if !PyErr_Occurred().is_null() {
                            return Err(PyErr);
                        }
                        break;
                    }
                    let item = PyRef::own(item)?;
                    if PyBytes_Check(item.ptr()) == 0 {
                        return Err(py::raise(
                            PyExc_ValueError,
                            "an early hint link is not bytes",
                        ));
                    }
                    let ok = py::with_bytes(item.ptr(), |v| {
                        head.extend_from_slice(b"link: ");
                        head.extend_from_slice(v);
                        head.extend_from_slice(b"\r\n");
                        valid_value(v)
                    })?;
                    if !ok {
                        return Err(py::raise(
                            PyExc_ValueError,
                            "header contains a control character",
                        ));
                    }
                }
            }
        } else if let Some(h) = py::dict_get(msg, i.headers)?.filter(|h| !py::is_none(h.ptr())) {
            let start = head.len();
            encode_headers(h.ptr(), &mut head, &mut HeadInfo::default(), None)?;
            let bad = head[start..]
                .split(|&b| b == b'\n')
                .filter_map(|line| line.iter().position(|&b| b == b':').map(|p| &line[..p]))
                .any(interim_forbidden);
            if bad {
                return Err(py::raise(
                    PyExc_ValueError,
                    "header is not valid on an informational response",
                ));
            }
        }
        head.extend_from_slice(b"\r\n");

        let Some(conn) = conn else {
            return Ok(ctx.done.clone());
        };
        let mut st = conn.st.borrow_mut();
        if st.disconnected || (st.rejected && st.seq == c.seq) {
            return Ok(ctx.done.clone());
        }
        if st.seq != c.seq || st.resp.phase != Phase::Idle {
            drop(st);
            return Err(py::runtime_error(
                "an informational response after http.response.start",
            ));
        }
        if !st.http11 {
            return Ok(ctx.done.clone());
        }
        st.out.extend_from_slice(&head);
        if st.out.len() >= CORK_LIMIT && crate::http::flush_st(conn, &mut st).is_err() {
            st.disconnected = true;
            st.out.clear();
        }
        let queued = st.out.len();
        drop(st);
        conn.notify(sh);
        backpressure(sh, ctx, conn, queued)
    }
}

/// What `send` returns: done; a future that resolves once the socket has
/// taken enough of the `queued` bytes; or, past the loop's send budget, an
/// awaitable that yields once.
pub unsafe fn backpressure(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Conn,
    queued: usize,
) -> PResult<PyRef> {
    if queued > WRITE_HWM {
        let fut = unsafe { new_future(ctx)? };
        conn.st.borrow_mut().drain_waiters.push(fut.clone());
        return Ok(fut);
    }
    if sh.over_send_budget() {
        return unsafe { types::yield_once() };
    }
    Ok(ctx.done.clone())
}

unsafe fn receive(c: &ChanObj) -> PResult<PyRef> {
    unsafe {
        let (sh, ctx) = worker()?;
        let Some(conn) = sh.conn(c.slot, c.generation) else {
            return types::ready(Some(disconnect_msg()?));
        };
        let mut st = conn.st.borrow_mut();
        if st.seq != c.seq || st.disconnected || st.rejected {
            drop(st);
            return types::ready(Some(disconnect_msg()?));
        }
        if st.recv_waiter.is_some() {
            drop(st);
            return Err(py::runtime_error("receive() is already being awaited"));
        }
        if !st.final_delivered && (!st.body.buf.is_empty() || st.body.done) {
            let data = st.body.buf.split();
            let more = !st.body.done;
            st.final_delivered = !more;
            drop(st);
            if more {
                // Room in the buffer again: reading may resume.
                conn.notify(sh);
            }
            return types::ready(Some(request_msg(&data, more)?));
        }
        if st.final_delivered && st.resp.phase == Phase::Done {
            drop(st);
            return types::ready(Some(disconnect_msg()?));
        }
        if st.expect_continue && st.resp.phase == Phase::Idle {
            st.expect_continue = false;
            st.out.extend_from_slice(b"HTTP/1.1 100 Continue\r\n\r\n");
            if crate::http::flush_st(&conn, &mut st).is_err() {
                st.disconnected = true;
                st.out.clear();
            }
        }
        drop(st);
        let fut = new_future(&ctx)?;
        conn.st.borrow_mut().recv_waiter = Some(fut.clone());
        conn.notify(sh);
        Ok(fut)
    }
}

/// The request task finished. Retrieves its exception (so asyncio does not
/// log it a second time), reports it, and finishes a response the
/// application left unfinished.
unsafe fn task_done(c: &ChanObj, task: *mut PyObject) -> PResult<PyRef> {
    unsafe {
        let (sh, ctx) = worker()?;
        let report = ctx.report.as_ref().map(PyRef::ptr);
        let failed = match py::call_method(s().exception, &[task]) {
            Ok(exc) if py::is_none(exc.ptr()) => false,
            Ok(exc) => {
                if let Some(f) = report
                    && py::call(f, &[exc.ptr()]).is_err()
                {
                    PyErr_WriteUnraisable(f);
                }
                true
            }
            // Cancelled.
            Err(_) => {
                PyErr_Clear();
                true
            }
        };
        if let Some(conn) = sh.conn(c.slot, c.generation) {
            let mut st = conn.st.borrow_mut();
            if st.seq == c.seq && st.ws.is_some() {
                crate::ws::task_finished(&mut st, failed, sh.stopping.get());
                if !st.disconnected && crate::http::flush_st(&conn, &mut st).is_err() {
                    st.disconnected = true;
                    st.out.clear();
                }
                drop(st);
                conn.notify(sh);
            } else if st.seq == c.seq && st.wt.is_some() {
                crate::wt::task_finished(&mut st, failed);
                drop(st);
                crate::wt::wake_recv(&conn);
                conn.notify(sh);
            } else if st.seq == c.seq && !st.disconnected {
                match st.resp.phase {
                    Phase::Idle => {
                        if !failed {
                            drop(st);
                            py::runtime_error("ASGI callable returned without starting a response");
                            py::report_exception(report);
                            st = conn.st.borrow_mut();
                        }
                        st.write_error(500);
                    }
                    Phase::Head | Phase::Body => {
                        // Half a response: the only honest ending is to close.
                        st.resp.close = true;
                        st.resp.phase = Phase::Done;
                    }
                    Phase::Done => {}
                }
                if crate::http::flush_st(&conn, &mut st).is_err() {
                    st.disconnected = true;
                    st.out.clear();
                }
                drop(st);
                conn.notify(sh);
            }
        }
        Ok(py::none())
    }
}
