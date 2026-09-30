//! WSGI (PEP 3333) over HTTP/1.1.
//!
//! The application is called inline on the worker thread, from the
//! connection task, once the request body has been read in full. The response
//! iterable is consumed block by block; between blocks the task flushes and
//! parks on the socket like any other, so a slow client stalls only its own
//! connection's generator. The legacy `write()` callable has no task to park
//! and waits for the socket with the GIL released instead.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_void;
use std::hash::BuildHasherDefault;
use std::io;
use std::ptr;
use std::rc::Rc;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::BytesMut;
use pyo3_ffi::*;

use crate::asgi::{self, eq_ci, scope_header, tokens, unhex, valid_name, valid_value};
use crate::core::{AppCtx, Fnv, Shared};
use crate::http::{self, Conn, Framing, HeadInfo, Next, Phase, flush_all, push_int, reason};
use crate::interned::{empty_bytes, s};
use crate::py::{self, PResult, PyErr, PyRef, Static};
use crate::types::{self, new_type, slot, types};

/// Distinct `HTTP_*` keys kept interned; beyond this they are built per request.
const KEY_CACHE: usize = 512;

pub struct WsgiCtx {
    /// The per-server part of every environ, copied for each request.
    pub proto: PyRef,
    pub bytes_io: PyRef,
    /// `SCRIPT_NAME`, stripped from the front of `PATH_INFO`.
    pub root: Vec<u8>,
    keys: RefCell<HashMap<Box<[u8]>, PyRef, BuildHasherDefault<Fnv>>>,
    pool: Option<Pool>,
}

impl WsgiCtx {
    pub unsafe fn new(proto: PyRef, root: &[u8], pool: Option<Pool>) -> PResult<WsgiCtx> {
        unsafe {
            let io = py::import(c"io")?;
            Ok(WsgiCtx {
                proto,
                bytes_io: py::getattr(io.ptr(), c"BytesIO")?,
                root: root.strip_suffix(b"/").unwrap_or(root).to_vec(),
                keys: RefCell::new(HashMap::default()),
                pool,
            })
        }
    }

    /// `HTTP_` plus the header name upper-cased, with `-` as `_`.
    unsafe fn key(&self, raw: &[u8]) -> PResult<PyRef> {
        if let Some(k) = self.keys.borrow().get(raw) {
            return Ok(k.clone());
        }
        let mut name = Vec::with_capacity(raw.len() + 5);
        name.extend_from_slice(b"HTTP_");
        name.extend(raw.iter().map(|&c| {
            if c == b'-' {
                b'_'
            } else {
                c.to_ascii_uppercase()
            }
        }));
        let k = unsafe {
            let mut p = PyUnicode_FromStringAndSize(name.as_ptr().cast(), name.len() as Py_ssize_t);
            if !p.is_null() {
                PyUnicode_InternInPlace(&mut p);
            }
            PyRef::own(p)?
        };
        let mut keys = self.keys.borrow_mut();
        if keys.len() < KEY_CACHE {
            keys.insert(raw.into(), k.clone());
        }
        Ok(k)
    }
}

#[inline]
unsafe fn latin1(b: &[u8]) -> PResult<PyRef> {
    unsafe {
        if b.is_empty() {
            return Ok(PyRef::borrow(s().empty_str));
        }
        if b.is_ascii() {
            return py::str_ascii(b);
        }
        PyRef::own(PyUnicode_DecodeLatin1(
            b.as_ptr().cast(),
            b.len() as Py_ssize_t,
            ptr::null(),
        ))
    }
}

/// A `str` as the latin-1 bytes WSGI says it stands for.
unsafe fn latin1_bytes(
    o: *mut PyObject,
    what: &str,
    f: impl FnOnce(&[u8]) -> PResult<()>,
) -> PResult<()> {
    unsafe {
        if PyUnicode_Check(o) == 0 {
            return Err(py::type_error(&format!("{what} must be a str")));
        }
        let v = py::str_view(o)?;
        if v.is_ascii() {
            return f(v);
        }
        let b = PyRef::own(PyUnicode_AsLatin1String(o))?;
        f(std::slice::from_raw_parts(
            PyBytes_AsString(b.ptr()) as *const u8,
            PyBytes_Size(b.ptr()) as usize,
        ))
    }
}

fn percent_decode(raw: &[u8]) -> std::borrow::Cow<'_, [u8]> {
    if !raw.contains(&b'%') {
        return raw.into();
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
        } else {
            out.push(raw[i]);
            i += 1;
        }
    }
    out.into()
}

impl Conn {
    unsafe fn remote(&self) -> PResult<(PyRef, PyRef)> {
        if let Some((a, p)) = self.remote.borrow().as_ref() {
            return Ok((a.clone(), p.clone()));
        }
        let pair = if self.unix {
            unsafe { (py::str_ascii(b"unix")?, py::str_ascii(b"0")?) }
        } else {
            let ip = self.peer.ip().to_canonical().to_string();
            let mut port = Vec::with_capacity(5);
            push_int(&mut port, self.peer.port() as u64);
            unsafe { (py::str_ascii(ip.as_bytes())?, py::str_ascii(&port)?) }
        };
        *self.remote.borrow_mut() = Some((pair.0.clone(), pair.1.clone()));
        Ok(pair)
    }
}

/// The environ for one request, without `wsgi.input`: that needs the body.
pub unsafe fn environ(
    w: &WsgiCtx,
    conn: &Conn,
    req: &httparse::Request<'_, '_>,
    http11: bool,
    meta: &crate::ops::Meta,
) -> PResult<PyRef> {
    let i = s();
    unsafe {
        let env = PyRef::own(PyDict_Copy(w.proto.ptr()))?;
        let d = env.ptr();
        py::dict_set(
            d,
            i.request_method,
            asgi::method_str(req.method.unwrap_or("GET"))?,
        )?;

        let target = req.path.unwrap_or("/").as_bytes();
        let (mut raw_path, query) = match target.iter().position(|&c| c == b'?') {
            Some(q) => (&target[..q], &target[q + 1..]),
            None => (target, &b""[..]),
        };
        if raw_path.first() != Some(&b'/')
            && let Some(p) = raw_path.windows(3).position(|w| w == b"://")
        {
            let rest = &raw_path[p + 3..];
            raw_path = rest
                .iter()
                .position(|&c| c == b'/')
                .map_or(&b"/"[..], |s| &rest[s..]);
        }
        let path = percent_decode(raw_path);
        let mut path = &path[..];
        if !w.root.is_empty()
            && path.starts_with(&w.root)
            && matches!(path.get(w.root.len()), None | Some(b'/'))
        {
            path = &path[w.root.len()..];
        }
        py::dict_set(d, i.path_info, latin1(path)?)?;
        py::dict_set(d, i.query_string_env, latin1(query)?)?;
        py::dict_set(
            d,
            i.server_protocol,
            PyRef::borrow(if conn.st.borrow().h3 {
                i.proto_3
            } else if conn.st.borrow().h2 {
                i.proto_2
            } else if http11 {
                i.proto_1_1
            } else {
                i.proto_1_0
            }),
        )?;
        let (addr, port) = conn.remote()?;
        match &meta.client {
            Some(c) => {
                py::dict_set(d, i.remote_addr, py::str_utf8_replace(c.as_bytes())?)?;
                py::dict_set(d, i.remote_port, py::str_ascii(b"0")?)?;
            }
            None => {
                py::dict_set(d, i.remote_addr, addr)?;
                py::dict_set(d, i.remote_port, port)?;
            }
        }
        if let Some(secure) = meta.secure {
            py::dict_set(
                d,
                i.wsgi_url_scheme,
                if secure {
                    py::str_ascii(b"https")?
                } else {
                    PyRef::borrow(i.http)
                },
            )?;
        }
        for (name, value) in meta.added() {
            py::dict_set(d, w.key(name)?.ptr(), latin1(value)?)?;
        }

        for h in req.headers.iter() {
            let name = h.name.as_bytes();
            if meta.hides(name) {
                continue;
            }
            let key = match name.len() {
                12 if eq_ci(name, b"content-type") => PyRef::borrow(i.content_type_env),
                14 if eq_ci(name, b"content-length") => PyRef::borrow(i.content_length_env),
                _ if scope_header(name) => w.key(name)?,
                _ => continue,
            };
            let value = latin1(h.value)?;
            let prev = PyDict_GetItemWithError(d, key.ptr());
            if prev.is_null() {
                if !PyErr_Occurred().is_null() {
                    return Err(PyErr);
                }
                py::dict_set(d, key.ptr(), value)?;
            } else {
                let sep: &[u8] = if key.ptr() == i.http_cookie {
                    b"; "
                } else {
                    b", "
                };
                let sep = py::str_ascii(sep)?;
                let joined = PyRef::own(PyUnicode_Concat(prev, sep.ptr()))?;
                let joined = PyRef::own(PyUnicode_Concat(joined.ptr(), value.ptr()))?;
                py::dict_set(d, key.ptr(), joined)?;
            }
        }
        Ok(env)
    }
}

// --- start_response / write ------------------------------------------------------

struct Pending {
    status: u16,
    head: Vec<u8>,
    info: HeadInfo,
}

#[repr(C)]
struct StartResponseObj {
    ob_base: PyObject,
    vectorcall: Option<vectorcallfunc>,
    slot: u32,
    generation: u32,
    seq: u32,
    /// The request is still running; afterwards `write()` has nowhere to go.
    live: bool,
    called: bool,
    /// The head has been handed to the connection.
    sent: bool,
    pending: Option<Box<Pending>>,
    /// Set while the request runs on a pool thread: where its output goes.
    sink: Option<Box<Sink>>,
}

static START_TYPE: OnceLock<Static> = OnceLock::new();

unsafe extern "C" fn sr_dealloc(o: *mut PyObject) {
    unsafe {
        ptr::drop_in_place(&mut (*(o as *mut StartResponseObj)).pending);
        ptr::drop_in_place(&mut (*(o as *mut StartResponseObj)).sink);
        let tp = Py_TYPE(o);
        PyObject_Free(o as *mut c_void);
        Py_DECREF(tp as *mut PyObject);
    }
}

pub unsafe fn init() -> PResult<()> {
    if START_TYPE.get().is_some() {
        return Ok(());
    }
    let members: &'static mut [PyMemberDef] = Box::leak(Box::new([
        PyMemberDef {
            name: c"__vectorcalloffset__".as_ptr(),
            type_code: Py_T_PYSSIZET,
            offset: std::mem::offset_of!(StartResponseObj, vectorcall) as Py_ssize_t,
            flags: Py_READONLY,
            doc: ptr::null(),
        },
        unsafe { std::mem::zeroed() },
    ]));
    let t = unsafe {
        new_type(
            c"weft._weft.StartResponse",
            std::mem::size_of::<StartResponseObj>(),
            Py_TPFLAGS_DEFAULT
                | Py_TPFLAGS_HAVE_VECTORCALL
                | Py_TPFLAGS_IMMUTABLETYPE
                | Py_TPFLAGS_DISALLOW_INSTANTIATION,
            vec![
                slot(Py_tp_dealloc, sr_dealloc as *mut c_void),
                slot(Py_tp_call, PyVectorcall_Call as *mut c_void),
                slot(Py_tp_members, members.as_mut_ptr() as *mut c_void),
            ],
        )?
    };
    let _ = START_TYPE.set(Static(t));
    Ok(())
}

unsafe fn start_response(conn: &Conn) -> PResult<PyRef> {
    unsafe {
        let tp = START_TYPE.get().expect("wsgi::init").0;
        let o = types::alloc(tp, std::mem::size_of::<StartResponseObj>())?;
        let sr = &mut *(o as *mut StartResponseObj);
        sr.vectorcall = Some(sr_vectorcall);
        sr.slot = conn.slot;
        sr.generation = conn.generation;
        sr.seq = conn.st.borrow().seq;
        sr.live = true;
        PyRef::own(o)
    }
}

unsafe extern "C" fn sr_vectorcall(
    callable: *mut PyObject,
    args: *const *mut PyObject,
    nargsf: usize,
    kwnames: *mut PyObject,
) -> *mut PyObject {
    unsafe {
        let sr = &mut *(callable as *mut StartResponseObj);
        let nargs = PyVectorcall_NARGS(nargsf) as usize;
        let nkw = if kwnames.is_null() {
            0
        } else {
            PyTuple_GET_SIZE(kwnames) as usize
        };
        let r = (|| {
            let mut exc = (nargs == 3).then(|| *args.add(2));
            if nkw > 0 {
                let k = PyTuple_GET_ITEM(kwnames, 0);
                if nkw > 1 || nargs != 2 || py::str_view(k)? != b"exc_info" {
                    return Err(py::type_error(
                        "start_response() takes (status, headers[, exc_info])",
                    ));
                }
                exc = Some(*args.add(2));
            }
            match nargs {
                1 if nkw == 0 => write(sr, *args).map(|()| py::none()),
                2 | 3 => {
                    begin(sr, *args, *args.add(1), exc.filter(|&e| !py::is_none(e)))?;
                    Ok(PyRef::borrow(callable))
                }
                _ => Err(py::type_error(
                    "start_response() takes (status, headers[, exc_info])",
                )),
            }
        })();
        r.map_or(ptr::null_mut(), PyRef::into_ptr)
    }
}

unsafe fn begin(
    sr: &mut StartResponseObj,
    status: *mut PyObject,
    headers: *mut PyObject,
    exc: Option<*mut PyObject>,
) -> PResult<()> {
    unsafe {
        if let Some(e) = exc {
            if sr.sent {
                // PEP 3333: too late to change anything; let the error go on up.
                if PyTuple_Check(e) == 0 || PyTuple_GET_SIZE(e) != 3 {
                    return Err(py::type_error(
                        "exc_info must be a (type, value, traceback) tuple",
                    ));
                }
                let (t, v, tb) = (
                    PyTuple_GET_ITEM(e, 0),
                    PyTuple_GET_ITEM(e, 1),
                    PyTuple_GET_ITEM(e, 2),
                );
                Py_INCREF(t);
                Py_INCREF(v);
                let tb = if py::is_none(tb) {
                    ptr::null_mut()
                } else {
                    Py_NewRef(tb)
                };
                #[allow(deprecated)]
                PyErr_Restore(t, v, tb);
                return Err(PyErr);
            }
        } else if sr.called {
            return Err(py::runtime_error(
                "start_response() called again without exc_info",
            ));
        }
        sr.pending = Some(Box::new(encode_head(status, headers)?));
        sr.called = true;
        Ok(())
    }
}

/// The status line and the application's headers, checked, as they go on
/// the wire. Framing headers are noted the same way ASGI's are; a
/// `Transfer-Encoding` from the application is dropped, since framing is ours.
unsafe fn encode_head(status: *mut PyObject, headers: *mut PyObject) -> PResult<Pending> {
    unsafe {
        let mut head = Vec::with_capacity(256);
        head.extend_from_slice(b"HTTP/1.1 ");
        let mut code = 0u16;
        latin1_bytes(status, "status", |v| {
            let digits = v.len() >= 3 && v[..3].iter().all(u8::is_ascii_digit);
            if !digits || !matches!(v.get(3), None | Some(b' ')) || !valid_value(v) {
                return Err(py::raise(
                    PyExc_ValueError,
                    &format!("invalid status {:?}", String::from_utf8_lossy(v)),
                ));
            }
            code = v[..3].iter().fold(0u16, |a, &d| a * 10 + (d - b'0') as u16);
            if !(100..=599).contains(&code) {
                return Err(py::raise(
                    PyExc_ValueError,
                    &format!("invalid status code {code}"),
                ));
            }
            let v = asgi::trim(v);
            head.extend_from_slice(v);
            if v.len() == 3 {
                head.push(b' ');
                head.extend_from_slice(reason(code));
            }
            Ok(())
        })?;
        head.extend_from_slice(b"\r\n");

        if PyList_Check(headers) == 0 {
            return Err(py::type_error(
                "headers must be a list of (name, value) tuples",
            ));
        }
        let mut info = HeadInfo::default();
        let n = PyList_GET_SIZE(headers);
        for k in 0..n {
            let item = PyList_GET_ITEM(headers, k);
            if PyTuple_Check(item) == 0 || PyTuple_GET_SIZE(item) != 2 {
                return Err(py::type_error(
                    "headers must be a list of (name, value) tuples",
                ));
            }
            let start = head.len();
            latin1_bytes(PyTuple_GET_ITEM(item, 0), "header name", |v| {
                if !valid_name(v) {
                    return Err(py::raise(
                        PyExc_ValueError,
                        &format!("invalid header name {:?}", String::from_utf8_lossy(v)),
                    ));
                }
                head.extend_from_slice(v);
                Ok(())
            })?;
            let name_len = head.len() - start;
            head.extend_from_slice(b": ");
            let vstart = head.len();
            latin1_bytes(PyTuple_GET_ITEM(item, 1), "header value", |v| {
                if !valid_value(v) {
                    return Err(py::raise(
                        PyExc_ValueError,
                        "header values must not contain CR, LF or NUL",
                    ));
                }
                head.extend_from_slice(v);
                Ok(())
            })?;
            let (name, value) = (&head[start..start + name_len], &head[vstart..]);
            match name.len() {
                14 if eq_ci(name, b"content-length") => match parse_len(value) {
                    Some(l) => info.length = Some(l),
                    None => {
                        return Err(py::raise(PyExc_ValueError, "invalid content-length header"));
                    }
                },
                17 if eq_ci(name, b"transfer-encoding") => {
                    head.truncate(start);
                    continue;
                }
                10 if eq_ci(name, b"connection") => {
                    info.close |= tokens(value).any(|t| eq_ci(t, b"close"))
                }
                4 if eq_ci(name, b"date") => info.has_date = true,
                12 if eq_ci(name, b"x-request-id") => info.has_request_id = true,
                6 if eq_ci(name, b"server") => info.has_server = true,
                _ => {}
            }
            head.extend_from_slice(b"\r\n");
        }
        Ok(Pending {
            status: code,
            head,
            info,
        })
    }
}

fn parse_len(v: &[u8]) -> Option<u64> {
    let v = asgi::trim(v);
    if v.is_empty() || v.len() > 19 || !v.iter().all(u8::is_ascii_digit) {
        return None;
    }
    v.iter().try_fold(0u64, |acc, &d| {
        acc.checked_mul(10)?.checked_add((d - b'0') as u64)
    })
}

unsafe fn disconnected() -> PyErr {
    unsafe { py::raise(types().client_disconnected.0, "the client went away") }
}

/// Hands the head `start_response` was given to the connection, adding a
/// `Content-Length` when the whole body is known up front.
unsafe fn commit(sr: &mut StartResponseObj, conn: &Conn, length: Option<u64>) -> PResult<()> {
    if let Some(p) = unsafe { take_head(sr, length)? } {
        conn.st.borrow_mut().start(p.status, p.head, p.info);
    }
    Ok(())
}

/// The head to send, once: `None` if it has gone already.
unsafe fn take_head(
    sr: &mut StartResponseObj,
    length: Option<u64>,
) -> PResult<Option<Box<Pending>>> {
    if sr.sent {
        return Ok(None);
    }
    let Some(mut p) = sr.pending.take() else {
        return Err(unsafe { py::runtime_error("the application did not call start_response()") });
    };
    if p.info.length.is_none()
        && let Some(n) = length
        && p.status >= 200
        && p.status != 204
        && p.status != 304
    {
        p.head.extend_from_slice(b"content-length: ");
        push_int(&mut p.head, n);
        p.head.extend_from_slice(b"\r\n");
        p.info.length = Some(n);
    }
    sr.sent = true;
    Ok(Some(p))
}

/// Queues one block of body; `false` once the client is gone.
unsafe fn queue(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Conn,
    block: *mut PyObject,
    more: bool,
) -> PResult<bool> {
    unsafe {
        let owned;
        let block = if PyBytes_Check(block) != 0 {
            block
        } else {
            owned = PyRef::own(PyBytes_FromObject(block))
                .map_err(|_| py::type_error("the application must yield bytes"))?;
            owned.ptr()
        };
        let data = std::slice::from_raw_parts(
            PyBytes_AsString(block) as *const u8,
            PyBytes_Size(block) as usize,
        );
        Ok(queue_slice(sh, ctx, conn, data, more))
    }
}

fn queue_slice(sh: &Shared, ctx: &AppCtx, conn: &Conn, data: &[u8], more: bool) -> bool {
    let mut st = conn.st.borrow_mut();
    if st.disconnected {
        return false;
    }
    st.write_body(data, more, ctx, &sh.date(), sh.closing());
    true
}

/// After a complete response: whether the connection serves another.
fn next_request(sh: &Shared, conn: &Conn) -> Next {
    let st = conn.st.borrow();
    if st.keep_alive && !st.resp.close && !sh.closing() {
        Next::KeepAlive
    } else {
        Next::Close
    }
}

/// The legacy `write(data)` callable. It must have sent the bytes (or
/// handed them to the OS) before it returns, so it waits on the socket here;
/// an HTTP/2 or HTTP/3 stream can only queue them.
unsafe fn write(sr: &mut StartResponseObj, data: *mut PyObject) -> PResult<()> {
    unsafe {
        if PyBytes_Check(data) == 0 {
            return Err(py::type_error("write() takes a bytes object"));
        }
        if !sr.live {
            return Err(py::runtime_error(
                "write() called after its request finished",
            ));
        }
        if !sr.called {
            return Err(py::runtime_error("write() called before start_response()"));
        }
        let bytes = std::slice::from_raw_parts(
            PyBytes_AsString(data) as *const u8,
            PyBytes_Size(data) as usize,
        );
        if sr.sink.is_some() {
            return pool_write(sr, bytes);
        }
        let (sh, ctx) = asgi::worker()?;
        let conn = sh
            .conn(sr.slot, sr.generation)
            .filter(|c| c.st.borrow().seq == sr.seq)
            .ok_or_else(|| disconnected())?;
        commit(sr, &conn, None)?;
        let Some(over) = put(sh, &ctx, &conn, bytes) else {
            return Err(disconnected());
        };
        flush_blocking(&ctx, &conn)?;
        if over {
            return Err(over_length());
        }
        Ok(())
    }
}

unsafe fn over_length() -> PyErr {
    unsafe { py::runtime_error("write() went past the response's Content-Length") }
}

/// Queues a `write()` block. `None` if the client is gone, else whether the
/// block ran past the declared `Content-Length` (the excess is dropped).
fn put(sh: &Shared, ctx: &AppCtx, conn: &Conn, data: &[u8]) -> Option<bool> {
    let mut st = conn.st.borrow_mut();
    if st.disconnected {
        return None;
    }
    if st.resp.phase == Phase::Head {
        st.write_body(b"", true, ctx, &sh.date(), sh.closing());
    }
    let over = matches!(st.resp.framing, Framing::Length(rem) if data.len() as u64 > rem);
    st.write_body(data, true, ctx, &sh.date(), sh.closing());
    Some(over)
}

/// Writes all of the connection's output, waiting for the socket with the
/// GIL released. The send goes straight to the socket: tokio's readiness
/// cache would not see the socket drain while this thread blocks.
unsafe fn flush_blocking(ctx: &AppCtx, conn: &Conn) -> PResult<()> {
    let ms = ctx
        .request_timeout
        .map_or(-1, |d| d.as_millis().min(i32::MAX as u128) as i32);
    // An HTTP/2 or HTTP/3 stream has no socket of its own: it drains when
    // the connection's task runs, on this thread. Waiting here would wait
    // forever, so what does not fit now stays queued for that task.
    let multiplexed = conn.io.borrow().is_none();
    loop {
        let r = crate::http::flush_conn(conn);
        let ok = match r {
            Ok(true) => return Ok(()),
            Ok(false) if multiplexed => return Ok(()),
            Ok(false) => unsafe {
                let ts = PyEval_SaveThread();
                let ok = conn
                    .io
                    .borrow()
                    .as_ref()
                    .is_some_and(|io| crate::fdpoll::wait_writable(&io.tcp, ms));
                PyEval_RestoreThread(ts);
                ok
            },
            Err(_) => false,
        };
        if !ok {
            let mut st = conn.st.borrow_mut();
            st.disconnected = true;
            st.out.clear();
            return Err(unsafe { disconnected() });
        }
    }
}

// --- the request -----------------------------------------------------------------

/// Reads the whole request body into the connection state. `false` when the
/// request cannot go on; any answer the server owes is already queued.
async fn read_body(sh: &Shared, ctx: &AppCtx, conn: &Conn, rbuf: &mut BytesMut) -> bool {
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
            if st.expect_continue {
                st.expect_continue = false;
                st.out.extend_from_slice(b"HTTP/1.1 100 Continue\r\n\r\n");
            }
        }
        if !flush_all(ctx, conn).await {
            return false;
        }
        let limit = ctx.request_timeout.unwrap_or(Duration::from_secs(86_400));
        match http::read_some(sh, conn, rbuf, limit, false).await {
            Some(n) if n > 0 => {}
            Some(_) => {
                conn.st.borrow_mut().disconnected = true;
                return false;
            }
            None => {
                conn.st.borrow_mut().reject(408);
                return false;
            }
        }
    }
}

/// Runs one WSGI request whose head `dispatch` has parsed.
pub async fn run(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Rc<Conn>,
    rbuf: &mut BytesMut,
    environ: PyRef,
) -> Next {
    if !read_body(sh, ctx, conn, rbuf).await {
        return Next::Close;
    }
    let report = ctx.report.as_ref().map(PyRef::ptr);
    let Some(w) = &ctx.wsgi else {
        return Next::Close;
    };
    let sr = match unsafe { prepare(w, conn, &environ) } {
        Ok(sr) => sr,
        Err(_) => {
            unsafe { py::report_exception(report) };
            return fail(conn);
        }
    };
    if let Some(pool) = &w.pool {
        return run_pooled(sh, ctx, conn, pool, environ, sr).await;
    }
    let result = unsafe { py::call(ctx.app.ptr(), &[environ.ptr(), sr.ptr()]) };
    drop(environ);
    let next = match result {
        Ok(r) => {
            let next = respond(sh, ctx, conn, &sr, &r).await;
            if unsafe { close_iterable(r.ptr()) }.is_err() {
                unsafe { py::report_exception(report) };
            }
            next
        }
        Err(_) => {
            unsafe { py::report_exception(report) };
            fail(conn)
        }
    };
    unsafe { (*(sr.ptr() as *mut StartResponseObj)).live = false };
    next
}

unsafe fn prepare(w: &WsgiCtx, conn: &Conn, environ: &PyRef) -> PResult<PyRef> {
    let i = s();
    unsafe {
        let body = {
            let mut st = conn.st.borrow_mut();
            let buf = st.body.buf.split();
            if buf.is_empty() {
                PyRef::borrow(empty_bytes())
            } else {
                py::bytes(&buf)?
            }
        };
        let d = environ.ptr();
        let n = PyBytes_Size(body.ptr());
        if n > 0 && PyDict_Contains(d, i.content_length_env) == 0 {
            let mut v = Vec::with_capacity(20);
            push_int(&mut v, n as u64);
            py::dict_set(d, i.content_length_env, py::str_ascii(&v)?)?;
        }
        let input = py::call(w.bytes_io.ptr(), &[body.ptr()])?;
        py::dict_set(d, i.wsgi_input, input)?;
        start_response(conn)
    }
}

/// The application failed: a 500 if nothing has been sent, otherwise all
/// that can be done is to cut the response short.
fn fail(conn: &Conn) -> Next {
    let mut st = conn.st.borrow_mut();
    if matches!(st.resp.phase, Phase::Idle | Phase::Head) {
        st.resp.phase = Phase::Idle;
        st.write_error(500);
    }
    Next::Close
}

unsafe fn close_iterable(r: *mut PyObject) -> PResult<()> {
    unsafe {
        if PyList_CheckExact(r) != 0 || PyTuple_CheckExact(r) != 0 || PyBytes_CheckExact(r) != 0 {
            return Ok(());
        }
        let close = PyObject_GetAttr(r, s().close);
        if close.is_null() {
            if PyErr_ExceptionMatches(PyExc_AttributeError) != 0 {
                PyErr_Clear();
                return Ok(());
            }
            return Err(PyErr);
        }
        let close = PyRef::own(close)?;
        py::call(close.ptr(), &[]).map(drop)
    }
}

async fn respond(sh: &Shared, ctx: &AppCtx, conn: &Conn, sr: &PyRef, r: &PyRef) -> Next {
    let report = ctx.report.as_ref().map(PyRef::ptr);
    macro_rules! tryp {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(_) => {
                    py::report_exception(report);
                    return fail(conn);
                }
            }
        };
    }
    let srp = sr.ptr() as *mut StartResponseObj;
    let obj = r.ptr();
    unsafe {
        let seq = if PyList_CheckExact(obj) != 0 {
            Some((PyList_GET_SIZE(obj), true))
        } else if PyTuple_CheckExact(obj) != 0 {
            Some((PyTuple_GET_SIZE(obj), false))
        } else {
            None
        };
        if let Some((n, list)) = seq {
            let item = |k| {
                if list {
                    PyList_GET_ITEM(obj, k)
                } else {
                    PyTuple_GET_ITEM(obj, k)
                }
            };
            let mut total = 0u64;
            for k in 0..n {
                let b = item(k);
                if PyBytes_Check(b) == 0 {
                    tryp!(Err::<(), _>(py::type_error(
                        "the application must yield bytes"
                    )));
                }
                total += PyBytes_Size(b) as u64;
            }
            tryp!(commit(&mut *srp, conn, Some(total)));
            for k in 0..n {
                let b = PyRef::borrow(item(k));
                if !tryp!(queue(sh, ctx, conn, b.ptr(), true)) {
                    return Next::Close;
                }
                if conn.st.borrow().out.len() >= http::WRITE_HWM && !flush_all(ctx, conn).await {
                    return Next::Close;
                }
            }
        } else {
            let it = tryp!(PyRef::own(PyObject_GetIter(obj)));
            let next = || -> PResult<Option<PyRef>> {
                let item = PyIter_Next(it.ptr());
                if item.is_null() {
                    return if PyErr_Occurred().is_null() {
                        Ok(None)
                    } else {
                        Err(PyErr)
                    };
                }
                PyRef::own(item).map(Some)
            };
            // The head waits for the first non-empty block, so that an error
            // before any output still becomes a clean 500.
            let first = loop {
                match tryp!(next()) {
                    Some(b) if PyBytes_Check(b.ptr()) != 0 && PyBytes_Size(b.ptr()) == 0 => {
                        continue;
                    }
                    other => break other,
                }
            };
            tryp!(commit(
                &mut *srp,
                conn,
                if first.is_none() { Some(0) } else { None }
            ));
            let mut block = first;
            while let Some(b) = block {
                if !tryp!(queue(sh, ctx, conn, b.ptr(), true)) {
                    return Next::Close;
                }
                drop(b);
                if !flush_all(ctx, conn).await {
                    return Next::Close;
                }
                block = tryp!(next());
            }
        }
        if !tryp!(queue(sh, ctx, conn, empty_bytes(), false)) {
            return Next::Close;
        }
    }
    if !flush_all(ctx, conn).await {
        return Next::Close;
    }
    next_request(sh, conn)
}

// --- the thread pool -----------------------------------------------------------------

/// A Python object moved to or from a pool thread. It is only used, and
/// dropped, by a thread attached to the interpreter.
struct SendRef(PyRef);
unsafe impl Send for SendRef {}

/// What a pool thread tells the connection task running its request.
enum Msg {
    Head(Box<Pending>),
    Data(Vec<u8>),
    /// A `write()` block; the application waits for the reply.
    Write(Vec<u8>, std::sync::mpsc::SyncSender<Wrote>),
    End,
    /// The application failed; it has been reported.
    Fail,
}

#[derive(Clone, Copy)]
enum Wrote {
    Sent,
    Gone,
    Over,
}

type Sink = tokio::sync::mpsc::Sender<Msg>;

/// Blocks queued ahead of a slow client before the application's thread waits.
const SINK_DEPTH: usize = 4;

struct Job {
    app: SendRef,
    environ: SendRef,
    sr: SendRef,
    report: Option<SendRef>,
}

/// `--wsgi-threads`: applications run on these threads instead of the
/// worker's, so one slow request does not hold up the worker's other
/// connections. The connection task still does all the socket work.
pub struct Pool {
    jobs: std::sync::mpsc::Sender<Job>,
}

impl Pool {
    pub fn new(threads: usize) -> io::Result<Pool> {
        let (jobs, rx) = std::sync::mpsc::channel::<Job>();
        let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));
        for n in 0..threads {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("weft-wsgi-{n}"))
                .spawn(move || unsafe { pool_main(&rx) })?;
        }
        Ok(Pool { jobs })
    }
}

unsafe fn pool_main(rx: &std::sync::Mutex<std::sync::mpsc::Receiver<Job>>) {
    unsafe {
        let gil = PyGILState_Ensure();
        loop {
            let ts = PyEval_SaveThread();
            let job = rx.lock().ok().and_then(|rx| rx.recv().ok());
            PyEval_RestoreThread(ts);
            match job {
                Some(job) => run_job(job),
                None => break,
            }
        }
        PyGILState_Release(gil);
    }
}

/// Hands `msg` to the connection task, waiting with the GIL released if its
/// queue is full. `false` once the connection has finished with the request.
unsafe fn emit(sink: &Sink, msg: Msg) -> bool {
    match sink.try_send(msg) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
        Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => unsafe {
            let ts = PyEval_SaveThread();
            let ok = sink.blocking_send(msg).is_ok();
            PyEval_RestoreThread(ts);
            ok
        },
    }
}

unsafe fn run_job(job: Job) {
    unsafe {
        let Job {
            app,
            environ,
            sr,
            report,
        } = job;
        let report = report.as_ref().map(|r| r.0.ptr());
        let srp = sr.0.ptr() as *mut StartResponseObj;
        let Some(sink) = (*srp).sink.as_deref().cloned() else {
            return;
        };
        let result = py::call(app.0.ptr(), &[environ.0.ptr(), sr.0.ptr()]);
        drop(environ);
        match result {
            Ok(r) => {
                if pool_respond(srp, &sink, r.ptr()).is_err() {
                    py::report_exception(report);
                    emit(&sink, Msg::Fail);
                }
                if close_iterable(r.ptr()).is_err() {
                    py::report_exception(report);
                }
            }
            Err(_) => {
                py::report_exception(report);
                emit(&sink, Msg::Fail);
            }
        }
        (*srp).live = false;
        (*srp).sink = None;
    }
}

unsafe fn block_bytes(o: *mut PyObject) -> PResult<Vec<u8>> {
    unsafe {
        let owned;
        let o = if PyBytes_Check(o) != 0 {
            o
        } else {
            owned = PyRef::own(PyBytes_FromObject(o))
                .map_err(|_| py::type_error("the application must yield bytes"))?;
            owned.ptr()
        };
        Ok(
            std::slice::from_raw_parts(PyBytes_AsString(o) as *const u8, PyBytes_Size(o) as usize)
                .to_vec(),
        )
    }
}

/// `respond`, from a pool thread: the same rules, with the output sent to
/// the connection task. `Err` is a Python error still to be reported.
unsafe fn pool_respond(srp: *mut StartResponseObj, sink: &Sink, obj: *mut PyObject) -> PResult<()> {
    unsafe {
        let head = |length| -> PResult<bool> {
            Ok(match take_head(&mut *srp, length)? {
                Some(p) => emit(sink, Msg::Head(p)),
                None => true,
            })
        };
        let seq = if PyList_CheckExact(obj) != 0 {
            Some((PyList_GET_SIZE(obj), true))
        } else if PyTuple_CheckExact(obj) != 0 {
            Some((PyTuple_GET_SIZE(obj), false))
        } else {
            None
        };
        if let Some((n, list)) = seq {
            let item = |k| {
                if list {
                    PyList_GET_ITEM(obj, k)
                } else {
                    PyTuple_GET_ITEM(obj, k)
                }
            };
            let mut total = 0u64;
            for k in 0..n {
                let b = item(k);
                if PyBytes_Check(b) == 0 {
                    return Err(py::type_error("the application must yield bytes"));
                }
                total += PyBytes_Size(b) as u64;
            }
            if !head(Some(total))? {
                return Ok(());
            }
            for k in 0..n {
                if !emit(sink, Msg::Data(block_bytes(item(k))?)) {
                    return Ok(());
                }
            }
        } else {
            let it = PyRef::own(PyObject_GetIter(obj))?;
            let next = || -> PResult<Option<PyRef>> {
                let item = PyIter_Next(it.ptr());
                if item.is_null() {
                    return if PyErr_Occurred().is_null() {
                        Ok(None)
                    } else {
                        Err(PyErr)
                    };
                }
                PyRef::own(item).map(Some)
            };
            let first = loop {
                match next()? {
                    Some(b) if PyBytes_Check(b.ptr()) != 0 && PyBytes_Size(b.ptr()) == 0 => {
                        continue;
                    }
                    other => break other,
                }
            };
            if !head(if first.is_none() { Some(0) } else { None })? {
                return Ok(());
            }
            let mut block = first;
            while let Some(b) = block {
                if !emit(sink, Msg::Data(block_bytes(b.ptr())?)) {
                    return Ok(());
                }
                drop(b);
                block = next()?;
            }
        }
        emit(sink, Msg::End);
        Ok(())
    }
}

unsafe fn pool_write(sr: &mut StartResponseObj, data: &[u8]) -> PResult<()> {
    unsafe {
        let Some(sink) = sr.sink.as_deref().cloned() else {
            return Err(disconnected());
        };
        if let Some(p) = take_head(sr, None)?
            && !emit(&sink, Msg::Head(p))
        {
            return Err(disconnected());
        }
        let (reply, wrote) = std::sync::mpsc::sync_channel(1);
        if !emit(&sink, Msg::Write(data.to_vec(), reply)) {
            return Err(disconnected());
        }
        let ts = PyEval_SaveThread();
        let wrote = wrote.recv();
        PyEval_RestoreThread(ts);
        match wrote {
            Ok(Wrote::Sent) => Ok(()),
            Ok(Wrote::Over) => Err(over_length()),
            _ => Err(disconnected()),
        }
    }
}

/// The connection's side of a pooled request: writes what the pool thread
/// produces, flushing each block as PEP 3333 asks.
async fn run_pooled(
    sh: &Shared,
    ctx: &AppCtx,
    conn: &Conn,
    pool: &Pool,
    environ: PyRef,
    sr: PyRef,
) -> Next {
    let (sink, mut rx) = tokio::sync::mpsc::channel(SINK_DEPTH);
    unsafe { (*(sr.ptr() as *mut StartResponseObj)).sink = Some(Box::new(sink)) };
    let job = Job {
        app: SendRef(ctx.app.clone()),
        environ: SendRef(environ),
        sr: SendRef(sr),
        report: ctx.report.clone().map(SendRef),
    };
    if pool.jobs.send(job).is_err() {
        return fail(conn);
    }
    while let Some(msg) = rx.recv().await {
        match msg {
            Msg::Head(p) => conn.st.borrow_mut().start(p.status, p.head, p.info),
            Msg::Data(b) => {
                if !queue_slice(sh, ctx, conn, &b, true) || !flush_all(ctx, conn).await {
                    return Next::Close;
                }
            }
            Msg::Write(b, reply) => {
                let wrote = match put(sh, ctx, conn, &b) {
                    Some(over) if flush_all(ctx, conn).await => {
                        if over {
                            Wrote::Over
                        } else {
                            Wrote::Sent
                        }
                    }
                    _ => Wrote::Gone,
                };
                let _ = reply.send(wrote);
                if matches!(wrote, Wrote::Gone) {
                    return Next::Close;
                }
            }
            Msg::End => {
                if !queue_slice(sh, ctx, conn, b"", false) || !flush_all(ctx, conn).await {
                    return Next::Close;
                }
                return next_request(sh, conn);
            }
            Msg::Fail => return fail(conn),
        }
    }
    fail(conn)
}
