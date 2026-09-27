//! `weft._weft`: an ASGI server on Tokio that runs inside the asyncio thread.
//!
//! Python constructs a `Worker`, hands it to `weft.loop.WeftEventLoop` as the
//! selector's backend, and calls `serve` with a listening socket. From then
//! on every `select()` asyncio makes drives the server; see `core.rs`.

mod asgi;
mod cache;
mod compress;
mod core;
mod fdpoll;
mod h2c;
mod h3c;
mod http;
mod interned;
mod limit;
mod metrics;
mod ops;
mod py;
mod staticf;
mod tls;
mod types;
#[cfg(windows)]
mod winaccept;
mod ws;
mod wsgi;
mod wt;

use std::ffi::{c_int, c_void};
use std::ptr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use pyo3_ffi::*;

use crate::core::{AppCtx, Core, Remote, thread_key};
use crate::py::{PResult, PyErr, PyRef, Static};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[repr(C)]
struct WorkerObj {
    ob_base: PyObject,
    core: *mut Core,
    remote: *const Remote,
    owner: usize,
    in_select: bool,
}

static WORKER_TYPE: OnceLock<Static> = OnceLock::new();

unsafe fn os_error(e: std::io::Error) -> PyErr {
    unsafe {
        if let Some(code) = e.raw_os_error()
            && let (Ok(c), Ok(m)) = (
                py::int(code as i64),
                py::str_utf8_replace(e.to_string().as_bytes()),
            )
            && let Ok(args) = py::tuple2(c, m)
        {
            PyErr_SetObject(PyExc_OSError, args.ptr());
            return PyErr;
        }
        py::raise(PyExc_OSError, &e.to_string())
    }
}

unsafe fn worker<'a>(o: *mut PyObject) -> PResult<&'a mut WorkerObj> {
    unsafe {
        let w = &mut *(o as *mut WorkerObj);
        if w.core.is_null() {
            return Err(py::runtime_error("the worker is closed"));
        }
        if w.owner != thread_key() {
            return Err(py::runtime_error(
                "a worker can only be used from the thread that created it",
            ));
        }
        Ok(w)
    }
}

unsafe extern "C" fn worker_new(
    tp: *mut PyTypeObject,
    _args: *mut PyObject,
    _kw: *mut PyObject,
) -> *mut PyObject {
    unsafe {
        let o = PyType_GenericAlloc(tp, 0);
        if o.is_null() {
            return ptr::null_mut();
        }
        match Core::new() {
            Ok(core) => {
                let w = &mut *(o as *mut WorkerObj);
                w.remote = Arc::into_raw(core.remote.clone());
                w.owner = core.owner;
                w.core = Box::into_raw(Box::new(core));
                o
            }
            Err(e) => {
                Py_DECREF(o);
                os_error(e);
                ptr::null_mut()
            }
        }
    }
}

unsafe fn close_core(w: &mut WorkerObj) {
    if w.core.is_null() {
        return;
    }
    let core = unsafe { Box::from_raw(w.core) };
    w.core = ptr::null_mut();
    if core.owner == thread_key() {
        core.close();
    } else {
        // A runtime cannot be torn down from another thread; the process is
        // on its way out if a worker is collected anywhere but at home.
        std::mem::forget(core);
    }
}

unsafe extern "C" fn worker_dealloc(o: *mut PyObject) {
    unsafe {
        let w = &mut *(o as *mut WorkerObj);
        close_core(w);
        if !w.remote.is_null() {
            drop(Arc::from_raw(w.remote));
            w.remote = ptr::null();
        }
        let tp = Py_TYPE(o);
        if let Some(free) = (*tp).tp_free {
            free(o as *mut c_void);
        }
        Py_DECREF(tp as *mut PyObject);
    }
}

macro_rules! method {
    ($name:ident, |$o:ident, $args:ident, $nargs:ident| $body:block) => {
        unsafe extern "C" fn $name(
            $o: *mut PyObject,
            $args: *mut *mut PyObject,
            $nargs: Py_ssize_t,
        ) -> *mut PyObject {
            #[allow(clippy::redundant_closure_call)]
            let r: PResult<PyRef> = (|| unsafe { $body })();
            r.map_or(ptr::null_mut(), PyRef::into_ptr)
        }
    };
}

unsafe fn nargs_exact(name: &str, nargs: Py_ssize_t, want: Py_ssize_t) -> PResult<()> {
    if nargs != want {
        return Err(unsafe {
            py::type_error(&format!("{name}() takes {want} arguments ({nargs} given)"))
        });
    }
    Ok(())
}

method!(w_select, |o, args, nargs| {
    let w = worker(o)?;
    let timeout = if nargs == 0 || py::is_none(*args) {
        None
    } else {
        Some(py::f64_arg(*args)?)
    };
    w.in_select = true;
    let events = (*w.core).select(timeout);
    w.in_select = false;
    // A server task must never leave an exception pending; if one did, it
    // must not surface as this call's error.
    if !PyErr_Occurred().is_null() {
        PyErr_WriteUnraisable(o);
    }
    let list = PyRef::own(PyList_New(events.len() as Py_ssize_t))?;
    for (n, (fd, ev)) in events.into_iter().enumerate() {
        let t = py::tuple2(py::int(fd)?, py::int(ev as i64)?)?;
        PyList_SET_ITEM(list.ptr(), n as Py_ssize_t, t.into_ptr());
    }
    Ok(list)
});

method!(w_register, |o, args, nargs| {
    nargs_exact("register", nargs, 2)?;
    let w = worker(o)?;
    let (fd, ev) = (py::i64_arg(*args)?, py::i64_arg(*args.add(1))?);
    (*w.core).register(fd, ev as u32).map_err(|e| os_error(e))?;
    Ok(py::none())
});

method!(w_modify, |o, args, nargs| {
    nargs_exact("modify", nargs, 2)?;
    let w = worker(o)?;
    let (fd, ev) = (py::i64_arg(*args)?, py::i64_arg(*args.add(1))?);
    (*w.core).modify(fd, ev as u32).map_err(|e| os_error(e))?;
    Ok(py::none())
});

method!(w_unregister, |o, args, nargs| {
    nargs_exact("unregister", nargs, 1)?;
    let w = worker(o)?;
    (*w.core).unregister(py::i64_arg(*args)?);
    Ok(py::none())
});

method!(w_wakeup, |o, _args, _nargs| {
    // Any thread: this is what call_soon_threadsafe ends up calling.
    let w = &*(o as *mut WorkerObj);
    if !w.remote.is_null() {
        (*w.remote).wake();
    }
    Ok(py::none())
});

unsafe fn opt(opts: *mut PyObject, key: &std::ffi::CStr) -> PResult<Option<*mut PyObject>> {
    unsafe {
        if opts.is_null() || py::is_none(opts) {
            return Ok(None);
        }
        let v = PyDict_GetItemString(opts, key.as_ptr());
        if v.is_null() {
            if PyErr_Occurred().is_null() {
                Ok(None)
            } else {
                Err(PyErr)
            }
        } else if py::is_none(v) {
            Ok(None)
        } else {
            Ok(Some(v))
        }
    }
}

unsafe fn opt_secs(opts: *mut PyObject, key: &std::ffi::CStr, default: f64) -> PResult<Duration> {
    let v = match unsafe { opt(opts, key)? } {
        Some(v) => unsafe { py::f64_arg(v)? },
        None => default,
    };
    Ok(Duration::from_secs_f64(v.clamp(0.001, 86_400.0)))
}

unsafe fn opt_int(
    opts: *mut PyObject,
    key: &std::ffi::CStr,
    default: usize,
    min: usize,
    max: usize,
) -> PResult<usize> {
    match unsafe { opt(opts, key)? } {
        Some(v) => Ok((unsafe { py::i64_arg(v)? }).clamp(min as i64, max as i64) as usize),
        None => Ok(default),
    }
}

unsafe fn opt_bool(opts: *mut PyObject, key: &std::ffi::CStr, default: bool) -> PResult<bool> {
    match unsafe { opt(opts, key)? } {
        Some(v) => unsafe { py::truthy(v) },
        None => Ok(default),
    }
}

unsafe fn str_list(opts: *mut PyObject, key: &std::ffi::CStr) -> PResult<Vec<String>> {
    let Some(v) = (unsafe { opt(opts, key)? }) else {
        return Ok(Vec::new());
    };
    unsafe {
        if PyList_Check(v) != 0 {
            let mut out = Vec::with_capacity(PyList_GET_SIZE(v) as usize);
            for i in 0..PyList_GET_SIZE(v) {
                out.push(
                    String::from_utf8_lossy(py::str_view(PyList_GET_ITEM(v, i))?).into_owned(),
                );
            }
            Ok(out)
        } else {
            Ok(vec![String::from_utf8_lossy(py::str_view(v)?).into_owned()])
        }
    }
}

// serve(sock_fd, app, loop, options)
method!(w_serve, |o, args, nargs| {
    nargs_exact("serve", nargs, 4)?;
    let w = worker(o)?;
    let fd = py::i64_arg(*args)?;
    let (app, loop_, opts) = (*args.add(1), *args.add(2), *args.add(3));
    if !py::is_none(opts) && PyDict_Check(opts) == 0 {
        return Err(py::type_error("serve() options must be a dict"));
    }
    let i = interned::s();

    let asyncio = py::import(c"asyncio")?;
    let task_type = py::getattr(asyncio.ptr(), c"Task")?;
    let future_type = py::getattr(asyncio.ptr(), c"Future")?;
    let kw_loop = PyRef::own(PyTuple_New(1))?;
    PyTuple_SET_ITEM(kw_loop.ptr(), 0, PyRef::borrow(i.loop_).into_ptr());

    let (host, port, unix_sock) = core::bind_name(fd).map_err(|e| os_error(e))?;
    let server = py::tuple2(py::str_ascii(host.as_bytes())?, py::int(port as i64)?)?;

    let asgi = PyRef::own(PyDict_New())?;
    py::dict_set(asgi.ptr(), i.version, PyRef::borrow(i.v3_0))?;
    py::dict_set(asgi.ptr(), i.spec_version, PyRef::borrow(i.v2_4))?;
    let proto = PyRef::own(PyDict_New())?;
    py::dict_set(proto.ptr(), i.type_, PyRef::borrow(i.http))?;
    py::dict_set(proto.ptr(), i.asgi, asgi.clone())?;
    let scheme = match opt(opts, c"scheme")? {
        Some(v) => PyRef::borrow(v),
        None => PyRef::borrow(i.http),
    };
    let secure = py::str_view(scheme.ptr())? == b"https";
    py::dict_set(proto.ptr(), i.scheme, scheme)?;
    let root = match opt(opts, c"root_path")? {
        Some(v) => PyRef::borrow(v),
        None => py::str_ascii(b"")?,
    };
    py::dict_set(proto.ptr(), i.root_path, root.clone())?;
    py::dict_set(proto.ptr(), i.server, server.clone())?;
    let ext = PyRef::own(PyDict_New())?;
    py::dict_set(
        ext.ptr(),
        i.http_response_informational,
        PyRef::own(PyDict_New())?,
    )?;
    py::dict_set(
        ext.ptr(),
        i.http_response_early_hint,
        PyRef::own(PyDict_New())?,
    )?;
    py::dict_set(proto.ptr(), i.extensions, ext)?;

    let ws_proto = PyRef::own(PyDict_New())?;
    py::dict_set(ws_proto.ptr(), i.type_, PyRef::borrow(i.websocket))?;
    py::dict_set(ws_proto.ptr(), i.asgi, asgi)?;
    py::dict_set(
        ws_proto.ptr(),
        i.scheme,
        PyRef::borrow(if secure { i.wss } else { i.ws }),
    )?;
    py::dict_set(ws_proto.ptr(), i.root_path, root.clone())?;
    py::dict_set(ws_proto.ptr(), i.server, server)?;
    let ext = PyRef::own(PyDict_New())?;
    py::dict_set(
        ext.ptr(),
        i.websocket_http_response,
        PyRef::own(PyDict_New())?,
    )?;
    py::dict_set(ws_proto.ptr(), i.extensions, ext)?;
    let ping = match opt(opts, c"ws_ping_interval")? {
        Some(v) => py::f64_arg(v)?,
        None => 20.0,
    };
    let ws = ws::WsCfg {
        enabled: opt_bool(opts, c"websockets", true)?,
        max_message: opt_int(opts, c"ws_max_message", 16 << 20, 1024, 1 << 30)?,
        ping_interval: (ping > 0.0).then(|| Duration::from_secs_f64(ping.min(86_400.0))),
        ping_timeout: opt_secs(opts, c"ws_ping_timeout", 20.0)?,
        max_queue: opt_int(opts, c"ws_max_queue", 32, 1, 1 << 20)?,
        max_queue_bytes: opt_int(
            opts,
            c"ws_max_queue_bytes",
            4 << 20,
            1024,
            isize::MAX as usize,
        )?,
        compress: opt_bool(opts, c"ws_compress", false)?,
    };

    let state = match opt(opts, c"state")? {
        Some(v) if PyDict_Check(v) != 0 => Some(PyRef::borrow(v)),
        Some(_) => return Err(py::type_error("'state' must be a dict")),
        None => None,
    };
    let report = opt(opts, c"report")?.map(|v| PyRef::borrow(v));
    let max_head = opt_int(opts, c"max_head", 32 * 1024, 1024, 16 * 1024 * 1024)?;
    let max_body = opt_int(opts, c"max_body", 16 << 20, 0, isize::MAX as usize)? as u64;
    let request_timeout = match opt(opts, c"request_timeout")? {
        Some(v) => py::f64_arg(v)?,
        None => 30.0,
    };
    let request_timeout =
        (request_timeout > 0.0).then(|| Duration::from_secs_f64(request_timeout.min(86_400.0)));

    let trusted = match opt(opts, c"forwarded_allow_ips")? {
        Some(v) => Some(
            ops::Trusted::parse(&String::from_utf8_lossy(py::str_view(v)?))
                .map_err(|e| py::raise(PyExc_ValueError, &e))?,
        ),
        None => None,
    };
    let health = match opt(opts, c"health_check_path")? {
        Some(v) if !py::str_view(v)?.is_empty() => Some(py::str_view(v)?.to_vec()),
        _ => None,
    };

    let wsgi = match opt(opts, c"protocol")? {
        Some(p) if py::str_view(p)? == b"wsgi" => {
            let env = PyRef::own(PyDict_New())?;
            let d = env.ptr();
            py::dict_set(d, i.script_name, root.clone())?;
            py::dict_set(d, i.server_name, py::str_ascii(host.as_bytes())?)?;
            py::dict_set(
                d,
                i.server_port,
                py::str_ascii(port.to_string().as_bytes())?,
            )?;
            py::dict_set(d, i.query_string_env, PyRef::borrow(i.empty_str))?;
            py::dict_set(d, i.wsgi_version, py::tuple2(py::int(1)?, py::int(0)?)?)?;
            py::dict_set(
                d,
                i.wsgi_url_scheme,
                if secure {
                    py::str_ascii(b"https")?
                } else {
                    PyRef::borrow(i.http)
                },
            )?;
            let sys = py::import(c"sys")?;
            py::dict_set(d, i.wsgi_errors, py::getattr(sys.ptr(), c"stderr")?)?;
            let flag = |b: bool| PyRef::borrow(if b { Py_True() } else { Py_False() });
            let threads = opt_int(opts, c"wsgi_threads", 0, 0, 1024)?;
            let multithread = opt_bool(opts, c"wsgi_multithread", false)? || threads > 1;
            py::dict_set(d, i.wsgi_multithread, flag(multithread))?;
            py::dict_set(
                d,
                i.wsgi_multiprocess,
                flag(opt_bool(opts, c"wsgi_multiprocess", false)?),
            )?;
            py::dict_set(d, i.wsgi_run_once, flag(false))?;
            py::dict_set(d, i.wsgi_input_terminated, flag(true))?;
            let wrapper = py::import(c"weft.wsgi")?;
            py::dict_set(
                d,
                i.wsgi_file_wrapper,
                py::getattr(wrapper.ptr(), c"FileWrapper")?,
            )?;
            let pool = if threads > 0 {
                Some(wsgi::Pool::new(threads).map_err(|e| os_error(e))?)
            } else {
                None
            };
            Some(wsgi::WsgiCtx::new(env, py::str_view(root.ptr())?, pool)?)
        }
        _ => None,
    };

    let rate = if let Some(spec) = opt(opts, c"rate_limit")? {
        let spec = String::from_utf8_lossy(py::str_view(spec)?);
        let (count, period) =
            crate::limit::parse(&spec).map_err(|e| py::raise(PyExc_ValueError, &e))?;
        let burst = opt_int(opts, c"rate_limit_burst", count as usize, 1, 1 << 24)? as u32;
        let map = match opt(opts, c"rate_limit_map")? {
            Some(v) => Some(String::from_utf8_lossy(py::str_view(v)?).into_owned()),
            None => None,
        };
        let rate = crate::limit::RateLimit::new(count, period, burst);
        rate.install(map.as_deref()).map_err(|e| os_error(e))?;
        Some(rate)
    } else {
        None
    };

    let static_dirs = match opt(opts, c"static_dirs")? {
        Some(v) => {
            let mut specs = Vec::new();
            let n = PyList_Check(v);
            if n != 0 {
                for i in 0..PyList_GET_SIZE(v) {
                    specs.push(
                        String::from_utf8_lossy(py::str_view(PyList_GET_ITEM(v, i))?).into_owned(),
                    );
                }
            } else {
                specs.push(String::from_utf8_lossy(py::str_view(v)?).into_owned());
            }
            crate::staticf::parse(&specs).map_err(|e| py::raise(PyExc_ValueError, &e))?
        }
        None => Vec::new(),
    };
    let cache = match opt_int(opts, c"cache_size", 0, 0, 1 << 20)? {
        0 => None,
        mib => {
            let max_object = opt_int(opts, c"cache_max_object", 1024, 1, 65_536)? * 1024;
            let ttl_max = opt_int(opts, c"cache_ttl_max", 300, 1, 86_400)? as u64;
            let map = match opt(opts, c"cache_map")? {
                Some(v) => Some(String::from_utf8_lossy(py::str_view(v)?).into_owned()),
                None => None,
            };
            crate::cache::install(mib * 1024 * 1024, max_object, map.as_deref())
                .map_err(|e| os_error(e))?;
            if opt_bool(opts, c"cache_flush", false)? {
                crate::cache::flush();
            }
            Some(crate::cache::Cfg {
                max_object,
                ttl_max,
            })
        }
    };

    let http2 = opt_bool(opts, c"http2", true)?;
    let http2_only = opt_bool(opts, c"http2_only", false)?;
    let tls_certs = str_list(opts, c"tls_certs")?;
    let tls_keys = str_list(opts, c"tls_keys")?;
    if tls_certs.len() != tls_keys.len() {
        return Err(py::raise(
            PyExc_ValueError,
            "--tls-cert and --tls-key go together, one key per certificate",
        ));
    }
    let pairs: Vec<_> = tls_certs.into_iter().zip(tls_keys).collect();
    let tls = if pairs.is_empty() {
        None
    } else {
        Some(
            crate::tls::load(&pairs, crate::tls::alpn_list(http2, http2_only))
                .map_err(|e| py::raise(PyExc_ValueError, &e))?,
        )
    };
    let http3 = opt_bool(opts, c"http3", false)?;
    if http3 && unix_sock {
        return Err(py::raise(
            PyExc_ValueError,
            "--http3 cannot be served over a unix socket",
        ));
    }
    if http3 && pairs.is_empty() {
        return Err(py::raise(
            PyExc_ValueError,
            "--http3 needs --tls-cert and --tls-key: QUIC has no cleartext form",
        ));
    }
    let quic_port = match opt_int(opts, c"quic_port", 0, 0, 65535)? {
        0 => port,
        p => p as u16,
    };
    let quic = if http3 && opt_bool(opts, c"http3_listen", true)? {
        Some(crate::core::QuicListen {
            host: host.clone(),
            port: quic_port,
            server: crate::h3c::server_config(&pairs)
                .map_err(|e| py::raise(PyExc_ValueError, &e))?,
        })
    } else {
        None
    };
    let alt_svc = http3.then_some(quic_port);
    let h3_extensions = if http3 {
        let ext = PyRef::own(PyDict_New())?;
        py::dict_set(
            ext.ptr(),
            i.http_response_informational,
            PyRef::own(PyDict_New())?,
        )?;
        py::dict_set(
            ext.ptr(),
            i.http_response_early_hint,
            PyRef::own(PyDict_New())?,
        )?;
        py::dict_set(ext.ptr(), i.webtransport, PyRef::own(PyDict_New())?)?;
        Some(ext)
    } else {
        None
    };
    if tls.is_some() {
        py::dict_set(proto.ptr(), i.scheme, py::str_ascii(b"https")?)?;
        py::dict_set(ws_proto.ptr(), i.scheme, PyRef::borrow(i.wss))?;
        if let Some(w) = &wsgi {
            py::dict_set(w.proto.ptr(), i.wsgi_url_scheme, py::str_ascii(b"https")?)?;
        }
    }
    let hsts = match opt(opts, c"hsts")? {
        Some(v) => {
            let n = py::i64_arg(v)?.max(0) as u64;
            let mut s = b"max-age=".to_vec();
            crate::http::push_int(&mut s, n);
            Some(s)
        }
        None => None,
    };

    let max_conns = opt_int(opts, c"max_connections", 4096, 1, 1 << 24)?;
    let metrics_port = opt_int(opts, c"metrics_port", 0, 0, 65535)? as u16;
    let metrics_bind = if metrics_port == 0 {
        None
    } else {
        let map = match opt(opts, c"metrics_map")? {
            Some(v) => Some(String::from_utf8_lossy(py::str_view(v)?).into_owned()),
            None => None,
        };
        let workers = opt_int(opts, c"metrics_workers", 1, 1, 4096)? as u64;
        crate::metrics::install(map.as_deref(), workers).map_err(|e| os_error(e))?;
        crate::metrics::bind(opt_int(opts, c"metrics_slot", 0, 0, 63)?, max_conns as u64);
        if opt_bool(opts, c"metrics_listen", true)? {
            let host = match opt(opts, c"metrics_host")? {
                Some(v) => String::from_utf8_lossy(py::str_view(v)?).into_owned(),
                None => String::from("127.0.0.1"),
            };
            Some((host, metrics_port))
        } else {
            None
        }
    };

    let ctx = AppCtx {
        app: PyRef::borrow(app),
        loop_: PyRef::borrow(loop_),
        task_type,
        future_type,
        kw_loop,
        scope_proto: proto,
        ws_scope_proto: ws_proto,
        ws,
        state,
        report,
        done: types::ready(None)?,
        keep_alive: opt_secs(opts, c"keep_alive", 5.0)?,
        header_timeout: opt_secs(opts, c"header_timeout", 10.0)?,
        max_head,
        max_body,
        request_timeout,
        server_header: opt_bool(opts, c"server_header", true)?,
        date_header: opt_bool(opts, c"date_header", true)?,
        wsgi,
        trusted,
        request_id: opt_bool(opts, c"request_id", false)?,
        trace_context: opt_bool(opts, c"trace_context", false)?,
        request_start: opt_bool(opts, c"request_start_header", false)?,
        health,
        max_conns,
        rate,
        static_dirs,
        compress: opt_bool(opts, c"compress", false)?,
        compress_static: opt_bool(opts, c"compress_static", false)?,
        compress_min: opt_int(opts, c"compress_min_size", 1024, 0, 1 << 30)?,
        cache,
        https: secure || tls.is_some(),
        tls,
        hsts,
        http2,
        http2_only,
        metrics_bind,
        quic,
        alt_svc,
        h3_extensions,
    };
    ops::configure(match opt(opts, c"access_log")? {
        Some(v) if py::str_view(v)? == b"json" => Some(true),
        Some(v) if py::str_view(v)? == b"text" => Some(false),
        _ => None,
    });
    let exclusive = opt_bool(opts, c"exclusive", false)?;
    (*w.core)
        .serve(fd, ctx, exclusive)
        .map_err(|e| os_error(e))?;
    Ok(py::none())
});

method!(w_shutdown, |o, _args, _nargs| {
    (*worker(o)?.core).shutdown();
    Ok(py::none())
});

method!(w_drain, |o, _args, _nargs| {
    (*worker(o)?.core).drain();
    Ok(py::none())
});

method!(w_connections, |o, _args, _nargs| {
    py::int((*worker(o)?.core).connections() as i64)
});

method!(w_close, |o, _args, _nargs| {
    let w = &mut *(o as *mut WorkerObj);
    if w.core.is_null() {
        return Ok(py::none());
    }
    let w = worker(o)?;
    if w.in_select {
        return Err(py::runtime_error(
            "cannot close a worker from inside its own select()",
        ));
    }
    close_core(w);
    Ok(py::none())
});

unsafe fn worker_type() -> PResult<*mut PyObject> {
    if let Some(t) = WORKER_TYPE.get() {
        return Ok(t.0);
    }
    macro_rules! m {
        ($name:literal, $f:ident, $flags:expr) => {
            PyMethodDef {
                ml_name: $name.as_ptr(),
                ml_meth: PyMethodDefPointer {
                    PyCFunctionFast: $f,
                },
                ml_flags: $flags,
                ml_doc: ptr::null(),
            }
        };
    }
    let methods: &'static mut [PyMethodDef] = Box::leak(Box::new([
        m!(c"select", w_select, METH_FASTCALL),
        m!(c"register", w_register, METH_FASTCALL),
        m!(c"modify", w_modify, METH_FASTCALL),
        m!(c"unregister", w_unregister, METH_FASTCALL),
        m!(c"wakeup", w_wakeup, METH_FASTCALL),
        m!(c"serve", w_serve, METH_FASTCALL),
        m!(c"shutdown", w_shutdown, METH_FASTCALL),
        m!(c"drain", w_drain, METH_FASTCALL),
        m!(c"connections", w_connections, METH_FASTCALL),
        m!(c"close", w_close, METH_FASTCALL),
        PyMethodDef::zeroed(),
    ]));
    let t = unsafe {
        types::new_type(
            c"weft._weft.Worker",
            std::mem::size_of::<WorkerObj>(),
            Py_TPFLAGS_DEFAULT | Py_TPFLAGS_IMMUTABLETYPE,
            vec![
                types::slot(Py_tp_new, worker_new as *mut c_void),
                types::slot(Py_tp_dealloc, worker_dealloc as *mut c_void),
                types::slot(Py_tp_methods, methods.as_mut_ptr() as *mut c_void),
            ],
        )?
    };
    let _ = WORKER_TYPE.set(Static(t));
    Ok(t)
}

unsafe extern "C" fn module_exec(m: *mut PyObject) -> c_int {
    unsafe {
        let r: PResult<()> = (|| {
            interned::init()?;
            types::init()?;
            wsgi::init()?;
            let wt = worker_type()?;
            if PyModule_AddObjectRef(m, c"Worker".as_ptr(), wt) < 0
                || PyModule_AddObjectRef(
                    m,
                    c"ClientDisconnected".as_ptr(),
                    types::types().client_disconnected.0,
                ) < 0
            {
                return Err(PyErr);
            }
            for (name, v) in [
                (c"EVENT_READ", fdpoll::READ),
                (c"EVENT_WRITE", fdpoll::WRITE),
            ] {
                let v = py::int(v as i64)?;
                if PyModule_AddObjectRef(m, name.as_ptr(), v.ptr()) < 0 {
                    return Err(PyErr);
                }
            }
            let version = py::str_ascii(env!("CARGO_PKG_VERSION").as_bytes())?;
            if PyModule_AddObjectRef(m, c"__version__".as_ptr(), version.ptr()) < 0 {
                return Err(PyErr);
            }
            Ok(())
        })();
        if r.is_ok() { 0 } else { -1 }
    }
}

const SLOTS_LEN: usize = 2 + cfg!(Py_3_12) as usize + cfg!(Py_GIL_DISABLED) as usize;

static mut SLOTS: [PyModuleDef_Slot; SLOTS_LEN] = [
    PyModuleDef_Slot {
        slot: Py_mod_exec,
        value: module_exec as *mut c_void,
    },
    #[cfg(Py_3_12)]
    PyModuleDef_Slot {
        slot: Py_mod_multiple_interpreters,
        value: Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED,
    },
    #[cfg(Py_GIL_DISABLED)]
    PyModuleDef_Slot {
        slot: Py_mod_gil,
        value: Py_MOD_GIL_NOT_USED,
    },
    PyModuleDef_Slot {
        slot: 0,
        value: ptr::null_mut(),
    },
];

static mut MODULE_DEF: PyModuleDef = PyModuleDef {
    m_base: PyModuleDef_HEAD_INIT,
    m_name: c"_weft".as_ptr(),
    m_doc: c"ASGI server on Tokio, running in the asyncio thread.".as_ptr(),
    m_size: 0,
    m_methods: ptr::null_mut(),
    m_slots: (&raw mut SLOTS).cast(),
    m_traverse: None,
    m_clear: None,
    m_free: None,
};

#[allow(non_snake_case, clippy::missing_safety_doc)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn PyInit__weft() -> *mut PyObject {
    unsafe { PyModuleDef_Init(&raw mut MODULE_DEF) }
}
