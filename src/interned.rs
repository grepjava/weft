//! Interned strings, created once per process.
//!
//! Every scope and message key is interned so that dict insertion and lookup
//! compare a cached hash and a pointer instead of hashing key bytes again.
//! Keys written as literals in application code (`'type'`, `'body'`) are
//! interned by the compiler, so lookups in messages the application builds
//! hit on pointer identity.

use std::sync::OnceLock;

use pyo3_ffi::*;

use crate::py::{PResult, PyRef, Static, intern};

macro_rules! interned {
    ($($field:ident = $s:literal),* $(,)?) => {
        pub struct Interned {
            $(pub $field: *mut PyObject,)*
        }

        unsafe impl Send for Interned {}
        unsafe impl Sync for Interned {}

        impl Interned {
            unsafe fn create() -> PResult<Self> {
                Ok(Self { $($field: unsafe { intern($s)? }.into_ptr(),)* })
            }
        }
    };
}

interned! {
    // scope keys
    type_ = c"type",
    asgi = c"asgi",
    version = c"version",
    spec_version = c"spec_version",
    http_version = c"http_version",
    server = c"server",
    client = c"client",
    scheme = c"scheme",
    method = c"method",
    root_path = c"root_path",
    path = c"path",
    raw_path = c"raw_path",
    query_string = c"query_string",
    headers = c"headers",
    state = c"state",
    // message keys
    body = c"body",
    more_body = c"more_body",
    status = c"status",
    // values
    http = c"http",
    v1_0 = c"1.0",
    v1_1 = c"1.1",
    v2 = c"2",
    v3 = c"3",
    v3_0 = c"3.0",
    v2_4 = c"2.4",
    http_request = c"http.request",
    http_disconnect = c"http.disconnect",
    websocket = c"websocket",
    ws = c"ws",
    wss = c"wss",
    websocket_connect = c"websocket.connect",
    websocket_receive = c"websocket.receive",
    websocket_disconnect = c"websocket.disconnect",
    websocket_http_response = c"websocket.http.response",
    http_response_informational = c"http.response.informational",
    http_response_early_hint = c"http.response.early_hint",
    links = c"links",
    text = c"text",
    bytes_ = c"bytes",
    code = c"code",
    reason = c"reason",
    subprotocol = c"subprotocol",
    subprotocols = c"subprotocols",
    extensions = c"extensions",
    webtransport = c"webtransport",
    webtransport_connect = c"webtransport.connect",
    webtransport_accept = c"webtransport.accept",
    webtransport_close = c"webtransport.close",
    webtransport_disconnect = c"webtransport.disconnect",
    webtransport_stream_opened = c"webtransport.stream.opened",
    webtransport_stream_receive = c"webtransport.stream.receive",
    webtransport_stream_open = c"webtransport.stream.open",
    webtransport_stream_send = c"webtransport.stream.send",
    webtransport_stream_pause = c"webtransport.stream.pause",
    webtransport_stream_resume = c"webtransport.stream.resume",
    webtransport_datagram_receive = c"webtransport.datagram.receive",
    webtransport_datagram_send = c"webtransport.datagram.send",
    stream = c"stream",
    bidirectional = c"bidirectional",
    more_data = c"more_data",
    end_stream = c"end_stream",
    data = c"data",
    https = c"https",
    m_get = c"GET",
    m_head = c"HEAD",
    m_post = c"POST",
    m_put = c"PUT",
    m_delete = c"DELETE",
    m_patch = c"PATCH",
    m_options = c"OPTIONS",
    // WSGI environ keys and values
    request_method = c"REQUEST_METHOD",
    script_name = c"SCRIPT_NAME",
    path_info = c"PATH_INFO",
    query_string_env = c"QUERY_STRING",
    content_type_env = c"CONTENT_TYPE",
    content_length_env = c"CONTENT_LENGTH",
    server_name = c"SERVER_NAME",
    server_port = c"SERVER_PORT",
    server_protocol = c"SERVER_PROTOCOL",
    remote_addr = c"REMOTE_ADDR",
    remote_port = c"REMOTE_PORT",
    http_cookie = c"HTTP_COOKIE",
    wsgi_version = c"wsgi.version",
    wsgi_url_scheme = c"wsgi.url_scheme",
    wsgi_input = c"wsgi.input",
    wsgi_errors = c"wsgi.errors",
    wsgi_multithread = c"wsgi.multithread",
    wsgi_multiprocess = c"wsgi.multiprocess",
    wsgi_run_once = c"wsgi.run_once",
    wsgi_input_terminated = c"wsgi.input_terminated",
    wsgi_file_wrapper = c"wsgi.file_wrapper",
    proto_1_0 = c"HTTP/1.0",
    proto_1_1 = c"HTTP/1.1",
    proto_3 = c"HTTP/3",
    empty_str = c"",
    // method and keyword names
    close = c"close",
    loop_ = c"loop",
    set_result = c"set_result",
    exception = c"exception",
    add_done_callback = c"add_done_callback",
}

static INTERNED: OnceLock<Interned> = OnceLock::new();
static EMPTY_BYTES: OnceLock<Static> = OnceLock::new();

#[inline]
pub fn s() -> &'static Interned {
    // Initialised in the module's exec slot, before any other code can run.
    unsafe { INTERNED.get().unwrap_unchecked() }
}

#[inline]
pub fn empty_bytes() -> *mut PyObject {
    unsafe { EMPTY_BYTES.get().unwrap_unchecked().0 }
}

pub unsafe fn init() -> PResult<()> {
    if INTERNED.get().is_none() {
        let i = unsafe { Interned::create()? };
        let _ = INTERNED.set(i);
    }
    if EMPTY_BYTES.get().is_none() {
        let b = unsafe { PyRef::own(PyBytes_FromStringAndSize(std::ptr::null(), 0))? };
        let _ = EMPTY_BYTES.set(Static(b.into_ptr()));
    }
    Ok(())
}
