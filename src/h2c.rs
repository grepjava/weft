//! HTTP/2: ALPN `h2` after TLS, or the cleartext prior-knowledge preface.
//! Each stream is a `Conn` in the slab so `send`/`receive` keep working;
//! hop-by-hop HTTP/1.1 framing is stripped when the response is sent.

use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::task::{Context, Poll};

use ::http::{HeaderName, HeaderValue, Request, Response, StatusCode};
use bytes::{Bytes, BytesMut};
use h2::SendStream;
use h2::server::{Connection, SendResponse};

use crate::asgi;
use crate::core::{AppCtx, Shared};
use crate::http::{self, Conn, Phase};
use crate::tls::{AsyncIo, Io};

pub const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub struct H2Tx {
    respond: Option<SendResponse<Bytes>>,
    send: Option<SendStream<Bytes>>,
}

impl H2Tx {
    fn new(respond: SendResponse<Bytes>) -> H2Tx {
        H2Tx {
            respond: Some(respond),
            send: None,
        }
    }

    pub fn flush(&mut self, st: &mut crate::http::State) -> io::Result<bool> {
        while self.send.is_none() && self.respond.is_some() {
            let Some(end) = find_head_end(&st.out) else {
                return Ok(true);
            };
            let head = st.out.split_to(end);
            let eos = st.out.is_empty() && st.resp.phase == Phase::Done && st.send_file.is_none();
            if !self.send_head(&head, eos)? {
                continue;
            }
            if eos {
                return Ok(true);
            }
            break;
        }
        if !st.out.is_empty() {
            let Some(send) = self.send.as_mut() else {
                return Ok(true);
            };
            send.reserve_capacity(st.out.len());
            let cap = send.capacity();
            if cap == 0 {
                return Ok(false);
            }
            let n = cap.min(st.out.len());
            let eos = st.resp.phase == Phase::Done && st.send_file.is_none() && n == st.out.len();
            let data = st.out.split_to(n).freeze();
            send.send_data(data, eos).map_err(h2_err)?;
            if !st.out.is_empty() {
                send.reserve_capacity(st.out.len());
                return Ok(false);
            }
        } else if st.resp.phase == Phase::Done
            && st.send_file.is_none()
            && let Some(send) = self.send.as_mut()
        {
            let _ = send.send_data(Bytes::new(), true);
        }
        Ok(true)
    }

    pub fn poll_capacity(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(send) = self.send.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        match send.poll_capacity(cx) {
            Poll::Ready(Some(Ok(_))) | Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(h2_err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn send_reset(tx: &mut H2Tx) {
    if let Some(mut r) = tx.respond.take() {
        r.send_reset(h2::Reason::INTERNAL_ERROR);
    } else if let Some(mut s) = tx.send.take() {
        s.send_reset(h2::Reason::INTERNAL_ERROR);
    }
}

impl H2Tx {
    fn send_head(&mut self, head: &[u8], eos: bool) -> io::Result<bool> {
        let Some(respond) = self.respond.as_mut() else {
            return Ok(true);
        };
        let (status, headers) = parse_h1_head(head);
        let mut res = Response::new(());
        *res.status_mut() =
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        for (name, value) in headers {
            if hop_by_hop(&name) {
                continue;
            }
            let Ok(n) = HeaderName::from_bytes(&name) else {
                continue;
            };
            let Ok(v) = HeaderValue::from_bytes(&value) else {
                continue;
            };
            res.headers_mut().append(n, v);
        }
        if (100..200).contains(&status) {
            respond.send_informational(res).map_err(h2_err)?;
            return Ok(false);
        }
        match respond.send_response(res, eos) {
            Ok(send) => {
                self.respond = None;
                if !eos {
                    self.send = Some(send);
                }
                Ok(true)
            }
            Err(e) => Err(h2_err(e)),
        }
    }
}

fn hop_by_hop(name: &[u8]) -> bool {
    matches!(
        name,
        b"connection"
            | b"keep-alive"
            | b"proxy-connection"
            | b"transfer-encoding"
            | b"upgrade"
            | b"te"
    )
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

#[allow(clippy::type_complexity)]
fn parse_h1_head(head: &[u8]) -> (u16, Vec<(Vec<u8>, Vec<u8>)>) {
    let mut status = 200u16;
    let mut headers = Vec::new();
    for (i, line) in head.split(|&c| c == b'\n').enumerate() {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            break;
        }
        if i == 0 {
            let mut parts = line.split(|&c| c == b' ');
            let _ = parts.next();
            if let Some(code) = parts.next()
                && let Ok(s) = std::str::from_utf8(code)
                && let Ok(n) = s.parse()
            {
                status = n;
            }
            continue;
        }
        let Some(col) = line.iter().position(|&c| c == b':') else {
            continue;
        };
        let name = line[..col].trim_ascii().to_ascii_lowercase();
        let value = crate::asgi::trim(&line[col + 1..]).to_vec();
        headers.push((name, value));
    }
    (status, headers)
}

fn h2_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, e.to_string())
}

pub async fn serve(
    sh: Rc<Shared>,
    ctx: Rc<AppCtx>,
    io: Io,
    prefix: BytesMut,
    peer: SocketAddr,
    unix: bool,
) {
    let io = if prefix.is_empty() {
        AsyncIo::new(io)
    } else {
        AsyncIo::prefixed(io, prefix)
    };
    let mut h2: Connection<AsyncIo, Bytes> = match h2::server::handshake(io).await {
        Ok(c) => c,
        Err(_) => return,
    };
    while let Some(Ok((req, respond))) = h2.accept().await {
        let sh = sh.clone();
        let ctx = ctx.clone();
        tokio::task::spawn_local(async move {
            stream(sh, ctx, req, respond, peer, unix).await;
        });
    }
}

async fn stream(
    sh: Rc<Shared>,
    ctx: Rc<AppCtx>,
    req: Request<h2::RecvStream>,
    respond: SendResponse<Bytes>,
    peer: SocketAddr,
    unix: bool,
) {
    let (parts, mut body) = req.into_parts();
    let mut payload = BytesMut::new();
    while let Some(chunk) = body.data().await {
        let Ok(chunk) = chunk else { return };
        let _ = body.flow_control().release_capacity(chunk.len());
        payload.extend_from_slice(&chunk);
        if payload.len() as u64 > ctx.max_body {
            let mut tx = H2Tx::new(respond);
            send_reset(&mut tx);
            return;
        }
    }
    let conn = alloc(&sh, peer, unix, respond);
    conn.st.borrow_mut().h2 = true;
    let mut rbuf = synthesize(&parts, payload.len());
    rbuf.extend_from_slice(&payload);
    let mut timer = std::pin::pin!(tokio::time::sleep(std::time::Duration::from_secs(86_400)));
    match asgi::dispatch(&sh, &ctx, &conn, &mut rbuf) {
        asgi::Parsed::Dispatched => {
            let mut p = http::Pump {
                eof: false,
                stall: http::Stall {
                    since: None,
                    armed: None,
                },
            };
            let _ = std::future::poll_fn(|cx| {
                http::pump(&sh, &ctx, &conn, &mut rbuf, &mut p, timer.as_mut(), cx)
            })
            .await;
        }
        asgi::Parsed::Wsgi(environ) => {
            let _ = crate::wsgi::run(&sh, &ctx, &conn, &mut rbuf, environ).await;
        }
        asgi::Parsed::Answered(_) => {
            let _ = http::flush_all(&ctx, &conn).await;
        }
        asgi::Parsed::Upgraded => {
            conn.st.borrow_mut().write_error(501);
            let _ = http::flush_all(&ctx, &conn).await;
        }
        asgi::Parsed::Partial => {
            conn.st.borrow_mut().write_error(400);
            let _ = http::flush_all(&ctx, &conn).await;
        }
        asgi::Parsed::Reject(status) => {
            conn.st.borrow_mut().write_error(status);
            let _ = http::flush_all(&ctx, &conn).await;
        }
    }
    let _ = http::flush_all(&ctx, &conn).await;
    sh.conns.borrow_mut().try_remove(conn.slot as usize);
}

fn alloc(sh: &Shared, peer: SocketAddr, unix: bool, respond: SendResponse<Bytes>) -> Rc<Conn> {
    let generation = sh.next_generation();
    let mut conns = sh.conns.borrow_mut();
    let entry = conns.vacant_entry();
    let conn = Rc::new(Conn {
        slot: entry.key() as u32,
        generation,
        io: RefCell::new(None),
        peer,
        unix,
        stream: true,
        client: RefCell::new(None),
        remote: RefCell::new(None),
        st: RefCell::new(crate::http::State::new()),
        h2: RefCell::new(Some(H2Tx::new(respond))),
        h3: RefCell::new(None),
        waker: RefCell::new(None),
    });
    entry.insert(conn.clone());
    conn
}

fn synthesize(parts: &::http::request::Parts, body_len: usize) -> BytesMut {
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
    if !has_cl {
        out.extend_from_slice(b"content-length: ");
        crate::http::push_int(&mut out, body_len as u64);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::http::Request;

    #[test]
    fn explicit_host_is_kept() {
        let req = Request::builder()
            .method("GET")
            .uri("/fresh")
            .header("host", "example.com")
            .body(())
            .unwrap();
        let parts = req.into_parts().0;
        let out = synthesize(&parts, 0);
        let text = String::from_utf8_lossy(&out).to_ascii_lowercase();
        assert!(text.contains("host: example.com"), "{text}");
    }

    #[test]
    fn informational_status_is_interim() {
        assert!((100..200).contains(&103));
        assert!((100..200).contains(&104));
        assert!(!(100..200).contains(&200));
    }
}
