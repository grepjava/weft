//! HTTP/3 over QUIC. Each request stream is a slab `Conn` so ASGI/WSGI
//! dispatch is the same path as HTTP/1.1; the response is written as HTTP/1.1
//! text and this adapter sends it as QPACK + DATA.
//!
//! WebTransport sessions share the connection: incoming WT streams are told
//! apart from requests by their first varint, and datagrams name a session by
//! the CONNECT stream's quarter identifier.

use std::cell::RefCell;
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::asgi;
use crate::core::{AppCtx, QuicListen, Shared};
use crate::http::{self, Conn, Phase};
use crate::wt::{self, Hub};
use ::http::{HeaderName, HeaderValue, Method, Request, Response, StatusCode};
use bytes::{Buf, Bytes, BytesMut};
use h3::ext::Protocol;
use h3::frame::FrameStream;
use h3::proto::frame::Frame;
use h3::quic::{
    BidiStream as BidiStreamTrait, OpenStreams, RecvStream, SendStream, SendStreamUnframed,
};
use h3::server::RequestStream;
use h3::stream::BufRecvStream;
use quinn::crypto::rustls::QuicServerConfig;

type H3Stream = RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;
type H3Send = RequestStream<h3_quinn::SendStream<Bytes>, Bytes>;
type H3Recv = RequestStream<h3_quinn::RecvStream, Bytes>;

pub struct H3Tx {
    stream: Option<H3Send>,
    /// A WebTransport session's capsules; a request's body is read by its
    /// own task instead.
    recv: Option<H3Recv>,
    head_sent: bool,
    finished: bool,
}

impl H3Tx {
    fn new(stream: H3Send, recv: Option<H3Recv>) -> H3Tx {
        H3Tx {
            stream: Some(stream),
            recv,
            head_sent: false,
            finished: false,
        }
    }
}

impl http::BodySource for H3Recv {
    fn poll_piece(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>, ()>> {
        match self.poll_recv_data(cx) {
            Poll::Ready(Ok(Some(mut chunk))) => {
                Poll::Ready(Ok(Some(chunk.copy_to_bytes(chunk.remaining()))))
            }
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(None)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct RestoreH3<'a> {
    conn: &'a Conn,
    tx: Option<H3Tx>,
}

impl RestoreH3<'_> {
    fn take(conn: &Conn) -> RestoreH3<'_> {
        RestoreH3 {
            conn,
            tx: conn.h3.borrow_mut().take(),
        }
    }
}

impl Drop for RestoreH3<'_> {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            *self.conn.h3.borrow_mut() = Some(tx);
        }
    }
}

pub async fn serve(sh: Rc<Shared>, cfg: QuicListen) {
    let Ok(endpoint) = bind(&cfg.host, cfg.port, cfg.server) else {
        return;
    };
    loop {
        if sh.stopping.get() {
            endpoint.close(0u32.into(), b"");
            return;
        }
        let incoming = tokio::select! {
            i = endpoint.accept() => i,
            _ = sh.stop.notified() => {
                endpoint.close(0u32.into(), b"");
                return;
            }
        };
        let Some(incoming) = incoming else { return };
        let sh = sh.clone();
        tokio::task::spawn_local(async move {
            connection(sh, incoming).await;
        });
    }
}

fn bind(host: &str, port: u16, server: quinn::ServerConfig) -> io::Result<quinn::Endpoint> {
    let spec = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let addr: SocketAddr = spec
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    quinn::Endpoint::server(server, addr)
}

enum H3Ev {
    Bidi(Box<h3_quinn::BidiStream<Bytes>>),
    Uni,
    Done,
}

#[allow(clippy::while_let_loop)]
async fn connection(sh: Rc<Shared>, incoming: quinn::Incoming) {
    let Some(ctx) = sh.app() else { return };
    let tcp = sh.conns.borrow().iter().filter(|(_, c)| !c.stream).count();
    if tcp >= sh.max_conns() {
        incoming.ignore();
        crate::metrics::rejected();
        return;
    }
    let connecting = match incoming.accept() {
        Ok(c) => c,
        Err(_) => return,
    };
    let conn = match connecting.await {
        Ok(c) => c,
        Err(_) => return,
    };
    if conn.handshake_data().and_then(|d| {
        d.downcast_ref::<quinn::crypto::rustls::HandshakeData>()
            .and_then(|h| h.protocol.as_ref().map(|p| p.as_slice() == b"h3"))
    }) != Some(true)
    {
        conn.close(0u32.into(), b"");
        return;
    }
    let peer = conn.remote_address();
    let parent = alloc_parent(&sh, peer);
    crate::metrics::accepted();
    let quic = conn.clone();
    let backend = h3_quinn::Connection::new(conn);
    let hub = Hub::new(quic, h3::quic::Connection::<Bytes>::opener(&backend));
    let mut builder = h3::server::builder();
    builder.max_field_section_size(ctx.max_head as u64);
    builder.enable_extended_connect(true);
    builder.enable_datagram(true);
    builder.enable_webtransport(true);
    builder.max_webtransport_sessions(16);
    builder.send_grease(false);
    let mut h3 = match builder.build::<h3_quinn::Connection, Bytes>(backend).await {
        Ok(c) => c,
        Err(_) => {
            teardown_parent(&sh, &parent);
            return;
        }
    };
    loop {
        drain_uni(&mut h3, &hub, &sh);
        let ev = tokio::select! {
            ev = poll_h3(&mut h3) => ev,
            dg = hub.quic.read_datagram() => {
                match dg {
                    Ok(b) => {
                        hub.datagram(&sh, b);
                        continue;
                    }
                    Err(_) => {
                        if hub.session_count() > 0 {
                            hold_wt(&sh, &hub).await;
                        }
                        H3Ev::Done
                    }
                }
            }
            _ = sh.stop.notified() => H3Ev::Done,
        };
        match ev {
            H3Ev::Bidi(bidi) => {
                let framed = FrameStream::new(BufRecvStream::new(*bidi));
                let mut resolver = h3.create_resolver(framed);
                let frame = poll_fn(|cx| resolver.frame_stream.poll_next(cx)).await;
                match frame {
                    Ok(Some(Frame::Headers(h))) => {
                        let sh = sh.clone();
                        let ctx = ctx.clone();
                        let hub = hub.clone();
                        let frame = Ok(Some(Frame::Headers(h)));
                        tokio::task::spawn_local(async move {
                            if let Ok(resolved) = resolver.accept_with_frame(frame)
                                && let Ok((req, stream)) = resolved.resolve().await
                            {
                                request(sh, ctx, req, stream, peer, hub).await;
                            }
                        });
                    }
                    Ok(Some(Frame::WebTransportStream(sid))) => {
                        let session_id = wt::session_u64(sid);
                        let stream_id = resolver.frame_stream.id().into_inner();
                        hub.incoming_bidi(
                            &sh,
                            session_id,
                            stream_id,
                            resolver.frame_stream.into_inner(),
                        );
                    }
                    _ => {
                        let stream_id = resolver.frame_stream.id().into_inner();
                        hub.incoming_bidi(&sh, 0, stream_id, resolver.frame_stream.into_inner());
                    }
                }
            }
            H3Ev::Uni => drain_uni(&mut h3, &hub, &sh),
            H3Ev::Done => {
                // Dropping `h3` closes the QUIC connection (H3_NO_ERROR) and
                // kills every WebTransport stream still using it.
                if hub.session_count() > 0 {
                    hold_wt(&sh, &hub).await;
                }
                break;
            }
        }
    }
    teardown_parent(&sh, &parent);
}

async fn hold_wt(sh: &Shared, hub: &Hub) {
    loop {
        if hub.session_count() == 0 || sh.stopping.get() {
            return;
        }
        tokio::select! {
            dg = hub.quic.read_datagram() => {
                match dg {
                    Ok(b) => hub.datagram(sh, b),
                    Err(_) => {
                        tokio::task::yield_now().await;
                        return;
                    }
                }
            }
            _ = sh.stop.notified() => return,
        }
    }
}

async fn poll_h3(h3: &mut h3::server::Connection<h3_quinn::Connection, Bytes>) -> H3Ev {
    poll_fn(|cx| {
        let _ = h3.inner.poll_accept_recv(cx);
        if !h3.inner.accepted_streams_mut().wt_uni_streams.is_empty() {
            return Poll::Ready(H3Ev::Uni);
        }
        match h3.poll_accept_request_stream(cx) {
            Poll::Ready(Ok(Some(s))) => Poll::Ready(H3Ev::Bidi(Box::new(s))),
            Poll::Ready(Ok(None)) => Poll::Ready(H3Ev::Done),
            Poll::Ready(Err(_)) => Poll::Ready(H3Ev::Done),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

fn drain_uni(h3: &mut h3::server::Connection<h3_quinn::Connection, Bytes>, hub: &Hub, sh: &Shared) {
    let streams = std::mem::take(&mut h3.inner.accepted_streams_mut().wt_uni_streams);
    for (sid, io) in streams {
        let session_id = wt::session_u64(sid);
        let stream_id = RecvStream::recv_id(&io).into_inner();
        hub.incoming_uni(sh, session_id, stream_id, io);
    }
}

async fn request(
    sh: Rc<Shared>,
    ctx: Rc<AppCtx>,
    req: Request<()>,
    mut stream: H3Stream,
    peer: SocketAddr,
    hub: Rc<Hub>,
) {
    if req.method() == Method::CONNECT {
        if req.extensions().get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT) {
            webtransport(sh, ctx, req, stream, peer, hub).await;
            return;
        }
        let mut res = Response::new(());
        *res.status_mut() = StatusCode::NOT_IMPLEMENTED;
        let _ = stream.send_response(res).await;
        let _ = stream.finish().await;
        return;
    }
    let parts = req.into_parts().0;
    let (send, mut recv) = stream.split();
    // The body is read after dispatch, as the application asks for it. A
    // stream that ended with its headers has none; one look tells.
    let first = http::BodySource::poll_piece(&mut recv, &mut Context::from_waker(Waker::noop()));
    if matches!(first, Poll::Ready(Err(()))) {
        return;
    }
    let ended = matches!(first, Poll::Ready(Ok(None)));
    let (mut rbuf, chunked) = http::synthesize(&parts, ended);
    let mut body = http::StreamBody::new(recv, chunked);
    match first {
        Poll::Ready(Ok(Some(data))) => body.push(&mut rbuf, &data),
        Poll::Ready(Ok(None)) => body.end(&mut rbuf),
        _ => {}
    }
    let conn = alloc_stream(&sh, peer, send, None);
    conn.st.borrow_mut().h3 = true;
    match asgi::dispatch(&sh, &ctx, &conn, &mut rbuf) {
        asgi::Parsed::Dispatched => {
            drive(&ctx, &conn, &mut rbuf, &mut body).await;
        }
        asgi::Parsed::Wsgi(environ) => {
            if http::read_stream_body(&ctx, &conn, &mut rbuf, &mut body).await {
                let _ = crate::wsgi::run(&sh, &ctx, &conn, &mut rbuf, environ).await;
            }
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

fn wake_http_recv(conn: &Conn) {
    let mut st = conn.st.borrow_mut();
    let wake = if st.recv_waiter.is_some() {
        if st.rejected {
            st.recv_waiter.take().map(|w| (w, None))
        } else if !st.final_delivered && (!st.body.buf.is_empty() || st.body.done) {
            let data = st.body.buf.split();
            let more = !st.body.done;
            st.final_delivered = !more;
            st.recv_waiter.take().map(|w| (w, Some((data, more))))
        } else if st.disconnected || st.resp.phase == Phase::Done {
            st.recv_waiter.take().map(|w| (w, None))
        } else {
            None
        }
    } else {
        None
    };
    let drained = if !st.drain_waiters.is_empty() && (st.out.is_empty() || st.disconnected) {
        std::mem::take(&mut st.drain_waiters)
    } else {
        Vec::new()
    };
    drop(st);
    if wake.is_some() || !drained.is_empty() {
        unsafe {
            if let Some((fut, msg)) = wake {
                asgi::resolve_receive(fut, msg);
            }
            for fut in drained {
                asgi::resolve_none(fut);
            }
        }
    }
}

async fn webtransport(
    sh: Rc<Shared>,
    ctx: Rc<AppCtx>,
    req: Request<()>,
    mut stream: H3Stream,
    peer: SocketAddr,
    hub: Rc<Hub>,
) {
    if ctx.wsgi.is_some() {
        let mut res = Response::new(());
        *res.status_mut() = StatusCode::NOT_IMPLEMENTED;
        let _ = stream.send_response(res).await;
        let _ = stream.finish().await;
        return;
    }
    if hub.session_count() >= wt::MAX_SESSIONS {
        let mut res = Response::new(());
        *res.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        let _ = stream.send_response(res).await;
        let _ = stream.finish().await;
        return;
    }
    let session_id = wt::session_id_from_connect(stream.id().into_inner());
    let session = wt::Session::new(session_id);
    let parts = req.into_parts().0;
    let (rbuf, _) = http::synthesize(&parts, true);
    let (send, recv) = stream.split();
    let conn = alloc_stream(&sh, peer, send, Some(recv));
    conn.st.borrow_mut().h3 = true;
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut parsed = httparse::Request::new(&mut headers);
    if !matches!(parsed.parse(&rbuf), Ok(httparse::Status::Complete(_))) {
        fail_connect(&conn, StatusCode::BAD_REQUEST).await;
        sh.conns.borrow_mut().try_remove(conn.slot as usize);
        return;
    }
    let meta = request_meta_h3(&ctx, &conn, &parsed);
    let started = unsafe { wt::start(&sh, &ctx, &conn, &parsed, session.clone(), &meta) };
    if started.is_err() {
        unsafe { crate::py::report_exception(ctx.report.as_ref().map(|r| r.ptr())) };
        fail_connect(&conn, StatusCode::INTERNAL_SERVER_ERROR).await;
        sh.conns.borrow_mut().try_remove(conn.slot as usize);
        return;
    }
    hub.register(&session, conn.slot, conn.generation);
    hub.adopt(&sh, &session);
    drive_wt(&sh, &hub, &session, &conn).await;
    hub.forget(session_id);
    sh.conns.borrow_mut().try_remove(conn.slot as usize);
}

async fn fail_connect(conn: &Conn, status: StatusCode) {
    let mut tx = conn.h3.borrow_mut().take();
    if let Some(t) = tx.as_mut()
        && let Some(s) = t.stream.as_mut()
    {
        let mut res = Response::new(());
        *res.status_mut() = status;
        let _ = s.send_response(res).await;
        let _ = s.finish().await;
    }
}

fn request_meta_h3(ctx: &AppCtx, conn: &Conn, req: &httparse::Request<'_, '_>) -> crate::ops::Meta {
    let mut meta = crate::ops::Meta::default();
    let trusted = ctx
        .trusted
        .as_ref()
        .filter(|t| t.allows(conn.unix, conn.peer.ip()));
    if let Some(t) = trusted {
        let via = crate::ops::forwarded(t, req.headers);
        meta.client = via.client;
        meta.secure = via.secure;
    }
    meta.secure = Some(true);
    meta
}

async fn drive_wt(sh: &Shared, hub: &Hub, session: &Rc<wt::Session>, conn: &Conn) {
    loop {
        let wait = session.kick.notified();
        wt::wake_recv(conn);
        if session.gone.get() && !session.accepted.get() {
            let _ = refuse_wt(conn).await;
            wt::wake_recv(conn);
            return;
        }
        if session.accepted.get() {
            break;
        }
        wait.await;
    }
    if accept_wt(conn).await.is_err() {
        session.mark_gone(0, Vec::new());
        wt::wake_recv(conn);
        return;
    }
    loop {
        let wait = session.kick.notified();
        if session.gone.get() {
            let _ = close_wt(hub, session, conn).await;
            wt::wake_recv(conn);
            return;
        }
        for dg in session.take_out_datagrams() {
            let mut pkt = Vec::with_capacity(8 + dg.len());
            wt::encode_varint(&mut pkt, wt::session_quarter(session.id));
            pkt.extend_from_slice(&dg);
            let _ = hub.quic.send_datagram(Bytes::from(pkt));
        }
        while let Some(bidi) = session.take_pending_open() {
            if open_stream(hub, session, bidi).await.is_ok() {
                wt::wake_recv(conn);
            }
        }
        wt::wake_recv(conn);
        if session.gone.get() {
            continue;
        }
        tokio::select! {
            r = recv_capsule(conn) => {
                if let Ok(Some(b)) = r {
                    wt::parse_capsules(&b, |code, reason| {
                        hub.close_peer(sh, session.id, code, reason);
                    });
                } else {
                    session.mark_gone(0, Vec::new());
                }
            }
            _ = wait => {}
        }
    }
}

async fn recv_capsule(conn: &Conn) -> Result<Option<Bytes>, ()> {
    let mut hold = RestoreH3::take(conn);
    let Some(tx) = hold.tx.as_mut() else {
        return std::future::pending().await;
    };
    if let Some(s) = tx.recv.as_mut() {
        match s.recv_data().await {
            Ok(Some(mut chunk)) => Ok(Some(chunk.copy_to_bytes(chunk.remaining()))),
            Ok(None) => Ok(None),
            Err(_) => Err(()),
        }
    } else {
        Err(())
    }
}

async fn accept_wt(conn: &Conn) -> io::Result<()> {
    let mut hold = RestoreH3::take(conn);
    let Some(tx) = hold.tx.as_mut() else {
        return Ok(());
    };
    if tx.head_sent {
        return Ok(());
    }
    let extra = conn.st.borrow_mut().out.split();
    let mut res = Response::new(());
    *res.status_mut() = StatusCode::OK;
    for (name, value) in parse_extra_headers(&extra) {
        if let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(&name),
            HeaderValue::from_bytes(&value),
        ) {
            res.headers_mut().append(n, v);
        }
    }
    let ok = if let Some(s) = tx.stream.as_mut() {
        s.send_response(res).await.is_ok()
    } else {
        false
    };
    if !ok {
        return Err(io::ErrorKind::BrokenPipe.into());
    }
    tx.head_sent = true;
    Ok(())
}

async fn refuse_wt(conn: &Conn) -> io::Result<()> {
    let mut hold = RestoreH3::take(conn);
    let Some(tx) = hold.tx.as_mut() else {
        return Ok(());
    };
    if tx.finished {
        return Ok(());
    }
    let status = conn.st.borrow().resp.status;
    let status = if status >= 400 { status } else { 403 };
    let mut res = Response::new(());
    *res.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::FORBIDDEN);
    if let Some(s) = tx.stream.as_mut() {
        let _ = s.send_response(res).await;
        let _ = s.finish().await;
    }
    tx.finished = true;
    Ok(())
}

async fn close_wt(hub: &Hub, session: &wt::Session, conn: &Conn) -> io::Result<()> {
    let _ = hub;
    {
        let hold = RestoreH3::take(conn);
        let sent = hold.tx.as_ref().is_some_and(|t| t.head_sent);
        drop(hold);
        if !sent {
            let _ = accept_wt(conn).await;
        }
    }
    let mut hold = RestoreH3::take(conn);
    let Some(tx) = hold.tx.as_mut() else {
        return Ok(());
    };
    let (code, reason) = session.close_info();
    if let Some(s) = tx.stream.as_mut() {
        let _ = s.send_data(wt::close_capsule(code, &reason)).await;
        let _ = s.finish().await;
    }
    tx.finished = true;
    Ok(())
}

async fn write_prefix<S>(send: &mut S, prefix: &[u8]) -> io::Result<()>
where
    S: SendStreamUnframed<Bytes>,
{
    let mut buf = BytesMut::from(prefix);
    while !buf.is_empty() {
        match poll_fn(|cx| SendStreamUnframed::poll_send(send, cx, &mut buf)).await {
            Ok(0) => tokio::task::yield_now().await,
            Ok(_) => {}
            Err(e) => return Err(io::Error::other(format!("{e:?}"))),
        }
    }
    Ok(())
}

async fn open_stream(hub: &Hub, session: &Rc<wt::Session>, bidi: bool) -> io::Result<()> {
    if bidi {
        let mut stream =
            poll_fn(|cx| OpenStreams::poll_open_bidi(&mut *hub.opener.borrow_mut(), cx))
                .await
                .map_err(|e| io::Error::other(format!("{e:?}")))?;
        write_prefix(&mut stream, &wt::wt_bidi_prefix(session.id)).await?;
        let id = SendStream::send_id(&stream).into_inner();
        let (send, recv) = BidiStreamTrait::split(stream);
        session.spawn_outgoing_h3(id, send, Some(recv));
    } else {
        let mut send = poll_fn(|cx| OpenStreams::poll_open_send(&mut *hub.opener.borrow_mut(), cx))
            .await
            .map_err(|e| io::Error::other(format!("{e:?}")))?;
        write_prefix(&mut send, &wt::wt_uni_prefix(session.id)).await?;
        let id = SendStream::send_id(&send).into_inner();
        session.spawn_outgoing_h3(id, send, None);
    }
    Ok(())
}

fn parse_extra_headers(extra: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut headers = Vec::new();
    for line in extra.split(|&c| c == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let Some(col) = line.iter().position(|&c| c == b':') else {
            continue;
        };
        let name = line[..col].trim_ascii().to_ascii_lowercase();
        let value = asgi::trim(&line[col + 1..]).to_vec();
        headers.push((name, value));
    }
    headers
}

async fn drive(
    ctx: &AppCtx,
    conn: &Conn,
    rbuf: &mut BytesMut,
    body: &mut http::StreamBody<H3Recv>,
) {
    let limit = ctx.request_timeout.unwrap_or(Duration::from_secs(30));
    let body_limit = ctx.request_timeout.unwrap_or(Duration::from_secs(86_400));
    loop {
        let want_body = {
            let mut st = conn.st.borrow_mut();
            if !st.body.done
                && !st.rejected
                && let Err(status) = st.body.feed(rbuf)
            {
                st.reject(status);
            }
            if !st.body.done && !st.rejected && body.ended() {
                // Shorter than its Content-Length.
                st.disconnected = true;
            }
            crate::staticf::pump(&mut st);
            !st.body.done
                && !st.rejected
                && !st.disconnected
                && !body.ended()
                && st.body.buf.len() < http::BODY_HWM
        };
        if flush_h3(conn).await.is_err() {
            conn.st.borrow_mut().disconnected = true;
            wake_http_recv(conn);
            return;
        }
        wake_http_recv(conn);
        {
            let st = conn.st.borrow();
            if st.disconnected
                || (st.resp.phase == Phase::Done && st.out.is_empty() && st.send_file.is_none())
            {
                return;
            }
        }
        let notified = std::future::poll_fn(|cx| {
            conn.set_waker(cx);
            let st = conn.st.borrow();
            if st.disconnected
                || !st.out.is_empty()
                || st.send_file.is_some()
                || st.resp.phase == Phase::Done
            {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        });
        if want_body {
            // `--request-timeout` bounds how long the body may stall.
            let got = tokio::select! {
                r = poll_fn(|cx| body.poll_into(rbuf, cx)) => Some(r),
                _ = notified => None,
                _ = tokio::time::sleep(body_limit) => {
                    conn.st.borrow_mut().reject(408);
                    None
                }
            };
            if let Some(Err(())) = got {
                conn.st.borrow_mut().disconnected = true;
                wake_http_recv(conn);
                return;
            }
        } else if tokio::time::timeout(limit, notified).await.is_err() {
            conn.st.borrow_mut().disconnected = true;
            wake_http_recv(conn);
            return;
        }
    }
}

pub async fn flush_h3(conn: &Conn) -> io::Result<bool> {
    let mut hold = RestoreH3::take(conn);
    let Some(tx) = hold.tx.as_mut() else {
        return Ok(true);
    };
    if tx.finished {
        return Ok(true);
    }
    loop {
        let (head, body, eos, interim) = {
            if tx.stream.is_none() {
                return Ok(true);
            }
            let mut st = conn.st.borrow_mut();
            if !tx.head_sent {
                let Some(end) = find_head_end(&st.out) else {
                    return Ok(true);
                };
                let head = st.out.split_to(end);
                let (status, _) = parse_h1_head(&head);
                if (100..200).contains(&status) {
                    (Some(head), Bytes::new(), false, true)
                } else {
                    let body = st.out.split().freeze();
                    let eos = st.resp.phase == Phase::Done && st.send_file.is_none();
                    (Some(head), body, eos, false)
                }
            } else {
                let body = st.out.split().freeze();
                let eos = st.resp.phase == Phase::Done && st.send_file.is_none();
                (None, body, eos, false)
            }
        };
        if let Some(head) = head {
            let (status, headers) = parse_h1_head(&head);
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
            let Some(stream) = tx.stream.as_mut() else {
                return Ok(true);
            };
            if stream.send_response(res).await.is_err() {
                conn.st.borrow_mut().disconnected = true;
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            if !interim {
                tx.head_sent = true;
            }
        }
        if interim {
            continue;
        }
        let Some(stream) = tx.stream.as_mut() else {
            return Ok(true);
        };
        if !body.is_empty() && stream.send_data(body).await.is_err() {
            conn.st.borrow_mut().disconnected = true;
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if eos {
            let _ = stream.finish().await;
            tx.finished = true;
        }
        return Ok(true);
    }
}

fn alloc_parent(sh: &Shared, peer: SocketAddr) -> Rc<Conn> {
    let generation = sh.next_generation();
    let mut conns = sh.conns.borrow_mut();
    let entry = conns.vacant_entry();
    let conn = Rc::new(Conn {
        slot: entry.key() as u32,
        generation,
        io: RefCell::new(None),
        peer,
        unix: false,
        stream: false,
        client: RefCell::new(None),
        remote: RefCell::new(None),
        st: RefCell::new(crate::http::State::new()),
        h2: RefCell::new(None),
        h3: RefCell::new(None),
        waker: RefCell::new(None),
    });
    entry.insert(conn.clone());
    conn
}

fn teardown_parent(sh: &Shared, conn: &Conn) {
    crate::metrics::closed();
    sh.conns.borrow_mut().try_remove(conn.slot as usize);
}

fn alloc_stream(sh: &Shared, peer: SocketAddr, stream: H3Send, recv: Option<H3Recv>) -> Rc<Conn> {
    let generation = sh.next_generation();
    let mut conns = sh.conns.borrow_mut();
    let entry = conns.vacant_entry();
    let conn = Rc::new(Conn {
        slot: entry.key() as u32,
        generation,
        io: RefCell::new(None),
        peer,
        unix: false,
        stream: true,
        client: RefCell::new(None),
        remote: RefCell::new(None),
        st: RefCell::new(crate::http::State::new()),
        h2: RefCell::new(None),
        h3: RefCell::new(Some(H3Tx::new(stream, recv))),
        waker: RefCell::new(None),
    });
    entry.insert(conn.clone());
    conn
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

/// rustls config for QUIC: ALPN `h3` only.
pub fn server_config(pairs: &[(String, String)]) -> Result<quinn::ServerConfig, String> {
    let rustls = crate::tls::load_config(pairs, vec![b"h3".to_vec()])?;
    let crypto = QuicServerConfig::try_from(rustls).map_err(|e| format!("quic tls: {e}"))?;
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let mut transport = quinn::TransportConfig::default();
    transport.datagram_receive_buffer_size(Some(65_536 * 4));
    transport.datagram_send_buffer_size(65_536 * 4);
    transport.max_concurrent_bidi_streams(100u32.into());
    transport.max_concurrent_uni_streams(100u32.into());
    server.transport_config(Arc::new(transport));
    Ok(server)
}
