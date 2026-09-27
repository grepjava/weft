//! TLS on the existing TCP readiness path: rustls records, Tokio `try_read` /
//! `try_write`. `--tls-cert` / `--tls-key` are paired; the first is the
//! default, the rest are chosen by SNI. A name no certificate claims still
//! gets the default, so a browser can explain the mismatch.

use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use rustls::pki_types::CertificateDer;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{ServerConfig, ServerConnection};
use tokio::io::Interest;
use tokio::net::TcpStream;

pub struct Io {
    pub tcp: TcpStream,
    tls: RefCell<Option<Box<Tls>>>,
}

struct Tls {
    conn: ServerConnection,
    plain: BytesMut,
}

impl Io {
    pub fn plain(tcp: TcpStream) -> Io {
        Io {
            tcp,
            tls: RefCell::new(None),
        }
    }

    pub fn server(tcp: TcpStream, cfg: Arc<ServerConfig>) -> io::Result<Io> {
        let conn = ServerConnection::new(cfg).map_err(tls_err)?;
        Ok(Io {
            tcp,
            tls: RefCell::new(Some(Box::new(Tls {
                conn,
                plain: BytesMut::new(),
            }))),
        })
    }

    pub fn is_tls(&self) -> bool {
        self.tls.borrow().is_some()
    }

    pub fn alpn(&self) -> Option<Vec<u8>> {
        self.tls
            .borrow()
            .as_ref()
            .and_then(|t| t.conn.alpn_protocol().map(|p| p.to_vec()))
    }

    pub fn tls_wants_write(&self) -> bool {
        self.tls
            .borrow()
            .as_ref()
            .is_some_and(|t| t.conn.wants_write())
    }

    pub async fn handshake(&self) -> io::Result<()> {
        loop {
            {
                let mut g = self.tls.borrow_mut();
                let Some(t) = g.as_mut() else { return Ok(()) };
                if !t.conn.is_handshaking() {
                    return Ok(());
                }
                // Read any bytes already in the socket before waiting: the
                // ClientHello often arrives with the accept, and an edge-
                // triggered ready() would miss it.
                match pull_tls(&self.tcp, t) {
                    Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
                match flush_tls(&self.tcp, &mut t.conn) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
                if !t.conn.is_handshaking() {
                    return Ok(());
                }
            }
            let want_read = self
                .tls
                .borrow()
                .as_ref()
                .is_some_and(|t| t.conn.wants_read());
            let want_write = self
                .tls
                .borrow()
                .as_ref()
                .is_some_and(|t| t.conn.wants_write());
            match (want_read, want_write) {
                (true, true) => {
                    self.tcp
                        .ready(Interest::READABLE | Interest::WRITABLE)
                        .await?
                }
                (true, false) => self.tcp.ready(Interest::READABLE).await?,
                (false, true) => self.tcp.ready(Interest::WRITABLE).await?,
                (false, false) => return Ok(()),
            };
        }
    }

    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut g = self.tls.borrow_mut();
        let Some(t) = g.as_mut() else {
            drop(g);
            return self.tcp.try_read(buf);
        };
        if t.plain.is_empty() {
            match pull_tls(&self.tcp, t) {
                Ok(0) => return Ok(0),
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            let _ = flush_tls(&self.tcp, &mut t.conn);
        }
        if t.plain.is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = t.plain.len().min(buf.len());
        buf[..n].copy_from_slice(&t.plain[..n]);
        t.plain.advance(n);
        Ok(n)
    }

    pub fn try_read_buf(&self, buf: &mut BytesMut) -> io::Result<usize> {
        let mut tmp = [0u8; 16 * 1024];
        let n = self.try_read(&mut tmp)?;
        if n > 0 {
            buf.extend_from_slice(&tmp[..n]);
        }
        Ok(n)
    }

    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        let mut g = self.tls.borrow_mut();
        let Some(t) = g.as_mut() else {
            drop(g);
            return self.tcp.try_write(buf);
        };
        flush_tls(&self.tcp, &mut t.conn)?;
        if buf.is_empty() {
            return Ok(0);
        }
        let n = t.conn.writer().write(buf)?;
        let _ = flush_tls(&self.tcp, &mut t.conn);
        Ok(n)
    }

    pub fn flush_tls_only(&self) -> io::Result<bool> {
        let mut g = self.tls.borrow_mut();
        let Some(t) = g.as_mut() else {
            return Ok(true);
        };
        match flush_tls(&self.tcp, &mut t.conn) {
            Ok(()) => Ok(!t.conn.wants_write()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn poll_read_ready(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if self
            .tls
            .borrow()
            .as_ref()
            .is_some_and(|t| !t.plain.is_empty())
        {
            return std::task::Poll::Ready(Ok(()));
        }
        self.tcp.poll_read_ready(cx)
    }

    pub fn poll_write_ready(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        self.tcp.poll_write_ready(cx)
    }

    pub async fn readable(&self) -> io::Result<()> {
        if self
            .tls
            .borrow()
            .as_ref()
            .is_some_and(|t| !t.plain.is_empty())
        {
            return Ok(());
        }
        self.tcp.readable().await
    }

    pub async fn writable(&self) -> io::Result<()> {
        self.tcp.writable().await
    }

    pub fn shutdown_write(&self) {
        if let Some(t) = self.tls.borrow_mut().as_mut() {
            t.conn.send_close_notify();
            let _ = flush_tls(&self.tcp, &mut t.conn);
        }
        let _ = socket2::SockRef::from(&self.tcp).shutdown(std::net::Shutdown::Write);
    }
}

fn pull_tls(tcp: &TcpStream, t: &mut Tls) -> io::Result<usize> {
    let mut tmp = [0u8; 16 * 1024];
    let n = tcp.try_read(&mut tmp)?;
    if n == 0 {
        return Ok(0);
    }
    let mut rest = &tmp[..n];
    while !rest.is_empty() {
        match t.conn.read_tls(&mut rest) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
    let state = t.conn.process_new_packets().map_err(tls_err)?;
    let want = state.plaintext_bytes_to_read();
    if want > 0 {
        t.plain.reserve(want);
        let mut buf = vec![0u8; want];
        let got = t.conn.reader().read(&mut buf).unwrap_or(0);
        t.plain.extend_from_slice(&buf[..got]);
    }
    Ok(n)
}

fn flush_tls(tcp: &TcpStream, conn: &mut ServerConnection) -> io::Result<()> {
    let mut w = TryTcp(tcp);
    while conn.wants_write() {
        match conn.write_tls(&mut w) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Err(e),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

struct TryTcp<'a>(&'a TcpStream);

impl Write for TryTcp<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.try_write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn tls_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

/// Loads PEM certificate/key pairs. The first is the default.
pub fn load(pairs: &[(String, String)], alpn: Vec<Vec<u8>>) -> Result<Arc<ServerConfig>, String> {
    Ok(Arc::new(load_config(pairs, alpn)?))
}

pub fn load_config(pairs: &[(String, String)], alpn: Vec<Vec<u8>>) -> Result<ServerConfig, String> {
    if pairs.is_empty() {
        return Err("tls needs a certificate and a key".into());
    }
    let mut names = Vec::new();
    let mut keys = Vec::new();
    for (cert, key) in pairs {
        let (ck, sans) = certified(cert, key)?;
        let ck = Arc::new(ck);
        for n in sans {
            names.push((n, ck.clone()));
        }
        keys.push(ck);
    }
    let resolver = Arc::new(Resolver {
        default: keys[0].clone(),
        names,
    });
    let mut cfg =
        ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_no_client_auth()
            .with_cert_resolver(resolver);
    cfg.alpn_protocols = alpn;
    Ok(cfg)
}

pub fn alpn_list(http2: bool, http2_only: bool) -> Vec<Vec<u8>> {
    match (http2, http2_only) {
        (_, true) => vec![b"h2".to_vec()],
        (false, _) => vec![b"http/1.1".to_vec()],
        (true, false) => vec![b"h2".to_vec(), b"http/1.1".to_vec()],
    }
}

fn certified(cert_path: &str, key_path: &str) -> Result<(CertifiedKey, Vec<String>), String> {
    let cert_pem = std::fs::read(cert_path).map_err(|e| format!("tls-cert {cert_path}: {e}"))?;
    let key_pem = std::fs::read(key_path).map_err(|e| format!("tls-key {key_path}: {e}"))?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("tls-cert {cert_path}: {e}"))?;
    if certs.is_empty() {
        return Err(format!("tls-cert {cert_path} has no certificates"));
    }
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(|e| format!("tls-key {key_path}: {e}"))?
        .ok_or_else(|| format!("tls-key {key_path} has no private key"))?;
    let signing = rustls::crypto::ring::sign::any_supported_type(&key)
        .map_err(|e| format!("tls-key {key_path}: {e}"))?;
    let sans = dns_names(&certs[0]);
    Ok((CertifiedKey::new(certs, signing), sans))
}

fn dns_names(der: &CertificateDer<'_>) -> Vec<String> {
    let Ok((_, cert)) = x509_parser::parse_x509_certificate(der.as_ref()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for n in &san.value.general_names {
            if let x509_parser::extensions::GeneralName::DNSName(d) = n {
                out.push(d.to_ascii_lowercase());
            }
        }
    }
    if out.is_empty()
        && let Some(cn) = cert
            .subject()
            .iter_common_name()
            .next()
            .and_then(|a| a.as_str().ok())
    {
        out.push(cn.to_ascii_lowercase());
    }
    out
}

#[derive(Debug)]
struct Resolver {
    default: Arc<CertifiedKey>,
    names: Vec<(String, Arc<CertifiedKey>)>,
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        if let Some(name) = hello.server_name()
            && let Some(k) = self.lookup(name)
        {
            return Some(k);
        }
        Some(self.default.clone())
    }
}

impl Resolver {
    fn lookup(&self, name: &str) -> Option<Arc<CertifiedKey>> {
        let name = name.to_ascii_lowercase();
        for (pat, key) in &self.names {
            if host_matches(pat, &name) {
                return Some(key.clone());
            }
        }
        None
    }
}

/// RFC 6125: a `*` covers exactly one label.
fn host_matches(pat: &str, name: &str) -> bool {
    if let Some(rest) = pat.strip_prefix("*.") {
        let Some((left, right)) = name.split_once('.') else {
            return false;
        };
        return !left.is_empty() && !left.contains('.') && right == rest;
    }
    pat.eq_ignore_ascii_case(name)
}

/// `Io` as Tokio `AsyncRead`/`AsyncWrite` for the `h2` crate.
pub struct AsyncIo {
    pub io: Io,
    prefix: BytesMut,
}

impl AsyncIo {
    pub fn new(io: Io) -> AsyncIo {
        AsyncIo {
            io,
            prefix: BytesMut::new(),
        }
    }

    pub fn prefixed(io: Io, prefix: BytesMut) -> AsyncIo {
        AsyncIo { io, prefix }
    }
}

impl tokio::io::AsyncRead for AsyncIo {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if !self.prefix.is_empty() {
            let n = self.prefix.len().min(buf.remaining());
            buf.put_slice(&self.prefix[..n]);
            self.prefix.advance(n);
            return std::task::Poll::Ready(Ok(()));
        }
        loop {
            let dest = buf.initialize_unfilled();
            match self.io.try_read(dest) {
                Ok(0) => return std::task::Poll::Ready(Ok(())),
                Ok(n) => {
                    buf.advance(n);
                    return std::task::Poll::Ready(Ok(()));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    match self.io.poll_read_ready(cx) {
                        std::task::Poll::Ready(Ok(())) => continue,
                        std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }
                Err(e) => return std::task::Poll::Ready(Err(e)),
            }
        }
    }
}

impl tokio::io::AsyncWrite for AsyncIo {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        loop {
            match self.io.try_write(buf) {
                Ok(n) => return std::task::Poll::Ready(Ok(n)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    match self.io.poll_write_ready(cx) {
                        std::task::Poll::Ready(Ok(())) => continue,
                        std::task::Poll::Ready(Err(e)) => return std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }
                Err(e) => return std::task::Poll::Ready(Err(e)),
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.io.flush_tls_only() {
            Ok(true) => std::task::Poll::Ready(Ok(())),
            Ok(false) => match self.io.poll_write_ready(cx) {
                std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(())),
                std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            Err(e) => std::task::Poll::Ready(Err(e)),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        self.io.shutdown_write();
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_one_label() {
        assert!(host_matches("*.example.com", "a.example.com"));
        assert!(!host_matches("*.example.com", "a.b.example.com"));
        assert!(!host_matches("*.example.com", "example.com"));
        assert!(host_matches("shop.example.com", "SHOP.example.com"));
    }
}
