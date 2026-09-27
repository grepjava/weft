//! What the server decides about a request before the application sees it:
//! trusted proxies, request IDs, trace context, queue time, and the access
//! log.

use std::cell::{Cell, RefCell};
use std::hash::{BuildHasher, Hasher};
use std::io::Write;
use std::net::IpAddr;
use std::time::Instant;

use crate::asgi::{eq_ci, trim};

// --- trusted proxies -------------------------------------------------------------

/// `--forwarded-allow-ips`: peers whose `X-Forwarded-*` and `Forwarded`
/// headers are believed.
pub struct Trusted {
    any: bool,
    unix: bool,
    nets: Vec<(IpAddr, u8)>,
}

impl Trusted {
    pub fn parse(spec: &str) -> Result<Trusted, String> {
        let mut t = Trusted {
            any: false,
            unix: false,
            nets: Vec::new(),
        };
        for item in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match item {
                "*" => t.any = true,
                "unix" => t.unix = true,
                _ => {
                    let (addr, bits) = match item.split_once('/') {
                        Some((a, b)) => (a, Some(b)),
                        None => (item, None),
                    };
                    let bad = || format!("invalid entry {item:?} in forwarded-allow-ips");
                    let ip = addr.parse::<IpAddr>().map_err(|_| bad())?.to_canonical();
                    let max = if ip.is_ipv4() { 32 } else { 128 };
                    let bits = match bits {
                        Some(b) => b.parse::<u8>().ok().filter(|&b| b <= max).ok_or_else(bad)?,
                        None => max,
                    };
                    t.nets.push((ip, bits));
                }
            }
        }
        Ok(t)
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        if self.any {
            return true;
        }
        let ip = ip.to_canonical();
        self.nets.iter().any(|&(net, bits)| in_net(ip, net, bits))
    }

    pub fn allows(&self, unix: bool, ip: IpAddr) -> bool {
        if self.any {
            return true;
        }
        if unix { self.unix } else { self.contains(ip) }
    }
}

fn in_net(ip: IpAddr, net: IpAddr, bits: u8) -> bool {
    match (ip, net) {
        (IpAddr::V4(a), IpAddr::V4(b)) => {
            let m = if bits == 0 {
                0
            } else {
                u32::MAX << (32 - bits as u32)
            };
            u32::from(a) & m == u32::from(b) & m
        }
        (IpAddr::V6(a), IpAddr::V6(b)) => {
            let m = if bits == 0 {
                0
            } else {
                u128::MAX << (128 - bits as u32)
            };
            u128::from(a) & m == u128::from(b) & m
        }
        _ => false,
    }
}

/// The address in a `Forwarded` `for=` node or an `X-Forwarded-For` entry,
/// without quotes, brackets or port.
fn node_addr(v: &[u8]) -> &[u8] {
    let v = trim(v);
    let v = v
        .strip_prefix(b"\"")
        .and_then(|v| v.strip_suffix(b"\""))
        .unwrap_or(v);
    if let Some(rest) = v.strip_prefix(b"[") {
        return rest.split(|&c| c == b']').next().unwrap_or(rest);
    }
    match v.iter().filter(|&&c| c == b':').count() {
        1 => &v[..v.iter().position(|&c| c == b':').unwrap_or(v.len())],
        _ => v,
    }
}

/// The client a chain of proxies reports: the nearest hop that is not itself
/// a trusted proxy, or the farthest one if they all are.
fn pick_client<'a>(t: &Trusted, hops: &[&'a [u8]]) -> Option<&'a [u8]> {
    for &h in hops.iter().rev() {
        let trusted = std::str::from_utf8(h)
            .ok()
            .and_then(|s| s.parse::<IpAddr>().ok())
            .is_some_and(|ip| t.contains(ip));
        if !trusted {
            return Some(h);
        }
    }
    hops.first().copied()
}

/// What a trusted proxy said about the request.
#[derive(Default)]
pub struct Via {
    pub client: Option<String>,
    pub secure: Option<bool>,
}

fn proto_secure(v: &[u8]) -> Option<bool> {
    let v = trim(v);
    let v = v
        .strip_prefix(b"\"")
        .and_then(|v| v.strip_suffix(b"\""))
        .unwrap_or(v);
    if eq_ci(v, b"https") || eq_ci(v, b"wss") {
        Some(true)
    } else if eq_ci(v, b"http") || eq_ci(v, b"ws") {
        Some(false)
    } else {
        None
    }
}

/// Reads `Forwarded` (RFC 7239), or failing that `X-Forwarded-For` and
/// `X-Forwarded-Proto`, from a request sent by a trusted proxy.
pub fn forwarded(t: &Trusted, headers: &[httparse::Header<'_>]) -> Via {
    let mut fwd_for: Vec<&[u8]> = Vec::new();
    let mut fwd_proto = None;
    let mut xff: Vec<&[u8]> = Vec::new();
    let mut xfp = None;
    let mut rfc = false;
    for h in headers {
        let name = h.name.as_bytes();
        match name.len() {
            9 if eq_ci(name, b"forwarded") => {
                rfc = true;
                for element in h.value.split(|&c| c == b',') {
                    for pair in element.split(|&c| c == b';') {
                        let Some(eq) = pair.iter().position(|&c| c == b'=') else {
                            continue;
                        };
                        let (k, v) = (trim(&pair[..eq]), &pair[eq + 1..]);
                        if eq_ci(k, b"for") {
                            fwd_for.push(node_addr(v));
                        } else if eq_ci(k, b"proto") {
                            fwd_proto = proto_secure(v);
                        }
                    }
                }
            }
            15 if eq_ci(name, b"x-forwarded-for") => {
                xff.extend(
                    h.value
                        .split(|&c| c == b',')
                        .map(node_addr)
                        .filter(|v| !v.is_empty()),
                );
            }
            17 if eq_ci(name, b"x-forwarded-proto") => {
                xfp = h
                    .value
                    .split(|&c| c == b',')
                    .next_back()
                    .and_then(proto_secure);
            }
            _ => {}
        }
    }
    let (hops, secure) = if rfc {
        (fwd_for, fwd_proto)
    } else {
        (xff, xfp)
    };
    Via {
        client: pick_client(t, &hops).map(|c| String::from_utf8_lossy(c).into_owned()),
        secure,
    }
}

// --- request IDs and trace context --------------------------------------------------

pub fn valid_request_id(v: &[u8]) -> bool {
    (1..=128).contains(&v.len())
        && v.iter()
            .all(|&c| c.is_ascii_alphanumeric() || b"-_.:+/=@~".contains(&c))
}

thread_local! {
    static RNG: Cell<u64> = Cell::new({
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos()));
        h.finish() | 1
    });
}

fn next_u64() -> u64 {
    RNG.with(|s| {
        // splitmix64
        let x = s.get().wrapping_add(0x9e3779b97f4a7c15);
        s.set(x);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    })
}

/// A random (version 4) UUID, in its usual text form.
pub fn new_request_id() -> Vec<u8> {
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&next_u64().to_le_bytes());
    b[8..].copy_from_slice(&next_u64().to_le_bytes());
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(36);
    for (i, byte) in b.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push(b'-');
        }
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 15) as usize]);
    }
    out
}

fn lower_hex(v: &[u8]) -> bool {
    v.iter()
        .all(|&c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// The trace ID and parent span ID of a W3C `traceparent`, when it follows
/// the specification.
pub fn traceparent(v: &[u8]) -> Option<([u8; 32], [u8; 16])> {
    let v = trim(v);
    if v.len() < 55 || v[2] != b'-' || v[35] != b'-' || v[52] != b'-' {
        return None;
    }
    let (version, trace, parent, flags) = (&v[..2], &v[3..35], &v[36..52], &v[53..55]);
    if !lower_hex(version)
        || version == b"ff"
        || !lower_hex(trace)
        || !lower_hex(parent)
        || !lower_hex(flags)
    {
        return None;
    }
    if trace.iter().all(|&c| c == b'0') || parent.iter().all(|&c| c == b'0') {
        return None;
    }
    let exact = if version == b"00" {
        v.len() == 55
    } else {
        v.len() == 55 || v[55] == b'-'
    };
    exact.then(|| (trace.try_into().unwrap(), parent.try_into().unwrap()))
}

// --- the access log -------------------------------------------------------------

/// What the access line says about a request, gathered at dispatch.
pub struct LogReq {
    pub started: Instant,
    /// The method, a space, then the target.
    pub line: Vec<u8>,
    pub method_len: usize,
    pub http11: bool,
    pub trace: Option<([u8; 32], [u8; 16])>,
}

struct AccessLog {
    json: bool,
    pid: u32,
    buf: Vec<u8>,
}

thread_local! {
    static LOG: RefCell<Option<AccessLog>> = const { RefCell::new(None) };
    /// Wakes `flusher` when a line goes into an empty buffer.
    static KICK: std::rc::Rc<tokio::sync::Notify> = std::rc::Rc::new(tokio::sync::Notify::new());
}

/// Writes buffered lines shortly after the first of them, so that a line
/// never waits on the next request, while a busy worker still writes many
/// lines per syscall.
pub async fn flusher() {
    let kick = KICK.with(|k| k.clone());
    loop {
        kick.notified().await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        flush_log();
    }
}

/// Lines held before they are written; the rest go out when `select` returns.
const LOG_FLUSH_AT: usize = 32 * 1024;

/// Turns this worker thread's access log on (text or JSON) or off.
pub fn configure(format: Option<bool>) {
    LOG.with(|l| {
        flush_into(&mut l.borrow_mut());
        *l.borrow_mut() = format.map(|json| AccessLog {
            json,
            pid: std::process::id(),
            buf: Vec::with_capacity(LOG_FLUSH_AT),
        });
    });
}

pub fn log_enabled() -> bool {
    LOG.with(|l| l.borrow().is_some())
}

fn flush_into(l: &mut Option<AccessLog>) {
    if let Some(l) = l
        && !l.buf.is_empty()
    {
        let _ = std::io::stderr().lock().write_all(&l.buf);
        l.buf.clear();
    }
}

pub fn flush_log() {
    LOG.with(|l| flush_into(&mut l.borrow_mut()));
}

fn push_int(out: &mut Vec<u8>, v: u64) {
    crate::http::push_int(out, v);
}

fn json_str(out: &mut Vec<u8>, s: &[u8]) {
    let utf8 = simdutf8::basic::from_utf8(s).is_ok();
    out.push(b'"');
    for &c in s {
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            c if c < 0x20 || (!utf8 && c >= 0x80) => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                out.extend_from_slice(b"\\u00");
                out.push(HEX[(c >> 4) as usize]);
                out.push(HEX[(c & 15) as usize]);
            }
            c => out.push(c),
        }
    }
    out.push(b'"');
}

/// One access line, for a response whose head has just been settled.
pub fn record(req: &LogReq, status: u16, request_id: &[u8]) {
    LOG.with(|l| {
        let mut l = l.borrow_mut();
        let Some(log) = l.as_mut() else { return };
        let micros = req.started.elapsed().as_micros() as u64;
        let (method, target) = (&req.line[..req.method_len], &req.line[req.method_len + 1..]);
        if log.buf.is_empty() {
            KICK.with(|k| k.notify_one());
        }
        let b = &mut log.buf;
        if log.json {
            b.extend_from_slice(b"{\"level\":\"info\",\"pid\":");
            push_int(b, log.pid as u64);
            b.extend_from_slice(b",\"method\":");
            json_str(b, method);
            b.extend_from_slice(b",\"target\":");
            json_str(b, target);
            b.extend_from_slice(b",\"status\":");
            push_int(b, status as u64);
            b.extend_from_slice(b",\"duration_us\":");
            push_int(b, micros);
            b.extend_from_slice(if req.http11 {
                b",\"proto\":\"HTTP/1.1\""
            } else {
                b",\"proto\":\"HTTP/1.0\""
            });
            if !request_id.is_empty() {
                b.extend_from_slice(b",\"request_id\":");
                json_str(b, request_id);
            }
            if let Some((trace, parent)) = &req.trace {
                b.extend_from_slice(b",\"trace_id\":\"");
                b.extend_from_slice(trace);
                b.extend_from_slice(b"\",\"parent_id\":\"");
                b.extend_from_slice(parent);
                b.push(b'"');
            }
            b.extend_from_slice(b"}\n");
        } else {
            b.extend_from_slice(b"[info]  pid=");
            push_int(b, log.pid as u64);
            b.push(b' ');
            b.extend(
                req.line
                    .iter()
                    .map(|&c| if c < 0x20 || c == 0x7f { b'?' } else { c }),
            );
            b.push(b' ');
            push_int(b, status as u64);
            b.push(b' ');
            push_int(b, micros);
            b.extend_from_slice(b"us");
            if !request_id.is_empty() {
                b.extend_from_slice(b" id=");
                b.extend_from_slice(request_id);
            }
            if let Some((trace, parent)) = &req.trace {
                b.extend_from_slice(b" trace=");
                b.extend_from_slice(trace);
                b.extend_from_slice(b" span=");
                b.extend_from_slice(parent);
            }
            b.push(b'\n');
        }
        if log.buf.len() >= LOG_FLUSH_AT {
            flush_into(&mut l);
        }
    });
}

// --- per request ---------------------------------------------------------------------

/// Headers and scope values the server supplies for one request.
#[derive(Default)]
pub struct Meta {
    /// The client a trusted proxy reported.
    pub client: Option<String>,
    /// The scheme a trusted proxy reported: `true` for https.
    pub secure: Option<bool>,
    /// An `X-Request-ID` that replaces whatever the request carried.
    pub inject_id: Option<Vec<u8>>,
    /// An `X-Request-Start` value to add.
    pub request_start: Option<Vec<u8>>,
}

impl Meta {
    /// A header the request carried that the application must not see.
    pub fn hides(&self, name: &[u8]) -> bool {
        self.inject_id.is_some() && name.len() == 12 && eq_ci(name, b"x-request-id")
    }

    /// Headers to add, as (lowercase name, value).
    pub fn added(&self) -> impl Iterator<Item = (&'static [u8], &[u8])> {
        self.inject_id
            .as_deref()
            .map(|v| (&b"x-request-id"[..], v))
            .into_iter()
            .chain(
                self.request_start
                    .as_deref()
                    .map(|v| (&b"x-request-start"[..], v)),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traceparents() {
        let t = "4bf92f3577b34da6a3ce929d0e0e4736";
        let p = "00f067aa0ba902b7";
        assert!(traceparent(format!("00-{t}-{p}-01").as_bytes()).is_some());
        assert!(traceparent(format!("00-{t}-{p}-01-x").as_bytes()).is_none());
        assert!(traceparent(format!("01-{t}-{p}-01-future").as_bytes()).is_some());
        assert!(traceparent(format!("ff-{t}-{p}-01").as_bytes()).is_none());
        assert!(traceparent(format!("00-{}-{p}-01", t.to_uppercase()).as_bytes()).is_none());
        assert!(traceparent(format!("00-{}-{p}-01", "0".repeat(32)).as_bytes()).is_none());
        assert!(traceparent(format!("00-{t}-{}-01", "0".repeat(16)).as_bytes()).is_none());
    }

    #[test]
    fn trusted_nets() {
        let t = Trusted::parse("10.0.0.0/8, 127.0.0.1, 2001:db8::/32").unwrap();
        assert!(t.contains("10.2.3.4".parse().unwrap()));
        assert!(t.contains("::ffff:127.0.0.1".parse().unwrap()));
        assert!(t.contains("2001:db8::9".parse().unwrap()));
        assert!(!t.contains("11.0.0.1".parse().unwrap()));
        assert!(Trusted::parse("10.0.0.0/33").is_err());
        assert!(
            Trusted::parse("*")
                .unwrap()
                .contains("8.8.8.8".parse().unwrap())
        );
    }

    #[test]
    fn request_ids() {
        let id = new_request_id();
        assert_eq!(id.len(), 36);
        assert_eq!(id[14], b'4');
        assert!(valid_request_id(&id));
        assert!(!valid_request_id(b""));
        assert!(!valid_request_id(b"a b"));
        assert!(!valid_request_id(&[b'a'; 129]));
        assert_ne!(new_request_id(), id);
    }
}
