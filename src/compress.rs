//! `--compress`: gzip, brotli or zstd on text-like application responses.
//!
//! The client’s `Accept-Encoding` is settled when the request is read. Whether
//! this response should be compressed is settled from its headers as they are
//! written. A response that could have been compressed says
//! `Vary: Accept-Encoding` even when it was not, so a cache in front does not
//! mix clients. A strong `ETag` on a compressed body is sent weak: the bytes
//! are no longer the ones the application tagged.

use std::io::Write;

use flate2::Compression;
use flate2::write::GzEncoder;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Coding {
    Identity,
    Gzip,
    Br,
    Zstd,
}

impl Coding {
    pub fn token(self) -> &'static [u8] {
        match self {
            Coding::Identity => b"identity",
            Coding::Gzip => b"gzip",
            Coding::Br => b"br",
            Coding::Zstd => b"zstd",
        }
    }

    pub fn suffix(self) -> &'static [u8] {
        match self {
            Coding::Identity => b"",
            Coding::Gzip => b".gz",
            Coding::Br => b".br",
            Coding::Zstd => b".zst",
        }
    }

    pub fn file_token(self) -> &'static [u8] {
        match self {
            Coding::Gzip => b"gzip",
            Coding::Br => b"br",
            Coding::Zstd => b"zstd",
            Coding::Identity => b"",
        }
    }
}

const PREFERENCE: [Coding; 3] = [Coding::Br, Coding::Zstd, Coding::Gzip];

/// Weights in thousandths; `-1` means the header did not name the coding.
#[derive(Clone, Copy, Debug)]
struct Accept {
    gzip: i32,
    br: i32,
    zstd: i32,
    wildcard: i32,
}

impl Default for Accept {
    fn default() -> Self {
        Accept {
            gzip: -1,
            br: -1,
            zstd: -1,
            wildcard: -1,
        }
    }
}

impl Accept {
    fn parse(v: &[u8]) -> Accept {
        let mut a = Accept::default();
        for part in v.split(|&c| c == b',') {
            let part = trim(part);
            if part.is_empty() {
                continue;
            }
            let (name, rest) = match part.iter().position(|&c| c == b';') {
                Some(i) => (&part[..i], &part[i + 1..]),
                None => (part, &b""[..]),
            };
            let name = trim(name);
            let mut weight = 1000i32;
            for p in rest.split(|&c| c == b';') {
                let p = trim(p);
                if p.len() >= 2 && p[0] | 0x20 == b'q' && p[1] == b'=' {
                    weight = parse_q(&p[2..]);
                }
            }
            if eq_ci(name, b"gzip") {
                a.gzip = weight;
            } else if eq_ci(name, b"x-gzip") {
                if a.gzip < 0 {
                    a.gzip = weight;
                }
            } else if eq_ci(name, b"br") {
                a.br = weight;
            } else if eq_ci(name, b"zstd") {
                a.zstd = weight;
            } else if name == b"*" {
                a.wildcard = weight;
            }
        }
        a
    }

    fn merge(&mut self, other: Accept) {
        if other.gzip >= 0 {
            self.gzip = other.gzip;
        }
        if other.br >= 0 {
            self.br = other.br;
        }
        if other.zstd >= 0 {
            self.zstd = other.zstd;
        }
        if other.wildcard >= 0 {
            self.wildcard = other.wildcard;
        }
    }

    fn weight(self, c: Coding) -> i32 {
        let named = match c {
            Coding::Identity => return 1000,
            Coding::Gzip => self.gzip,
            Coding::Br => self.br,
            Coding::Zstd => self.zstd,
        };
        if named >= 0 {
            named
        } else if self.wildcard >= 0 {
            self.wildcard
        } else {
            0
        }
    }

    fn choose(self) -> Coding {
        let mut best = Coding::Identity;
        let mut best_w = 0;
        for c in PREFERENCE {
            let w = self.weight(c);
            if w > best_w {
                best = c;
                best_w = w;
            }
        }
        best
    }

    fn ranked(self) -> Vec<Coding> {
        let mut out = Vec::new();
        for c in PREFERENCE {
            if self.weight(c) <= 0 {
                continue;
            }
            let mut at = out.len();
            while at > 0 && self.weight(out[at - 1]) < self.weight(c) {
                at -= 1;
            }
            out.insert(at, c);
        }
        out
    }
}

fn parse_q(v: &[u8]) -> i32 {
    let v = trim(v);
    if v.is_empty() || (v[0] != b'0' && v[0] != b'1') {
        return 0;
    }
    let mut value = if v[0] == b'1' { 1000 } else { 0 };
    if v.len() > 1 && v[1] == b'.' {
        let mut scale = 100;
        for &d in &v[2..] {
            if !d.is_ascii_digit() {
                break;
            }
            if v[0] == b'0' && scale > 0 {
                value += (d - b'0') as i32 * scale;
            }
            scale /= 10;
        }
    }
    value.min(1000)
}

fn trim(v: &[u8]) -> &[u8] {
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

fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_ascii_lowercase() == *y)
}

/// Folds every `Accept-Encoding` line. Identity when the client named nothing.
pub fn negotiate(headers: &[httparse::Header<'_>]) -> Coding {
    accept(headers).choose()
}

/// Codings the client prefers, best first, for `--compress-static` sidecars.
pub fn ranked(headers: &[httparse::Header<'_>]) -> Vec<Coding> {
    accept(headers).ranked()
}

fn accept(headers: &[httparse::Header<'_>]) -> Accept {
    let mut a = Accept::default();
    for h in headers {
        if h.name.len() == 15 && eq_ci(h.name.as_bytes(), b"accept-encoding") {
            a.merge(Accept::parse(h.value));
        }
    }
    a
}

pub fn raw_accept(headers: &[httparse::Header<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    for h in headers {
        if h.name.len() == 15 && eq_ci(h.name.as_bytes(), b"accept-encoding") {
            if !out.is_empty() {
                out.push(b',');
            }
            out.extend_from_slice(h.value);
        }
    }
    out
}

/// What the response headers say about compressing it.
#[derive(Default)]
pub struct Eligibility {
    pub compressible_type: bool,
    pub already_encoded: bool,
    pub no_transform: bool,
    pub partial: bool,
    pub vary_covered: bool,
}

impl Eligibility {
    pub fn observe(&mut self, name: &[u8], value: &[u8]) {
        match name.len() {
            4 if eq_ci(name, b"vary") => {
                if has_token(value, b"accept-encoding") || trim(value) == b"*" {
                    self.vary_covered = true;
                }
            }
            12 if eq_ci(name, b"content-type") => self.compressible_type = is_compressible(value),
            13 if eq_ci(name, b"cache-control") => {
                if has_token(value, b"no-transform") {
                    self.no_transform = true;
                }
            }
            13 if eq_ci(name, b"content-range") => self.partial = true,
            16 if eq_ci(name, b"content-encoding") => self.already_encoded = true,
            _ => {}
        }
    }

    pub fn from_head(head: &[u8]) -> Eligibility {
        let mut e = Eligibility::default();
        for (n, v) in crate::http::header_lines(head) {
            e.observe(n, v);
        }
        e
    }

    pub fn may_vary(&self, status: u16) -> bool {
        self.compressible_type
            && !self.already_encoded
            && !self.no_transform
            && !self.partial
            && status != 206
    }

    pub fn choose(
        &self,
        offered: Coding,
        status: u16,
        body_allowed: bool,
        declared: Option<u64>,
        min: usize,
    ) -> Coding {
        if offered == Coding::Identity || !body_allowed || !self.may_vary(status) {
            return Coding::Identity;
        }
        if declared.is_some_and(|n| n < min as u64) {
            return Coding::Identity;
        }
        offered
    }
}

pub fn is_compressible(value: &[u8]) -> bool {
    let media = match value
        .iter()
        .position(|&c| c == b';' || c.is_ascii_whitespace())
    {
        Some(i) => &value[..i],
        None => value,
    };
    if media.is_empty() {
        return false;
    }
    if starts_ci(media, b"text/") {
        return !eq_ci(media, b"text/event-stream");
    }
    ends_ci(media, b"+json")
        || ends_ci(media, b"+xml")
        || eq_ci(media, b"application/json")
        || eq_ci(media, b"application/javascript")
        || eq_ci(media, b"application/x-javascript")
        || eq_ci(media, b"application/ecmascript")
        || eq_ci(media, b"application/xml")
        || eq_ci(media, b"application/wasm")
        || eq_ci(media, b"application/x-ndjson")
        || eq_ci(media, b"application/graphql-response+json")
        || eq_ci(media, b"application/vnd.ms-fontobject")
        || eq_ci(media, b"font/ttf")
        || eq_ci(media, b"font/otf")
        || eq_ci(media, b"image/svg+xml")
        || eq_ci(media, b"image/x-icon")
        || eq_ci(media, b"image/bmp")
}

fn starts_ci(a: &[u8], p: &[u8]) -> bool {
    a.len() >= p.len() && eq_ci(&a[..p.len()], p)
}

fn ends_ci(a: &[u8], s: &[u8]) -> bool {
    a.len() >= s.len() && eq_ci(&a[a.len() - s.len()..], s)
}

fn has_token(v: &[u8], want: &[u8]) -> bool {
    v.split(|&c| c == b',')
        .map(|t| {
            let t = trim(t);
            match t.iter().position(|&c| c == b'=') {
                Some(i) => trim(&t[..i]),
                None => t,
            }
        })
        .any(|t| eq_ci(t, want))
}

/// Incremental compressor. Each `push` returns bytes ready for the wire.
pub struct Encoder {
    inner: Inner,
}

enum Inner {
    Gzip(GzEncoder<Vec<u8>>),
    Br(Box<brotli::CompressorWriter<Vec<u8>>>),
    Zstd(zstd::stream::write::Encoder<'static, Vec<u8>>),
}

impl Encoder {
    pub fn new(coding: Coding) -> Option<Encoder> {
        let inner = match coding {
            Coding::Identity => return None,
            Coding::Gzip => Inner::Gzip(GzEncoder::new(
                Vec::with_capacity(4096),
                Compression::fast(),
            )),
            Coding::Br => Inner::Br(Box::new(brotli::CompressorWriter::new(
                Vec::with_capacity(4096),
                4096,
                4,
                22,
            ))),
            Coding::Zstd => {
                let enc = zstd::stream::write::Encoder::new(Vec::with_capacity(4096), 1).ok()?;
                Inner::Zstd(enc)
            }
        };
        Some(Encoder { inner })
    }

    pub fn push(&mut self, data: &[u8], finish: bool) -> Vec<u8> {
        match &mut self.inner {
            Inner::Gzip(z) => {
                let _ = z.write_all(data);
                if finish {
                    std::mem::replace(z, GzEncoder::new(Vec::new(), Compression::fast()))
                        .finish()
                        .unwrap_or_default()
                } else {
                    let _ = z.flush();
                    std::mem::take(z.get_mut())
                }
            }
            Inner::Br(z) => {
                let _ = z.write_all(data);
                let _ = z.flush();
                std::mem::take(z.get_mut())
            }
            Inner::Zstd(z) => {
                let _ = z.write_all(data);
                if finish {
                    std::mem::replace(
                        z,
                        zstd::stream::write::Encoder::new(Vec::new(), 1).expect("zstd"),
                    )
                    .finish()
                    .unwrap_or_default()
                } else {
                    let _ = z.flush();
                    std::mem::take(z.get_mut())
                }
            }
        }
    }
}

/// Drops `Content-Length` and weakens a strong ETag when `coding` is not identity.
pub fn rewrite_head(head: &[u8], coding: Coding) -> Vec<u8> {
    if coding == Coding::Identity {
        return head.to_vec();
    }
    let mut out = Vec::with_capacity(head.len() + 32);
    let mut rest = head;
    if let Some(i) = rest.windows(2).position(|w| w == b"\r\n") {
        out.extend_from_slice(&rest[..i + 2]);
        rest = &rest[i + 2..];
    }
    while let Some(i) = rest.windows(2).position(|w| w == b"\r\n") {
        let line = &rest[..i];
        rest = &rest[i + 2..];
        if line.is_empty() {
            break;
        }
        let col = match line.iter().position(|&c| c == b':') {
            Some(c) => c,
            None => {
                out.extend_from_slice(line);
                out.extend_from_slice(b"\r\n");
                continue;
            }
        };
        let name = &line[..col];
        let value = crate::asgi::trim(&line[col + 1..]);
        if name.len() == 14 && eq_ci(name, b"content-length") {
            continue;
        }
        out.extend_from_slice(name);
        out.extend_from_slice(b": ");
        if name.len() == 4 && eq_ci(name, b"etag") {
            if let Some(w) = weaken_etag(value) {
                out.extend_from_slice(&w);
            } else {
                out.extend_from_slice(value);
            }
        } else {
            out.extend_from_slice(value);
        }
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Rewrites a strong quoted ETag to its weak form when the body is compressed.
pub fn weaken_etag(value: &[u8]) -> Option<Vec<u8>> {
    let v = trim(value);
    if v.len() >= 2
        && v[0] == b'"'
        && v[v.len() - 1] == b'"'
        && !v.starts_with(b"W/")
        && !v.starts_with(b"w/")
    {
        let mut out = Vec::with_capacity(v.len() + 2);
        out.extend_from_slice(b"W/");
        out.extend_from_slice(v);
        return Some(out);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefer_br_on_a_tie() {
        assert_eq!(Accept::parse(b"gzip, deflate, br").choose(), Coding::Br);
        assert_eq!(
            Accept::parse(b"gzip;q=1.0, br;q=0.8").choose(),
            Coding::Gzip
        );
        assert_eq!(Accept::parse(b"identity").choose(), Coding::Identity);
        assert_eq!(Accept::parse(b"gzip;q=0").choose(), Coding::Identity);
    }

    #[test]
    fn q_malformed_is_zero() {
        assert_eq!(Accept::parse(b"br;q=xyz").choose(), Coding::Identity);
    }

    #[test]
    fn later_line_overrides() {
        let mut a = Accept::parse(b"gzip");
        a.merge(Accept::parse(b"br, gzip;q=0"));
        assert_eq!(a.choose(), Coding::Br);
    }

    #[test]
    fn text_is_compressible_except_sse() {
        assert!(is_compressible(b"text/html; charset=utf-8"));
        assert!(is_compressible(b"application/json"));
        assert!(is_compressible(b"application/ld+json"));
        assert!(!is_compressible(b"text/event-stream"));
        assert!(!is_compressible(b"image/png"));
    }

    #[test]
    fn gzip_round_trip() {
        let mut e = Encoder::new(Coding::Gzip).unwrap();
        let a = e.push(b"hello ", false);
        let b = e.push(b"world", true);
        let mut z = flate2::read::GzDecoder::new(std::io::Cursor::new([a, b].concat()));
        let mut out = String::new();
        std::io::Read::read_to_string(&mut z, &mut out).unwrap();
        assert_eq!(out, "hello world");
    }
}
