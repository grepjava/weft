//! `--static-dir`: a URL prefix answered from a directory, without Python.
//!
//! GET and HEAD only. A missing file, a directory, a path that escapes the
//! tree, or any other method falls through to the application. No indexes,
//! no `Range`, no `Last-Modified`: ETag is the validator.

use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::compress::{self, Coding};
use crate::http::{State, push_int, reason};

const INLINE: u64 = 16 * 1024;

pub struct Route {
    pub prefix: Vec<u8>,
    pub root: PathBuf,
}

pub struct SendFile {
    pub file: File,
    pub remaining: u64,
}

/// `PREFIX=DIRECTORY`, prefix starting with `/`. Longest prefix first.
pub fn parse(specs: &[String]) -> Result<Vec<Route>, String> {
    let mut routes = Vec::with_capacity(specs.len());
    for spec in specs {
        let (prefix, dir) = spec
            .split_once('=')
            .ok_or_else(|| format!("--static-dir wants PREFIX=DIRECTORY, got {spec:?}"))?;
        if !prefix.starts_with('/') {
            return Err(format!("static prefix must start with /, got {prefix:?}"));
        }
        let root = PathBuf::from(dir);
        if !root.is_dir() {
            return Err(format!("static directory {dir:?} does not exist"));
        }
        routes.push(Route {
            prefix: prefix.as_bytes().to_vec(),
            root: fs::canonicalize(&root).unwrap_or(root),
        });
    }
    routes.sort_by_key(|a| std::cmp::Reverse(a.prefix.len()));
    Ok(routes)
}

/// Opens a file under a matching route. `None` means fall through.
pub fn open(
    routes: &[Route],
    target: &[u8],
    headers: &[httparse::Header<'_>],
    compress_static: bool,
) -> Option<Opened> {
    let path = target.split(|&c| c == b'?').next().unwrap_or(target);
    let decoded = percent_decode(path)?;
    if decoded.len() > 4095 || decoded.contains(&0) {
        return None;
    }
    let (route, relative) = match_route(routes, &decoded)?;
    let rel = std::str::from_utf8(relative).ok()?;
    if rel.is_empty() || rel.as_bytes().last() == Some(&b'/') {
        return None;
    }
    let (file, size, mtime_ns) = open_under(&route.root, rel)?;
    let ctype = content_type(rel.as_bytes());
    let mut coding = Coding::Identity;
    let mut send = file;
    let mut send_size = size;
    if compress_static {
        for c in compress::ranked(headers) {
            let mut name = rel.to_string();
            name.push_str(std::str::from_utf8(c.suffix()).unwrap_or(""));
            if let Some((f, sz, _)) = open_under(&route.root, &name) {
                coding = c;
                send = f;
                send_size = sz;
                break;
            }
        }
    }
    let mut etag = format!("\"{mtime_ns:x}-{size:x}\"");
    if coding != Coding::Identity {
        etag.push('-');
        etag.push_str(std::str::from_utf8(coding.file_token()).unwrap());
    }
    Some(Opened {
        file: send,
        size: send_size,
        ctype,
        etag,
        coding,
        vary: coding != Coding::Identity || compress::is_compressible(ctype),
    })
}

pub struct Opened {
    pub file: File,
    pub size: u64,
    pub ctype: &'static [u8],
    pub etag: String,
    pub coding: Coding,
    pub vary: bool,
}

impl Opened {
    /// 412, 304, or 200. `body` is `None` on 412/304/HEAD.
    pub fn precondition(&self, headers: &[httparse::Header<'_>]) -> (u16, bool) {
        if let Some(v) = header(headers, b"if-match")
            && !if_match(v, self.etag.as_bytes())
        {
            return (412, false);
        }
        if let Some(v) = header(headers, b"if-none-match")
            && if_none_match(v, self.etag.as_bytes())
        {
            return (304, false);
        }
        (200, true)
    }
}

/// Writes the response. A large file is left on `st` for `flush_all` to pump.
#[allow(clippy::too_many_arguments)]
pub fn write(
    st: &mut State,
    opened: Opened,
    head_only: bool,
    keep: bool,
    date: &[u8; 29],
    server: bool,
    hsts: Option<&[u8]>,
    stopping: bool,
) {
    let keep = keep && !stopping;
    st.keep_alive = keep;
    st.resp.close = !keep;
    st.resp.phase = crate::http::Phase::Done;
    st.out.extend_from_slice(b"HTTP/1.1 ");
    push_int(&mut st.out, st.resp.status as u64);
    st.out.extend_from_slice(b" ");
    st.out.extend_from_slice(reason(st.resp.status));
    st.out.extend_from_slice(b"\r\ncontent-type: ");
    st.out.extend_from_slice(opened.ctype);
    st.out.extend_from_slice(b"\r\netag: ");
    st.out.extend_from_slice(opened.etag.as_bytes());
    if opened.coding != Coding::Identity {
        st.out.extend_from_slice(b"\r\ncontent-encoding: ");
        st.out.extend_from_slice(opened.coding.token());
    }
    if opened.vary {
        st.out.extend_from_slice(b"\r\nvary: accept-encoding");
    }
    if server {
        st.out.extend_from_slice(b"\r\nserver: weft");
    }
    if let Some(hsts) = hsts {
        st.out.extend_from_slice(b"\r\nstrict-transport-security: ");
        st.out.extend_from_slice(hsts);
    }
    st.out.extend_from_slice(b"\r\ndate: ");
    st.out.extend_from_slice(date);
    if st.resp.status == 200 {
        st.out.extend_from_slice(b"\r\ncontent-length: ");
        push_int(&mut st.out, opened.size);
    } else {
        st.out.extend_from_slice(b"\r\ncontent-length: 0");
    }
    if !keep {
        st.out.extend_from_slice(b"\r\nconnection: close");
    } else if !st.http11 {
        st.out.extend_from_slice(b"\r\nconnection: keep-alive");
    }
    if !st.request_id.is_empty() {
        st.out.extend_from_slice(b"\r\nx-request-id: ");
        st.out.extend_from_slice(&st.request_id);
    }
    st.out.extend_from_slice(b"\r\n\r\n");
    if st.resp.status != 200 || head_only || opened.size == 0 {
        return;
    }
    let mut file = opened.file;
    if opened.size <= INLINE {
        let mut buf = vec![0u8; opened.size as usize];
        let n = file.read(&mut buf).unwrap_or(0);
        st.out.extend_from_slice(&buf[..n]);
        return;
    }
    st.send_file = Some(Box::new(SendFile {
        file,
        remaining: opened.size,
    }));
}

/// Fills `st.out` from a pending file. `true` while more remains.
pub fn pump(st: &mut State) -> bool {
    let Some(f) = st.send_file.as_mut() else {
        return false;
    };
    if st.out.len() >= 64 * 1024 {
        return true;
    }
    let mut buf = [0u8; 65_536];
    let n = f.file.read(&mut buf).unwrap_or(0);
    if n == 0 {
        st.send_file = None;
        return false;
    }
    let n = n.min(f.remaining as usize);
    st.out.extend_from_slice(&buf[..n]);
    f.remaining -= n as u64;
    if f.remaining == 0 {
        st.send_file = None;
        return false;
    }
    true
}

fn match_route<'r, 'p>(routes: &'r [Route], path: &'p [u8]) -> Option<(&'r Route, &'p [u8])> {
    for r in routes {
        let p = r.prefix.as_slice();
        if !path.starts_with(p) {
            continue;
        }
        let rest = &path[p.len()..];
        if !rest.is_empty() && rest[0] != b'/' && !p.ends_with(b"/") {
            continue;
        }
        let off = p.len() + usize::from(path.get(p.len()) == Some(&b'/'));
        return Some((r, &path[off.min(path.len())..]));
    }
    None
}

fn open_under(root: &Path, relative: &str) -> Option<(File, u64, u64)> {
    if relative.is_empty() || Path::new(relative).is_absolute() {
        return None;
    }
    for c in Path::new(relative).components() {
        match c {
            std::path::Component::Normal(s) if !s.is_empty() && s != "." && s != ".." => {}
            _ => return None,
        }
    }
    let joined = root.join(relative);
    let file = File::open(&joined).ok()?;
    let meta = file.metadata().ok()?;
    if !meta.is_file() {
        return None;
    }
    let canon = fs::canonicalize(&joined).ok()?;
    if !contained(root, &canon) {
        return None;
    }
    let size = meta.len();
    let mtime_ns = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // The unused binding keeps rustc from thinking we only needed metadata.
    let _ = &file;
    Some((file, size, mtime_ns))
}

fn contained(root: &Path, child: &Path) -> bool {
    let (r, c) = (norm(root), norm(child));
    c.starts_with(&r)
}

fn norm(p: &Path) -> PathBuf {
    let s = p.to_string_lossy();
    if let Some(rest) = s.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        p.to_path_buf()
    }
}

fn percent_decode(raw: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%' {
            if i + 2 >= raw.len() {
                return None;
            }
            let (h, l) = (
                crate::asgi::unhex(raw[i + 1])?,
                crate::asgi::unhex(raw[i + 2])?,
            );
            out.push(h << 4 | l);
            i += 3;
        } else {
            out.push(raw[i]);
            i += 1;
        }
    }
    Some(out)
}

fn header<'a>(headers: &'a [httparse::Header<'_>], name: &[u8]) -> Option<&'a [u8]> {
    headers
        .iter()
        .find(|h| crate::asgi::eq_ci(h.name.as_bytes(), name))
        .map(|h| crate::asgi::trim(h.value))
}

fn if_match(v: &[u8], etag: &[u8]) -> bool {
    if crate::asgi::trim(v) == b"*" {
        return true;
    }
    tags(v).any(|t| t == etag)
}

fn if_none_match(v: &[u8], etag: &[u8]) -> bool {
    if crate::asgi::trim(v) == b"*" {
        return true;
    }
    tags(v).any(|t| weak_eq(t, etag))
}

fn tags(v: &[u8]) -> impl Iterator<Item = &[u8]> {
    v.split(|&c| c == b',')
        .map(crate::asgi::trim)
        .filter(|t| !t.is_empty())
}

fn weak_eq(a: &[u8], b: &[u8]) -> bool {
    strip_weak(a) == strip_weak(b)
}

fn strip_weak(t: &[u8]) -> &[u8] {
    if t.len() >= 2 && (t.starts_with(b"W/") || t.starts_with(b"w/")) {
        &t[2..]
    } else {
        t
    }
}

fn content_type(name: &[u8]) -> &'static [u8] {
    let ext = match name.iter().rposition(|&c| c == b'.' || c == b'/') {
        Some(i) if name[i] == b'.' => &name[i + 1..],
        _ => return b"application/octet-stream",
    };
    let mut buf = [0u8; 8];
    if ext.len() > buf.len() {
        return b"application/octet-stream";
    }
    for (i, &c) in ext.iter().enumerate() {
        buf[i] = c.to_ascii_lowercase();
    }
    match &buf[..ext.len()] {
        b"html" | b"htm" => b"text/html; charset=utf-8",
        b"css" => b"text/css; charset=utf-8",
        b"js" | b"mjs" => b"text/javascript; charset=utf-8",
        b"json" | b"map" => b"application/json",
        b"txt" => b"text/plain; charset=utf-8",
        b"xml" => b"application/xml",
        b"svg" => b"image/svg+xml",
        b"png" => b"image/png",
        b"jpg" | b"jpeg" => b"image/jpeg",
        b"gif" => b"image/gif",
        b"webp" => b"image/webp",
        b"avif" => b"image/avif",
        b"ico" => b"image/x-icon",
        b"woff2" => b"font/woff2",
        b"woff" => b"font/woff",
        b"ttf" => b"font/ttf",
        b"otf" => b"font/otf",
        b"wasm" => b"application/wasm",
        b"pdf" => b"application/pdf",
        b"mp4" => b"video/mp4",
        b"webm" => b"video/webm",
        b"mp3" => b"audio/mpeg",
        b"zip" => b"application/zip",
        _ => b"application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_is_a_segment() {
        let routes = [Route {
            prefix: b"/static".to_vec(),
            root: PathBuf::from("."),
        }];
        assert!(match_route(&routes, b"/static/a.css").is_some());
        assert!(match_route(&routes, b"/staticky").is_none());
        assert!(match_route(&routes, b"/static").is_some());
    }

    #[test]
    fn traversal_rejected() {
        let root = std::env::current_dir().unwrap();
        assert!(open_under(&root, "../Cargo.toml").is_none());
        assert!(
            open_under(&root, "Cargo.toml").is_some() || open_under(&root, "README.md").is_some()
        );
    }

    #[test]
    fn mime_from_extension() {
        assert_eq!(content_type(b"app.CSS"), b"text/css; charset=utf-8");
        assert_eq!(content_type(b"x"), b"application/octet-stream");
    }
}
