//! `--static-dir`: a URL prefix answered from a directory, without Python.
//!
//! GET and HEAD only. A missing file, a directory, a path that escapes the
//! tree, or any other method falls through to the application. No indexes,
//! no `Range`, no `Last-Modified`: ETag is the validator.

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
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
    let mut send_mtime = mtime_ns;
    if compress_static {
        for c in compress::ranked(headers) {
            let mut name = rel.to_string();
            name.push_str(std::str::from_utf8(c.suffix()).unwrap_or(""));
            if let Some((f, sz, mt)) = open_under(&route.root, &name) {
                coding = c;
                send = f;
                send_size = sz;
                send_mtime = mt;
                break;
            }
        }
    }
    // The validator is the bytes actually sent: a precompressed copy has its
    // own, and its coding stays inside the quotes.
    let etag = if coding == Coding::Identity {
        format!("\"{send_mtime:x}-{send_size:x}\"")
    } else {
        let token = std::str::from_utf8(coding.file_token()).unwrap_or("");
        format!("\"{send_mtime:x}-{send_size:x}-{token}\"")
    };
    Some(Opened {
        file: send,
        size: send_size,
        mtime_secs: send_mtime / 1_000_000_000,
        range: None,
        ctype,
        etag,
        coding,
        vary: compress_static && (coding != Coding::Identity || compress::is_compressible(ctype)),
    })
}

pub struct Opened {
    pub file: File,
    pub size: u64,
    pub mtime_secs: u64,
    /// Optional inclusive `(start, end)` byte range for `206 Partial Content`.
    pub range: Option<(u64, u64)>,
    pub ctype: &'static [u8],
    pub etag: String,
    pub coding: Coding,
    pub vary: bool,
}

impl Opened {
    /// 412, 304, 206, 416, or 200. `body` is `false` on 412/304/416.
    pub fn precondition(&mut self, headers: &[httparse::Header<'_>]) -> (u16, bool) {
        if let Some(v) = header(headers, b"if-match")
            && !if_match(v, self.etag.as_bytes())
        {
            return (412, false);
        }
        if let Some(v) = header(headers, b"if-none-match") {
            if if_none_match(v, self.etag.as_bytes()) {
                return (304, false);
            }
        } else if self.mtime_secs > 0
            && let Some(v) = header(headers, b"if-modified-since")
            && let Some(ims) = crate::cache::parse_http_date(v)
            && self.mtime_secs <= ims
        {
            return (304, false);
        }
        if let Some(v) = header(headers, b"range") {
            // Respect `If-Range` when present (ETag match only).
            let range_ok = match header(headers, b"if-range") {
                Some(ir) => ir.trim_ascii() == self.etag.as_bytes(),
                None => true,
            };
            if range_ok {
                match parse_single_range(v, self.size) {
                    RangeResult::Satisfiable(start, end) => {
                        self.range = Some((start, end));
                        return (206, true);
                    }
                    RangeResult::Unsatisfiable => return (416, false),
                    RangeResult::Ignore => {}
                }
            }
        }
        (200, true)
    }
}

enum RangeResult {
    Satisfiable(u64, u64),
    Unsatisfiable,
    Ignore,
}

/// Parses a single `bytes=start-end`, `bytes=start-`, or `bytes=-suffix` range
/// (RFC 9110 §14.1.2). Multi-range requests are ignored (served as 200).
fn parse_single_range(raw: &[u8], size: u64) -> RangeResult {
    let raw = raw.trim_ascii();
    let Some(spec) = raw.strip_prefix(b"bytes=").or_else(|| {
        if raw.len() >= 6 && raw[..6].eq_ignore_ascii_case(b"bytes=") {
            Some(&raw[6..])
        } else {
            None
        }
    }) else {
        return RangeResult::Ignore;
    };
    if spec.contains(&b',') {
        return RangeResult::Ignore;
    }
    let Some(dash) = spec.iter().position(|&c| c == b'-') else {
        return RangeResult::Ignore;
    };
    let left = spec[..dash].trim_ascii();
    let right = spec[dash + 1..].trim_ascii();
    if left.is_empty() && right.is_empty() {
        return RangeResult::Ignore;
    }
    if size == 0 {
        return RangeResult::Unsatisfiable;
    }
    if left.is_empty() {
        let Ok(suffix) = std::str::from_utf8(right)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(())
        else {
            return RangeResult::Ignore;
        };
        if suffix == 0 {
            return RangeResult::Unsatisfiable;
        }
        let start = size.saturating_sub(suffix);
        return RangeResult::Satisfiable(start, size - 1);
    }
    let Ok(start) = std::str::from_utf8(left)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(())
    else {
        return RangeResult::Ignore;
    };
    if start >= size {
        return RangeResult::Unsatisfiable;
    }
    let end = if right.is_empty() {
        size - 1
    } else {
        let Ok(e) = std::str::from_utf8(right)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(())
        else {
            return RangeResult::Ignore;
        };
        if e < start {
            return RangeResult::Ignore;
        }
        e.min(size - 1)
    };
    RangeResult::Satisfiable(start, end)
}

/// Writes the response. A large file is left on `st` for `flush_all` to pump.
/// `false` when the file could not be read whole and a 500 went out instead.
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
) -> bool {
    let mut file = opened.file;
    let (send_len, seek_to) = match (st.resp.status, opened.range) {
        (206, Some((start, end))) => (end - start + 1, Some(start)),
        (200, _) => (opened.size, None),
        _ => (0, None),
    };
    let body = matches!(st.resp.status, 200 | 206) && !head_only && send_len > 0;
    if body
        && let Some(pos) = seek_to
        && file.seek(SeekFrom::Start(pos)).is_err()
    {
        st.keep_alive = false;
        st.write_error(500);
        return false;
    }
    // A small file/range is read before the head goes out, so a file that shrank
    // since it was opened is an error rather than a short body.
    let inline = if body && send_len <= INLINE {
        let mut buf = Vec::with_capacity(send_len as usize);
        match (&mut file).take(send_len).read_to_end(&mut buf) {
            Ok(n) if n as u64 == send_len => Some(buf),
            _ => {
                st.keep_alive = false;
                st.write_error(500);
                return false;
            }
        }
    } else {
        None
    };
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
    st.out
        .extend_from_slice(b"\r\naccept-ranges: bytes\r\netag: ");
    st.out.extend_from_slice(opened.etag.as_bytes());
    if opened.mtime_secs > 0 {
        let lm = crate::http::format_date(opened.mtime_secs);
        st.out.extend_from_slice(b"\r\nlast-modified: ");
        st.out.extend_from_slice(&lm);
    }
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
    if st.resp.status == 206
        && let Some((start, end)) = opened.range
    {
        st.out.extend_from_slice(b"\r\ncontent-range: bytes ");
        push_int(&mut st.out, start);
        st.out.extend_from_slice(b"-");
        push_int(&mut st.out, end);
        st.out.extend_from_slice(b"/");
        push_int(&mut st.out, opened.size);
        st.out.extend_from_slice(b"\r\ncontent-length: ");
        push_int(&mut st.out, send_len);
    } else if st.resp.status == 416 {
        st.out.extend_from_slice(b"\r\ncontent-range: bytes */");
        push_int(&mut st.out, opened.size);
        st.out.extend_from_slice(b"\r\ncontent-length: 0");
    } else if st.resp.status == 200 {
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
    if let Some(buf) = inline {
        st.out.extend_from_slice(&buf);
    } else if body {
        st.send_file = Some(Box::new(SendFile {
            file,
            remaining: send_len,
        }));
    }
    true
}

/// Fills `st.out` from a pending file. `true` while more remains. A file
/// that ends early or fails to read leaves `st.aborted`: its Content-Length
/// is already out, so the response can only be cut off.
pub fn pump(st: &mut State) -> bool {
    let Some(f) = st.send_file.as_mut() else {
        return false;
    };
    if st.out.len() >= 64 * 1024 {
        return true;
    }
    let mut buf = [0u8; 65_536];
    let n = loop {
        match f.file.read(&mut buf) {
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Ok(n) if n > 0 => break n,
            _ => {
                st.send_file = None;
                st.aborted = true;
                return false;
            }
        }
    };
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
    let mut buf = [0u8; 16];
    if ext.len() > buf.len() {
        return b"application/octet-stream";
    }
    for (i, &c) in ext.iter().enumerate() {
        buf[i] = c.to_ascii_lowercase();
    }
    match &buf[..ext.len()] {
        b"html" | b"htm" => b"text/html; charset=utf-8",
        b"css" => b"text/css; charset=utf-8",
        b"js" | b"mjs" | b"cjs" => b"text/javascript; charset=utf-8",
        b"json" | b"map" => b"application/json",
        b"webmanifest" => b"application/manifest+json",
        b"txt" => b"text/plain; charset=utf-8",
        b"md" => b"text/markdown; charset=utf-8",
        b"csv" => b"text/csv; charset=utf-8",
        b"yaml" | b"yml" => b"application/yaml",
        b"toml" => b"application/toml",
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
        b"ogg" => b"audio/ogg",
        b"wav" => b"audio/wav",
        b"flac" => b"audio/flac",
        b"aac" => b"audio/aac",
        b"zip" => b"application/zip",
        b"gz" => b"application/gzip",
        b"tar" => b"application/x-tar",
        b"7z" => b"application/x-7z-compressed",
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

    /// A file of `len` bytes, opened as if it were `claimed` bytes long:
    /// what a file that shrank after it was opened looks like.
    fn shrunk(name: &str, len: usize, claimed: u64) -> Opened {
        let path = std::env::temp_dir().join(format!("weft-{}-{name}", std::process::id()));
        fs::write(&path, vec![b'x'; len]).unwrap();
        let file = File::open(&path).unwrap();
        let _ = fs::remove_file(&path);
        Opened {
            file,
            size: claimed,
            mtime_secs: 0,
            range: None,
            ctype: b"application/octet-stream",
            etag: "\"0-0\"".into(),
            coding: Coding::Identity,
            vary: false,
        }
    }

    fn ok_state() -> State {
        let mut st = State::new();
        st.resp.status = 200;
        st
    }

    #[test]
    fn small_file_read_short_is_an_error() {
        let mut st = ok_state();
        let served = write(
            &mut st,
            shrunk("small", 50, 100),
            false,
            true,
            &[b'x'; 29],
            false,
            None,
            false,
        );
        assert!(!served);
        assert!(
            st.out.starts_with(b"HTTP/1.1 500 "),
            "{:?}",
            String::from_utf8_lossy(&st.out)
        );
        assert!(st.resp.close);
    }

    #[test]
    fn small_file_read_whole() {
        let mut st = ok_state();
        assert!(write(
            &mut st,
            shrunk("whole", 100, 100),
            false,
            true,
            &[b'x'; 29],
            false,
            None,
            false
        ));
        assert!(st.out.ends_with(&[b'x'; 100]));
        assert!(!st.aborted);
    }

    #[test]
    fn large_file_cut_short_aborts() {
        let mut st = ok_state();
        let len = 200 * 1024;
        assert!(write(
            &mut st,
            shrunk("large", len, 300 * 1024),
            false,
            true,
            &[b'x'; 29],
            false,
            None,
            false
        ));
        let head = st.out.len();
        st.out.clear();
        let mut sent = 0;
        while pump(&mut st) {
            sent += st.out.len();
            st.out.clear();
        }
        sent += st.out.len();
        assert!(head > 0);
        assert_eq!(sent, len);
        assert!(st.aborted);
        assert!(st.send_file.is_none());
    }

    #[test]
    fn large_file_sent_whole() {
        let mut st = ok_state();
        let len = 200 * 1024;
        assert!(write(
            &mut st,
            shrunk("large-whole", len, len as u64),
            false,
            true,
            &[b'x'; 29],
            false,
            None,
            false
        ));
        st.out.clear();
        let mut sent = 0;
        while pump(&mut st) {
            sent += st.out.len();
            st.out.clear();
        }
        sent += st.out.len();
        assert_eq!(sent, len);
        assert!(!st.aborted);
    }

    #[test]
    fn mime_from_extension() {
        assert_eq!(content_type(b"app.CSS"), b"text/css; charset=utf-8");
        assert_eq!(content_type(b"x"), b"application/octet-stream");
    }
}
