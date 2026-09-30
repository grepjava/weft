//! `--cache-size`: copies of responses the application marks as fresh.
//!
//! Nothing is stored unless the application sent `s-maxage` or `max-age`.
//! Process workers share one mapped file; thread workers share one heap
//! table. A successful change to a URL retires every copy of its path and
//! query. Bodies are stored uncompressed and compressed per client on the
//! way out.

use std::fs::OpenOptions;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use memmap2::MmapMut;

use crate::asgi::{eq_ci, trim};

const MAGIC: u64 = 0x5746_5443_4143_4846;
const MAX_KEY: usize = 2048;
const MAX_HEAD: usize = 16 * 1024;
const META: usize = 80;

#[derive(Clone, Copy)]
pub struct Cfg {
    pub max_object: usize,
    pub ttl_max: u64,
}

struct Table {
    _map: Option<MmapMut>,
    base: *mut u8,
    epoch: *const AtomicU64,
    small: Class,
    large: Class,
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

#[derive(Clone, Copy)]
struct Class {
    off: usize,
    slot: usize,
    n: usize,
    payload: usize,
}

#[repr(C)]
struct Slot {
    lock: AtomicU64,
    seq: AtomicU64,
    hash: u64,
    target: u64,
    epoch: u64,
    expiry_ms: u64,
    stored_ms: u64,
    age_sec: u32,
    key_len: u16,
    head_len: u16,
    body_len: u32,
    variant: u64,
}

struct SlotGuard {
    lock: *const AtomicU64,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        unsafe { (*self.lock).store(0, Ordering::Release) };
    }
}

fn lock_slot(lock: &AtomicU64) -> SlotGuard {
    let mut spins = 0u32;
    while lock
        .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        spins = spins.saturating_add(1);
        if spins & 63 == 0 {
            std::thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    }
    SlotGuard { lock }
}

const _: () = assert!(std::mem::size_of::<Slot>() <= META);

static TABLE: OnceLock<Table> = OnceLock::new();

pub fn install(bytes: usize, max_object: usize, map: Option<&str>) -> std::io::Result<()> {
    if TABLE.get().is_some() || bytes < 64 * 1024 {
        return Ok(());
    }
    let (keep, ptr, len) = match map {
        Some(path) => {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            f.set_len(bytes as u64)?;
            let mut m = unsafe { MmapMut::map_mut(&f)? };
            let p = m.as_mut_ptr();
            (Some(m), p, bytes)
        }
        None => {
            let mut v = vec![0u8; bytes];
            let p = v.as_mut_ptr();
            std::mem::forget(v);
            (None, p, bytes)
        }
    };
    let (small, large) = init(ptr, len, max_object);
    let epoch = unsafe { ptr.add(8) as *const AtomicU64 };
    let _ = TABLE.set(Table {
        _map: keep,
        base: ptr,
        epoch,
        small,
        large,
    });
    Ok(())
}

fn layout(len: usize, max_object: usize) -> (Class, Class) {
    let hdr = 64usize;
    let large_payload = MAX_KEY + MAX_HEAD + max_object.max(1024);
    let small_payload = MAX_KEY + MAX_HEAD + 8 * 1024;
    let large_slot = META + large_payload;
    let small_slot = META + small_payload;
    let rest = len.saturating_sub(hdr);
    // Only keep slots that fit. `.max(1)` used to invent a large slot that
    // started past the allocation (1 MiB cache + 1 MiB objects → 1.8 MiB).
    let large_n = (rest / 4) / large_slot;
    let small_n = (rest - large_n * large_slot) / small_slot;
    let small = Class {
        off: hdr,
        slot: small_slot,
        n: small_n,
        payload: small_payload,
    };
    let large = Class {
        off: hdr + small_n * small_slot,
        slot: large_slot,
        n: large_n,
        payload: large_payload,
    };
    debug_assert!(hdr + small_n * small_slot + large_n * large_slot <= len);
    (small, large)
}

fn init(ptr: *mut u8, len: usize, max_object: usize) -> (Class, Class) {
    let (small, large) = layout(len, max_object);
    let used = small.off + small.n * small.slot + large.n * large.slot;
    unsafe {
        if read_u64(ptr) != MAGIC {
            ptr.write_bytes(0, used.min(len));
            write_u64(ptr, MAGIC);
            write_u64(ptr.add(8), 1);
        }
    }
    (small, large)
}

fn read_u64(p: *const u8) -> u64 {
    unsafe { std::ptr::read_unaligned(p as *const u64) }
}

fn write_u64(p: *mut u8, v: u64) {
    unsafe { std::ptr::write_unaligned(p as *mut u64, v) }
}

fn generation() -> u64 {
    TABLE
        .get()
        .map(|t| unsafe { (*t.epoch).load(Ordering::Acquire) })
        .unwrap_or(1)
}

pub fn flush() {
    if let Some(t) = TABLE.get() {
        unsafe { (*t.epoch).fetch_add(1, Ordering::AcqRel) };
    }
}

fn classes() -> Option<(&'static Table, [Class; 2])> {
    let t = TABLE.get()?;
    Some((t, [t.small, t.large]))
}

fn slot_at(t: &Table, c: Class, i: usize) -> *mut Slot {
    unsafe { t.base.add(c.off + i * c.slot) as *mut Slot }
}

#[allow(clippy::mut_from_ref)]
fn payload(t: &Table, c: Class, i: usize) -> &mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(t.base.add(c.off + i * c.slot + META), c.payload) }
}

pub fn target_hash(target: &[u8]) -> u64 {
    fnv(target)
}

pub fn variant_hash(ae: &[u8]) -> u64 {
    fnv(ae)
}

fn fnv(b: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for &x in b {
        h ^= x as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    if h == 0 { 1 } else { h }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Builds `scheme\\0host\\0target[\\0xfp\\0xfh\\0fwd]`. Host is lowercased.
pub fn key(
    https: bool,
    host: &[u8],
    target: &[u8],
    forwarded: Option<(&[u8], &[u8], &[u8])>,
) -> Option<Vec<u8>> {
    let mut k = Vec::with_capacity(host.len() + target.len() + 16);
    k.extend_from_slice(if https { b"https" } else { b"http" });
    k.push(0);
    for &c in host {
        k.push(c.to_ascii_lowercase());
    }
    k.push(0);
    k.extend_from_slice(target);
    if let Some((p, h, f)) = forwarded {
        k.push(0);
        k.extend_from_slice(p);
        k.push(0);
        k.extend_from_slice(h);
        k.push(0);
        k.extend_from_slice(f);
    }
    (k.len() <= MAX_KEY).then_some(k)
}

pub struct Hit {
    pub status: u16,
    pub head: Vec<u8>,
    pub body: Vec<u8>,
    pub age: u32,
    pub ttl: u32,
}

pub fn lookup(key: &[u8], variant: u64, now: Option<u64>) -> Option<Hit> {
    let (t, classes) = classes()?;
    let hash = fnv(key);
    let now = now.unwrap_or_else(now_ms);
    let epoch = generation();
    for c in classes {
        if c.n == 0 {
            continue;
        }
        let start = (hash as usize) % c.n;
        for p in 0..8 {
            let i = (start + p) % c.n;
            if let Some(hit) = read_slot(t, c, i, key, hash, variant, now, epoch) {
                return Some(hit);
            }
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn read_slot(
    t: &Table,
    c: Class,
    i: usize,
    key: &[u8],
    hash: u64,
    variant: u64,
    now: u64,
    epoch: u64,
) -> Option<Hit> {
    let s = unsafe { &*slot_at(t, c, i) };
    let _g = lock_slot(&s.lock);
    if s.hash != hash || s.epoch != epoch || s.expiry_ms <= now || s.variant != variant {
        return None;
    }
    let (kl, hl, bl) = (s.key_len as usize, s.head_len as usize, s.body_len as usize);
    if kl + hl + bl > c.payload {
        return None;
    }
    let pay = payload(t, c, i);
    if &pay[..kl] != key {
        return None;
    }
    let raw = &pay[kl..kl + hl];
    let body = pay[kl + hl..kl + hl + bl].to_vec();
    let stored = s.stored_ms;
    let age0 = s.age_sec;
    let expiry = s.expiry_ms;
    let status = status_of(raw);
    let head = match raw.iter().position(|&c| c == b'\n') {
        Some(i) => raw[i + 1..].to_vec(),
        None => raw.to_vec(),
    };
    let age = age0.saturating_add(((now.saturating_sub(stored)) / 1000) as u32);
    let ttl = ((expiry.saturating_sub(now)) / 1000) as u32;
    Some(Hit {
        status,
        head,
        body,
        age,
        ttl,
    })
}

fn status_of(head: &[u8]) -> u16 {
    // Stored head is `STATUS\\n` then `name: value\\n` lines, no HTTP line.
    head.iter()
        .take_while(|c| c.is_ascii_digit())
        .fold(0u16, |a, &d| a * 10 + (d - b'0') as u16)
}

#[allow(clippy::too_many_arguments)]
pub fn store(
    key: &[u8],
    target: u64,
    variant: u64,
    status: u16,
    head: &[u8],
    body: &[u8],
    ttl_sec: u64,
    age_sec: u32,
) {
    let Some((t, classes)) = classes() else {
        return;
    };
    let need = key.len() + head.len() + body.len();
    let hash = fnv(key);
    let now = now_ms();
    let epoch = generation();
    let expiry = now.saturating_add(ttl_sec.saturating_mul(1000));
    for c in classes {
        if c.n == 0 || need > c.payload {
            continue;
        }
        let start = (hash as usize) % c.n;
        let mut best = start;
        let mut best_rank = 4u8;
        for p in 0..8 {
            let i = (start + p) % c.n;
            let s = unsafe { &*slot_at(t, c, i) };
            let _g = lock_slot(&s.lock);
            let rank = if s.hash == hash && payload_key(t, c, i, s.key_len as usize) == key {
                0
            } else if s.hash == 0 || s.epoch != epoch || s.expiry_ms <= now {
                1
            } else {
                2
            };
            drop(_g);
            if rank < best_rank {
                best_rank = rank;
                best = i;
                if rank == 0 {
                    break;
                }
            }
        }
        write_slot(
            t, c, best, hash, target, epoch, expiry, now, age_sec, variant, key, status, head, body,
        );
        return;
    }
}

fn payload_key(t: &Table, c: Class, i: usize, n: usize) -> &[u8] {
    let p = payload(t, c, i);
    &p[..n.min(p.len())]
}

#[allow(clippy::too_many_arguments)]
fn write_slot(
    t: &Table,
    c: Class,
    i: usize,
    hash: u64,
    target: u64,
    epoch: u64,
    expiry: u64,
    now: u64,
    age_sec: u32,
    variant: u64,
    key: &[u8],
    status: u16,
    head: &[u8],
    body: &[u8],
) {
    let s = unsafe { &mut *slot_at(t, c, i) };
    let _g = lock_slot(&s.lock);
    s.seq.fetch_add(1, Ordering::Relaxed);
    s.hash = hash;
    s.target = target;
    s.epoch = epoch;
    s.expiry_ms = expiry;
    s.stored_ms = now;
    s.age_sec = age_sec;
    s.variant = variant;
    let pay = payload(t, c, i);
    let mut stored = Vec::with_capacity(8 + head.len());
    stored.extend_from_slice(status.to_string().as_bytes());
    stored.push(b'\n');
    stored.extend_from_slice(head);
    if key.len() + stored.len() + body.len() > pay.len() {
        s.hash = 0;
        s.seq.fetch_add(1, Ordering::Relaxed);
        return;
    }
    pay[..key.len()].copy_from_slice(key);
    pay[key.len()..key.len() + stored.len()].copy_from_slice(&stored);
    pay[key.len() + stored.len()..key.len() + stored.len() + body.len()].copy_from_slice(body);
    s.key_len = key.len() as u16;
    s.head_len = stored.len() as u16;
    s.body_len = body.len() as u32;
    s.seq.fetch_add(1, Ordering::Relaxed);
    crate::metrics::cache_store();
}

pub fn invalidate(target: u64) {
    let Some((t, classes)) = classes() else {
        return;
    };
    for c in classes {
        for i in 0..c.n {
            let s = unsafe { &mut *slot_at(t, c, i) };
            let _g = lock_slot(&s.lock);
            if s.target == target && s.hash != 0 {
                s.seq.fetch_add(1, Ordering::Relaxed);
                s.expiry_ms = 0;
                s.seq.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Whether this request may be answered from the cache, or capture a response.
pub fn request_ok(method: &str, headers: &[httparse::Header<'_>]) -> bool {
    if !matches!(method, "GET" | "HEAD") {
        return false;
    }
    for h in headers {
        let n = h.name.as_bytes();
        let v = trim(h.value);
        match n.len() {
            13 if eq_ci(n, b"authorization") => return false,
            6 if eq_ci(n, b"cookie") => return false,
            5 if eq_ci(n, b"range") => return false,
            6 if eq_ci(n, b"pragma") && has_token(v, b"no-cache") => return false,
            13 if eq_ci(n, b"cache-control") => {
                if has_token(v, b"no-cache") || has_token(v, b"no-store") || max_age_zero(v) {
                    return false;
                }
            }
            8 if eq_ci(n, b"if-match") => return false,
            8 if eq_ci(n, b"if-range") => return false,
            19 if eq_ci(n, b"if-unmodified-since") => return false,
            _ => {}
        }
    }
    true
}

pub fn mutating(method: &str) -> bool {
    !matches!(method, "GET" | "HEAD" | "OPTIONS" | "TRACE" | "CONNECT")
}

pub struct Policy {
    pub ttl: u64,
    pub age: u32,
    pub vary_ae: bool,
}

/// `None` if this response must not be stored.
pub fn response_ok(
    status: u16,
    head: &[u8],
    body_len: usize,
    max_object: usize,
    ttl_max: u64,
) -> Option<Policy> {
    if !matches!(
        status,
        200 | 203 | 204 | 300 | 301 | 308 | 404 | 405 | 410 | 414 | 501
    ) {
        return None;
    }
    if body_len > max_object || head.len() > MAX_HEAD {
        return None;
    }
    let mut cc = Cc::default();
    let mut vary_ae = false;
    let mut vary_other = false;
    let mut set_cookie = false;
    let mut encoded = false;
    let mut date_ms = 0u64;
    let mut age = 0u32;
    for (n, v) in crate::http::header_lines(head) {
        match n.len() {
            10 if eq_ci(n, b"set-cookie") => set_cookie = true,
            16 if eq_ci(n, b"content-encoding") => encoded = true,
            4 if eq_ci(n, b"date") => date_ms = parse_http_date(v).unwrap_or(0),
            3 if eq_ci(n, b"age") => age = parse_u32(trim(v)).unwrap_or(0),
            13 if eq_ci(n, b"cache-control") => cc.observe(v),
            4 if eq_ci(n, b"vary") => {
                for t in v.split(|&c| c == b',') {
                    let t = trim(t);
                    if eq_ci(t, b"accept-encoding") {
                        vary_ae = true;
                    } else if t == b"*" || !t.is_empty() {
                        vary_other = true;
                    }
                }
            }
            _ => {}
        }
    }
    if set_cookie || encoded || cc.private || cc.no_store || cc.no_cache || vary_other {
        return None;
    }
    let life = cc.s_maxage.or(cc.max_age).filter(|&n| n > 0)?;
    let now = now_ms();
    let apparent = if date_ms > 0 {
        now.saturating_sub(date_ms) / 1000
    } else {
        0
    };
    let corrected = age.max(apparent as u32);
    let remain = (life as u64).saturating_sub(corrected as u64);
    if remain == 0 {
        return None;
    }
    Some(Policy {
        ttl: remain.min(ttl_max),
        age: corrected,
        vary_ae,
    })
}

/// Headers kept in a stored copy: hop-by-hop and freshness fields are dropped.
pub fn store_head(head: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for (n, v) in crate::http::header_lines(head) {
        if skip_store(n) {
            continue;
        }
        out.extend_from_slice(n);
        out.extend_from_slice(b": ");
        out.extend_from_slice(v);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn skip_store(n: &[u8]) -> bool {
    (n.len() == 4 && eq_ci(n, b"date"))
        || (n.len() == 3 && eq_ci(n, b"age"))
        || (n.len() == 14 && eq_ci(n, b"content-length"))
        || (n.len() == 17 && eq_ci(n, b"transfer-encoding"))
        || (n.len() == 10 && eq_ci(n, b"connection"))
        || (n.len() == 10 && eq_ci(n, b"keep-alive"))
        || (n.len() == 2 && eq_ci(n, b"te"))
        || (n.len() == 7 && eq_ci(n, b"trailer"))
        || (n.len() == 7 && eq_ci(n, b"upgrade"))
        || (n.len() == 12 && eq_ci(n, b"x-request-id"))
}

#[derive(Default)]
struct Cc {
    max_age: Option<u32>,
    s_maxage: Option<u32>,
    private: bool,
    no_store: bool,
    no_cache: bool,
}

impl Cc {
    fn observe(&mut self, v: &[u8]) {
        for t in v.split(|&c| c == b',') {
            let t = trim(t);
            let (name, val) = match t.iter().position(|&c| c == b'=') {
                Some(i) => (trim(&t[..i]), trim(&t[i + 1..])),
                None => (t, &b""[..]),
            };
            if eq_ci(name, b"max-age") {
                self.max_age = parse_u32(val);
            } else if eq_ci(name, b"s-maxage") {
                self.s_maxage = parse_u32(val);
            } else if eq_ci(name, b"private") {
                self.private = true;
            } else if eq_ci(name, b"no-store") {
                self.no_store = true;
            } else if eq_ci(name, b"no-cache") {
                self.no_cache = true;
            }
        }
    }
}

fn has_token(v: &[u8], want: &[u8]) -> bool {
    v.split(|&c| c == b',').map(trim).any(|t| {
        let name = match t.iter().position(|&c| c == b'=') {
            Some(i) => trim(&t[..i]),
            None => t,
        };
        eq_ci(name, want)
    })
}

fn max_age_zero(v: &[u8]) -> bool {
    v.split(|&c| c == b',').map(trim).any(|t| {
        let (n, val) = match t.iter().position(|&c| c == b'=') {
            Some(i) => (trim(&t[..i]), trim(&t[i + 1..])),
            None => return false,
        };
        eq_ci(n, b"max-age") && parse_u32(val) == Some(0)
    })
}

fn parse_u32(v: &[u8]) -> Option<u32> {
    let v = trim(v).strip_prefix(b"\"").unwrap_or(v);
    let v = v.strip_suffix(b"\"").unwrap_or(v);
    if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
        return None;
    }
    String::from_utf8_lossy(v).parse().ok()
}

/// An IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) as Unix milliseconds.
/// The obsolete forms are not read: without a Date the application's Age
/// alone ages the response (RFC 9111 4.2.3).
fn parse_http_date(v: &[u8]) -> Option<u64> {
    let v = trim(v);
    if v.len() != 29 || &v[3..5] != b", " || &v[25..] != b" GMT" {
        return None;
    }
    let num = |s: &[u8]| -> Option<u64> {
        s.iter().try_fold(0u64, |n, &c| {
            c.is_ascii_digit().then(|| n * 10 + u64::from(c - b'0'))
        })
    };
    const MONTHS: [&[u8; 3]; 12] = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ];
    let day = num(&v[5..7])?;
    let month = MONTHS.iter().position(|m| &v[8..11] == m.as_slice())? as u64 + 1;
    let year = num(&v[12..16])?;
    if v[7] != b' ' || v[11] != b' ' || v[16] != b' ' || v[19] != b':' || v[22] != b':' {
        return None;
    }
    let (h, m, s) = (num(&v[17..19])?, num(&v[20..22])?, num(&v[23..25])?);
    if !(1..=31).contains(&day) || year < 1970 || h > 23 || m > 59 || s > 60 {
        return None;
    }
    // Days since 1970-01-01 in the proleptic Gregorian calendar.
    let (y, mp) = if month > 2 {
        (year, month - 3)
    } else {
        (year - 1, month + 9)
    };
    let era = y / 400;
    let yoe = y - era * 400;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = (era * 146_097 + doe).checked_sub(719_468)?;
    Some((days * 86_400 + h * 3600 + m * 60 + s) * 1000)
}

pub struct Capture {
    pub key: Vec<u8>,
    pub target: u64,
    pub variant: u64,
    pub status: u16,
    pub head: Vec<u8>,
    pub body: Vec<u8>,
    pub max_object: usize,
    pub ttl_max: u64,
}

impl Capture {
    pub fn push(&mut self, data: &[u8]) -> bool {
        if self.body.len().saturating_add(data.len()) > self.max_object {
            return false;
        }
        self.body.extend_from_slice(data);
        true
    }

    pub fn finish(self) {
        let Some(p) = response_ok(
            self.status,
            &self.head,
            self.body.len(),
            self.max_object,
            self.ttl_max,
        ) else {
            return;
        };
        let variant = if p.vary_ae { self.variant } else { 0 };
        store(
            &self.key,
            self.target,
            variant,
            self.status,
            &store_head(&self.head),
            &self.body,
            p.ttl,
            p.age,
        );
    }
}

/// Serves a cache hit: `Cache-Status`, `Age`, optional compression.
pub fn write_hit(
    st: &mut crate::http::State,
    ctx: &crate::core::AppCtx,
    hit: &Hit,
    head_only: bool,
    date: &[u8; 29],
    stopping: bool,
) {
    let mut head = format!("HTTP/1.1 {} ", hit.status).into_bytes();
    head.extend_from_slice(crate::http::reason(hit.status));
    head.extend_from_slice(b"\r\n");
    head.extend_from_slice(&hit.head);
    let mut info = crate::http::HeadInfo::default();
    if hit.status != 204 && hit.status != 304 && !head_only {
        info.length = Some(hit.body.len() as u64);
    }
    st.cache_ok = false;
    st.start(hit.status, head, info);
    st.cache_hit = Some((hit.age, hit.ttl));
    if head_only || hit.status == 204 || hit.status == 304 {
        st.write_body(b"", false, ctx, date, stopping);
    } else {
        st.write_body(&hit.body, false, ctx, date, stopping);
    }
}

pub fn inm_matches(headers: &[httparse::Header<'_>], etag: &[u8]) -> bool {
    for h in headers {
        if h.name.len() == 13 && eq_ci(h.name.as_bytes(), b"if-none-match") {
            let v = trim(h.value);
            if v == b"*" {
                return true;
            }
            for t in v.split(|&c| c == b',') {
                let t = trim(t);
                let t = if t.len() >= 2 && (t.starts_with(b"W/") || t.starts_with(b"w/")) {
                    &t[2..]
                } else {
                    t
                };
                let e = if etag.len() >= 2 && (etag.starts_with(b"W/") || etag.starts_with(b"w/")) {
                    &etag[2..]
                } else {
                    etag
                };
                if t == e {
                    return true;
                }
            }
        }
    }
    false
}

pub fn stored_etag(head: &[u8]) -> Vec<u8> {
    for (n, v) in crate::http::header_lines(head) {
        if n.len() == 4 && eq_ci(n, b"etag") {
            return trim(v).to_vec();
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_and_hit() {
        install(2 * 1024 * 1024, 64 * 1024, None).unwrap();
        let key = key(false, b"localhost", b"/fresh", None).unwrap();
        store(
            &key,
            target_hash(b"/fresh"),
            0,
            200,
            b"content-type: text/plain\r\ncache-control: max-age=60\r\n",
            b"hi",
            60,
            0,
        );
        let hit = lookup(&key, 0, None).expect("hit");
        assert_eq!(hit.status, 200);
        assert_eq!(hit.body, b"hi");
        invalidate(target_hash(b"/fresh"));
        assert!(lookup(&key, 0, None).is_none());
    }

    #[test]
    fn private_not_stored() {
        assert!(
            response_ok(200, b"cache-control: private, max-age=60\r\n", 2, 1024, 300).is_none()
        );
        assert!(response_ok(200, b"cache-control: max-age=60\r\n", 2, 1024, 300).is_some());
    }

    #[test]
    fn http_date() {
        assert_eq!(
            parse_http_date(b"Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784_111_777_000)
        );
        assert_eq!(parse_http_date(b"Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
        assert_eq!(
            parse_http_date(b"Tue, 29 Feb 2000 23:59:59 GMT"),
            Some(951_868_799_000)
        );
        assert_eq!(parse_http_date(b"Sunday, 06-Nov-94 08:49:37 GMT"), None);
        assert_eq!(parse_http_date(b"Sun, 06 Foo 1994 08:49:37 GMT"), None);
        assert_eq!(parse_http_date(b"Sun, 06 Nov 1994 08:49:37 UTC"), None);
    }

    #[test]
    fn date_ages_the_response() {
        let stale = b"date: Sat, 01 Jan 2000 00:00:00 GMT\r\ncache-control: max-age=60\r\n";
        assert!(response_ok(200, stale, 2, 1024, 300).is_none());
        let mut fresh = b"date: ".to_vec();
        fresh.extend_from_slice(&crate::http::format_date(now_ms() / 1000));
        fresh.extend_from_slice(b"\r\ncache-control: max-age=60\r\n");
        let p = response_ok(200, &fresh, 2, 1024, 300).expect("fresh response is stored");
        assert!(p.ttl > 55 && p.age < 5);
        assert!(
            response_ok(
                200,
                b"cache-control: max-age=60\r\nvary: user-agent\r\n",
                2,
                1024,
                300
            )
            .is_none()
        );
    }

    #[test]
    fn layout_fits_one_mib_default_object() {
        let len = 1024 * 1024;
        let max_object = 1024 * 1024;
        let (small, large) = layout(len, max_object);
        let end = small.off + small.n * small.slot + large.n * large.slot;
        assert!(end <= len, "layout {end} exceeds allocation {len}");
        assert_eq!(large.n, 0);
        assert!(small.n > 0);
    }

    #[test]
    fn store_ranks_under_lock() {
        install(2 * 1024 * 1024, 64 * 1024, None).unwrap();
        let key = key(false, b"localhost", b"/locked", None).unwrap();
        store(
            &key,
            target_hash(b"/locked"),
            0,
            200,
            b"content-type: text/plain\r\ncache-control: max-age=60\r\n",
            b"one",
            60,
            0,
        );
        store(
            &key,
            target_hash(b"/locked"),
            0,
            200,
            b"content-type: text/plain\r\ncache-control: max-age=60\r\n",
            b"two",
            60,
            0,
        );
        let hit = lookup(&key, 0, None).expect("hit");
        assert_eq!(hit.body, b"two");
    }

    #[test]
    fn layout_fits_eight_mib_default_object() {
        let len = 8 * 1024 * 1024;
        let max_object = 1024 * 1024;
        let (small, large) = layout(len, max_object);
        let end = small.off + small.n * small.slot + large.n * large.slot;
        assert!(end <= len, "layout {end} exceeds allocation {len}");
        assert!(large.n >= 1);
        assert!(small.n > 0);
    }
}
