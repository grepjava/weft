//! Prometheus counters on `--metrics-port`: a port of its own, summed
//! across every worker from a shared page.

use std::cell::Cell;
use std::io;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::MmapMut;
use tokio::net::TcpListener;

use crate::http::push_int;
use crate::tls::Io;

const MAGIC: u64 = 0x5746_544D_4554_5249;
const SLOTS: usize = 64;
const BUCKETS: usize = 13;
/// Microsecond bucket edges; Prometheus `le` is seconds to six places.
const EDGES_US: [u64; BUCKETS] = [
    1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000,
    5_000_000, 10_000_000,
];

const I_1XX: usize = 0;
const I_2XX: usize = 1;
const I_3XX: usize = 2;
const I_4XX: usize = 3;
const I_5XX: usize = 4;
const I_ACCEPTED: usize = 5;
const I_CLOSED: usize = 6;
const I_REJECTED: usize = 7;
const I_ACTIVE: usize = 8;
const I_SLOTS: usize = 9;
const I_RATE: usize = 10;
const I_CACHE_HIT: usize = 11;
const I_CACHE_MISS: usize = 12;
const I_CACHE_STORE: usize = 13;
const I_DUR_COUNT: usize = 14;
const I_DUR_SUM: usize = 15;
const I_BUCKET0: usize = 16;
const N: usize = I_BUCKET0 + BUCKETS;

#[repr(C)]
struct Header {
    magic: AtomicU64,
    workers: AtomicU64,
}

#[repr(C)]
struct Slot {
    v: [AtomicU64; N],
}

struct Table {
    _map: Option<MmapMut>,
    header: *const Header,
    slots: *const Slot,
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

static TABLE: OnceLock<Table> = OnceLock::new();
thread_local! {
    static SLOT: Cell<usize> = const { Cell::new(0) };
}

pub const PAGE_BYTES: usize = std::mem::size_of::<Header>() + SLOTS * std::mem::size_of::<Slot>();

pub fn enabled() -> bool {
    TABLE.get().is_some()
}

pub fn install(map: Option<&str>, workers: u64) -> io::Result<()> {
    if TABLE.get().is_some() {
        return Ok(());
    }
    let (keep, base) = match map {
        Some(path) => {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?;
            if file.metadata()?.len() < PAGE_BYTES as u64 {
                file.set_len(PAGE_BYTES as u64)?;
            }
            let mut mmap = unsafe { MmapMut::map_mut(&file)? };
            let ptr = mmap.as_mut_ptr();
            (Some(mmap), ptr)
        }
        None => {
            let mut v = vec![0u8; PAGE_BYTES];
            let ptr = v.as_mut_ptr();
            std::mem::forget(v);
            (None, ptr)
        }
    };
    let header = base as *mut Header;
    let slots = unsafe { base.add(std::mem::size_of::<Header>()) as *mut Slot };
    unsafe {
        if (*header).magic.load(Ordering::Relaxed) != MAGIC {
            (*header).magic.store(MAGIC, Ordering::Relaxed);
            (*header).workers.store(workers.max(1), Ordering::Relaxed);
            for i in 0..SLOTS {
                let s = &mut *slots.add(i);
                for c in &mut s.v {
                    c.store(0, Ordering::Relaxed);
                }
            }
        } else if workers > 0 {
            (*header).workers.store(workers, Ordering::Relaxed);
        }
    }
    let _ = TABLE.set(Table {
        _map: keep,
        header,
        slots,
    });
    Ok(())
}

pub fn bind(slot: usize, max_conns: u64) {
    let slot = slot.min(SLOTS - 1);
    SLOT.with(|c| c.set(slot));
    set(I_SLOTS, max_conns);
}

#[inline]
pub fn add(i: usize, n: u64) {
    let Some(t) = TABLE.get() else { return };
    let slot = SLOT.with(|c| c.get());
    unsafe { (*t.slots.add(slot)).v[i].fetch_add(n, Ordering::Relaxed) };
}

fn set(i: usize, n: u64) {
    let Some(t) = TABLE.get() else { return };
    let slot = SLOT.with(|c| c.get());
    unsafe { (*t.slots.add(slot)).v[i].store(n, Ordering::Relaxed) };
}

fn sum(i: usize) -> u64 {
    let Some(t) = TABLE.get() else { return 0 };
    let mut tot = 0u64;
    for s in 0..SLOTS {
        tot = tot.wrapping_add(unsafe { (*t.slots.add(s)).v[i].load(Ordering::Relaxed) });
    }
    tot
}

fn workers() -> u64 {
    TABLE.get().map_or(0, |t| unsafe {
        (*t.header).workers.load(Ordering::Relaxed)
    })
}

pub fn accepted() {
    add(I_ACCEPTED, 1);
    add(I_ACTIVE, 1);
}

pub fn closed() {
    add(I_CLOSED, 1);
    let Some(t) = TABLE.get() else { return };
    let slot = SLOT.with(|c| c.get());
    unsafe {
        let a = &(*t.slots.add(slot)).v[I_ACTIVE];
        let _ = a.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(1))
        });
    }
}

pub fn rejected() {
    add(I_REJECTED, 1);
}

pub fn rate_limited() {
    add(I_RATE, 1);
}

pub fn cache_hit() {
    add(I_CACHE_HIT, 1);
}

pub fn cache_miss() {
    add(I_CACHE_MISS, 1);
}

pub fn cache_store() {
    add(I_CACHE_STORE, 1);
}

pub fn request_finished(status: u16, micros: u64) {
    add(
        match status / 100 {
            2 => I_2XX,
            3 => I_3XX,
            4 => I_4XX,
            5 => I_5XX,
            _ => I_1XX,
        },
        1,
    );
    add(I_DUR_COUNT, 1);
    add(I_DUR_SUM, micros);
    if let Some(i) = EDGES_US.iter().position(|&e| micros <= e) {
        add(I_BUCKET0 + i, 1);
    }
}

pub fn render(cache: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(2048);
    labelled(
        &mut out,
        b"weft_requests_total",
        b"Responses sent, by status class.",
        b"status",
        &[
            (b"1xx", I_1XX),
            (b"2xx", I_2XX),
            (b"3xx", I_3XX),
            (b"4xx", I_4XX),
            (b"5xx", I_5XX),
        ],
    );
    simple(
        &mut out,
        b"weft_connections_accepted_total",
        b"counter",
        b"Connections accepted.",
        sum(I_ACCEPTED),
    );
    simple(
        &mut out,
        b"weft_connections_closed_total",
        b"counter",
        b"Connections closed.",
        sum(I_CLOSED),
    );
    simple(
        &mut out,
        b"weft_connections_rejected_total",
        b"counter",
        b"Connections refused for want of a slot.",
        sum(I_REJECTED),
    );
    simple(
        &mut out,
        b"weft_connections_active",
        b"gauge",
        b"Connections open right now.",
        sum(I_ACTIVE),
    );
    simple(
        &mut out,
        b"weft_connection_slots",
        b"gauge",
        b"Connection table capacity, summed over workers.",
        sum(I_SLOTS),
    );
    simple(
        &mut out,
        b"weft_requests_rate_limited_total",
        b"counter",
        b"Requests refused with 429 by --rate-limit.",
        sum(I_RATE),
    );
    if cache {
        simple(
            &mut out,
            b"weft_cache_hits_total",
            b"counter",
            b"Requests answered from the response cache.",
            sum(I_CACHE_HIT),
        );
        simple(
            &mut out,
            b"weft_cache_misses_total",
            b"counter",
            b"Requests looked up in the response cache and not found.",
            sum(I_CACHE_MISS),
        );
        simple(
            &mut out,
            b"weft_cache_stores_total",
            b"counter",
            b"Responses stored in the response cache.",
            sum(I_CACHE_STORE),
        );
    }
    simple(
        &mut out,
        b"weft_workers",
        b"gauge",
        b"Workers sharing these counters.",
        workers(),
    );
    histogram(&mut out);
    out
}

fn simple(out: &mut Vec<u8>, name: &[u8], kind: &[u8], help: &[u8], value: u64) {
    out.extend_from_slice(b"# HELP ");
    out.extend_from_slice(name);
    out.push(b' ');
    out.extend_from_slice(help);
    out.extend_from_slice(b"\n# TYPE ");
    out.extend_from_slice(name);
    out.push(b' ');
    out.extend_from_slice(kind);
    out.push(b'\n');
    out.extend_from_slice(name);
    out.push(b' ');
    push_int(out, value);
    out.push(b'\n');
}

fn labelled(out: &mut Vec<u8>, name: &[u8], help: &[u8], label: &[u8], rows: &[(&[u8], usize)]) {
    out.extend_from_slice(b"# HELP ");
    out.extend_from_slice(name);
    out.push(b' ');
    out.extend_from_slice(help);
    out.extend_from_slice(b"\n# TYPE ");
    out.extend_from_slice(name);
    out.extend_from_slice(b" counter\n");
    for (val, i) in rows {
        out.extend_from_slice(name);
        out.extend_from_slice(b"{");
        out.extend_from_slice(label);
        out.extend_from_slice(b"=\"");
        out.extend_from_slice(val);
        out.extend_from_slice(b"\"} ");
        push_int(out, sum(*i));
        out.push(b'\n');
    }
}

fn histogram(out: &mut Vec<u8>) {
    out.extend_from_slice(b"# HELP weft_request_duration_seconds Time from dispatch to the response head being queued.\n");
    out.extend_from_slice(b"# TYPE weft_request_duration_seconds histogram\n");
    let mut cum = 0u64;
    for (i, &edge) in EDGES_US.iter().enumerate() {
        cum = cum.wrapping_add(sum(I_BUCKET0 + i));
        out.extend_from_slice(b"weft_request_duration_seconds_bucket{le=\"");
        write_seconds(out, edge);
        out.extend_from_slice(b"\"} ");
        push_int(out, cum);
        out.push(b'\n');
    }
    let count = sum(I_DUR_COUNT);
    out.extend_from_slice(b"weft_request_duration_seconds_bucket{le=\"+Inf\"} ");
    push_int(out, count);
    out.extend_from_slice(b"\nweft_request_duration_seconds_sum ");
    write_seconds(out, sum(I_DUR_SUM));
    out.extend_from_slice(b"\nweft_request_duration_seconds_count ");
    push_int(out, count);
    out.push(b'\n');
}

fn write_seconds(out: &mut Vec<u8>, micros: u64) {
    push_int(out, micros / 1_000_000);
    out.push(b'.');
    let mut frac = micros % 1_000_000;
    let mut div = 100_000u64;
    while div > 0 {
        out.push(b'0' + (frac / div) as u8);
        frac %= div;
        div /= 10;
    }
}

pub async fn serve_scrape(host: String, port: u16, cache: bool) {
    loop {
        match bind_listener(&host, port) {
            Ok(lis) => accept(lis, cache).await,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    }
}

async fn accept(listener: TcpListener, cache: bool) {
    loop {
        match listener.accept().await {
            Ok((tcp, _)) => {
                let _ = tcp.set_nodelay(true);
                tokio::task::spawn_local(scrape(Io::plain(tcp), cache));
            }
            Err(_) => return,
        }
    }
}

async fn scrape(io: Io, cache: bool) {
    let mut buf = bytes::BytesMut::with_capacity(1024);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if tokio::time::timeout_at(deadline, io.readable())
            .await
            .is_err()
        {
            return;
        }
        match io.try_read_buf(&mut buf) {
            Ok(0) => return,
            Ok(_) => {
                if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 8192 {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => return,
        }
    }
    let body = render(cache);
    let mut head = Vec::with_capacity(160 + body.len());
    head.extend_from_slice(b"HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4; charset=utf-8\r\ncontent-length: ");
    push_int(&mut head, body.len() as u64);
    head.extend_from_slice(b"\r\nconnection: close\r\n\r\n");
    head.extend_from_slice(&body);
    let mut off = 0;
    while off < head.len() {
        if tokio::time::timeout_at(deadline, io.writable())
            .await
            .is_err()
        {
            return;
        }
        match io.try_write(&head[off..]) {
            Ok(0) => return,
            Ok(n) => off += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => return,
        }
    }
    io.shutdown_write();
}

fn bind_listener(host: &str, port: u16) -> io::Result<TcpListener> {
    let spec = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let addr: SocketAddr = spec
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let std = std::net::TcpListener::bind(addr)?;
    std.set_nonblocking(true)?;
    TcpListener::from_std(std)
}
