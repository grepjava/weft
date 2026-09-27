//! `--rate-limit`: a token bucket every worker shares.
//!
//! The count is the server's, not each worker's. Process workers map the same
//! file; thread workers share one heap table. A full table allows the request
//! rather than refuse everyone because the limiter itself ran out of room.

use std::fs::OpenOptions;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use memmap2::MmapMut;

const SLOTS: usize = 65_536;
const PROBES: usize = 16;

#[repr(C)]
struct Slot {
    /// Client key; 0 is empty.
    tag: AtomicU64,
    /// GCRA theoretical arrival time, microseconds since an arbitrary epoch.
    tat: AtomicU64,
}

#[derive(Clone, Copy)]
pub struct RateLimit {
    /// Microseconds of bucket one request occupies.
    emission_us: u64,
    /// How far ahead of `now` a client may be and still be admitted.
    burst_us: u64,
}

static TABLE: OnceLock<Table> = OnceLock::new();

struct Table {
    _map: Option<MmapMut>,
    slots: *const Slot,
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

impl Table {
    fn slots(&self) -> &[Slot] {
        unsafe { std::slice::from_raw_parts(self.slots, SLOTS) }
    }
}

/// `100/s`, `600/m`, `10/h` → (count, period in milliseconds).
pub fn parse(spec: &str) -> Result<(u32, u64), String> {
    let spec = spec.trim();
    let (n, unit) = spec
        .split_once('/')
        .ok_or_else(|| format!("rate limit {spec:?} wants N/s, N/m or N/h"))?;
    let count: u32 = n
        .trim()
        .parse()
        .map_err(|_| format!("invalid rate {spec:?}"))?;
    if count == 0 {
        return Err("rate limit count must be at least 1".into());
    }
    let period = match unit.trim() {
        "s" | "sec" | "second" => 1_000,
        "m" | "min" | "minute" => 60_000,
        "h" | "hour" => 3_600_000,
        _ => return Err(format!("rate limit unit must be s, m or h, not {unit:?}")),
    };
    Ok((count, period))
}

impl RateLimit {
    pub fn new(count: u32, period_ms: u64, burst: u32) -> RateLimit {
        let emission_us = (period_ms * 1_000).div_ceil(count as u64).max(1);
        let burst = burst.max(1);
        RateLimit {
            emission_us,
            burst_us: emission_us.saturating_mul((burst - 1) as u64),
        }
    }

    /// Installs the shared table. `map` is a file every process worker maps;
    /// without it the table lives on the heap of this process.
    pub fn install(self, map: Option<&str>) -> std::io::Result<()> {
        if TABLE.get().is_some() {
            return Ok(());
        }
        let (keep, ptr) = match map {
            Some(path) => {
                let file = OpenOptions::new().read(true).write(true).open(path)?;
                file.set_len((SLOTS * std::mem::size_of::<Slot>()) as u64)?;
                let mut mmap = unsafe { MmapMut::map_mut(&file)? };
                let ptr = mmap.as_mut_ptr() as *const Slot;
                (Some(mmap), ptr)
            }
            None => {
                let boxed = (0..SLOTS)
                    .map(|_| Slot {
                        tag: AtomicU64::new(0),
                        tat: AtomicU64::new(0),
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let ptr = Box::leak(boxed).as_ptr();
                (None, ptr)
            }
        };
        let _ = TABLE.set(Table {
            _map: keep,
            slots: ptr,
        });
        Ok(())
    }
}

/// Microseconds until this client may send another request, or 0 when allowed.
/// `None` if limiting is off or this peer is not counted (a unix socket with
/// no forwarded client).
pub fn wait(cfg: RateLimit, key: Option<u64>) -> Option<u64> {
    let table = TABLE.get()?;
    let key = key?;
    let now = now_us();
    Some(charge(
        table.slots(),
        key,
        now,
        cfg.emission_us,
        cfg.burst_us,
    ))
}

/// A 64-bit key for an IP: the address itself for IPv4, the `/64` for IPv6.
pub fn ip_key(ip: std::net::IpAddr) -> u64 {
    match ip.to_canonical() {
        std::net::IpAddr::V4(v) => u32::from(v) as u64,
        std::net::IpAddr::V6(v) => u64::from_be_bytes(v.octets()[..8].try_into().unwrap()),
    }
}

pub fn name_key(name: &[u8]) -> u64 {
    let mut h = crate::core::Fnv::default();
    std::hash::Hasher::write(&mut h, name);
    let v = std::hash::Hasher::finish(&h);
    if v == 0 { 1 } else { v }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

/// GCRA: one request occupies `emission` microseconds of the bucket.
/// Returns microseconds to wait, or 0 when this one is allowed.
fn charge(slots: &[Slot], key: u64, now: u64, emission: u64, burst: u64) -> u64 {
    let key = if key == 0 { 1 } else { key };
    let start = (key as usize) & (SLOTS - 1);
    let mut empty = None;
    for n in 0..PROBES {
        let slot = &slots[(start + n) & (SLOTS - 1)];
        let tag = slot.tag.load(Ordering::Acquire);
        if tag == key {
            return update(slot, now, emission, burst);
        }
        if tag == 0 {
            empty = empty.or(Some(slot));
            continue;
        }
        // Quiet long enough to have the whole burst back: the slot is free.
        let tat = slot.tat.load(Ordering::Acquire);
        if now.saturating_sub(tat) >= burst.saturating_add(emission) {
            empty = empty.or(Some(slot));
        }
    }
    let Some(slot) = empty else {
        return 0;
    };
    let tag = slot.tag.load(Ordering::Acquire);
    if tag == key {
        return update(slot, now, emission, burst);
    }
    let tat = slot.tat.load(Ordering::Acquire);
    if tag != 0 && now.saturating_sub(tat) < burst.saturating_add(emission) {
        return 0;
    }
    match slot
        .tag
        .compare_exchange(tag, key, Ordering::AcqRel, Ordering::Acquire)
    {
        Ok(_) => update(slot, now, emission, burst),
        Err(now_tag) if now_tag == key => update(slot, now, emission, burst),
        Err(_) => 0,
    }
}

fn update(slot: &Slot, now: u64, emission: u64, burst: u64) -> u64 {
    loop {
        let tat = slot.tat.load(Ordering::Acquire);
        let earliest = now.saturating_add(burst);
        if tat > earliest {
            return tat - earliest;
        }
        let next = tat.max(now).saturating_add(emission);
        if slot
            .tat
            .compare_exchange_weak(tat, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rate() {
        assert_eq!(parse("100/s").unwrap(), (100, 1_000));
        assert_eq!(parse("600/m").unwrap(), (600, 60_000));
        assert_eq!(parse("10/h").unwrap(), (10, 3_600_000));
        assert!(parse("100").is_err());
        assert!(parse("0/s").is_err());
    }

    #[test]
    fn gcra_burst_then_wait() {
        let slots: Vec<Slot> = (0..SLOTS)
            .map(|_| Slot {
                tag: AtomicU64::new(0),
                tat: AtomicU64::new(0),
            })
            .collect();
        let emission = 10_000;
        let burst = emission * 2;
        let now = 1_000_000;
        let key = 42;
        assert_eq!(charge(&slots, key, now, emission, burst), 0);
        assert_eq!(charge(&slots, key, now, emission, burst), 0);
        assert_eq!(charge(&slots, key, now, emission, burst), 0);
        let wait = charge(&slots, key, now, emission, burst);
        assert!(wait >= emission, "{wait}");
        assert_eq!(charge(&slots, key, now + wait, emission, burst), 0);
    }

    #[test]
    fn other_clients_independent() {
        let slots: Vec<Slot> = (0..SLOTS)
            .map(|_| Slot {
                tag: AtomicU64::new(0),
                tat: AtomicU64::new(0),
            })
            .collect();
        let emission = 10_000;
        for i in 0..8 {
            assert_eq!(charge(&slots, i + 1, 1_000_000, emission, 0), 0);
        }
    }

    #[test]
    fn ipv6_is_slash_64() {
        let a: std::net::IpAddr = "2001:db8:1:2:3:4:5:6".parse().unwrap();
        let b: std::net::IpAddr = "2001:db8:1:2:ffff:ffff:ffff:ffff".parse().unwrap();
        let c: std::net::IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_eq!(ip_key(a), ip_key(b));
        assert_ne!(ip_key(a), ip_key(c));
    }
}
