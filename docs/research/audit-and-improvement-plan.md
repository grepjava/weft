# Weft Performance, Reliability & Functionality Audit

Comprehensive code audit of `src/` and `weft/` with exact file/line references, root-cause analyses, and a phased remediation plan.

---

## Phase 1 — Critical Correctness, Security & DoS Fixes

### 1.1 Compression (`src/compress.rs`, `src/http.rs`)
- [x] **Brotli stream never finalized on `finish: true`** (`src/compress.rs:350-420`)
  - Wrapped `Inner` in `Option<Inner>`, finalized Brotli via `z.into_inner()` on `finish == true`, eliminated double-construction of `GzEncoder`/`zstd::Encoder`, lowered dynamic Brotli `lgwin` from `22` to `18`, and added `br_round_trip` & `zstd_round_trip` unit tests.
- [x] **`Vary: Accept-Encoding` emitted when `--compress` is disabled** (`src/http.rs:500-505`)
  - Guarded with `if ctx.compress && elig.may_vary(status) && !elig.vary_covered`, and passed `first_len` when `!more` to `elig.choose()`.

### 1.2 ASGI Framing (`src/asgi.rs`, `src/http.rs`)
- [x] **Duplicate `transfer-encoding: chunked` header** (`src/asgi.rs:1024-1035`, `src/http.rs:528-530`)
  - Removed duplicate `out.extend_from_slice(b"transfer-encoding: chunked\r\n");` in `encode_headers` so `State::write_head` is the sole emitter.
- [x] **Control chars in header values silently dropped on H2/H3** (`src/asgi.rs:940-945`)
  - Aligned `asgi::valid_value` with RFC 9110 (`b == b'\t' || (0x20..=0x7e).contains(&b) || b >= 0x80`) and replaced `valid_name` linear scan with a 256-byte `const` lookup table.
- [x] **Leaked WebSocket task on `Upgrade: websocket` over H2/H3** (`src/asgi.rs:389-400`)
  - Guarded WebSocket upgrade branch with `!is_h2_or_h3` so `ws::start` is never invoked on H2/H3 streams.

### 1.3 HTTP/2 (`src/h2c.rs`)
- [x] **Busy-spin loop on reset/closed stream in `H2Tx::poll_capacity`** (`src/h2c.rs:77-86`)
  - Mapped `Poll::Ready(None)` to `Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))`.
- [x] **`self.send` not cleared after EOS** (`src/h2c.rs:61-75`)
  - Cleared `self.send = None` on EOS and used `self.send.take()` on empty EOS flush.
- [x] **Unbounded `max_concurrent_streams` & unconfigured `max_header_list_size`** (`src/h2c.rs:214-225`)
  - Configured `h2::server::Builder` with `.max_concurrent_streams(256)`, `.max_header_list_size(ctx.max_head)`, `.initial_window_size(256 * 1024)`, and `.initial_connection_window_size(1024 * 1024)`.
- [x] **Graceful `GOAWAY` on shutdown** (`src/h2c.rs:226-250`)
  - `tokio::select!` on `sh.stop.notified()` and `h2.accept()`, calling `h2.graceful_shutdown()` and polling `h2.poll_closed()` to drain active streams.

### 1.4 HTTP/3 & WebTransport (`src/h3c.rs`, `src/wt.rs`)
- [x] **Head-of-line blocking DoS in H3 connection accept loop** (`src/h3c.rs:212-245`)
  - Immediately `spawn_local` on `H3Ev::Bidi(bidi)` and read the first frame inside the spawned task.
- [x] **Malformed bidi streams routed to WebTransport session 0** (`src/h3c.rs:239-243`)
  - Dropped malformed/empty/unexpected bidi streams instead of routing them to session 0.
- [x] **Hardcoded 30 s idle timeout kills SSE/long-lived H3 streams** (`src/h3c.rs:696, 767-776`)
  - Enforced timeout only when `ctx.request_timeout` is `Some(limit)`.
- [x] **Refuse excess QUIC connections cleanly** (`src/h3c.rs:150-154`)
  - Used `incoming.refuse()` instead of `incoming.ignore()`.
- [x] **Double `parse_h1_head` on every H3 response** (`src/h3c.rs:788-820`)
  - Parsed response head once in `flush_h3`.
- [x] **WebTransport lost wakeups, flush-before-close & safe capsule parsing** (`src/wt.rs:285-340, 655-695, 1090-1105`, `src/h3c.rs:485-545`)
  - Pinned and `.enable()`d `notified()` before queue checks in `drive_wt`, `run_h3send`, and `run_h3recv`; switched `stream_send` to `notify_one()`.
  - Flushed queued `out_datagrams` and `pending_open` (plus yielded once for `run_h3send`) before sending `CLOSE_WEBTRANSPORT_SESSION` when `session.gone.get()`.
  - Replaced stateless `parse_capsules` with `BytesMut`-buffered `parse_capsules_buf` guarded by `usize::try_from(len)` and `len > buf.len() - i`.
  - Rejected `webtransport.stream.send` after `end_stream=True`.
- [x] **WebTransport task lifecycle & queue bounds** (`src/wt.rs:262-320, 470-480, 1105-1120`)
  - Passed `session` into `run_h3send` so send tasks exit on `session.gone`, removed closed streams from `Session.streams` after `end_delivered`, and bounded `st.out` (4 MiB) to prevent OOM.

### 1.5 TLS & WebSocket (`src/tls.rs`, `src/ws.rs`)
- [x] **`ws::pump` TLS write stall / truncation** (`src/ws.rs:804-832, 932`)
  - Checked `conn.io().tls_wants_write()` alongside `!st.out.is_empty()` in `ws::pump`, `shutdown_write`, and `flushed`.
- [x] **`pull_tls` ciphertext loss on `Ok(0)` & single-record read** (`src/tls.rs:242-285`)
  - Implemented `Read` for `TryTcp` and fed it directly into `t.conn.read_tls(&mut r)`, looping `t.conn.reader().read(...)` until `WouldBlock`/`Ok(0)`.
- [x] **TLS `close_notify` hangs instead of returning EOF** (`src/tls.rs:119-175`)
  - Tracked `peer_closed` on `Tls` and returned `Ok(0)` once `t.plain.is_empty() && t.peer_closed`.
- [x] **`AsyncIo::poll_flush` & `poll_shutdown` contract fixes** (`src/tls.rs:530-562`)
  - Looped `flush_tls_only()` after `poll_write_ready`; flushed `close_notify` via `AsyncWrite::poll_flush` before shutting down TCP write half.
- [x] **`Io::try_write` swallows non-`WouldBlock` errors** (`src/tls.rs:175-190`)
- [x] **SNI wildcard shadowing & linear scan** (`src/tls.rs:332-355, 412-435`)
  - Split `Resolver` into `exact: HashMap` and `wildcards: HashMap`, checking `exact` first.
- [x] **WebSocket RFC 6455 / ASGI & inflate fixes** (`src/ws.rs:403, 443-490, 580-615, 1276-1305`)
  - Guarded `max_message` arithmetic with `saturating_add`/`checked_add`, rejected non-minimal payload lengths, raised `ValueError` when both `text` and `bytes` are non-`None`, and eliminated `BytesMut` reallocation in `Ws::inflate`.

### 1.6 Rate Limiting, Metrics, fdpoll, WinAccept & Python Layer
- [x] **Rate limiter unhashed `ip_key`, non-monotonic clock & idle-slot reclaim race** (`src/limit.rs:146-215`, `src/asgi.rs:200-215`)
  - Mixed `key` with splitmix64 for slot probing, switched `now_us()` to `CLOCK_MONOTONIC` / `QueryUnbiasedInterruptTime`, atomically reset `tat` on idle-slot reclaim, and parsed proxied `meta.client` as `IpAddr`.
- [x] **Metrics alignment UB, false sharing & `/metrics` HTTP checks** (`src/metrics.rs:44-53, 92-105, 133-145, 423-448`, `src/lib.rs:756-766`, `weft/server.py:19-28`)
  - Added `#[repr(C, align(64))]` to `Header` and `Slot`, allocated 64-byte-aligned backing storage when `map` is `None`, used load+store for single-writer `add()`, exported `METRICS_PAGE_BYTES` & `RATE_MAP_BYTES` from `_weft`, and enforced `GET`/`HEAD` on `/metrics` or `/`.
- [x] **`fdpoll` `EINTR` retry & GIL release in `wait_writable`** (`src/fdpoll.rs:68-78, 145-180`)
- [x] **`winaccept` `POLLERR|POLLHUP|POLLNVAL` loop fix** (`src/winaccept.rs:147-158`)
- [x] **`--redirect-http` bounded concurrency & IPv6/CRLF/open-redirect fixes** (`weft/server.py:138-205`)
- [x] **Supervisor fixes** (`weft/server.py:206-250, 486-545`)
  - Reset `config.cache_flush = False` after first replacement worker in rolling reload, used `/dev/shm` for mmaps on Linux, and added exponential crash-loop backoff.
- [x] **Lifespan `auto` mode crash swallowing** (`weft/lifespan.py:24-70`)
  - Never swallows `KeyboardInterrupt`/`SystemExit`/`CancelledError`, and tracks `_startup_delivered` so crashes after entering lifespan fail startup.

---

## Phase 2 — Request Hot-Path & Event-Loop Performance

- [x] **Zero-copy `Io::try_read_buf` for plain TCP & TLS** (`src/tls.rs:148-174`)
  - Plain TCP delegates directly to `self.tcp.try_read_buf(buf)`, eliminating 16 KiB stack zeroing + `memcpy` on every read.
- [x] **Eliminate per-request & per-`select()` allocations**
  - `Cow::Borrowed` for uppercase HTTP methods (`src/asgi.rs:166-173`).
  - Reuse `resp.head` across keep-alive requests (`src/http.rs:328-340, 597-602`).
  - Borrow `&self.sh`/`&self.remote` and stack-pin `Sleep` in `Core::select()` (`src/core.rs:471-514`); return empty tuple `()` from `w_select` (`src/lib.rs:181-186`) and `WeftSelector.select` (`weft/loop.py:71-82`).
  - Guard `sh.date()` (`src/asgi.rs:1152-1156`) and `Instant::now()` (`src/http.rs:348-350`, `src/asgi.rs:198-200`).
  - `fmt_hex` in `StreamBody::push` (`src/http.rs:1161-1172`), interned `i.https` (`src/asgi.rs:640`, `src/wsgi.rs:235`), and stack `[u8; 20]` for `CONTENT_LENGTH` (`src/wsgi.rs:841-855`).
- [x] **Eager ASGI coroutine execution (`Task(..., eager_start=True)` on 3.12+)** (`src/asgi.rs:461-480, 790-845, 1425-1510`, `src/lib.rs:302-328`)
  - Fast HTTP endpoints finish inline without allocating `TaskDone`, without `add_done_callback`, without `py_scheduled()`, and skip `http::pump` (and its extra `WouldBlock` `recv()`), while preserving full `asyncio.current_task()` semantics.
  - **Now opt-in (`WEFT_EAGER_TASKS=1`).** An interleaved A/B (Windows, 1 worker, 256 connections, 8 rounds) showed eager start cost FastAPI hello ~8% req/s at saturation (9.8k vs 10.7k with it off) and gained nothing on raw ASGI; its only win was a lower p50 at moderate load.
- [x] **Asyncio-free WSGI runner & native C `WsgiInput`** (`weft/worker.py:32-125`, `src/wsgi.rs:33-55, 304-570, 1070-1085`)
  - WSGI workers drive Tokio directly via `Worker.select()` without `WeftEventLoop`, `WeftSelector`, or self-pipe; replaced `io.BytesIO` with a non-GC-tracked C `WsgiInput` implementing `read`/`readline`/`readlines`/`__iter__`/`__next__`, and looked up `sys.stderr` dynamically via `PySys_GetObject`.
- [x] **Per-worker object freelists** (`src/types.rs:18-60, 144-180, 275-315`)
  - Added thread-local freelists (up to 32 objects) for `ReadyObj` and `ChanObj` (`send`/`receive`/`done`), eliminating `PyObject_Malloc`/`Free` on the hot path.
- [x] **O(1) `tcp_conns` counter & reusable `poll_regs` scratch `Vec`s** (`src/core.rs:181-245, 377-405`, `src/http.rs:910, 1620`, `src/h3c.rs:149, 874-885`)

---

## Phase 3 — Subsystem & Protocol Performance Overhaul

- [x] **Shared Response Cache lock-free hints & single-pass lookup** (`src/cache.rs:51-72, 376-420, 554-595`, `src/asgi.rs:345-351`)
  - Added `hash_hint` and `target_hint` `AtomicU64`s on `Slot` (`META = 96`), combined double `lookup` into single-pass `lookup_any`, skipped non-matching slots lock-free on both lookup and `invalidate()`.
- [x] **CSPRNG request IDs & multi-hop `Forwarded` / `X-Forwarded-Proto` fix** (`src/ops.rs:164-185, 227-245`)
  - Used `rustls::crypto::ring::default_provider().secure_random` for UUIDv4 request IDs and selected the client-facing edge hop's `proto`.
- [x] **Cache LRU eviction** (`src/cache.rs:70-76, 443, 487-520, 557`)
  - Packed `used_offset_s: u32` into the existing 4-byte padding gap in `SlotData` (zero extra memory) and evicted the least-recently-used live slot in the 8-slot probe window.
- [x] **Background access-log writer thread, `--access-log-file` & accurate HTTP/2 / HTTP/3 protocol labels** (`src/ops.rs:284-370, 439-448`, `src/asgi.rs:542-565`, `src/lib.rs:687-705`)
  - Offloaded log writes to a dedicated `weft-log` thread over a bounded `sync_channel(256)` supporting both `stderr` and `--access-log-file`, and reported `HTTP/1.0`, `HTTP/1.1`, `HTTP/2`, and `HTTP/3` accurately in JSON logs.

---

## Phase 4 — Functionality & Operational Enhancements

- [x] **Static file serving (`Range` / `206` / `416`, `Accept-Ranges`, `Last-Modified` / `If-Modified-Since`, MIME expansion, `compress_static` `vary` guard)** (`src/staticf.rs:93-310, 485-530`, `tests/test_static.py:150-195`)
- [x] **ASGI `http.response.pathsend` extension & HTTP/2 `early_hint`/`informational` advertisement** (`src/asgi.rs:633-637, 1098, 1235-1315`, `src/lib.rs:354-360, 589-595`, `tests/test_static.py:175-195`)
- [x] **Public re-exports in `weft/contrib/__init__.py`**
- [x] **Config & CLI ergonomics (`--config` TOML / `[tool.weft]`, `WEFT_*` env vars, `--access-log-file`, range validation)** (`weft/config.py:97-105, 153-165, 215-330`, `weft/cli.py:6-15, 75-82, 131-155`, `tests/test_ops.py:263-295`)
