# Fast HTTP/1.1 server techniques vs. Weft (FastAPI workload)

Research notes (2026-09-06) on what the fastest HTTP servers do and which of
those techniques still matter once every request ends in a FastAPI call.
Claims about Weft were verified in this tree; external claims are linked.

## 0. Baseline: what Weft already has

- Response start + complete body are combined into one `Full` response;
  `send()` returns immediate awaitables that never touch the loop. hyper emits
  status line + headers + body in **one syscall** (writev on Tokio, flattened
  buffer on Compio and TLS).
- `Date` header cached per thread by hyper (re-rendered when the second rolls).
- `Content-Length` from the exact body size; chunked only for streams/SSE.
- `TCP_NODELAY` on every socket; `SO_REUSEPORT` per worker/thread on Linux;
  dispatched accept on Windows.
- Lazy self-pipe wake (`weft/_wake.py`): the largest measured win so far
  (FastAPI hello 6.9k -> 10.2k req/s, 219 -> 147 us server CPU per request).
- Retained (zero-copy) response bodies; byte-based streaming budgets with a
  lazy per-response semaphore.
- Scope: interned keys, cached `asgi`/`extensions` dicts; built on a
  GIL-holding blocking thread, then one `call_soon_threadsafe` to the loop.
- `task_impl=rust` (eager coroutine driver) exists but is forced off on
  Python >= 3.12; the shipping path is `call_soon_threadsafe(_run)` ->
  `loop.create_task(coro)` -> one more loop iteration before the app's first
  step.
- Measured server-side share (raw ASGI hello, after lazy wake): ~61 us CPU per
  request; FastAPI adds ~86 us on top. Everything below is judged against that
  61 us budget.

## 1. uWebSockets / uSockets / socketify.py

| Technique | Verdict for Weft |
|---|---|
| Corking (batch writes into one syscall) | done for one response (hyper writev/flatten); cross-response corking only matters with pipelining clients |
| Per-loop shared recv buffer, no per-request heap allocation | not applicable as-is: hyper owns per-connection buffers; Rust per-request allocation is dwarfed by Python's (scope dict, header tuples, coroutine, Task). Consolidating the protocol's ~7 Arcs into one is small/medium |
| Static pre-serialized header blocks, cached Date | Date done; header names are re-validated per response (`HeaderName::from_bytes`); a static table for common names is small gain, small effort |
| HTTP pipelining | plaintext-benchmark trick (japronto's author acknowledged it); hyper `pipeline_flush` is exposed, keep off |
| Timer wheel sweep | not applicable (hyper timers are per connection, nil per request on keep-alive) |
| One loop per core, SO_REUSEPORT | done |
| Backpressure via drain callbacks | done differently (byte budgets + bounded channel) |
| socketify's Python bridge | on CPython it is roughly what Weft does with PyO3; its transferable trick (drive the app coroutine directly, +17 % and +21 % on another Rust server's raw path) is Weft's `_future_watcher_wrapper` + `CallbackScheduler`, except the eager path is disabled on >= 3.12 |

## 2. TechEmpower top performers, hyper knobs, allocators

- may-minihttp / ntex / xitca / libreactor: no allocation on the plaintext
  path, buffer reuse, pipelining, mimalloc, CPU affinity, pre-built header
  bytes, cached date. libreactor's write-up is the only one with per-step
  numbers, and most of its gains are kernel-side (mitigations, iptables,
  locality, busy polling) on a server with ~1 us of application time. At
  147 us/request of Python each is worth well under 1 % except locality/pinning.
- hyper `http1::Builder`: nothing left to turn for the FastAPI case.
  Compio-specific: hyper Flatten + compat buffer = two copies per response;
  implementing `poll_write_vectored` on `CompioIo` (or writing straight into
  the compat buffer) is small gain for <= 10 KB, medium for 64 KiB - 1 MiB.
- Allocator: Weft has `mimalloc`/`jemalloc` features but ships the system
  allocator. Actix API benchmark +7.6 % with mimalloc; gains show up mostly in
  tail latency and in threads mode (N loops + N blocking pools + runtime
  threads on one heap). Trivial to try. CPython 3.13t+ already uses mimalloc
  for Python objects.

## 3. Python server specifics

uvicorn+httptools, Robyn, japronto, bjoern, nginx unit, velocem:
none publishes a bridge-cost breakdown; all confirm the same floor: once the
request reaches CPython, the scope dict / header list / coroutine / Task
construction is the cost.

Where Weft's Python-side cost goes per FastAPI GET: on the blocking thread
under the GIL: scope dict (~12 keys), list of n `(bytes, bytes)` tuples,
watcher + protocol pyclasses, coroutine object, contextvars copy, one Handle;
on the loop: `_run` -> `create_task` (Task + `call_soon` + a second loop
iteration), `_callback_wrapper._runner` (`scope.update(root_path=...,
state=state.copy())`), then Starlette/FastAPI (~86 us), then two `send()`
dicts parsed back in Rust. For FastAPI's JSON path the coroutine never truly
suspends: every `await send(...)` completes synchronously. That is exactly the
case CPython's eager tasks target (Instagram: ~70 % of coroutines complete
synchronously; ~4 % CPU saved on Django; 1.4x mixed / up to 8x all-sync in
microbenchmarks).

## 4. Response path

| Technique | Weft |
|---|---|
| Cached Date per second | done (hyper) |
| Status + headers + small body in one write/writev | done |
| Avoid chunked when length known | done |
| TCP_NODELAY + full-response writes | done |
| Cross-response corking (`pipeline_flush`) | exposed, keep off |
| `SO_ZEROCOPY` / io_uring `send_zc` | none for 20 B - 10 KB JSON (LWN: +0.4 % at 600 B, +22 % only at 4000 B on NIC); not applicable |
| Registered/fixed io_uring buffers | not applicable through hyper's borrowed-buffer model |
| Kernel TLS + sendfile | static files only; rustls has no kTLS; not applicable |

## 5. Accept / connection

| Technique | Weft |
|---|---|
| `SO_REUSEPORT` per core | done on Linux; Windows uses dispatched accept |
| `TCP_DEFER_ACCEPT`, `TCP_FASTOPEN` | ~0 with keep-alive; not applicable |
| `SO_BUSY_POLL` | steals the CPU Python needs; not applicable |
| CPU pinning / locality | applicable on Linux (pin worker + runtime + blocking threads next to its listener); small gain, small effort |
| Huge pages | no evidence; skip |
| Allocator | see section 2 |

## 6. Python-in-the-loop

- **Batching N requests per GIL acquisition**: `blocking.rs` detaches and
  re-attaches the GIL per queued task; draining `try_recv()` while attached
  amortizes the handoff under load. Small-medium gain at saturation; not
  needed on the free-threaded build.
- **Loop on the I/O thread** (socketify/japronto/bjoern): Weft keeps runtime
  threads GIL-free so Rust I/O overlaps Python (measured ~1.5 cores per
  worker); not applicable as a whole. The partial form is the `receive()`
  fast path.
- **Avoiding wakeups**: lazy wake done. Remaining hops per request: the
  schedule hop (blocking -> loop) and, per `receive()`, one runtime task + one
  blocking hop + one loop hop. For small POSTs the first body frame is almost
  always already buffered when the app calls `receive()`; polling the body
  once on the Python thread and returning a completed awaitable removes all
  three hops. Medium gain on POST endpoints.
- **Eager first step**: `task_impl=rust` is the eager driver, disabled on
  >= 3.12. The asyncio path can get most of it with
  `loop.set_task_factory(asyncio.eager_task_factory)` (3.12+).
- **PyObject reuse**: still per request: header-name `PyBytes`, host tuple,
  `state.copy()`, `root_path` string. Small.
- **Free-threaded**: CPython 3.14 asyncio scales linearly across loops in
  separate threads (per-thread task lists). Remaining FT costs: biased
  refcounting slow path for objects created on the blocking thread and used
  on the loop thread, the 10 ms lazy-wake cap (idle loops tick at 100 Hz),
  native allocator contention. PEP 684 subinterpreters are not a substitute
  (Django fails, only immutable data crosses, asyncio signaling broken).

## Ranked top 10 for a FastAPI workload (JSON 20 B - 10 KB, some POSTs, keep-alive, c = 64 - 512)

1. **Eager first step of the app coroutine** (`eager_task_factory`, or re-enable the Rust driver on >= 3.12). Removes a Task schedule + one loop iteration per request. Gain 3-8 % of total, effort small.
2. **Synchronous `receive()` fast path** for body frames already buffered by hyper (plus exhausted -> `http.disconnect`). Removes 1 runtime task, 1 blocking hop and 1 loop hop per POST. Medium gain on POST, effort small-medium.
3. **Drain the blocking queue per GIL acquisition** (`blocking.rs`). Small-medium gain at c >= 256, effort small.
4. **Measure winloop (Windows) / uvloop (Linux)** against asyncio + lazy wake before more asyncio work. Zero code.
5. **Move scope construction to the loop thread**: shortens the blocking thread's GIL hold; under FT every scope object is owned by the thread that uses it. Small (GIL) / medium (FT) gain, effort medium.
6. **Cache header-name objects both ways** (static `PyBytes` for request names, `HeaderName` statics for response names). 1-3 %, effort small.
7. **Ship mimalloc by default.** Small in processes mode, more in threads mode; trivial. Measure tail latency.
8. **Port `PyFutureAwaitable` to Windows** (Windows uses `create_future` + `add_done_callback` + `call_soon_threadsafe` setter: three extra Python calls per awaited future). ~0 for JSON GET, small-medium for uploads/streams/SSE, effort medium.
9. **Compio vectored writes on `CompioIo`.** Small for <= 10 KB, medium for 64 KiB - 1 MiB, effort medium.
10. **Linux placement**: pin each worker (and its runtime/blocking threads) to a core next to its `SO_REUSEPORT` listener. Small gain, effort small.

Explicitly not recommended: pipelining corking, `SO_ZEROCOPY`/`send_zc`,
kTLS, `TCP_DEFER_ACCEPT`/`FASTOPEN`, busy polling, and changes to the
one-syscall response path (already at its floor). Native routing is not where
FastAPI's 86 us goes (that is dependency solving, pydantic and serialization,
not `Router` matching). Application-side, avoiding `BaseHTTPMiddleware`
(35-44 % throughput loss in published measurements) outweighs every remaining
server item.

## Top 3 for free-threaded worker-threads mode

1. **mimalloc on the Rust side**: per-thread heaps remove the global heap lock.
2. **Thread ownership of per-request Python objects**: build scope/watcher on the loop thread so biased refcounting stays on its fast path.
3. **Replace the 10 ms lazy-wake safety cap with a proper atomic handshake**, and pin each worker thread + its runtime thread to a core.

## Sources

uWS READMORE and AsyncSocket.h/HttpResponse.h/HttpContext.h; uSockets loop.c;
socketify asgi.py and discussion #10; cirospaciari's 2022 write-up;
talawah.io "Extreme HTTP performance tuning"; hyper `http1::Builder` docs and
1.11.0 `common/date.rs`, `proto/h1/io.rs`; tokio TcpStream; uvicorn
httptools_impl.py/server.py; may-minihttp http_server.rs; TFB discussion
#9093; ntex main_plt.rs; japronto issue #21; Robyn architecture; velocem; bjoern; LWN io_uring zero-copy send and
test results; Actix allocator benchmark; NGINX socket sharding and kTLS
posts; SO_BUSY_POLL notes; "work-stealing vs executor-per-thread" study;
cpython#97696 (eager tasks) and Meta's Python 3.12 post; asyncio +
free-threading docs and Kumar Aditya's note; Quansight "Scaling asyncio on
free-threaded Python"; PyO3 free-threading guide; "sub-interpreter web
workers"; Winloop; BaseHTTPMiddleware overhead measurements.
