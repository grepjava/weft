# FastPySGI vs. Weft: techniques assessed and measured

Source inspected: [FastPySGI](https://github.com/remittor/fastpysgi) at commit
`439dbfb6e034812422d7000983552aa560f794b5`, read directly rather than taken
from its benchmark claims. FastPySGI is a C extension built on libuv and
llhttp; the interesting part is not its parser but how it drives native I/O
from inside asyncio.

**Outcome: no code and no design was adopted.** Its allocation techniques were
measured on Weft and are worth less than the benchmark noise floor; its
response-path techniques were already implemented; and on two points its ASGI
path is *less* efficient than Weft's, not more. The measurements are recorded
here so these directions are not re-opened without new evidence.

## 1. Where FastPySGI is behind Weft **[verified by reading]**

| Point | FastPySGI | Weft |
|---|---|---|
| `receive()` with the body already buffered | Still creates an asyncio Future and delivers its result through a timer callback ([`asgi.c#L622`](https://github.com/remittor/fastpysgi/blob/439dbfb6e034812422d7000983552aa560f794b5/fastwsgi/asgi.c#L622)) | Returns a already-completed awaitable — no Future, no loop iteration (`receive_sync`, `src/asgi/io.rs`) |
| `send()` of a response body | Creates a Future, completes it from the libuv write callback, schedules delivery back to asyncio ([`server.c#L305`](https://github.com/remittor/fastpysgi/blob/439dbfb6e034812422d7000983552aa560f794b5/fastwsgi/server.c#L305)) | Completes immediately when the bounded channel accepts the chunk; only waits under backpressure |
| Request body type | `BytesIO.getbuffer()`, i.e. a `memoryview`; ASGI specifies a byte string | `bytes` |
| Upload handling | Waits for the whole body before invoking the application | Opportunistic synchronous receive, streaming preserved |
| Scope `server`/`client` | Shares one **mutable list** between scopes, port stored as a **string** | Fresh immutable tuple, integer port (ASGI-conforming) |

The scope defects are worth noting because Weft once had the string-port bug
and has already fixed it; copying FastPySGI's scope approach verbatim would
reintroduce both that and cross-request aliasing.

## 2. Technique-by-technique

| Technique | Status for Weft |
|---|---|
| Retain Python response bytes; gather header and body into one write | **Already implemented** — `BodyPayload::Retained` (architecture.md §5.B) and response coalescing in `src/asgi/io.rs`. |
| Reuse connection-owned buffers and write-request storage | **Measured, rejected** — the Rust-side equivalent measured exactly zero; §3.1. |
| Copy a prebuilt scope template; cache callables and constants | **Measured, rejected** — the ceiling for *all* scope construction is +3.7%, and template copying can claim only a fraction of that; §3.2, §3.3. |
| Drive native I/O from an asyncio callback | **Not adopted.** It is not one integrated reactor: it is two event loops coordinated by polling. `uni_loop()` calls `uv_run(..., UV_RUN_NOWAIT)` up to 151 times before yielding and, after inactivity, falls back to a configured delay (default 3 ms) ([`asgi.c#L23`](https://github.com/remittor/fastpysgi/blob/439dbfb6e034812422d7000983552aa560f794b5/fastwsgi/asgi.c#L23)). That trades a thread handoff for idle CPU or added latency. |

## 3. Experiments

Common configuration: Weft on Tokio, 1 worker, 1 runtime thread,
`bench/apps/fastapi_app.py`, closed loop `c=64`, 10 s measured after 3 s
warmup, server pinned to CPUs 0-7 and `oha` to CPUs 12-19, Windows 11
(10.0.26200), CPython 3.14.7, release build.

**Noise floor.** Repeated `hello` runs in one period have a standard deviation
of roughly 400-700 req/s on 12,000-13,600 req/s, i.e. 3-6%. Absolute
throughput also drifts between periods — the same normal arm measured 13,573
req/s in one session and 12,201 req/s in another — so **only interleaved,
same-period comparisons are used below**, and paired sign tests carry more
weight than the means.

### 3.1 Consolidating per-request native allocations **[measured: neutral]**

`ASGIHTTPProtocol` allocated seven `Arc`s per request (request body, pushback
slot, four flags/notifies, response code). These were grouped into a single
`HTTPFlow` allocation, so a request taking the runtime path clones one pointer
instead of three to five; `sent_response_code` was never shared and became a
plain inline atomic.

A first attempt compared two labels measured in *different periods* and
appeared to show a 12% regression on `post-items`. That was a confound — the
"after" label ran immediately after a 10-minute test suite. Re-measured by
building both variants, saving each `.pyd`, and interleaving
before/after/before/after against the same binary swap:

| scenario | before (6 runs) | after (6 runs) |
|---|---|---|
| hello | 13,814 req/s | 13,605 req/s (14,021 excluding one 11,520 outlier) |
| post-items | 9,130 req/s | 9,180 req/s |

**No effect.** Six small allocations are on the order of 0.1 µs against
roughly 110 µs of CPU per request. The change was kept for its tighter state
grouping, not as an optimization, and must not be described as one.

### 3.2 Scope metadata and header names: partial ceiling **[measured: +2.5%]**

A temporary `WEFT_SCOPE_CEILING=1` toggle in `build_scope_common!` removed the
cacheable *values*: no header list materialization at all, one shared
`(ip, port)` tuple for `server` and `client`. Strictly more saving than a
correct cache can achieve, since header **values** are per-request and only
names are cacheable. The 13 `set_item` calls were still performed.

| arm (hello, 6 runs each) | mean |
|---|---|
| normal | 13,573 req/s |
| ceiling | 13,911 req/s |

+2.5%, ceiling winning 5 of 6 paired positions.

### 3.3 Whole-scope template copying: full ceiling **[measured: +3.7%]**

§3.2 left the dictionary construction itself in place, so it did **not** bound
template copying. A second toggle, `WEFT_SCOPE_TEMPLATE=1`, built the http
scope exactly once and handed out `dict.copy()` per request — no `set_item`
calls, no header materialization, no address construction, nothing. For
`hello` every request is genuinely identical, so the arm is semantically valid
for that scenario while doing none of the work.

| arm (hello, 14 runs each) | mean | sd |
|---|---|---|
| normal | 12,201 req/s | 583 |
| template copy | 12,652 req/s | 731 |

+3.7%; difference 451 ± 250 req/s (t ≈ 1.8), but **template copy wins 12 of 14
paired positions**, sign test p ≈ 0.013. The direction is real; the magnitude
is small.

**This is the ceiling for the whole family** — template copying, address
caching and header-name caching combined, since the arm performs none of them.
Subtracting §3.2 leaves roughly **1.2%** attributable to dictionary structure,
which is the only part template copying addresses. A *correct* template must
still re-set the nine per-request fields (`path`, `raw_path`, `query_string`,
`method`, `http_version`, `server`, `client`, `headers`, `state`) and pay for
the copy, so it recovers a fraction of that 1.2%. Below the noise floor, and
not worth the aliasing risk that FastPySGI's own implementation demonstrates
(§1).

Both toggles were removed after measurement: each produces a non-conforming
scope and must never ship.

### 3.4 What the results say together

Three independent experiments — Rust `Arc` grouping, scope value caching, and
whole-scope template copying — put the entire per-request allocation budget at
under 4% of `hello` throughput, with each individually realisable slice at or
below 1-2%. Weft's per-request time is not going into allocation on either
side of the language boundary. The allocation-reduction family, which is most
of what FastPySGI's design offers, does not transfer.

## 4. Verdict

FastPySGI does not establish that replacing hyper/Tokio would make a FastAPI
application faster. Its headline figures are plaintext throughput on a path
that skips FastAPI's routing, dependency resolution, validation and
serialization — the work that dominates Weft's request budget — and on the
ASGI details that matter for a real application (§1) it is behind Weft in two
places and non-conforming in two others.

Nothing here justifies a networking rewrite. If tighter loop integration is
ever revisited, it should be motivated by the earlier profiling finding that
the self-pipe wake write dominates server-side cost — not by FastPySGI, whose
architecture is two polling loops rather than one reactor. Weft already has a
local precedent for that trade going badly: the direct-dispatch A/B recorded
in `src/callbacks.rs` gave 18-53% worse p99 for no throughput gain. Any such
attempt needs a fairness budget, and sparse-traffic latency plus p99 in the
measurement from the first run.

## Sources

FastPySGI `439dbfb`: `fastwsgi/asgi.c` (event loop, scope, receive),
`fastwsgi/server.c` (write path, write completion, request dispatch),
`fastwsgi/pyhacks.c` (internal `BytesIO` layout — a CPython-internals coupling
not worth importing). Weft: `docs/architecture.md` §5.B and §6.1,
`docs/benchmarks.md`, `src/callbacks.rs` dispatch A/B notes.
