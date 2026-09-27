# FastAPI 0.141.1 / Starlette 1.6.0 request lifecycle vs. Weft's ASGI layer

Code review (2026-09-06) of the installed FastAPI/Starlette/pydantic/anyio
sources against Weft's ASGI implementation (`src/asgi/*`, `weft/asgi.py`,
`weft/_futures.py`).

## 1. What Weft handed the app (before the FastAPI tuning work)

- Scope (`src/asgi/http.rs`): fresh dict per request; `asgi` and
  `extensions` are shared process-global dicts; headers are a list of
  lowercase `(bytes, bytes)` tuples straight from hyper; `server`/`client`
  ports were strings (the spec says int; Starlette compares the port with an
  int when the Host header is absent). The Python shim adds `root_path` and
  `state = state.copy()`.
- `receive()` (`src/asgi/io.rs`): disconnected -> done awaitable; exhausted
  -> runtime future waiting for response end or disconnect; otherwise a
  runtime future that locks the body, polls one frame and returned
  `more_body=True` for every data frame, `False` only on the terminating
  empty frame. So a request with a body needed at least two `receive()`
  awaits, each a full round trip (runtime task, blocking-pool hop, GIL,
  `call_soon_threadsafe`, loop iteration).
- `send()`: `http.response.start` is buffered (unless SSE) and returns an
  immediately-complete awaitable; a first body with `more_body` false becomes
  one `Full` response. `more_body=True` switches to an mpsc + `StreamBody` and
  a lazily allocated budget semaphore.
- Scheduling: the app coroutine is created on a blocking thread and
  `loop.create_task` runs on the loop thread from a `call_soon_threadsafe`
  callback: one extra loop iteration before the app's first step.

## 2. Per request kind

- GET returning a dict: `receive()` is never called. Default stack:
  ServerErrorMiddleware -> ExceptionMiddleware -> AsyncExitStackMiddleware
  -> APIRouter (which matches the four docs routes before any user route).
  Two `Request` objects, three `AsyncExitStack`s, a throwaway `Response`
  whose `content-length` is deleted, and `request.headers` /
  `query_params` / `cookies` materialised even when the endpoint declares no
  parameters. `Response.__call__` does two `send()` awaits with no
  `more_body` key. Under Weft every await completes synchronously, so the
  whole request runs in a single `Task.__step`.
- Path param: as above plus the convertor and a pydantic validation.
- POST with a BaseModel: `await request.body()` loops on `more_body`;
  FastAPI parses the content-type with `email.message.Message()` per
  request, `json.loads` the body, then `validate_python` (never
  `validate_json`).
- Depends(): recursive `solve_dependencies`; sync dependencies go through
  `anyio.to_thread.run_sync` (a `sleep(0)` checkpoint, a capacity limiter, a
  shielded cancel scope, a future, a context copy, and
  `call_soon_threadsafe` back).
- BaseHTTPMiddleware (`@app.middleware("http")`): a third `Request`, an
  unbuffered memory stream, a task group and a second asyncio Task per
  request; the response is re-emitted as `start` + `body(more_body=True)` +
  `body(b"", more_body=False)`, so every wrapped response (even 3 bytes)
  took Weft's streamed path. Each downstream `receive()` costs a task group,
  a spawned disconnect-listener task and a cancellation.
- CORSMiddleware: copies the header list per request and, on responses
  with an Origin, copies the response header list again.
- StreamingResponse: Starlette reads `scope["asgi"]["spec_version"]`;
  below 2.4 it spawns a task group and loops on `receive()` waiting for
  `http.disconnect` (two receive round trips + a cancellation per stream).
  At 2.4+ it streams directly and expects `OSError` from `send()` after a
  client disconnect.
- `request.is_disconnected()`: cancels a scope, then awaits `receive()`.
  If receive returns a completed awaitable the message is consumed (same as
  under uvicorn). If it suspends, the cancellation is delivered on the next
  loop iteration; with Weft's biased `select!` a frame that is already
  buffered could be polled, consumed and delivered to an already-cancelled
  future: data loss (the body then arrives empty -> 422).

## 3. The contract

MUST: `scope["type"]`, `path`, `method`, `headers` (lowercase bytes tuples),
`query_string` (touched by `solve_dependencies` unconditionally),
`asgi.spec_version` parsable; `receive()` returning `http.request` with
`body`/`more_body` (both read with `.get()`) until `more_body` is falsy, or
`http.disconnect` at any time, and eventually `http.disconnect` after
exhaustion; `send()` accepting `start` and `body` (with `more_body`
omitted by `Response.__call__`), `http.response.pathsend` if advertised.

Tolerated-absent: `root_path`, `path_params`, `scheme`, `server`, `client`,
`state`, `extensions`, `raw_path`, `http_version`, `asgi`.

Written by the framework (the dict must be mutable and per request): `app`,
`starlette.exception_handlers`, `fastapi_*_astack`, `router`, `endpoint`,
`path_params`, `route`, `headers` (replaced by a list copy), `state`.

Extensions consumed: only `http.response.pathsend` and `http.response.push`.

## 4. Event loop dependencies

anyio detects asyncio via `asyncio.current_task()` and keys cancel scopes on
the task object, so the app must run in a genuine `asyncio.Task`. Weft's
`rust` task implementation uses one shared fake task object whose `cancel()`
returns False; under it anyio cancellation would silently not work. It is
unreachable on 3.12+ and must not be resurrected for FastAPI. anyio worker
threads wake the loop with `call_soon_threadsafe`, so the lazy wake helps
them too.

## 5. Headers

Request names from hyper are already lowercase, which is what Starlette
requires (it never lowercases stored names). Response headers arrive as
latin-1 bytes with lowercase names; hyper rejects CR/LF values, surfaced as a
500 through ServerErrorMiddleware, which is acceptable.

## 6. Ranked server-side opportunities (semantics preserved)

1. Single-message body delivery + opportunistic synchronous read. Report
   `more_body=False` on the frame that ends the stream; when the app calls
   `receive()`, poll the body once without waiting on the Python thread and
   return a completed awaitable if a frame is already buffered. A small POST
   goes from two runtime round trips to zero.
2. Done-future `receive()` when the body is known empty (GET/HEAD, or
   content-length 0) and when the request is exhausted and the response
   already finished (`http.disconnect` synchronously). Makes
   `is_disconnected()` free and race-free on GET.
3. Cancellation-safe `receive()`: if the Python future was cancelled
   when the frame is delivered, push the frame back so the next `receive()`
   returns it. A correctness fix.
4. Coalesce `start` + `body(more_body=True)` + `body(b"", False)` into
   one `Full` response when `content-length` equals the first chunk's
   length: the exact shape BaseHTTPMiddleware produces for every response.
5. Advertise spec 2.4 and raise `OSError` from streamed `send()` after
   disconnect: StreamingResponse then skips its task group and receive
   loop (uvicorn does the same).
6. Header conversion fast path: borrow the tuple items instead of
   extracting a Vec per header, `HeaderName::from_lowercase` first.
7. Integer ports in `server`/`client` (correctness).
8. `state.copy()`: skip when the lifespan state is empty.

## 7. "FastAPI-aware magic" assessed

- Injecting `ORJSONResponse` is counterproductive on 0.141: `ORJSONResponse`
  is deprecated and FastAPI's fastest path (pydantic-core `dump_json`) is
  used only when the response class is the default placeholder.
- Pre-building the middleware stack only helps the first request and breaks
  apps that add middleware during lifespan.
- Pre-parsed JSON or header dicts in scope would not be consumed.
- Replacing the sync-endpoint thread pool means monkeypatching anyio.
- The four docs routes are matched before user routes on every request:
  `docs_url=None` in production is documentation-level advice.

Bottom line: on GET the server's remaining cost is scope construction, two
send conversions and the hyper write; the real wins are on the receive side
and in the scheduling hop.
