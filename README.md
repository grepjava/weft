<div align="center">
  <img width="420" src="https://raw.githubusercontent.com/grepjava/weft/master/assets/weft-spectral-continuum.svg" alt="weft">
</div>

# Weft

Weft is an ASGI and WSGI server for FastAPI, Flask and the rest. It is written
in Rust on Tokio and runs **inside the Python thread that runs your
application**: there is no second runtime thread, no queue between Rust and
Python, and no handoff per request. The HTTP server and asyncio share one
thread, one event loop, and one wakeup.

```
weft app:app
weft app:app --workers 4
weft app:app --workers 4 --worker-mode thread     # free-threaded Python (3.14t)
```

```python
import weft

weft.run(app, host='0.0.0.0', port=8000)
```

HTTP/1.1, HTTP/2, HTTP/3, WebSocket and WebTransport. TLS, static files,
compression, a response cache, and Prometheus. ACME is left to Caddy.

**Further reading:** [ARCHITECTURE.md](https://github.com/grepjava/weft/blob/master/ARCHITECTURE.md) — how it is built.
[BENCHMARKS.md](https://github.com/grepjava/weft/blob/master/BENCHMARKS.md) — Weft, Peregrine and Granian on the-benchmarker
contract. [RELEASE.md](https://github.com/grepjava/weft/blob/master/RELEASE.md) — what changed. [bench/README.md](https://github.com/grepjava/weft/blob/master/bench/README.md)
— the closed-loop FastAPI matrix.

## How it works

* **Tokio is driven from asyncio's selector.** `WeftEventLoop` is a
  `SelectorEventLoop` whose selector is a Tokio `LocalRuntime`. Every
  `select()` runs the runtime until a Python-side file descriptor is ready, a
  timer is due, or the application has work to do. asyncio's own sockets,
  subprocesses and `call_soon_threadsafe` keep working; they are registered
  with the same Tokio driver. The GIL is released only while Tokio parks.
* **The ASGI interface is C, not Python.** `send` and `receive` are native
  vectorcall objects. The scope is a copy of a prebuilt dict with interned
  keys. Header names are cached. `await send(...)` writes to the socket
  before it returns, and hands back an already-completed awaitable unless the
  client is slow to read (then a real future provides backpressure). A body
  that has already arrived is returned by `await receive()` without
  suspending.
* **HTTP/1.1 in Rust:** `httparse` for heads, keep-alive and pipelining,
  content-length and chunked bodies both ways, `Expect: 100-continue`,
  HEAD/204/304 semantics, request smuggling checks (TE+CL, invalid lengths),
  header timeouts, a head size limit, and graceful shutdown that finishes
  in-flight requests.
* **WSGI too.** A two-argument application (Flask, Django's WSGI handler,
  anything PEP 3333) is detected and served the same way; `--protocol wsgi`
  or `asgi` overrides the guess. The application is called inline once its
  body has been read; `start_response` is a native object that doubles as
  `write()`, and the response iterable is sent block by block, parking the
  connection on the socket between blocks so a slow client holds up only its
  own generator. `--wsgi-threads N` runs the application on N threads per
  worker instead, for blocking handlers; the worker thread still does all
  the socket work.
* **Operations, in the server:** an access log (`--access-log`, text or
  `--access-log-format json`) buffered per worker and written in batches;
  `--health-check-path` answered without Python; `--drain-delay` for load
  balancers; `--max-connections`; `--forwarded-allow-ips` for
  `X-Forwarded-*` and `Forwarded`; `--request-id`, `--trace-context` and
  `--request-start-header` for tracing and queue-time APMs;
  `--rate-limit N/s` counted across every worker; `--unix` / `--uds`;
  `--reload` and `SIGHUP` replace process workers one at a time;
  `--venv` / `--no-auto-venv`; `--no-lifespan`;
  `--static-dir PREFIX=DIR`; `--compress` / `--compress-static`;
  `--cache-size` shared by every worker;
  `--tls-cert` / `--tls-key` (SNI when repeated), `--hsts`, `--redirect-http`;
  HTTP/2 by default (`--no-http2`, `--http2-only`);
  HTTP/3 over QUIC (`--http3`, `--quic-port`) and WebTransport
  (ASGI `webtransport.*`: streams and datagrams);
  Prometheus on `--metrics-port` / `--metrics-host`.
* **Workers:** one process by default; `--workers N` starts N processes that
  share the listening socket, or N threads with `--worker-mode thread`, which
  on free-threaded Python run in parallel in one process. Each worker is a
  complete, independent loop.

## Performance

the-benchmarker contract, 256 connections, 4 workers, `zrk` open-loop ramp,
CPython 3.14.7. Median of three 15 s runs, every request 2xx.
[How that was measured.](https://github.com/grepjava/weft/blob/master/BENCHMARKS.md)

| app | Weft | Peregrine 1.1.7 | Granian 2.8.3 |
|---|---:|---:|---:|
| raw ASGI | 140,570 | **141,791** | 74,026 |
| raw WSGI | 151,752 | **200,227** | 87,733 |
| FastAPI 0.141 | 34,551 | **36,433** | 23,061 |

Weft is 1.5–1.9× Granian on this machine. Raw ASGI and FastAPI match
Peregrine; WSGI does not — Peregrine's WSGI path has no asyncio.

Closed loop on Windows 11, one worker, 64 connections
(`python bench/matrix.py --set fastapi`), requests per second:

| scenario | uvicorn + httptools | weft | weft, 3.14t |
|---|---:|---:|---:|
| `GET /` (JSON) | 5,150 | 9,190 | 9,450 |
| path parameter + response model | 4,300 | 7,350 | 7,170 |
| 50-item response model list | 2,760 | 3,630 | 3,130 |
| `POST` JSON body | 4,080 | 6,360 | 6,570 |
| 64 KB response | 4,490 | 7,550 | 7,890 |
| `asyncio.sleep(0.01)`, 256 connections | 3,630 | 6,080 | 6,420 |

Each server uses one core. With four worker processes (`--set scaling`),
`GET /` reaches 35,200 requests per second against uvicorn's 24,300, and
the path parameter endpoint 26,800 against 20,100.

## Install

```
pip install weft-server
```

Distribution name: `weft-server` (the PyPI name `weft` belongs to an unrelated
project). Import package `weft`, CLI `weft`. CPython 3.10+; wheels for Linux,
macOS and Windows. Tested on 3.14 and free-threaded 3.14t.

From source:

```
uv venv --python 3.14 .venv
uv pip install maturin
maturin develop --release
```

## Scope

Supported: ASGI 3.0 HTTP (spec 2.4), WebSocket and lifespan; WSGI (PEP
3333); HTTP/1.0, 1.1, HTTP/2 (h2c prior knowledge, ALPN over TLS) and
HTTP/3 over QUIC; WebTransport (ASGI `webtransport.*`); TCP and unix
sockets; TLS (rustls). ACME is left to the reverse proxy (Caddy).

`weft.contrib` has the framework helpers a WebTransport or HTTP/3 app
usually writes once: `WebTransportRouter` (Starlette `{name}` / Django
`<int:pk>` paths), `WebTransportEndpoint`, `AltSvcMiddleware`,
`weft.webtransport.WebTransportSession`, and IETF resumable uploads
(`ResumableUploads`). Starlette, FastAPI and Django serve HTTP/3 unchanged;
the routers exist because those frameworks assert on `scope["type"]` before
they would see a `webtransport` session.

## Tests

```
pytest tests
```

The suite runs raw ASGI and WSGI apps and a FastAPI app over real sockets, in
process, thread and inline worker modes, on both the standard and the
free-threaded interpreter.
