<div align="center">
  <img width="420" src="assets/weft-spectral-continuum.svg" alt="weft">
</div>

# Releases

What changed in each version of Weft, newest first.

**Keeping this file.** A change someone using Weft would notice gets a line
under [Unreleased](#unreleased) in the commit that makes it. When a version
is cut, that section is renamed to the version and its date, and a new
empty Unreleased section goes above it.

Distribution name: `weft-server`. Import package `weft`, CLI `weft`.

---

## Unreleased

The tree on `master` since the rewrite (`2300a2e`). Not yet a numbered
PyPI release.

### Added

- ASGI 3.0 HTTP (spec 2.4) on Tokio, driven from `asyncio.select()`. Native
  `send` / `receive` vectorcall objects, a copied prototype scope, interned
  keys, and a pre-completed awaitable when the socket is ready.
- HTTP/1.1: keep-alive, pipelining, chunked transfer both ways, `Expect:
  100-continue`, HEAD/204/304, smuggling checks, header timeouts, a head
  size limit, graceful drain.
- WebSocket: handshake, framing, fragmentation, ping/pong, limits, queue
  backpressure, `permessage-deflate`.
- WSGI (PEP 3333): protocol detection, native environ and `start_response`
  (including `write()`), streamed iterables, `wsgi.file_wrapper`,
  `--wsgi-threads`.
- HTTP/2 over TLS (ALPN `h2`) and cleartext prior knowledge.
- HTTP/3 over QUIC (`--http3`, `--quic-port`) and WebTransport as ASGI
  `webtransport.*`. Incoming streams and datagrams; CONNECT accept/refuse.
  `weft.webtransport.WebTransportSession` and `weft.contrib` (Starlette /
  FastAPI / Django routers, `AltSvcMiddleware`, IETF resumable uploads).
- TLS via rustls: `--tls-cert` / `--tls-key`, SNI when repeated, `--hsts`,
  `--redirect-http`.
- Static files (`--static-dir`), compression, shared response cache
  (`--cache-size`).
- Access log (text or JSON), `--health-check-path`, `--drain-delay`,
  `--max-connections`, `--forwarded-allow-ips`, `--request-id`,
  `--trace-context`, `--request-start-header`, `--rate-limit`, unix
  sockets, `--reload` and `SIGHUP` rolling restart, `--venv`,
  `--no-lifespan`.
- Prometheus on `--metrics-port` / `--metrics-host`.
- Process workers and `--worker-mode thread` on free-threaded CPython
  3.14t. `Py_mod_gil = Py_MOD_GIL_NOT_USED`.

### Known

- WebTransport stream and datagram *sends* after an incoming session: the
  QUIC connection is locally closed on that path; five data-plane tests are
  skipped.
- ACME is not implemented; leave issuance to Caddy.
- Raw WSGI is slower than Peregrine's asyncio-free poller (76% at 256
  connections on the contract). Raw ASGI and FastAPI match Peregrine and
  are 1.5–1.9× Granian 2.8.3. [BENCHMARKS.md](BENCHMARKS.md).
