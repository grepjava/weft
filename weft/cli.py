from __future__ import annotations

import argparse
import sys

from . import __version__
from .config import Config


def parser() -> argparse.ArgumentParser:
    d = Config(app='')
    p = argparse.ArgumentParser(prog='weft', description='Serve an ASGI or WSGI application.')
    p.add_argument('app', help='the application, as module:attribute')
    p.add_argument('--host', default=d.host)
    p.add_argument('--port', type=int, default=d.port)
    p.add_argument('--unix', '--uds', dest='unix', default=d.unix, help='listen on a unix socket instead')
    p.add_argument('--venv', default=d.venv, help="virtualenv whose packages the app should import")
    p.add_argument('--no-auto-venv', action='store_true', help='ignore VIRTUAL_ENV from the environment')
    p.add_argument('--reload', action='store_true', help='restart workers when source files change')
    p.add_argument('--reload-interval', type=float, default=d.reload_interval,
                   help='how often to rescan the source tree, seconds')
    p.add_argument('--workers', type=int, default=d.workers, help='0 means one per CPU')
    p.add_argument('--worker-mode', choices=['process', 'thread'], default=d.worker_mode,
                   help='thread workers share one process; worthwhile on free-threaded Python')
    p.add_argument('--protocol', '--interface', dest='protocol', choices=['auto', 'asgi', 'wsgi'],
                   default=d.protocol, help='auto tells them apart by the application\'s signature')
    p.add_argument('--wsgi-threads', type=int, default=d.wsgi_threads,
                   help='threads per worker that run a WSGI application; 0 runs it on the worker thread')
    p.add_argument('--lifespan', choices=['auto', 'on', 'off'], default=d.lifespan)
    p.add_argument('--no-lifespan', action='store_true', help='skip the ASGI lifespan protocol')
    p.add_argument('--root-path', default=d.root_path)
    p.add_argument('--factory', action='store_true', help='APP is a callable returning the application')
    p.add_argument('--app-dir', default=d.app_dir, help='prepended to sys.path to import APP')
    p.add_argument('--backlog', type=int, default=d.backlog)
    p.add_argument('--keep-alive', type=float, default=d.keep_alive, help='idle keep-alive timeout, seconds')
    p.add_argument('--header-timeout', type=float, default=d.header_timeout,
                   help='time allowed to receive a request head, seconds')
    p.add_argument('--graceful-timeout', type=float, default=d.graceful_timeout,
                   help='time in-flight requests get to finish on shutdown, seconds')
    p.add_argument('--max-header-size', '--max-head', dest='max_head', type=int, default=d.max_head,
                   help='largest request head, and chunked trailer section, accepted, bytes')
    p.add_argument('--max-body', type=int, default=d.max_body, help='largest request body accepted, bytes')
    p.add_argument('--request-timeout', type=float, default=d.request_timeout,
                   help='how long a request may stall mid-message, seconds (0 disables)')
    p.add_argument('--log-level', choices=['critical', 'error', 'warning', 'info', 'debug'], default=d.log_level)
    p.add_argument('--no-server-header', dest='server_header', action='store_false')
    p.add_argument('--no-date-header', dest='date_header', action='store_false')
    ws = p.add_argument_group('WebSocket')
    ws.add_argument('--no-websockets', dest='websockets', action='store_false',
                    help='refuse WebSocket upgrades with 501')
    ws.add_argument('--ws-max-message', type=int, default=d.ws_max_message, help='largest message accepted, bytes')
    ws.add_argument('--ws-ping-interval', type=float, default=d.ws_ping_interval,
                    help='seconds between keepalive pings; 0 turns them off')
    ws.add_argument('--ws-ping-timeout', type=float, default=d.ws_ping_timeout,
                    help='seconds to wait for a pong before dropping the connection')
    ws.add_argument('--ws-max-queue', type=int, default=d.ws_max_queue,
                    help='messages buffered ahead of the application before reading stops')
    ws.add_argument('--ws-max-queue-bytes', type=int, default=d.ws_max_queue_bytes,
                    help='bytes buffered ahead of the application before reading stops')
    ws.add_argument('--ws-compress', action='store_true', help='negotiate permessage-deflate')
    p.add_argument('--scheme', choices=['http', 'https'], default=d.scheme,
                   help='scheme reported to the application')
    p.add_argument('--max-connections', type=int, default=d.max_connections,
                   help='concurrent connections per worker; past it a connection gets 503')
    p.add_argument('--drain-delay', type=float, default=d.drain_delay,
                   help='on SIGTERM, fail the health check and keep serving this many seconds before draining')
    ops = p.add_argument_group('operations')
    ops.add_argument('--forwarded-allow-ips', default=d.forwarded_allow_ips,
                     help='proxies whose X-Forwarded-* and Forwarded headers are trusted '
                          '(addresses, CIDR blocks, or *)')
    ops.add_argument('--health-check-path', default=d.health_check_path,
                     help='answer this path with 200 in the server, without calling the application')
    ops.add_argument('--access-log', action='store_true', help='log one line per request')
    ops.add_argument('--access-log-format', choices=['text', 'json'], default=d.access_log_format,
                     help='json implies --access-log')
    ops.add_argument('--request-id', action='store_true',
                     help='an X-Request-ID per request, for the app, the response and the access log')
    ops.add_argument('--trace-context', action='store_true',
                     help="log a request's W3C trace and parent span IDs")
    ops.add_argument('--request-start-header', action='store_true',
                     help='hand the application X-Request-Start, for queue-time APMs')
    ops.add_argument('--rate-limit', default=d.rate_limit,
                     help='429 past this many requests per client (100/s, 600/m), counted across all workers')
    ops.add_argument('--rate-limit-burst', type=int, default=d.rate_limit_burst,
                     help='requests allowed at once before the rate applies (default: the rate)')
    ops.add_argument('--static-dir', action='append', default=[], dest='static_dirs', metavar='PREFIX=DIR',
                     help='serve URL prefix PREFIX from DIR without the application (repeatable)')
    ops.add_argument('--compress', action='store_true',
                     help='compress text-like application responses (br, zstd or gzip)')
    ops.add_argument('--compress-static', action='store_true',
                     help='serve precompressed .br/.zst/.gz copies next to static files')
    ops.add_argument('--compress-min-size', type=int, default=d.compress_min_size,
                     help='do not compress a response shorter than this, bytes')
    ops.add_argument('--cache-size', type=int, default=d.cache_size, metavar='MIB',
                     help='shared response cache, mebibytes; 0 is off')
    ops.add_argument('--cache-max-object', type=int, default=d.cache_max_object, metavar='KIB',
                     help='largest body the cache keeps, kibibytes')
    ops.add_argument('--cache-ttl-max', type=int, default=d.cache_ttl_max,
                     help='longest a cached response is kept, seconds')
    tls = p.add_argument_group('TLS')
    tls.add_argument('--tls-cert', action='append', default=[], dest='tls_certs', metavar='PATH',
                     help='PEM certificate chain; enables TLS. Repeatable, paired with --tls-key')
    tls.add_argument('--tls-key', action='append', default=[], dest='tls_keys', metavar='PATH',
                     help='PEM private key for the preceding --tls-cert')
    tls.add_argument('--tls-ciphers', default=d.tls_ciphers,
                     help='ignored; rustls uses its own TLS 1.2/1.3 suite')
    tls.add_argument('--hsts', type=int, default=d.hsts, metavar='SECONDS',
                     help='send Strict-Transport-Security: max-age=SECONDS')
    tls.add_argument('--redirect-http', type=int, default=d.redirect_http, metavar='PORT',
                     help='answer plain HTTP on PORT with a redirect to https')
    tls.add_argument('--no-http2', dest='http2', action='store_false',
                     help='refuse HTTP/2 and answer HTTP/1.1 only')
    tls.add_argument('--http2-only', action='store_true',
                     help='serve only HTTP/2 (h2c / ALPN h2), with no HTTP/1 fallback')
    tls.add_argument('--http3', action='store_true',
                     help='also serve HTTP/3 over QUIC (needs --tls-cert)')
    tls.add_argument('--quic-port', type=int, default=d.quic_port,
                     help='UDP port for HTTP/3 (default: the TCP port)')
    ops.add_argument('--metrics-port', type=int, default=d.metrics_port,
                     help='serve Prometheus metrics on this port (0 is off)')
    ops.add_argument('--metrics-host', default=d.metrics_host,
                     help='what the metrics port binds (default --host)')
    p.add_argument('--version', action='version', version=f'weft {__version__}')
    return p


def main(argv: list[str] | None = None) -> int:
    ns = parser().parse_args(argv)
    if ns.no_lifespan:
        ns.lifespan = 'off'
    opts = vars(ns)
    opts.pop('no_lifespan', None)
    config = Config(**opts)
    from .server import run

    return run(config)


def entrypoint() -> None:
    sys.exit(main())
