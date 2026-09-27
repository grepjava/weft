from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass, field
from typing import Any


def activate_venv(path: str) -> str | None:
    """Puts ``path``'s site-packages on ``sys.path``. ``None`` on success."""
    path = os.path.abspath(path)
    if os.path.normcase(path) == os.path.normcase(os.path.abspath(sys.prefix)):
        return None
    if os.name == 'nt':
        site_packages = os.path.join(path, 'Lib', 'site-packages')
    else:
        ver = f'python{sys.version_info[0]}.{sys.version_info[1]}'
        site_packages = os.path.join(path, 'lib', ver, 'site-packages')
        if not os.path.isdir(site_packages):
            lib = os.path.join(path, 'lib')
            found = None
            if os.path.isdir(lib):
                for name in os.listdir(lib):
                    cand = os.path.join(lib, name, 'site-packages')
                    if os.path.isdir(cand):
                        found = cand
                        break
            site_packages = found or site_packages
    if not os.path.isdir(site_packages):
        return f'virtualenv {path} has no site-packages'
    import site

    if site_packages not in sys.path:
        site.addsitedir(site_packages)
        # addsitedir appends; the application should see the venv first.
        sys.path.remove(site_packages)
        sys.path.insert(0, site_packages)
    return None


@dataclass
class Config:
    #: ``module:attribute``, or the application object itself
    app: Any
    host: str = '127.0.0.1'
    port: int = 8000
    #: 0 means one per CPU
    workers: int = 1
    #: ``process``, or ``thread`` (worthwhile on free-threaded Python)
    worker_mode: str = 'process'
    #: ``auto`` (from the application's signature), ``asgi`` or ``wsgi``
    protocol: str = 'auto'
    #: WSGI applications run on this many threads per worker; 0 runs them
    #: inline on the worker thread
    wsgi_threads: int = 0
    #: ``auto``, ``on`` or ``off``
    lifespan: str = 'auto'
    root_path: str = ''
    factory: bool = False
    app_dir: str = '.'
    backlog: int = 2048
    keep_alive: float = 5.0
    header_timeout: float = 10.0
    graceful_timeout: float = 10.0
    max_head: int = 32 * 1024
    max_body: int = 16 * 1024 * 1024
    request_timeout: float = 30.0
    log_level: str = 'info'
    server_header: bool = True
    date_header: bool = True
    websockets: bool = True
    #: largest WebSocket message accepted, bytes
    ws_max_message: int = 16 * 1024 * 1024
    #: seconds between keepalive pings; 0 turns them off
    ws_ping_interval: float = 20.0
    ws_ping_timeout: float = 20.0
    #: messages received but not yet taken by the application, before reading stops
    ws_max_queue: int = 32
    ws_max_queue_bytes: int = 4 * 1024 * 1024
    #: permessage-deflate
    ws_compress: bool = False
    #: the scheme reported to the application, ``http`` or ``https``
    scheme: str = 'http'
    #: concurrent connections per worker; past it a connection gets 503
    max_connections: int = 4096
    #: seconds to keep serving after SIGTERM, failing the health check
    drain_delay: float = 0.0
    #: proxies whose X-Forwarded-* and Forwarded headers are believed:
    #: addresses, CIDR blocks, or ``*``
    forwarded_allow_ips: str | None = None
    request_id: bool = False
    trace_context: bool = False
    request_start_header: bool = False
    #: answered with 200 by the server, without the application
    health_check_path: str | None = None
    access_log: bool = False
    #: ``text`` or ``json``
    access_log_format: str = 'text'
    #: listen on this unix socket instead of ``host:port``
    unix: str | None = None
    #: ``N/s``, ``N/m`` or ``N/h``; ``None`` is off
    rate_limit: str | None = None
    rate_limit_burst: int | None = None
    #: shared table file for process workers; set by the supervisor
    rate_limit_map: str | None = None
    reload: bool = False
    reload_interval: float = 0.5
    #: virtualenv whose ``site-packages`` the application should import
    venv: str | None = None
    no_auto_venv: bool = False
    #: ``PREFIX=DIRECTORY`` pairs, longest prefix first once parsed
    static_dirs: list[str] = field(default_factory=list)
    compress: bool = False
    compress_static: bool = False
    compress_min_size: int = 1024
    #: mebibytes; 0 is off
    cache_size: int = 0
    #: kibibytes
    cache_max_object: int = 1024
    cache_ttl_max: int = 300
    cache_map: str | None = None
    cache_flush: bool = False
    tls_certs: list[str] = field(default_factory=list)
    tls_keys: list[str] = field(default_factory=list)
    tls_ciphers: str | None = None
    #: seconds; sent as Strict-Transport-Security max-age
    hsts: int | None = None
    #: plain HTTP port that answers 301 to https
    redirect_http: int | None = None
    http2: bool = True
    http2_only: bool = False
    http3: bool = False
    #: 0 means the TCP port
    quic_port: int = 0
    http3_listen: bool = True
    #: 0 is off. Scraped on its own port, never as an application route.
    metrics_port: int = 0
    #: defaults to ``host``
    metrics_host: str | None = None
    #: shared page for process workers; set by the supervisor
    metrics_map: str | None = None
    metrics_slot: int = 0
    metrics_listen: bool = True
    metrics_workers: int = 1

    @property
    def worker_count(self) -> int:
        return self.workers if self.workers > 0 else (os.cpu_count() or 1)

    def prepare(self) -> None:
        """Activates ``--venv`` / ``VIRTUAL_ENV`` before the application is imported."""
        if len(self.tls_certs) != len(self.tls_keys):
            raise SystemExit('--tls-cert and --tls-key go together, one key per certificate')
        if self.hsts is not None and not self.tls_certs:
            raise SystemExit('--hsts is only ever sent over TLS; it needs --tls-cert')
        if self.redirect_http is not None and not self.tls_certs:
            raise SystemExit('--redirect-http sends clients to https; it needs --tls-cert')
        if self.http2_only and not self.http2:
            raise SystemExit('--http2-only and --no-http2 cannot be combined')
        if self.http3 and not self.tls_certs:
            raise SystemExit('--http3 needs --tls-cert and --tls-key: QUIC has no cleartext form')
        if self.http3 and self.unix:
            raise SystemExit('--http3 cannot be served over a unix socket')
        path = self.venv
        if path is None and not self.no_auto_venv:
            path = os.environ.get('VIRTUAL_ENV')
        if not path:
            return
        err = activate_venv(path)
        if err is None:
            return
        if self.venv:
            raise SystemExit(err)
        from .log import logger

        logger.warning(err)

    def load_app(self):
        app = self.app
        if isinstance(app, str):
            module_name, _, attr = app.partition(':')
            if not module_name or not attr:
                raise ValueError(f'application must be given as "module:attribute", got {app!r}')
            app_dir = os.path.abspath(self.app_dir)
            if app_dir not in sys.path:
                sys.path.insert(0, app_dir)
            obj = importlib.import_module(module_name)
            for part in attr.split('.'):
                obj = getattr(obj, part)
            app = obj
        if self.factory:
            app = app()
        return app

    def protocol_for(self, app) -> str:
        if self.protocol != 'auto':
            return self.protocol
        from .wsgi import detect

        return detect(app)

    def access_log_mode(self) -> str | None:
        """``'text'``, ``'json'``, or ``None``: JSON implies the log, and the
        log is info level, so a quieter level turns it off."""
        on = self.access_log or self.access_log_format == 'json'
        if not on or self.log_level in ('warning', 'error', 'critical'):
            return None
        return self.access_log_format

    def serve_options(self) -> dict:
        many = self.worker_count > 1
        return {
            'scheme': self.scheme,
            'max_connections': self.max_connections,
            'forwarded_allow_ips': self.forwarded_allow_ips,
            'request_id': self.request_id,
            'trace_context': self.trace_context,
            'request_start_header': self.request_start_header,
            'health_check_path': self.health_check_path,
            'access_log': self.access_log_mode(),
            'rate_limit': self.rate_limit,
            'rate_limit_burst': self.rate_limit_burst,
            'rate_limit_map': self.rate_limit_map,
            'wsgi_multithread': many and self.worker_mode == 'thread',
            'wsgi_multiprocess': many and self.worker_mode == 'process',
            'wsgi_threads': self.wsgi_threads,
            'root_path': self.root_path,
            'keep_alive': self.keep_alive,
            'header_timeout': self.header_timeout,
            'max_head': self.max_head,
            'max_body': self.max_body,
            'request_timeout': self.request_timeout,
            'server_header': self.server_header,
            'date_header': self.date_header,
            'websockets': self.websockets,
            'ws_max_message': self.ws_max_message,
            'ws_ping_interval': self.ws_ping_interval,
            'ws_ping_timeout': self.ws_ping_timeout,
            'ws_max_queue': self.ws_max_queue,
            'ws_max_queue_bytes': self.ws_max_queue_bytes,
            'ws_compress': self.ws_compress,
            'static_dirs': self.static_dirs or None,
            'compress': self.compress,
            'compress_static': self.compress_static,
            'compress_min_size': self.compress_min_size,
            'cache_size': self.cache_size,
            'cache_max_object': self.cache_max_object,
            'cache_ttl_max': self.cache_ttl_max,
            'cache_map': self.cache_map,
            'cache_flush': self.cache_flush,
            'tls_certs': self.tls_certs or None,
            'tls_keys': self.tls_keys or None,
            'hsts': self.hsts,
            'http2': self.http2,
            'http2_only': self.http2_only,
            'http3': self.http3,
            'quic_port': self.quic_port,
            'http3_listen': self.http3_listen,
            'metrics_port': self.metrics_port,
            'metrics_host': self.metrics_host or self.host,
            'metrics_map': self.metrics_map,
            'metrics_slot': self.metrics_slot,
            'metrics_listen': self.metrics_listen,
            'metrics_workers': self.metrics_workers,
        }
