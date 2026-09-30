"""Binding the listener and running workers: in this thread, as threads, or as
processes. Process workers share the supervisor's socket; a SIGHUP or
`--reload` replaces them one at a time so something is always accepting."""

from __future__ import annotations

import copy
import multiprocessing
import multiprocessing.connection
import os
import signal
import socket
import sys
import tempfile
import threading
import time

from . import log, worker
from .config import Config
from .log import logger

# Must match `limit.rs` (`SLOTS * size_of::<Slot>()`).
_RATE_MAP_BYTES = 65_536 * 16
# Must match `metrics.rs` PAGE_BYTES (Header + 64 slots × 29 counters).
_METRICS_PAGE_BYTES = 16 + 64 * 29 * 8


def bind(host: str, port: int, backlog: int) -> socket.socket:
    family = socket.AF_INET6 if ':' in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    if os.name != 'nt':
        # On Windows SO_REUSEADDR would let another process take the port.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if family == socket.AF_INET6:
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
    sock.bind((host, port))
    sock.listen(backlog)
    sock.setblocking(False)
    return sock


def bind_unix(path: str, backlog: int) -> socket.socket:
    if os.path.exists(path):
        try:
            os.unlink(path)
        except OSError:
            pass
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(path)
    sock.listen(backlog)
    sock.setblocking(False)
    return sock


def run(config: Config, sock: socket.socket | None = None) -> int:
    log.configure(config.log_level)
    config.prepare()
    owned = sock is None
    if sock is None:
        if config.unix:
            sock = bind_unix(config.unix, config.backlog)
        else:
            sock = bind(config.host, config.port, config.backlog)
    n = config.worker_count
    _log_listen(config, sock, n)
    redirect_stop = _start_redirect(config, sock)
    map_path = _ensure_rate_map(config, n)
    cache_path = _ensure_cache_map(config, n)
    config.metrics_workers = n
    metrics_path = _ensure_metrics_map(config, n)
    try:
        if config.reload or (n > 1 and config.worker_mode == 'process'):
            if config.reload and config.worker_mode == 'thread':
                logger.warning('--reload replaces process workers; --worker-mode thread is ignored')
            if not isinstance(config.app, str):
                raise ValueError('process workers need the application as a "module:attribute" string')
            return _run_supervisor(config, sock, n)
        if n == 1:
            return worker.run(config, sock, exclusive=owned)
        if config.worker_mode == 'thread':
            return _run_threads(config, sock, n)
        if not isinstance(config.app, str):
            raise ValueError('process workers need the application as a "module:attribute" string')
        return _run_supervisor(config, sock, n)
    finally:
        if owned:
            sock.close()
            if config.unix:
                try:
                    os.unlink(config.unix)
                except OSError:
                    pass
        if map_path:
            try:
                os.unlink(map_path)
            except OSError:
                pass
        if cache_path:
            try:
                os.unlink(cache_path)
            except OSError:
                pass
        if metrics_path:
            try:
                os.unlink(metrics_path)
            except OSError:
                pass
        if redirect_stop:
            redirect_stop.set()


def _log_listen(config: Config, sock, n: int) -> None:
    kind = f'{config.worker_mode} worker' + ('s' if n != 1 else '')
    if config.unix:
        logger.info('weft serving on unix:%s (%d %s)', config.unix, n, kind)
        return
    host, port = sock.getsockname()[:2]
    scheme = 'https' if config.tls_certs else 'http'
    logger.info('weft serving on %s://%s:%d (%d %s)', scheme, host if ':' not in host else f'[{host}]', port, n, kind)


def _start_redirect(config: Config, https_sock) -> threading.Event | None:
    if config.redirect_http is None:
        return None
    https_port = https_sock.getsockname()[1]
    host = config.host
    port = config.redirect_http
    stop = threading.Event()

    def run():
        try:
            sock = bind(host, port, config.backlog)
        except OSError as e:
            logger.error('cannot listen on the --redirect-http port: %s', e)
            return
        sock.settimeout(0.3)
        logger.info('redirecting http://%s:%d to https port %d', host, port, https_port)
        try:
            while not stop.is_set():
                try:
                    conn, _ = sock.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break
                threading.Thread(target=_redirect_one, args=(conn, https_port), daemon=True).start()
        finally:
            sock.close()

    threading.Thread(target=run, name='weft-redirect', daemon=True).start()
    return stop


def _redirect_one(conn: socket.socket, https_port: int) -> None:
    try:
        conn.settimeout(2)
        buf = b''
        while b'\r\n\r\n' not in buf and len(buf) < 8192:
            chunk = conn.recv(2048)
            if not chunk:
                break
            buf += chunk
        line = buf.split(b'\r\n', 1)[0]
        parts = line.split(b' ')
        target = parts[1].decode('ascii', 'replace') if len(parts) > 1 else '/'
        if not target.startswith('/'):
            target = '/'
        host = 'localhost'
        for raw in buf.split(b'\r\n'):
            if raw.lower().startswith(b'host:'):
                host = raw.split(b':', 1)[1].strip().decode('ascii', 'replace')
                if ':' in host and not host.startswith('['):
                    host = host.rsplit(':', 1)[0]
                break
        loc = f'https://{host}{"" if https_port == 443 else f":{https_port}"}{target}'
        body = b''
        resp = (
            b'HTTP/1.1 301 Moved Permanently\r\n'
            b'location: ' + loc.encode() + b'\r\n'
            b'content-length: 0\r\n'
            b'connection: close\r\n\r\n' + body
        )
        conn.sendall(resp)
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


def _ensure_rate_map(config: Config, n: int) -> str | None:
    if not config.rate_limit:
        return None
    # One process can keep the table on the heap. Overlapping workers during a
    # reload, or more than one process, have to share a file.
    if not (config.reload or (n > 1 and config.worker_mode == 'process')):
        return None
    fd, path = tempfile.mkstemp(prefix='weft-rl-')
    os.ftruncate(fd, _RATE_MAP_BYTES)
    os.close(fd)
    config.rate_limit_map = path
    return path


def _ensure_cache_map(config: Config, n: int) -> str | None:
    if config.cache_size <= 0:
        return None
    if not (config.reload or (n > 1 and config.worker_mode == 'process')):
        return None
    fd, path = tempfile.mkstemp(prefix='weft-cache-')
    os.ftruncate(fd, max(1, config.cache_size) * 1024 * 1024)
    os.close(fd)
    config.cache_map = path
    return path


def _ensure_metrics_map(config: Config, n: int) -> str | None:
    if not config.metrics_port:
        return None
    if not (config.reload or (n > 1 and config.worker_mode == 'process')):
        return None
    fd, path = tempfile.mkstemp(prefix='weft-metrics-')
    os.ftruncate(fd, _METRICS_PAGE_BYTES)
    os.close(fd)
    config.metrics_map = path
    return path


def _run_threads(config: Config, sock, n: int) -> int:
    if getattr(sys, '_is_gil_enabled', lambda: True)():
        logger.warning('thread workers share one GIL on this interpreter; use processes for parallelism')
    app = config.load_app()
    stop = threading.Event()
    codes: list[int] = []

    def target(i):
        cfg = copy.copy(config)
        cfg.metrics_slot = i
        cfg.metrics_listen = i == 0
        cfg.http3_listen = i == 0 if os.name == 'nt' else True
        codes.append(worker.run(cfg, sock, stop, app))

    threads = [threading.Thread(target=target, args=(i,), name=f'weft-worker-{i}') for i in range(n)]
    for t in threads:
        t.start()
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        for t in threads:
            t.join()
    return max(codes, default=0)


def _process_main(config: Config, sock, stop, quit, ready, index: int) -> None:
    log.configure(config.log_level)
    parent = multiprocessing.parent_process()
    if parent is not None:
        def orphaned():
            multiprocessing.connection.wait([parent.sentinel])
            quit.set()

        threading.Thread(target=orphaned, name='weft-parent-watch', daemon=True).start()
    config.metrics_slot = index
    config.metrics_listen = index == 0
    config.http3_listen = index == 0 if os.name == 'nt' else True
    try:
        code = worker.run(config, sock, stop, ready=ready, quit=quit)
    except KeyboardInterrupt:
        code = 0
    sys.exit(code)


class _Slot:
    __slots__ = ('proc', 'stop', 'quit')

    def __init__(self, proc, stop, quit):
        self.proc = proc
        self.stop = stop
        self.quit = quit


class _Watcher:
    """Rescans ``.py`` files under `roots` every `interval` seconds."""

    def __init__(self, roots: list[str], interval: float):
        self.roots = [os.path.abspath(r) for r in roots if r]
        self.interval = max(0.05, interval)
        self.snap = self._scan()
        self.due = time.monotonic() + self.interval

    def changed(self) -> bool:
        if time.monotonic() < self.due:
            return False
        self.due = time.monotonic() + self.interval
        now = self._scan()
        if now != self.snap:
            self.snap = now
            return True
        return False

    def _scan(self) -> dict[str, int]:
        out: dict[str, int] = {}
        skip = {'__pycache__', '.git', '.venv', 'node_modules', 'target'}
        for root in self.roots:
            if os.path.isfile(root):
                try:
                    out[root] = os.stat(root).st_mtime_ns
                except OSError:
                    pass
                continue
            if not os.path.isdir(root):
                continue
            for dirpath, dirnames, files in os.walk(root):
                dirnames[:] = [d for d in dirnames if d not in skip]
                for name in files:
                    if name.endswith('.py'):
                        path = os.path.join(dirpath, name)
                        try:
                            out[path] = os.stat(path).st_mtime_ns
                        except OSError:
                            pass
        return out


def _watch_roots(config: Config) -> list[str]:
    roots = [os.path.abspath(config.app_dir)]
    if isinstance(config.app, str):
        module = config.app.partition(':')[0]
        # A ``pkg.mod`` lives under app_dir as pkg/mod.py; watching app_dir
        # is enough. A path-like name is included as itself.
        if os.path.sep in module or module.endswith('.py'):
            roots.append(module)
    return roots


def _run_supervisor(config: Config, sock, n: int) -> int:
    ctx = multiprocessing.get_context('spawn')
    shutting = False
    reload_wanted = False
    watcher = _Watcher(_watch_roots(config), config.reload_interval) if config.reload else None

    def spawn(i: int) -> _Slot:
        stop, quit, ready = ctx.Event(), ctx.Event(), ctx.Event()
        p = ctx.Process(target=_process_main, args=(config, sock, stop, quit, ready, i), name=f'weft-worker-{i}')
        p.start()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if ready.wait(0.1):
                return _Slot(p, stop, quit)
            if not p.is_alive():
                raise RuntimeError(f'worker {i} exited with {p.exitcode} before it started serving')
        quit.set()
        p.join(5)
        if p.is_alive():
            p.terminate()
            p.join(2)
        raise RuntimeError(f'worker {i} did not start serving')

    slots: list[_Slot] = []
    try:
        for i in range(n):
            slots.append(spawn(i))
    except BaseException as e:
        # The workers that did start are serving: stop them before giving up.
        for slot in slots:
            slot.stop.set()
        for slot in slots:
            slot.proc.join(config.graceful_timeout + 5)
            if slot.proc.is_alive():
                slot.proc.terminate()
                slot.proc.join(2)
        if not isinstance(e, RuntimeError):
            raise
        logger.error('%s', e)
        return 3

    def sighup(*_):
        nonlocal reload_wanted
        reload_wanted = True

    if threading.current_thread() is threading.main_thread() and hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, sighup)

    def rolling(why: str) -> None:
        logger.info(why)
        config.cache_flush = True
        for i, slot in enumerate(slots):
            try:
                fresh = spawn(i)
            except RuntimeError:
                logger.error('cannot spawn a replacement for worker %d; keeping the current one', i)
                return
            slot.quit.set()
            slot.proc.join(config.graceful_timeout + 5)
            if slot.proc.is_alive():
                slot.proc.terminate()
                slot.proc.join(2)
            slots[i] = fresh
        logger.info('workers reloaded')

    try:
        while True:
            if reload_wanted and not shutting:
                reload_wanted = False
                rolling('SIGHUP: reloading workers')
            if watcher is not None and not shutting and watcher.changed():
                rolling('source change detected; reloading workers')
            for i, slot in enumerate(slots):
                if slot.proc.is_alive():
                    continue
                if slot.proc.exitcode == 3:
                    logger.error('worker %d failed to start; shutting down', i)
                    return 3
                logger.warning('worker %d (pid %s) exited with %s; restarting', i, slot.proc.pid, slot.proc.exitcode)
                try:
                    slots[i] = spawn(i)
                except RuntimeError:
                    logger.error('cannot restart worker %d', i)
                    return 3
            time.sleep(0.15)
    except KeyboardInterrupt:
        pass
    finally:
        shutting = True
        for slot in slots:
            slot.stop.set()
        deadline = time.monotonic() + config.graceful_timeout + 5
        for slot in slots:
            slot.proc.join(max(0.0, deadline - time.monotonic()))
        for slot in slots:
            if slot.proc.is_alive():
                slot.proc.terminate()
                slot.proc.join(2)
    return 0
