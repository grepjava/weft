"""One worker: an event loop, the native server under it, and the lifespan."""

from __future__ import annotations

import asyncio
import selectors
import signal
import socket
import sys
import threading
import time

from . import _gc
from ._weft import Worker
from .config import Config
from .lifespan import Lifespan
from .log import logger, report_app_error
from .loop import WeftEventLoop


# Lifespan startups on one application object are serialised: under the
# thread worker mode every thread runs its own, and an application's startup
# code was never written to run twice at once.
_startup_lock = threading.Lock()


def run(config: Config, sock, stop=None, app=None, exclusive=False, ready=None, quit=None) -> int:
    """Serves on `sock` from this thread until stopped; returns an exit code.

    `stop` is anything with a blocking `wait()` (a `threading.Event`, the
    supervisor's `_Flag`) that tells the worker to shut down gracefully.
    `exclusive` promises that no other worker accepts from `sock`.
    """
    config.prepare()
    if app is None:
        app = config.load_app()
    protocol = config.protocol_for(app)
    worker = Worker()
    if protocol == 'wsgi':
        return _run_wsgi(config, app, worker, sock, stop, exclusive, ready, quit)
    loop = WeftEventLoop(worker)
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_main(config, app, worker, loop, sock, stop, exclusive, ready, quit))
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        asyncio.set_event_loop(None)
        loop.close()


def _run_wsgi(config: Config, app, worker: Worker, sock, stop, exclusive, ready, quit) -> int:
    """Asyncio-free WSGI worker loop: drives Tokio directly via `Worker.select`."""
    stopping = threading.Event()
    interrupted = threading.Event()

    def trigger(ev: threading.Event):
        ev.set()
        stopping.set()
        worker.wakeup()

    def watch(flag, ev):
        def wait():
            flag.wait()
            trigger(ev)

        threading.Thread(target=wait, name='weft-stop-watch', daemon=True).start()

    if stop is not None:
        watch(stop, stopping)
    if quit is not None:
        watch(quit, interrupted)

    prev_signals: dict = {}
    # A signal does not interrupt the native select(), and Python runs its
    # handler only once select() returns. Route signals through a socket
    # the worker watches so they wake it, as the asyncio path does.
    wake_r = wake_w = None
    prev_wakeup_fd = None
    if threading.current_thread() is threading.main_thread():
        try:
            wake_r, wake_w = socket.socketpair()
            wake_r.setblocking(False)
            wake_w.setblocking(False)
            worker.register(wake_r.fileno(), selectors.EVENT_READ)
            prev_wakeup_fd = signal.set_wakeup_fd(wake_w.fileno(), warn_on_full_buffer=False)
        except (OSError, ValueError, RuntimeError):
            for s_ in (wake_r, wake_w):
                if s_ is not None:
                    s_.close()
            wake_r = wake_w = None
        sigs = [
            (signal.SIGINT, interrupted),
            (signal.SIGTERM, stopping),
            (getattr(signal, 'SIGBREAK', None), interrupted),
            (getattr(signal, 'SIGQUIT', None), interrupted),
        ]
        for sig, ev in sigs:
            if sig is not None:
                prev_signals[sig] = signal.getsignal(sig)
                signal.signal(sig, lambda *_, ev=ev: trigger(ev))
        if hasattr(signal, 'SIGHUP'):
            prev_signals[signal.SIGHUP] = signal.getsignal(signal.SIGHUP)
            signal.signal(signal.SIGHUP, signal.SIG_IGN)

    try:
        options = config.serve_options()
        options['protocol'] = 'wsgi'
        options['state'] = None
        options['report'] = report_app_error
        options['exclusive'] = exclusive
        worker.serve(sock.fileno(), app, None, options)
        if ready is not None:
            ready.set()
        _gc.tune()
        try:
            name = sock.getsockname()
        except OSError:
            name = 'unix'
        logger.debug('wsgi worker serving on %s', name)

        def select(timeout):
            # The wakeup socket is level-triggered: a byte left unread would
            # make every later select return at once, so drain it whenever
            # select reports anything, in every loop below.
            if worker.select(timeout) and wake_r is not None:
                _drain(wake_r)

        while not stopping.is_set():
            select(None)

        if config.drain_delay > 0 and not interrupted.is_set():
            worker.drain()
            logger.info('draining for %gs before shutting down', config.drain_delay)
            drain_end = time.monotonic() + config.drain_delay
            while not interrupted.is_set():
                rem = drain_end - time.monotonic()
                if rem <= 0:
                    break
                select(min(rem, 0.05))

        worker.shutdown()
        deadline = time.monotonic() + config.graceful_timeout
        while worker.connections():
            rem = deadline - time.monotonic()
            if rem <= 0:
                break
            select(min(rem, 0.05))
        return 0
    finally:
        for sig, prev in prev_signals.items():
            try:
                signal.signal(sig, prev)
            except (OSError, ValueError):
                pass
        if wake_r is not None:
            try:
                signal.set_wakeup_fd(prev_wakeup_fd)
            except (OSError, ValueError):
                pass
            try:
                worker.unregister(wake_r.fileno())
            except (OSError, RuntimeError):
                pass
            wake_r.close()
            wake_w.close()
        worker.close()


def _drain(sock) -> None:
    try:
        while sock.recv(4096):
            pass
    except (BlockingIOError, InterruptedError, OSError):
        pass


def _watch(stop, loop, stopping: asyncio.Event) -> None:
    def wait():
        stop.wait()
        try:
            loop.call_soon_threadsafe(stopping.set)
        except RuntimeError:
            pass

    threading.Thread(target=wait, name='weft-stop-watch', daemon=True).start()


def _install_signals(loop, stopping: asyncio.Event, interrupted: asyncio.Event) -> None:
    """SIGTERM stops gracefully, after `--drain-delay`; SIGINT (and SIGQUIT)
    skip the delay, or cut short one already running."""
    if threading.current_thread() is not threading.main_thread():
        return

    def interrupt():
        interrupted.set()
        stopping.set()

    if sys.platform == 'win32':
        for sig, fn in ((signal.SIGINT, interrupt), (signal.SIGTERM, stopping.set),
                        (getattr(signal, 'SIGBREAK', None), interrupt)):
            if sig is not None:
                signal.signal(sig, lambda *_, fn=fn: loop.call_soon_threadsafe(fn))
    else:
        loop.add_signal_handler(signal.SIGTERM, stopping.set)
        for sig in (signal.SIGINT, signal.SIGQUIT):
            loop.add_signal_handler(sig, interrupt)


async def _main(config: Config, app, worker: Worker, loop, sock, stop, exclusive, ready, quit) -> int:
    stopping = asyncio.Event()
    lifespan = None
    state = None
    protocol = config.protocol_for(app)
    if protocol == 'asgi' and config.lifespan != 'off':
        lifespan = Lifespan(app, config.lifespan)
        with _startup_lock:
            ok = await lifespan.startup()
        if not ok:
            return 3
        if not lifespan.unsupported:
            state = lifespan.state

    interrupted = asyncio.Event()
    if stop is not None:
        _watch(stop, loop, stopping)
    if quit is not None:
        _watch(quit, loop, interrupted)
        _watch(quit, loop, stopping)
    _install_signals(loop, stopping, interrupted)
    if hasattr(signal, 'SIGHUP') and threading.current_thread() is threading.main_thread():
        # The supervisor owns SIGHUP; a worker that handled it would drain
        # itself while its replacement was still starting.
        try:
            loop.add_signal_handler(signal.SIGHUP, lambda: None)
        except (NotImplementedError, RuntimeError, OSError):
            signal.signal(signal.SIGHUP, signal.SIG_IGN)

    options = config.serve_options()
    options['protocol'] = protocol
    options['state'] = state
    options['report'] = report_app_error
    options['exclusive'] = exclusive
    worker.serve(sock.fileno(), app, loop, options)
    if ready is not None:
        ready.set()
    _gc.tune()
    try:
        name = sock.getsockname()
    except OSError:
        name = 'unix'
    logger.debug('worker serving on %s', name)

    await stopping.wait()

    if config.drain_delay > 0 and not interrupted.is_set():
        # Behind a load balancer: the health check fails and responses close
        # their connections, while traffic still routed here is served.
        worker.drain()
        logger.info('draining for %gs before shutting down', config.drain_delay)
        try:
            await asyncio.wait_for(interrupted.wait(), config.drain_delay)
        except TimeoutError:
            pass

    # Stop accepting and close idle connections; requests in flight get the
    # grace period, then whatever is left is cancelled. Only then does the
    # application get lifespan.shutdown -- cancelling first would cancel the
    # lifespan task too, and its cleanup after `yield` would never run.
    worker.shutdown()
    deadline = loop.time() + config.graceful_timeout
    while worker.connections() and loop.time() < deadline:
        await asyncio.sleep(0.05)
    current = asyncio.current_task()
    keep = lifespan.task if lifespan is not None else None
    pending = [t for t in asyncio.all_tasks() if t is not current and t is not keep and not t.done()]
    for t in pending:
        t.cancel()
    if pending:
        await asyncio.wait(pending, timeout=config.graceful_timeout)
    if lifespan is not None:
        await lifespan.shutdown(config.graceful_timeout)
    return 0
