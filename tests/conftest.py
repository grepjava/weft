import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from weft import worker
from weft.config import Config
from weft.server import bind


ROOT = Path(__file__).resolve().parent.parent


@contextmanager
def serve_thread(app, **options):
    """Runs one worker on a background thread; yields its base URL."""
    sock = bind('127.0.0.1', 0, 128)
    config = Config(**{'app': app, 'log_level': 'warning', 'graceful_timeout': 2.0, **options})
    stop = threading.Event()
    result = {}

    def target():
        try:
            result['code'] = worker.run(config, sock, stop)
        except BaseException as exc:
            result['error'] = exc

    t = threading.Thread(target=target, daemon=True)
    t.start()
    try:
        yield f'http://127.0.0.1:{sock.getsockname()[1]}'
    finally:
        stop.set()
        t.join(15)
        sock.close()
    assert not t.is_alive(), 'worker did not stop'
    if 'error' in result:
        raise result['error']


def free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def wait_connect(port: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f'server on port {port} did not come up')


def wait_port(port: int, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.5) as s:
                s.sendall(b'GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
                if s.recv(1):
                    return
        except OSError:
            pass
        time.sleep(0.1)
    raise TimeoutError(f'server on port {port} did not come up')


@contextmanager
def serve_process(app: str, *args: str, python: str | None = None, env: dict | None = None):
    """Runs the CLI in a subprocess; yields (base URL, process)."""
    port = free_port()
    proc = subprocess.Popen(
        [python or sys.executable, '-m', 'weft', app, '--port', str(port), '--log-level', 'warning', *args],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, 'PYTHONUNBUFFERED': '1', **(env or {})},
    )
    try:
        if '--tls-cert' in args:
            wait_connect(port)
        else:
            wait_port(port)
        yield f'http://127.0.0.1:{port}', proc
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(15)
            except subprocess.TimeoutExpired:
                proc.kill()


@pytest.fixture
def basic():
    from tests.apps.basic import app

    with serve_thread(app) as url:
        yield url
