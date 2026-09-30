import asyncio
import os
import socket
import sys
import threading
import time

import pytest

import httpx

from tests.conftest import serve_process, serve_thread
from weft import worker
from weft.config import Config
from weft.loop import WeftEventLoop
from weft.server import bind


def test_lifespan_failure_stops_worker():
    async def app(scope, receive, send):
        assert scope['type'] == 'lifespan'
        await receive()
        await send({'type': 'lifespan.startup.failed', 'message': 'no database'})

    sock = bind('127.0.0.1', 0, 16)
    try:
        assert worker.run(Config(app=app, log_level='critical'), sock) == 3
    finally:
        sock.close()


def test_lifespan_unsupported_is_ignored():
    async def app(scope, receive, send):
        if scope['type'] != 'http':
            raise RuntimeError('only http')
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    with serve_thread(app) as url:
        assert httpx.get(url).text == 'ok'


def test_lifespan_shutdown_runs():
    events = []

    async def app(scope, receive, send):
        if scope['type'] == 'lifespan':
            while True:
                m = await receive()
                events.append(m['type'])
                await send({'type': m['type'] + '.complete'})
                if m['type'] == 'lifespan.shutdown':
                    return
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'ok'})

    with serve_thread(app) as url:
        assert httpx.get(url).text == 'ok'
    assert events == ['lifespan.startup', 'lifespan.shutdown']


def test_graceful_shutdown_finishes_inflight():
    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        await asyncio.sleep(0.5)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'finished'})

    sock = bind('127.0.0.1', 0, 16)
    stop = threading.Event()
    t = threading.Thread(target=worker.run, args=(Config(app=app, lifespan='off', log_level='critical'), sock, stop))
    t.start()
    try:
        url = f'http://127.0.0.1:{sock.getsockname()[1]}'
        result = {}
        c = threading.Thread(target=lambda: result.setdefault('r', httpx.get(url, timeout=10)))
        c.start()
        import time

        time.sleep(0.2)
        stop.set()
        c.join(10)
        assert result['r'].text == 'finished'
        assert result['r'].headers['connection'] == 'close'
    finally:
        stop.set()
        t.join(10)
        sock.close()


def test_loop_without_server():
    """The loop is usable as a plain asyncio loop: timers, sockets, threads."""
    loop = WeftEventLoop()
    try:
        async def main():
            await asyncio.sleep(0.01)
            fut = loop.create_future()
            threading.Timer(0.02, lambda: loop.call_soon_threadsafe(fut.set_result, 42)).start()
            v = await fut
            srv = await asyncio.start_server(lambda r, w: w.write(b'pong') or w.close(), '127.0.0.1', 0)
            port = srv.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            data = await reader.read()
            writer.close()
            srv.close()
            await srv.wait_closed()
            out = await asyncio.to_thread(sum, [1, 2, 3])
            return v, data, out

        assert loop.run_until_complete(main()) == (42, b'pong', 6)
    finally:
        loop.close()


def test_cli_process_workers():
    with serve_process('tests.apps.basic:app', '--workers', '2') as (url, proc):
        for _ in range(40):
            with httpx.Client() as c:
                r = c.get(url + '/scope')
                assert r.status_code == 200
        assert proc.poll() is None


def _alive(pid: int) -> bool:
    if sys.platform == 'win32':
        import ctypes

        k = ctypes.WinDLL('kernel32')
        k.OpenProcess.restype = ctypes.c_void_p
        h = k.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE
        if not h:
            return False
        try:
            return k.WaitForSingleObject(ctypes.c_void_p(h), 0) != 0
        finally:
            k.CloseHandle(ctypes.c_void_p(h))
    import os

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_process_workers_exit_with_supervisor():
    """Workers racing to accept from one listener must stay responsive, and
    leave when the supervisor dies, however it dies."""
    import time

    with serve_process('tests.apps.basic:app', '--workers', '2') as (url, proc):
        pids = set()
        for _ in range(100):
            pids.add(int(httpx.get(url + '/pid').text))
            if len(pids) == 2:
                break
        for _ in range(3):
            for _ in range(40):
                with httpx.Client() as c:
                    assert c.get(url + '/scope').status_code == 200
            time.sleep(0.3)
        proc.kill()
        proc.wait(10)
    deadline = time.monotonic() + 10
    while any(_alive(p) for p in pids) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not [p for p in pids if _alive(p)]


def test_cli_thread_workers():
    with serve_process('tests.apps.basic:app', '--workers', '2', '--worker-mode', 'thread') as (url, proc):
        for _ in range(20):
            assert httpx.get(url + '/').text == 'Hello, world!'


def test_cli_fastapi():
    with serve_process('tests.apps.fastapi_app:app') as (url, proc):
        assert httpx.get(url + '/').json() == {'hello': 'world'}
        assert httpx.get(url + '/info').json()['lifespan_state'] == 'ready'


def test_no_lifespan_skips_startup():
    from tests.conftest import serve_process

    with serve_process('tests.apps.lifespan_fail:app', '--no-lifespan') as (url, _):
        assert httpx.get(url + '/').text == 'ok'


def test_venv_puts_site_packages_first(tmp_path):
    from weft.config import activate_venv

    if os.name == 'nt':
        site = tmp_path / 'Lib' / 'site-packages'
    else:
        site = tmp_path / 'lib' / f'python{sys.version_info[0]}.{sys.version_info[1]}' / 'site-packages'
    site.mkdir(parents=True)
    (site / 'weft_venv_marker.py').write_text('VALUE = 7\n')
    assert activate_venv(str(tmp_path)) is None
    import weft_venv_marker

    assert weft_venv_marker.VALUE == 7


def test_unix_socket(tmp_path):
    if not hasattr(socket, 'AF_UNIX'):
        pytest.skip('AF_UNIX is not available')
    path = str(tmp_path / 'weft.sock')
    try:
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        probe.close()
    except OSError:
        pytest.skip('AF_UNIX sockets cannot be created')
    from tests.conftest import ROOT
    import subprocess

    proc = subprocess.Popen(
        [sys.executable, '-m', 'weft', 'tests.apps.basic:app', '--unix', path, '--log-level', 'warning'],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if os.path.exists(path):
                try:
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.settimeout(1)
                    s.connect(path)
                    s.sendall(b'GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
                    data = s.recv(1024)
                    s.close()
                    if data.startswith(b'HTTP/1.1 200'):
                        assert b'Hello, world!' in data
                        return
                except OSError:
                    pass
            time.sleep(0.1)
        raise TimeoutError('unix server did not come up')
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_reload_replaces_worker(tmp_path):
    app = tmp_path / 'rlapp.py'
    app.write_text(
        'import os\n'
        'async def app(scope, receive, send):\n'
        "    if scope['type'] != 'http':\n"
        '        return\n'
        "    body = str(os.getpid()).encode() if scope['path'] == '/pid' else b'v1'\n"
        "    await send({'type': 'http.response.start', 'status': 200, 'headers': []})\n"
        "    await send({'type': 'http.response.body', 'body': body})\n"
    )
    from tests.conftest import serve_process

    env_extra = {'PYTHONDONTWRITEBYTECODE': '1'}
    with serve_process('rlapp:app', '--app-dir', str(tmp_path), '--reload', '--reload-interval', '0.2',
                       env=env_extra) as (url, proc):
        assert httpx.get(url + '/').text == 'v1'
        pid = int(httpx.get(url + '/pid').text)
        app.write_text(
            'import os\n'
            'async def app(scope, receive, send):\n'
            "    if scope['type'] != 'http':\n"
            '        return\n'
            "    body = str(os.getpid()).encode() if scope['path'] == '/pid' else b'v2'\n"
            "    await send({'type': 'http.response.start', 'status': 200, 'headers': []})\n"
            "    await send({'type': 'http.response.body', 'body': body})\n"
        )
        os.utime(app, (time.time() + 2, time.time() + 2))
        deadline = time.monotonic() + 15
        last = None
        while time.monotonic() < deadline:
            try:
                r = httpx.get(url + '/')
                new = int(httpx.get(url + '/pid').text)
                last = (r.text, new)
            except httpx.HTTPError:
                time.sleep(0.1)
                continue
            if r.text == 'v2' and new != pid:
                assert proc.poll() is None
                return
            time.sleep(0.15)
        out = proc.stdout.read() if proc.stdout else b''
        raise AssertionError(f'worker did not reload; last={last} pid={pid} log={out!r}')


@pytest.mark.skipif(sys.platform == 'win32', reason='SIGHUP is a Unix signal')
def test_sighup_reloads_worker(tmp_path):
    import signal

    app = tmp_path / 'hupapp.py'
    app.write_text(
        'import os\n'
        'async def app(scope, receive, send):\n'
        "    if scope['type'] != 'http':\n"
        '        return\n'
        "    await send({'type': 'http.response.start', 'status': 200, 'headers': []})\n"
        "    await send({'type': 'http.response.body', 'body': str(os.getpid()).encode()})\n"
    )
    from tests.conftest import serve_process

    with serve_process('hupapp:app', '--app-dir', str(tmp_path), '--reload', '--reload-interval', '60') as (url, proc):
        pid = int(httpx.get(url).text)
        os.kill(proc.pid, signal.SIGHUP)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                new = int(httpx.get(url).text)
            except httpx.HTTPError:
                time.sleep(0.1)
                continue
            if new != pid:
                assert proc.poll() is None
                return
            time.sleep(0.15)
        raise AssertionError('SIGHUP did not replace the worker')


def test_cli_version():
    import subprocess

    out = subprocess.run([sys.executable, '-m', 'weft', '--version'], capture_output=True, text=True)
    assert out.stdout.startswith('weft ')


def test_supervisor_stops_started_workers_when_a_later_one_fails(monkeypatch):
    import multiprocessing

    from weft import server

    procs = []

    class Proc:
        def __init__(self, target, args, name):
            self.stop, self.ready, self.index = args[2], args[4], args[5]
            self.alive = False
            self.exitcode = None
            self.pid = 1000 + self.index
            procs.append(self)

        def start(self):
            self.alive = True
            if self.index == 0:
                self.ready.set()
            else:
                self.alive = False
                self.exitcode = 1

        def is_alive(self):
            return self.alive

        def join(self, timeout=None):
            if self.stop.is_set():
                self.alive = False
                self.exitcode = 0

        def terminate(self):
            self.alive = False

    class Ctx:
        Event = threading.Event
        Process = Proc

    monkeypatch.setattr(multiprocessing, 'get_context', lambda method: Ctx)
    code = server._run_supervisor(Config(app='tests.apps.basic:app', workers=2), None, 2)
    assert code == 3
    assert len(procs) == 2
    assert procs[0].stop.is_set()
    assert not procs[0].alive


@pytest.mark.skipif(sys.platform == 'win32', reason='SIGTERM ends a Windows process at once')
@pytest.mark.parametrize('mode', ['process', 'thread'])
def test_sigterm_drains_and_shuts_down(tmp_path, mode):
    import signal

    mark = tmp_path / 'mark'
    args = ('--workers', '2', '--worker-mode', mode, '--drain-delay', '0.5')
    with serve_process('tests.apps.shutdown_app:app', *args, env={'WEFT_TEST_MARK': str(mark)}) as (url, proc):
        result = {}
        t = threading.Thread(target=lambda: result.update(r=httpx.get(url + '/slow', timeout=10)))
        t.start()
        time.sleep(0.3)
        start = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        t.join(10)
        assert result['r'].status_code == 200
        assert result['r'].text == 'done'
        assert proc.wait(15) == 0
        # Drained first, then every worker ran its lifespan shutdown.
        assert time.monotonic() - start >= 0.5
        assert len(mark.read_text().split()) == 2
