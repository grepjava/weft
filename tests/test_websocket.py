"""WebSockets: the ASGI websocket scope, RFC 6455 framing and its limits."""

import base64
import contextlib
import json
import os
import socket
import struct
import threading
import time
from urllib.parse import urlsplit

import httpx
import pytest
from websockets.exceptions import ConnectionClosed, InvalidStatus
from websockets.sync.client import connect

from tests.conftest import serve_thread


def ws_url(base: str, path: str) -> str:
    return 'ws' + base[len('http'):] + path


@pytest.fixture(scope='module')
def raw():
    from tests.apps.ws_app import app

    with serve_thread(app) as url:
        yield url


@pytest.fixture(scope='module')
def api():
    from tests.apps.fastapi_app import app

    with serve_thread(app) as url:
        yield url


# --- a hand-rolled client, for what well-behaved libraries will not send --------


class Raw:
    def __init__(self, base: str, path: str = '/echo', extra: str = ''):
        u = urlsplit(base)
        self.sock = socket.create_connection((u.hostname, u.port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall(
            f'GET {path} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
            f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n{extra}\r\n'.encode()
        )
        self.buf = b''
        while b'\r\n\r\n' not in self.buf:
            chunk = self.sock.recv(4096)
            assert chunk, 'closed during the handshake'
            self.buf += chunk
        head, _, self.buf = self.buf.partition(b'\r\n\r\n')
        self.head = head.decode()
        self.status = int(self.head.split()[1])

    def frame(self, opcode: int, payload: bytes, fin=True, mask=True, rsv=0) -> None:
        b0 = (0x80 if fin else 0) | rsv | opcode
        n = len(payload)
        m = 0x80 if mask else 0
        if n < 126:
            head = struct.pack('!BB', b0, m | n)
        elif n < 65536:
            head = struct.pack('!BBH', b0, m | 126, n)
        else:
            head = struct.pack('!BBQ', b0, m | 127, n)
        if mask:
            key = os.urandom(4)
            payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
            head += key
        self.sock.sendall(head + payload)

    def _need(self, n: int) -> bytes:
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def read(self) -> tuple[int, bytes]:
        b0, b1 = self._need(2)
        n = b1 & 0x7F
        if n == 126:
            (n,) = struct.unpack('!H', self._need(2))
        elif n == 127:
            (n,) = struct.unpack('!Q', self._need(8))
        assert not b1 & 0x80, 'server frames are never masked'
        return b0 & 0x0F, self._need(n)

    def close_code(self) -> int:
        while True:
            op, payload = self.read()
            if op == 8:
                return struct.unpack('!H', payload[:2])[0] if payload else 1005

    def eof(self, timeout: float = 5) -> bool:
        self.sock.settimeout(timeout)
        try:
            while self.sock.recv(65536):
                pass
            return True
        except (TimeoutError, ConnectionError):
            return False

    def close(self):
        self.sock.close()


# --- the scope and the handshake ------------------------------------------------


def test_echo_text_and_bytes(raw):
    with connect(ws_url(raw, '/echo')) as ws:
        ws.send('hello')
        assert ws.recv() == 'hello'
        ws.send(b'\x00\x01\x02')
        assert ws.recv() == b'\x00\x01\x02'
        ws.send('')
        assert ws.recv() == ''
        ws.send('héllo wörld ✓')
        assert ws.recv() == 'héllo wörld ✓'


def test_large_and_fragmented(raw):
    big = os.urandom(3 * 1024 * 1024)
    with connect(ws_url(raw, '/echo'), max_size=None) as ws:
        ws.send(big)
        assert ws.recv() == big
        ws.send(['frag', 'ment', 'ed'])
        assert ws.recv() == 'fragmented'
        ws.send([b'a' * 70000, b'b' * 10])
        assert ws.recv() == b'a' * 70000 + b'b' * 10


def test_scope(raw):
    with connect(ws_url(raw, '/scope?x=1'), subprotocols=['one', 'two'], additional_headers={'X-Test': 'yes'}) as ws:
        info = json.loads(ws.recv())
    assert info['type'] == 'websocket'
    assert info['scheme'] == 'ws'
    assert info['path'] == '/scope'
    assert info['query_string'] == 'x=1'
    assert info['http_version'] == '1.1'
    assert info['subprotocols'] == ['one', 'two']
    assert info['extensions'] == ['websocket.http.response']
    assert info['headers']['x-test'] == 'yes'
    assert info['client'][0] == '127.0.0.1'


def test_subprotocol_and_headers(raw):
    with connect(ws_url(raw, '/subprotocol'), subprotocols=['a', 'b']) as ws:
        assert ws.subprotocol == 'b'
        assert ws.response.headers['set-cookie'] == 'session=abc'
        # The server owns the handshake headers.
        assert ws.response.headers['connection'] == 'Upgrade'
        assert 'sec-websocket-extensions' not in ws.response.headers


def test_rejections(raw):
    for path, status in [('/reject', 403), ('/noaccept', 403), ('/crash', 500)]:
        with pytest.raises(InvalidStatus) as e:
            connect(ws_url(raw, path))
        assert e.value.response.status_code == status, path


def test_denial_response(raw):
    with pytest.raises(InvalidStatus) as e:
        connect(ws_url(raw, '/deny'))
    assert e.value.response.status_code == 418
    assert e.value.response.headers['x-deny'] == 'yes'
    assert e.value.response.body == b'no websockets for you'


def test_server_close_codes(raw):
    with connect(ws_url(raw, '/close')) as ws:
        with pytest.raises(ConnectionClosed):
            ws.recv()
        assert (ws.close_code, ws.close_reason) == (4001, 'bye')
    with connect(ws_url(raw, '/crash-open')) as ws:
        with pytest.raises(ConnectionClosed):
            ws.recv()
        assert ws.close_code == 1011


def test_client_close_reaches_app(raw):
    with connect(ws_url(raw, '/record')) as ws:
        for _ in range(3):
            ws.send('x')
        ws.close(4321, 'client says bye')
    time.sleep(0.3)
    last = httpx.get(raw + '/').json()
    assert last == {'code': 4321, 'reason': 'client says bye', 'count': 3, 'send_after_close': 'ClientDisconnected'}


def test_abrupt_disconnect_reaches_app(raw):
    c = Raw(raw, '/record')
    c.frame(1, b'one')
    c.close()
    time.sleep(0.3)
    last = httpx.get(raw + '/').json()
    assert last['code'] == 1006
    assert last['count'] == 1


def test_push_backpressure(raw):
    n = 2000
    with connect(ws_url(raw, f'/push?{n}'), max_queue=4) as ws:
        got = []
        for m in ws:
            got.append(m)
            if len(got) == 1:
                # Let the server run into the socket's buffers.
                time.sleep(0.3)
    assert len(got) == n
    assert [int(m[:6]) for m in got] == list(range(n))


def test_push_loop_does_not_starve_the_worker(raw):
    # A task that only awaits sends which complete at once must still let
    # the event loop accept, read and run other tasks.
    stop = threading.Event()
    c = Raw(raw, '/push?100000000')

    def reader():
        try:
            while not stop.is_set():
                c.read()
        except OSError:
            # Closed under it by the test's cleanup.
            if not stop.is_set():
                raise

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    try:
        time.sleep(0.2)
        t0 = time.monotonic()
        for _ in range(5):
            assert httpx.get(raw + '/', timeout=5).status_code == 200
        with connect(ws_url(raw, '/echo')) as ws:
            ws.send('hi')
            assert ws.recv(timeout=5) == 'hi'
        assert time.monotonic() - t0 < 5
    finally:
        stop.set()
        c.close()
        t.join(5)


def test_slow_reader_queue():
    from tests.apps.ws_app import app

    with serve_thread(app, ws_max_queue=2) as url, connect(ws_url(url, '/slow')) as ws:
        sent = [f'm{i}' for i in range(200)] + ['end']
        for m in sent:
            ws.send(m)
        assert json.loads(ws.recv(timeout=10)) == sent


# --- protocol enforcement -------------------------------------------------------


def test_ping_is_answered(raw):
    c = Raw(raw)
    assert c.status == 101
    c.frame(9, b'are you there')
    assert c.read() == (10, b'are you there')
    c.close()


def test_unmasked_frame_is_protocol_error(raw):
    c = Raw(raw)
    c.frame(1, b'hi', mask=False)
    assert c.close_code() == 1002
    assert c.eof()


def test_invalid_utf8_is_1007(raw):
    c = Raw(raw)
    c.frame(1, b'\xff\xfe')
    assert c.close_code() == 1007


def test_bad_frames(raw):
    cases = [
        dict(opcode=3, payload=b''),                 # reserved opcode
        dict(opcode=1, payload=b'x', rsv=0x40),      # RSV1 without compression
        dict(opcode=9, payload=b'x', fin=False),     # fragmented control frame
        dict(opcode=0, payload=b'x'),                # continuation with nothing to continue
        dict(opcode=8, payload=b'\x03\xed'),         # close code 1005 may not be sent
        dict(opcode=8, payload=b'\x03'),             # one-byte close body
    ]
    for case in cases:
        c = Raw(raw)
        c.frame(**case)
        assert c.close_code() == 1002, case
        c.close()


def test_new_message_inside_fragmented_one(raw):
    c = Raw(raw)
    c.frame(1, b'part', fin=False)
    c.frame(1, b'again')
    assert c.close_code() == 1002


def test_control_frames_interleave_fragments(raw):
    c = Raw(raw)
    c.frame(1, b'frag', fin=False)
    c.frame(9, b'p')
    assert c.read() == (10, b'p')
    c.frame(0, b'mented')
    assert c.read() == (1, b'fragmented')
    c.close()


def test_close_handshake_is_echoed(raw):
    c = Raw(raw)
    c.frame(8, struct.pack('!H', 1000) + b'done')
    assert c.close_code() == 1000
    assert c.eof()


def test_message_too_big():
    from tests.apps.ws_app import app

    with serve_thread(app, ws_max_message=1024) as url:
        with connect(ws_url(url, '/echo')) as ws:
            ws.send('x' * 1024)
            assert len(ws.recv()) == 1024
            ws.send('x' * 1025)
            with pytest.raises(ConnectionClosed):
                ws.recv()
            assert ws.close_code == 1009
        # The same limit holds across fragments.
        c = Raw(url)
        c.frame(2, b'a' * 600, fin=False)
        c.frame(0, b'b' * 600)
        assert c.close_code() == 1009


def test_keepalive_drops_silent_peer():
    from tests.apps.ws_app import app

    with serve_thread(app, ws_ping_interval=0.2, ws_ping_timeout=0.3) as url:
        # A real client answers pings and stays up.
        with connect(ws_url(url, '/echo')) as ws:
            time.sleep(1.2)
            ws.send('still here')
            assert ws.recv() == 'still here'
        # One that never answers is dropped, and the application is told.
        c = Raw(url, '/idle')
        assert c.read() == (9, b'')
        assert c.eof(timeout=3)
        time.sleep(0.2)
        assert httpx.get(url + '/').json() == {'code': 1006, 'type': 'websocket.disconnect'}


def test_no_websockets():
    from tests.apps.ws_app import app

    with serve_thread(app, websockets=False) as url:
        with pytest.raises(InvalidStatus) as e:
            connect(ws_url(url, '/echo'))
        assert e.value.response.status_code == 501


def test_shutdown_closes_with_1001():
    from tests.apps.ws_app import app

    result = {}
    with contextlib.ExitStack() as stack, serve_thread(app) as url:
        ws = stack.enter_context(connect(ws_url(url, '/echo')))
        ws.send('x')
        assert ws.recv() == 'x'

        def drain():
            try:
                ws.recv(timeout=10)
            except ConnectionClosed:
                result['code'] = ws.close_code

        t = threading.Thread(target=drain)
        t.start()
    t.join(10)
    assert result == {'code': 1001}


# --- permessage-deflate ---------------------------------------------------------


def test_compression_negotiated_and_round_trips():
    from tests.apps.ws_app import app

    with serve_thread(app, ws_compress=True) as url:
        with connect(ws_url(url, '/echo'), compression='deflate') as ws:
            ext = ws.response.headers['sec-websocket-extensions']
            assert ext.startswith('permessage-deflate')
            assert 'server_max_window_bits=12' in ext
            for i in range(50):
                msg = json.dumps({'seq': i, 'payload': 'abc' * (i * 10)})
                ws.send(msg)
                assert ws.recv() == msg
            blob = os.urandom(200_000)
            ws.send(blob)
            assert ws.recv() == blob
        # Uncompressed clients are unaffected.
        with connect(ws_url(url, '/echo'), compression=None) as ws:
            assert 'sec-websocket-extensions' not in ws.response.headers
            ws.send('plain')
            assert ws.recv() == 'plain'


def test_compression_off_by_default(raw):
    with connect(ws_url(raw, '/echo'), compression='deflate') as ws:
        assert 'sec-websocket-extensions' not in ws.response.headers


def test_compression_bomb_is_1009():
    import zlib

    from tests.apps.ws_app import app

    with serve_thread(app, ws_compress=True, ws_max_message=64 * 1024) as url:
        c = Raw(url, extra='Sec-WebSocket-Extensions: permessage-deflate\r\n')
        assert 'permessage-deflate' in c.head
        z = zlib.compressobj(9, zlib.DEFLATED, -15)
        payload = z.compress(b'\0' * (4 << 20)) + z.flush(zlib.Z_SYNC_FLUSH)
        assert len(payload) < 16 * 1024
        c.frame(2, payload[:-4], rsv=0x40)
        assert c.close_code() == 1009


# --- FastAPI --------------------------------------------------------------------


def test_fastapi_echo(api):
    with connect(ws_url(api, '/ws/echo')) as ws:
        for i in range(20):
            ws.send(f'msg {i}')
            assert ws.recv() == f'msg {i}'


def test_fastapi_json_params_subprotocol(api):
    with connect(ws_url(api, '/ws/json/lobby?token=t1'), subprotocols=['chat']) as ws:
        assert ws.subprotocol == 'chat'
        ws.send(json.dumps({'a': [1, 2]}))
        assert json.loads(ws.recv()) == {'room': 'lobby', 'token': 't1', 'echo': {'a': [1, 2]}}


def test_fastapi_bytes(api):
    with connect(ws_url(api, '/ws/bytes')) as ws:
        ws.send(b'abc')
        assert ws.recv() == b'cba'


def test_fastapi_policy_close(api):
    with pytest.raises(InvalidStatus) as e:
        connect(ws_url(api, '/ws/private'))
    assert e.value.response.status_code == 403
    with connect(ws_url(api, '/ws/private'), additional_headers={'Authorization': 'Bearer ok'}) as ws:
        assert ws.recv() == 'welcome'
        with pytest.raises(ConnectionClosed):
            ws.recv()
        assert ws.close_code == 1000


def test_fastapi_concurrent(api):
    import concurrent.futures

    def one(i):
        with connect(ws_url(api, '/ws/echo')) as ws:
            out = []
            for j in range(20):
                ws.send(f'{i}:{j}')
                out.append(ws.recv())
            return out

    with concurrent.futures.ThreadPoolExecutor(32) as ex:
        results = list(ex.map(one, range(64)))
    assert results == [[f'{i}:{j}' for j in range(20)] for i in range(64)]


def test_http_still_works_alongside(api):
    with connect(ws_url(api, '/ws/echo')) as ws:
        assert httpx.get(api + '/').json() == {'hello': 'world'}
        ws.send('ok')
        assert ws.recv() == 'ok'
