import socket
import time

import httpx
import pytest

from tests.conftest import serve_thread
from tests.test_https import _cert, _client, _openssl


def test_h2c_prior_knowledge():
    import h2.config
    import h2.connection
    import h2.events

    from tests.apps.basic import app
    from urllib.parse import urlparse

    with serve_thread(app) as url:
        u = urlparse(url)
        sock = socket.create_connection((u.hostname, u.port), timeout=5)
        try:
            conn = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True))
            conn.initiate_connection()
            sock.sendall(conn.data_to_send())
            conn.send_headers(1, [
                (':method', 'GET'), (':path', '/'), (':scheme', 'http'),
                (':authority', f'{u.hostname}:{u.port}'),
            ], end_stream=True)
            sock.sendall(conn.data_to_send())
            status, body = None, b''
            while True:
                data = sock.recv(65535)
                if not data:
                    break
                for ev in conn.receive_data(data):
                    if isinstance(ev, h2.events.ResponseReceived):
                        status = dict(ev.headers)[b':status']
                    elif isinstance(ev, h2.events.DataReceived):
                        body += ev.data
                        conn.acknowledge_received_data(ev.flow_controlled_length, ev.stream_id)
                    elif isinstance(ev, h2.events.StreamEnded):
                        sock.sendall(conn.data_to_send())
                        assert status == b'200'
                        assert body == b'Hello, world!'
                        return
                sock.sendall(conn.data_to_send())
            raise AssertionError('stream never ended')
        finally:
            sock.close()


def test_h2_tls_get(tmp_path):
    h2 = pytest.importorskip('h2')
    del h2
    from tests.apps.basic import app

    _openssl()
    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)]) as url:
        https = url.replace('http://', 'https://')
        with httpx.Client(verify=False, http2=True, timeout=5.0) as c:
            r = c.get(https + '/')
            assert r.status_code == 200
            assert r.text == 'Hello, world!'
            assert r.http_version == 'HTTP/2'
            s = c.get(https + '/scope').json()
            assert s['scheme'] == 'https'
            assert s['http_version'] == '2'


def test_h2_scope_has_no_webtransport_with_http3(tmp_path):
    # HTTP/3 on the same server must not make HTTP/2 requests claim WebTransport.
    pytest.importorskip('h2')
    from tests.apps.basic import app

    _openssl()
    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], http3=True) as url:
        https = url.replace('http://', 'https://')
        with httpx.Client(verify=False, http2=True, timeout=5.0) as c:
            s = c.get(https + '/scope').json()
            assert s['http_version'] == '2'
            assert 'webtransport' not in s['extensions']
            assert 'http.response.pathsend' in s['extensions']


def test_h2_hsts(tmp_path):
    from tests.apps.basic import app

    _openssl()
    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], hsts=3600) as url:
        with httpx.Client(verify=False, http2=True, timeout=5.0) as c:
            r = c.get(url.replace('http://', 'https://') + '/')
            assert r.headers['strict-transport-security'] == 'max-age=3600'


def test_h2_wsgi(tmp_path):
    from tests.apps.wsgi_app import app

    _openssl()
    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)]) as url:
        with httpx.Client(verify=False, http2=True, timeout=5.0) as c:
            r = c.get(url.replace('http://', 'https://') + '/')
            assert r.status_code == 200
            assert r.text == 'Hello, world!'
            env = c.get(url.replace('http://', 'https://') + '/environ').json()
            assert env['wsgi.url_scheme'] == 'https'
            assert env['SERVER_PROTOCOL'] == 'HTTP/2'


def test_h2_multiplex(tmp_path):
    from tests.apps.basic import app

    _openssl()
    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)]) as url:
        https = url.replace('http://', 'https://')
        with httpx.Client(verify=False, http2=True, timeout=5.0) as c:
            a = c.get(https + '/')
            b = c.get(https + '/scope')
            assert a.text == 'Hello, world!'
            assert b.json()['http_version'] == '2'


def test_no_http2_stays_http11(tmp_path):
    from tests.apps.basic import app

    _openssl()
    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], http2=False) as url:
        r = _client().get(url.replace('http://', 'https://') + '/scope')
        assert r.json()['http_version'] == '1.1'


async def _first_chunk(scope, receive, send):
    """Answers as soon as the first piece of the body arrives."""
    m = await receive()
    await send({'type': 'http.response.start', 'status': 200, 'headers': []})
    await send({'type': 'http.response.body', 'body': m['body'] + (b'+' if m['more_body'] else b'.')})


async def _whole_body(scope, receive, send):
    body = b''
    while True:
        m = await receive()
        if m['type'] != 'http.request':
            return
        body += m['body']
        if not m['more_body']:
            break
    await send({'type': 'http.response.start', 'status': 200, 'headers': []})
    await send({'type': 'http.response.body', 'body': body})


def _h2c_open(url, path, data, *, headers=(), timeout=5.0):
    """Sends a POST whose stream stays open after `data`; returns the status
    and body of the response."""
    import h2.config
    import h2.connection
    import h2.events
    from urllib.parse import urlparse

    u = urlparse(url)
    sock = socket.create_connection((u.hostname, u.port), timeout=timeout)
    try:
        conn = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True))
        conn.initiate_connection()
        conn.send_headers(1, [
            (':method', 'POST'), (':path', path), (':scheme', 'http'),
            (':authority', f'{u.hostname}:{u.port}'), *headers,
        ])
        conn.send_data(1, data)
        sock.sendall(conn.data_to_send())
        status, body = None, b''
        while True:
            chunk = sock.recv(65535)
            if not chunk:
                raise AssertionError('connection closed before the response ended')
            for ev in conn.receive_data(chunk):
                if isinstance(ev, h2.events.ResponseReceived):
                    status = dict(ev.headers)[b':status']
                elif isinstance(ev, h2.events.DataReceived):
                    body += ev.data
                    conn.acknowledge_received_data(ev.flow_controlled_length, ev.stream_id)
                elif isinstance(ev, (h2.events.StreamEnded, h2.events.StreamReset)):
                    return status, body
            sock.sendall(conn.data_to_send())
    finally:
        sock.close()


def test_h2_dispatches_before_body_ends():
    with serve_thread(_first_chunk, lifespan='off') as url:
        assert _h2c_open(url, '/', b'hello') == (b'200', b'hello+')


def test_h2_dispatches_before_body_ends_with_length():
    with serve_thread(_first_chunk, lifespan='off') as url:
        assert _h2c_open(url, '/', b'hello', headers=[('content-length', '10')]) == (b'200', b'hello+')


def test_h2_request_timeout_stalled_body():
    with serve_thread(_whole_body, lifespan='off', request_timeout=0.5) as url:
        t = time.monotonic()
        status, _ = _h2c_open(url, '/', b'abc')
        assert status == b'408'
        assert 0.4 < time.monotonic() - t < 3


def test_h2c_body_in_pieces():
    import h2.config
    import h2.connection
    import h2.events
    from urllib.parse import urlparse

    with serve_thread(_whole_body, lifespan='off') as url:
        u = urlparse(url)
        with socket.create_connection((u.hostname, u.port), timeout=5) as sock:
            conn = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True))
            conn.initiate_connection()
            conn.send_headers(1, [
                (':method', 'POST'), (':path', '/'), (':scheme', 'http'),
                (':authority', f'{u.hostname}:{u.port}'),
            ])
            for piece in (b'ab', b'', b'cd'):
                conn.send_data(1, piece)
                sock.sendall(conn.data_to_send())
                time.sleep(0.05)
            conn.send_data(1, b'ef', end_stream=True)
            sock.sendall(conn.data_to_send())
            body = b''
            while True:
                chunk = sock.recv(65535)
                assert chunk, 'connection closed before the response ended'
                events = conn.receive_data(chunk)
                for ev in events:
                    if isinstance(ev, h2.events.DataReceived):
                        body += ev.data
                        conn.acknowledge_received_data(ev.flow_controlled_length, ev.stream_id)
                if any(isinstance(ev, h2.events.StreamEnded) for ev in events):
                    break
                sock.sendall(conn.data_to_send())
        assert body == b'abcdef'


def test_h2_large_static_file(tmp_path):
    import os

    from tests.apps.basic import app

    _openssl()
    cert, key = _cert(tmp_path, 'localhost')
    root = tmp_path / 'www'
    root.mkdir()
    data = os.urandom(1024 * 1024)
    (root / 'big.bin').write_bytes(data)
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], static_dirs=[f'/static={root}']) as url:
        with httpx.Client(verify=False, http2=True, timeout=10.0) as c:
            r = c.get(url.replace('http://', 'https://') + '/static/big.bin')
            assert r.http_version == 'HTTP/2'
            assert r.status_code == 200
            assert r.content == data
            # The connection is still good for another stream.
            assert c.get(url.replace('http://', 'https://') + '/').text == 'Hello, world!'


def _big_write(environ, start_response):
    """Legacy write() with more than an HTTP/2 window holds."""
    write = start_response('200 OK', [('content-type', 'application/octet-stream')])
    write(b'w' * (1024 * 1024))
    return [b'end']


def test_h2_wsgi_large_write(tmp_path):
    _openssl()
    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(_big_write, protocol='wsgi', request_timeout=2.0,
                      tls_certs=[str(cert)], tls_keys=[str(key)]) as url:
        with httpx.Client(verify=False, http2=True, timeout=10.0) as c:
            r = c.get(url.replace('http://', 'https://') + '/')
            assert r.http_version == 'HTTP/2'
            assert r.content == b'w' * (1024 * 1024) + b'end'
            # The worker is still serving.
            assert c.get(url.replace('http://', 'https://') + '/').status_code == 200


def test_h2c_preface_in_pieces():
    """TCP keeps no write boundaries: a preface arriving in parts is still
    HTTP/2, not a bad HTTP/1 request."""
    from urllib.parse import urlparse

    from tests.apps.basic import app

    PREFACE = b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'
    SETTINGS = b'\x00\x00\x00\x04\x00\x00\x00\x00\x00'  # empty SETTINGS, stream 0

    with serve_thread(app) as url:
        u = urlparse(url)
        with socket.create_connection((u.hostname, u.port), timeout=5) as sock:
            sock.sendall(PREFACE[:3])
            time.sleep(0.2)
            sock.sendall(PREFACE[3:] + SETTINGS)
            data = sock.recv(65535)
            assert not data.startswith(b'HTTP/1.1')
            assert data[3] == 0x4  # the server's SETTINGS frame


def _h2c_wait_disconnect(url, end):
    import h2.config
    import h2.connection
    from urllib.parse import urlparse

    from tests.apps.basic import app

    before = getattr(app, 'disconnected', 0)
    u = urlparse(url)
    sock = socket.create_connection((u.hostname, u.port), timeout=5)
    conn = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True))
    conn.initiate_connection()
    conn.send_headers(1, [
        (':method', 'GET'), (':path', '/wait-disconnect'), (':scheme', 'http'),
        (':authority', f'{u.hostname}:{u.port}'),
    ], end_stream=True)
    sock.sendall(conn.data_to_send())
    time.sleep(0.3)
    end(sock, conn)
    deadline = time.monotonic() + 5
    while getattr(app, 'disconnected', 0) == before:
        assert time.monotonic() < deadline, 'the application never saw http.disconnect'
        time.sleep(0.05)
    sock.close()


def test_h2_reset_after_body_reaches_app():
    from tests.apps.basic import app

    def reset(sock, conn):
        conn.reset_stream(1)
        sock.sendall(conn.data_to_send())

    with serve_thread(app) as url:
        _h2c_wait_disconnect(url, reset)


def test_h2_connection_close_after_body_reaches_app():
    from tests.apps.basic import app

    with serve_thread(app) as url:
        _h2c_wait_disconnect(url, lambda sock, conn: sock.close())


def test_h2_request_sent_during_shutdown_is_answered():
    """A request the client sends after shutdown starts, before it has read
    the server's GOAWAY, still gets its response. Raw frames: the h2 client
    library refuses to go on once it has read a GOAWAY."""
    import asyncio
    import struct
    import threading

    import hpack

    from weft import worker
    from weft.config import Config
    from weft.server import bind

    async def app(scope, receive, send):
        if scope['type'] != 'http':
            return
        if scope['path'] == '/slow':
            await asyncio.sleep(1.0)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': scope['path'].encode()})

    def frame(kind, flags, sid, payload=b''):
        return struct.pack('>I', len(payload))[1:] + bytes([kind, flags]) + struct.pack('>I', sid) + payload

    sock = bind('127.0.0.1', 0, 16)
    stop = threading.Event()
    cfg = Config(app=app, lifespan='off', log_level='critical', graceful_timeout=5.0)
    t = threading.Thread(target=worker.run, args=(cfg, sock, stop))
    t.start()
    port = sock.getsockname()[1]
    c = socket.create_connection(('127.0.0.1', port), timeout=5)
    enc = hpack.Encoder()

    def request(sid, path):
        block = enc.encode([(':method', 'GET'), (':path', path), (':scheme', 'http'),
                            (':authority', f'127.0.0.1:{port}')])
        c.sendall(frame(0x1, 0x5, sid, block))  # HEADERS, END_STREAM | END_HEADERS

    try:
        c.sendall(b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n' + frame(0x4, 0, 0))
        request(1, '/slow')  # keeps the connection busy through the stop
        time.sleep(0.3)
        stop.set()
        time.sleep(0.3)  # the server has sent GOAWAY and PING; we have not read them
        request(3, '/late')
        bodies = {1: b'', 3: b''}
        ended = set()
        buf = b''
        deadline = time.monotonic() + 4.0
        while ended != {1, 3} and time.monotonic() < deadline:
            try:
                data = c.recv(65535)
            except TimeoutError:
                break
            if not data:
                break
            buf += data
            while len(buf) >= 9:
                n = int.from_bytes(buf[:3], 'big')
                if len(buf) < 9 + n:
                    break
                kind, flags = buf[3], buf[4]
                sid = int.from_bytes(buf[5:9], 'big') & 0x7FFFFFFF
                payload, buf = buf[9:9 + n], buf[9 + n:]
                if kind == 0x4 and not flags & 0x1:
                    c.sendall(frame(0x4, 0x1, 0))  # SETTINGS ack
                elif kind == 0x6 and not flags & 0x1:
                    c.sendall(frame(0x6, 0x1, 0, payload))  # PING ack
                elif kind == 0x0:
                    bodies[sid] += payload
                if kind in (0x0, 0x1) and flags & 0x1:
                    ended.add(sid)
        assert bodies == {1: b'/slow', 3: b'/late'}
    finally:
        c.close()
        stop.set()
        t.join(10)
        sock.close()
