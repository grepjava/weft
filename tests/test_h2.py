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
