import asyncio
import ssl

import httpx
import pytest

from tests.conftest import serve_thread
from tests.test_https import _cert, _client

aioquic = pytest.importorskip('aioquic')
from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration


class Client(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http = H3Connection(self._quic)
        self._done = {}

    def quic_event_received(self, event):
        for ev in self._http.handle_event(event):
            if isinstance(ev, HeadersReceived):
                status = None
                headers = {}
                for k, v in ev.headers:
                    if k == b':status':
                        status = int(v)
                    else:
                        headers[k] = v
                slot = self._done.setdefault(ev.stream_id, {'status': None, 'headers': {}, 'body': b''})
                slot['status'] = status
                slot['headers'] = headers
                slot['ended'] = ev.stream_ended
            elif isinstance(ev, DataReceived):
                self._done.setdefault(ev.stream_id, {'status': None, 'headers': {}, 'body': b''})
                self._done[ev.stream_id]['body'] += ev.data
                self._done[ev.stream_id]['ended'] = ev.stream_ended

    async def request(self, method, path, headers=None, body=b''):
        stream_id = self._quic.get_next_available_stream_id()
        hdrs = [
            (b':method', method.encode()),
            (b':scheme', b'https'),
            (b':authority', b'localhost'),
            (b':path', path.encode()),
        ]
        for k, v in headers or []:
            hdrs.append((k, v))
        if body:
            hdrs.append((b'content-length', str(len(body)).encode()))
        self._http.send_headers(stream_id, hdrs, end_stream=not body)
        if body:
            self._http.send_data(stream_id, body, end_stream=True)
        self.transmit()
        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            if stream_id in self._done and self._done[stream_id]['status'] is not None:
                d = self._done[stream_id]
                return d['status'], d['headers'], d['body']
            await asyncio.sleep(0.01)
        raise TimeoutError(f'no HTTP/3 response for {method} {path}')


def configuration():
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    cfg.verify_mode = ssl.CERT_NONE
    return cfg


async def h3_request(port, method, path, headers=None, body=b''):
    async with connect('127.0.0.1', port, configuration=configuration(), create_protocol=Client) as client:
        return await client.request(method, path, headers, body)


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=30))


@pytest.fixture
def h3(tmp_path):
    from tests.apps.basic import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], http3=True) as url:
        port = int(url.rsplit(':', 1)[1])
        yield port, url


def test_http3_get(h3):
    port, _ = h3
    status, headers, body = run(h3_request(port, 'GET', '/'))
    assert status == 200
    assert body == b'Hello, world!'
    assert headers.get(b'server') == b'weft'
    assert b'date' in headers


def test_http3_head_and_404(h3):
    port, _ = h3
    status, _, body = run(h3_request(port, 'HEAD', '/'))
    assert status == 200
    assert body == b''
    status, _, _ = run(h3_request(port, 'GET', '/nope'))
    assert status == 404


def test_http3_echo(h3):
    port, _ = h3
    status, _, body = run(h3_request(port, 'POST', '/echo', body=b'hello-h3'))
    assert status == 200
    assert body == b'hello-h3'


def test_http3_scope(h3):
    port, _ = h3
    _, _, body = run(h3_request(port, 'GET', '/scope'))
    assert b'"http_version": "3"' in body
    assert b'"scheme": "https"' in body
    assert b'"webtransport"' in body


def test_http3_hsts(tmp_path):
    from tests.apps.basic import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], http3=True, hsts=600) as url:
        port = int(url.rsplit(':', 1)[1])
        _, headers, _ = run(h3_request(port, 'GET', '/'))
        assert headers.get(b'strict-transport-security') == b'max-age=600'


def test_http3_health(tmp_path):
    from tests.apps.basic import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], http3=True, health_check_path='/healthz') as url:
        port = int(url.rsplit(':', 1)[1])
        status, _, body = run(h3_request(port, 'GET', '/healthz'))
        assert status == 200
        assert body == b''
        status, _, body = run(h3_request(port, 'GET', '/'))
        assert status == 200
        assert body == b'Hello, world!'


def test_http3_concurrent(h3):
    port, _ = h3

    async def many():
        async with connect('127.0.0.1', port, configuration=configuration(), create_protocol=Client) as client:
            got = await asyncio.gather(*[client.request('GET', '/') for _ in range(10)])
            assert all(s == 200 and b == b'Hello, world!' for s, _, b in got)

    run(many())


def test_http1_advertises_h3(tmp_path):
    from tests.apps.basic import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], http3=True) as url:
        https = url.replace('http://', 'https://')
        r = _client().get(https + '/')
        assert r.status_code == 200
        port = int(url.rsplit(':', 1)[1])
        assert r.headers['alt-svc'] == f'h3=":{port}"; ma=86400'


def test_http3_wsgi(tmp_path):
    from tests.apps.wsgi_app import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, protocol='wsgi', tls_certs=[str(cert)], tls_keys=[str(key)], http3=True) as url:
        port = int(url.rsplit(':', 1)[1])
        status, _, body = run(h3_request(port, 'GET', '/'))
        assert status == 200
        assert body == b'Hello, world!'
        _, _, env = run(h3_request(port, 'GET', '/environ'))
        assert b'HTTP/3' in env


def test_http3_dispatches_before_body_ends(tmp_path):
    from tests.test_h2 import _first_chunk

    async def open_post(port):
        async with connect('127.0.0.1', port, configuration=configuration(), create_protocol=Client) as client:
            stream_id = client._quic.get_next_available_stream_id()
            client._http.send_headers(stream_id, [
                (b':method', b'POST'), (b':scheme', b'https'),
                (b':authority', b'localhost'), (b':path', b'/'),
            ])
            client._http.send_data(stream_id, b'hello', end_stream=False)
            client.transmit()
            for _ in range(500):
                d = client._done.get(stream_id)
                if d and d['status'] is not None and d['body']:
                    return d['status'], d['body']
                await asyncio.sleep(0.01)
            raise TimeoutError('the application never saw the first piece of the body')

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(_first_chunk, lifespan='off', tls_certs=[str(cert)], tls_keys=[str(key)], http3=True) as url:
        port = int(url.rsplit(':', 1)[1])
        assert run(open_post(port)) == (200, b'hello+')


def test_http3_large_static_file(tmp_path):
    import os

    from tests.apps.basic import app

    root = tmp_path / 'www'
    root.mkdir()
    data = os.urandom(1024 * 1024)
    (root / 'big.bin').write_bytes(data)

    async def fetch(port):
        async with connect('127.0.0.1', port, configuration=configuration(), create_protocol=Client) as client:
            stream_id = client._quic.get_next_available_stream_id()
            client._http.send_headers(stream_id, [
                (b':method', b'GET'), (b':scheme', b'https'),
                (b':authority', b'localhost'), (b':path', b'/static/big.bin'),
            ], end_stream=True)
            client.transmit()
            for _ in range(2000):
                d = client._done.get(stream_id)
                if d and d.get('ended'):
                    return d['status'], d['body']
                await asyncio.sleep(0.01)
            raise TimeoutError('the response never ended')

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], http3=True, static_dirs=[f'/static={root}']) as url:
        port = int(url.rsplit(':', 1)[1])
        status, body = run(fetch(port))
        assert status == 200
        assert len(body) == len(data)
        assert body == data


def test_http3_upload_resumes_after_backpressure(tmp_path):
    """The body stops being read at the server's high-water mark while the
    application is busy, and resumes once it takes what was read."""

    async def late_reader(scope, receive, send):
        await asyncio.sleep(0.4)
        n = 0
        while True:
            m = await receive()
            if m['type'] != 'http.request':
                return
            n += len(m['body'])
            if not m['more_body']:
                break
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': str(n).encode()})

    size = 1024 * 1024
    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(late_reader, lifespan='off', tls_certs=[str(cert)], tls_keys=[str(key)], http3=True) as url:
        port = int(url.rsplit(':', 1)[1])
        status, _, body = run(h3_request(port, 'POST', '/', body=b'u' * size))
        assert status == 200
        assert body == str(size).encode()
