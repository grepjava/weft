import socket

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
