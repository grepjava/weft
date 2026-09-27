import gzip

import httpx

from tests.apps.compress_app import BODY, app
from tests.conftest import serve_thread
from tests.test_http import raw


def _head_body(resp: bytes):
    h, _, b = resp.partition(b'\r\n\r\n')
    return h.lower(), b


def test_gzip_and_vary():
    with serve_thread(app, compress=True) as url:
        h, b = _head_body(raw(url, b'GET / HTTP/1.1\r\nhost: x\r\naccept-encoding: gzip\r\nconnection: close\r\n\r\n'))
        assert b'content-encoding: gzip' in h
        assert b'vary: accept-encoding' in h
        assert b'content-length' not in h
        assert b'transfer-encoding: chunked' in h
        assert gzip.decompress(_unchunk(b)) == BODY


def _unchunk(body: bytes) -> bytes:
    out = bytearray()
    s = body
    while s:
        line, _, rest = s.partition(b'\r\n')
        n = int(line.split(b';')[0], 16)
        if n == 0:
            break
        out.extend(rest[:n])
        s = rest[n + 2:]
    return bytes(out)


def test_br_preferred_on_a_tie():
    with serve_thread(app, compress=True) as url:
        h, _ = _head_body(raw(url, b'GET / HTTP/1.1\r\nhost: x\r\naccept-encoding: gzip, deflate, br\r\nconnection: close\r\n\r\n'))
        assert b'content-encoding: br' in h


def test_identity_still_varies():
    with serve_thread(app, compress=True) as url:
        h, b = _head_body(raw(url, b'GET / HTTP/1.1\r\nhost: x\r\naccept-encoding: identity\r\nconnection: close\r\n\r\n'))
        assert b'content-encoding' not in h
        assert b'vary: accept-encoding' in h
        assert BODY in b


def test_small_and_png_and_encoded_and_no_transform():
    with serve_thread(app, compress=True) as url:
        for path in ('/small', '/png', '/encoded', '/no-transform'):
            h, _ = _head_body(raw(
                url,
                f'GET {path} HTTP/1.1\r\nhost: x\r\naccept-encoding: gzip\r\nconnection: close\r\n\r\n'.encode(),
            ))
            if path == '/encoded':
                assert b'content-encoding: gzip' in h
            else:
                assert b'content-encoding: gzip' not in h or path == '/encoded'


def test_etag_weakened():
    with serve_thread(app, compress=True) as url:
        h, _ = _head_body(raw(url, b'GET /etag HTTP/1.1\r\nhost: x\r\naccept-encoding: gzip\r\nconnection: close\r\n\r\n'))
        assert b'etag: w/"v1"' in h
        h, _ = _head_body(raw(url, b'GET /etag HTTP/1.1\r\nhost: x\r\naccept-encoding: identity\r\nconnection: close\r\n\r\n'))
        assert b'etag: "v1"' in h


def test_head_not_encoded():
    with serve_thread(app, compress=True) as url:
        h, b = _head_body(raw(url, b'HEAD / HTTP/1.1\r\nhost: x\r\naccept-encoding: gzip\r\nconnection: close\r\n\r\n'))
        assert b'content-encoding' not in h
        assert b'vary: accept-encoding' in h
        assert b == b''


def test_stream_is_chunked():
    with serve_thread(app, compress=True) as url:
        h, b = _head_body(raw(url, b'GET /stream HTTP/1.1\r\nhost: x\r\naccept-encoding: gzip\r\nconnection: close\r\n\r\n'))
        assert b'transfer-encoding: chunked' in h
        assert gzip.decompress(_unchunk(b)) == BODY


def test_off_by_default():
    with serve_thread(app) as url:
        r = httpx.get(url + '/', headers={'Accept-Encoding': 'gzip'})
        assert r.text == BODY.decode()
        assert 'content-encoding' not in r.headers
