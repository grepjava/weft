"""HTTP/1.1 strictness and the 1xx extensions."""

import json
import socket
import time
from urllib.parse import urlparse

import pytest

from tests.conftest import serve_thread
from tests.test_http import raw


@pytest.fixture(scope='module')
def strict():
    from tests.apps.basic import app

    with serve_thread(app, max_body=1000, request_timeout=0.5, max_head=4096) as url:
        yield url


def status(out: bytes) -> int:
    return int(out.split(b' ', 2)[1])


def get(url: str, path: str = '/', headers: bytes = b'host: x\r\n', version: bytes = b'1.1') -> bytes:
    return raw(url, b'GET ' + path.encode() + b' HTTP/' + version + b'\r\n' + headers + b'connection: close\r\n\r\n')


def post(url: str, framing: bytes, body: bytes, path: bytes = b'/echo') -> bytes:
    return raw(url, b'POST ' + path + b' HTTP/1.1\r\nhost: x\r\n' + framing + b'connection: close\r\n\r\n' + body)


# --- the request head --------------------------------------------------------------


def test_host_required_on_http11(basic):
    assert status(get(basic, headers=b'')) == 400
    assert status(get(basic, headers=b'', version=b'1.0')) == 200


def test_second_host_refused(basic):
    assert status(get(basic, headers=b'host: x\r\nhost: x\r\n')) == 400
    assert status(get(basic, headers=b'host: x\r\nhost: y\r\n')) == 400


@pytest.mark.parametrize('te, code', [
    (b'transfer-encoding: gzip\r\n', 400),
    (b'transfer-encoding: chunked, gzip\r\n', 400),
    (b'transfer-encoding: chunked;x=1\r\n', 400),
    (b'transfer-encoding: xchunked\r\n', 400),
    (b'transfer-encoding: chunked, chunked\r\n', 400),
    (b'transfer-encoding: chunked\r\ntransfer-encoding: chunked\r\n', 400),
    (b'transfer-encoding: gzip, chunked\r\n', 501),
    (b'transfer-encoding: gzip\r\ntransfer-encoding: chunked\r\n', 501),
    (b'transfer-encoding: chunked\r\ncontent-length: 4\r\n', 400),
    (b'content-length: 4\r\ncontent-length: 5\r\n', 400),
    (b'content-length: +4\r\n', 400),
])
def test_body_framing_refused(basic, te, code):
    assert status(post(basic, te, b'4\r\nWiki\r\n0\r\n\r\n')) == code


def test_chunked_case_and_spacing(basic):
    out = post(basic, b'Transfer-Encoding:  CHUNKED \r\n', b'4\r\nWiki\r\n0\r\n\r\n')
    assert out.startswith(b'HTTP/1.1 200') and out.endswith(b'Wiki')


def test_obs_fold_and_space_before_colon(basic):
    assert status(get(basic, headers=b'host: x\r\nx-a: b\r\n c\r\n')) == 400
    assert status(get(basic, headers=b'host : x\r\n')) == 400
    assert status(get(basic, headers=b'host: x\r\nx-a : b\r\n')) == 400


def test_http10_transfer_coding_closes(basic):
    # The pipelined second request is never answered.
    req = b'POST /echo HTTP/1.0\r\ntransfer-encoding: chunked\r\nconnection: keep-alive\r\n\r\n4\r\nWiki\r\n0\r\n\r\n'
    out = raw(basic, req + b'GET / HTTP/1.0\r\n\r\n')
    assert out.count(b'HTTP/1.1 ') == 1
    assert b'connection: close' in out and out.endswith(b'Wiki')


def test_underscore_and_proxy_headers_dropped(basic):
    out = get(basic, '/scope', b'host: x\r\nx_real_ip: 1.2.3.4\r\nx-real-ip: 5.6.7.8\r\nproxy: http://evil\r\n')
    scope = json.loads(out.split(b'\r\n\r\n', 1)[1])
    names = [n for n, _ in scope['headers']]
    assert 'x_real_ip' not in names and 'proxy' not in names
    assert ['x-real-ip', '5.6.7.8'] in scope['headers']


def test_head_limit_default():
    from tests.apps.basic import app

    with serve_thread(app) as url:
        assert status(get(url, headers=b'host: x\r\nx-a: ' + b'a' * 30_000 + b'\r\n')) == 200
        assert status(get(url, headers=b'host: x\r\nx-a: ' + b'a' * 34_000 + b'\r\n')) == 431


# --- the request body --------------------------------------------------------------


def test_max_body_content_length(strict):
    assert status(post(strict, b'content-length: 1001\r\n', b'x' * 1001)) == 413
    out = post(strict, b'content-length: 1000\r\n', b'x' * 1000)
    assert status(out) == 200 and out.endswith(b'x' * 1000)


def test_max_body_chunked(strict):
    body = b'200\r\n' + b'x' * 512 + b'\r\n'
    out = post(strict, b'transfer-encoding: chunked\r\n', body * 2 + b'0\r\n\r\n')
    # The server answers; what the application sends afterwards goes nowhere.
    assert status(out) == 413 and out.count(b'HTTP/1.1') == 1
    out = post(strict, b'transfer-encoding: chunked\r\n', body + b'0\r\n\r\n')
    assert status(out) == 200


def test_malformed_chunk_mid_body(strict):
    out = post(strict, b'transfer-encoding: chunked\r\n', b'4\r\nWiki\r\nzz\r\n')
    assert status(out) == 400 and out.count(b'HTTP/1.1') == 1


def test_bare_lf_in_chunked_body(basic):
    assert status(post(basic, b'transfer-encoding: chunked\r\n', b'4\nWiki\r\n0\r\n\r\n')) == 400


def test_trailers_bounded(strict):
    trailers = b'x-t: ' + b'a' * 100 + b'\r\n'
    ok = post(strict, b'transfer-encoding: chunked\r\n', b'4\r\nWiki\r\n0\r\n' + trailers * 10 + b'\r\n')
    assert status(ok) == 200
    out = post(strict, b'transfer-encoding: chunked\r\n', b'4\r\nWiki\r\n0\r\n' + trailers * 50 + b'\r\n')
    assert status(out) == 431


def test_chunk_extensions_bounded(strict):
    out = post(strict, b'transfer-encoding: chunked\r\n', b'4;' + b'e' * 5000 + b'\r\nWiki\r\n0\r\n\r\n')
    assert status(out) == 431


def test_request_timeout_stalled_body(strict):
    t = time.monotonic()
    out = post(strict, b'content-length: 10\r\n', b'abc')
    assert status(out) == 408
    assert 0.4 < time.monotonic() - t < 3


def test_request_timeout_slow_but_moving(strict):
    u = urlparse(strict)
    with socket.create_connection((u.hostname, u.port), timeout=5) as s:
        s.sendall(b'POST /echo HTTP/1.1\r\nhost: x\r\ncontent-length: 8\r\nconnection: close\r\n\r\n')
        for b in b'abcdefgh':
            time.sleep(0.2)
            s.sendall(bytes([b]))
        out = b''
        while chunk := s.recv(4096):
            out += chunk
    assert status(out) == 200 and out.endswith(b'abcdefgh')


def test_request_timeout_ignores_slow_application(strict):
    # Time the application takes is not a stall.
    assert raw(strict, b'GET /sleep?0.8 HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n').endswith(b'slept')


def test_unread_body_after_response(strict):
    out = post(strict, b'content-length: 5\r\n', b'hello', path=b'/respond-early')
    assert status(out) == 200 and out.endswith(b'early')


# --- 1xx ---------------------------------------------------------------------------


def test_interim_responses(basic):
    out = post(basic, b'content-length: 3\r\n', b'abc', path=b'/interim')
    hint, rest = out.split(b'\r\n\r\n', 1)
    assert hint == (b'HTTP/1.1 103 Early Hints\r\nlink: </a.css>; rel=preload; as=style\r\n'
                    b'link: </b.js>; rel=preload; as=script')
    info, rest = rest.split(b'\r\n\r\n', 1)
    assert info == b'HTTP/1.1 104 Upload Resumption Supported\r\nlocation: /uploads/7'
    assert rest.startswith(b'HTTP/1.1 200 OK') and rest.endswith(b'final:abc')


def test_interim_responses_http10(basic):
    out = raw(basic, b'POST /interim HTTP/1.0\r\ncontent-length: 3\r\n\r\nabc')
    assert out.startswith(b'HTTP/1.1 200') and out.count(b'HTTP/1.1') == 1


def test_interim_refused(basic):
    out = get(basic, '/interim-bad')
    body = out.split(b'\r\n\r\n', 1)[1].decode().split(',')
    assert body == ['ValueError'] * 9 + ['RuntimeError']


def test_extensions_in_scope(basic):
    ext = json.loads(get(basic, '/extensions').split(b'\r\n\r\n', 1)[1])
    assert ext == {'http.response.informational': {}, 'http.response.early_hint': {}}
