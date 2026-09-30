"""WSGI (PEP 3333)."""

import socket
from urllib.parse import urlparse

import httpx
import pytest

from tests.conftest import serve_thread
from tests.test_http import raw


@pytest.fixture(scope='module', params=[0, 4], ids=['inline', 'pool'])
def wsgi(request):
    from tests.apps.wsgi_app import app

    with serve_thread(app, root_path='/root', request_timeout=2.0, wsgi_threads=request.param) as url:
        yield url


def test_pool_runs_requests_concurrently():
    import threading
    import time

    from tests.apps.wsgi_app import app

    with serve_thread(app, wsgi_threads=4) as url:
        slow = threading.Thread(target=httpx.get, args=(url + '/sleep',), kwargs={'timeout': 10})
        slow.start()
        time.sleep(0.1)
        t = time.monotonic()
        assert httpx.get(url + '/').status_code == 200
        assert time.monotonic() - t < 0.5
        assert httpx.get(url + '/environ').json()['wsgi.multithread'] is True
        slow.join()


def get(url, path, **kw):
    return httpx.get(url + path, **kw)


def test_detection():
    from tests.apps.basic import app as asgi_app
    from tests.apps.wsgi_app import CallableApp, app
    from weft.wsgi import detect

    assert detect(app) == 'wsgi'
    assert detect(CallableApp()) == 'wsgi'
    assert detect(asgi_app) == 'asgi'


def test_hello(wsgi):
    r = get(wsgi, '/')
    assert r.status_code == 200
    assert r.text == 'Hello, world!'
    assert r.headers['content-length'] == '13'
    assert r.headers['server'] == 'weft'
    assert 'date' in r.headers
    out = raw(wsgi, b'GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
    assert out.lower().count(b'content-length:') == 1


def test_environ(wsgi, request):
    r = get(wsgi, '/root/environ?a=1&b=%20', headers={'X-Thing': 'one', 'Cookie': 'a=1'})
    e = r.json()
    assert e['REQUEST_METHOD'] == 'GET'
    assert e['SCRIPT_NAME'] == '/root'
    assert e['PATH_INFO'] == '/environ'
    assert e['QUERY_STRING'] == 'a=1&b=%20'
    assert e['SERVER_PROTOCOL'] == 'HTTP/1.1'
    assert e['SERVER_NAME'] == '127.0.0.1'
    assert e['SERVER_PORT'] == str(urlparse(wsgi).port)
    assert e['REMOTE_ADDR'] == '127.0.0.1'
    assert e['REMOTE_PORT'].isdigit()
    assert e['HTTP_X_THING'] == 'one'
    assert e['HTTP_COOKIE'] == 'a=1'
    assert e['HTTP_HOST'].startswith('127.0.0.1')
    assert e['wsgi.url_scheme'] == 'http'
    assert e['wsgi.version'] == [1, 0]
    assert e['wsgi.errors_is_stderr']
    assert e['wsgi.multithread'] is ('pool' in request.node.callspec.id)
    assert e['wsgi.run_once'] is False
    assert e['wsgi.input_terminated'] is True
    assert 'CONTENT_LENGTH' not in e


def test_environ_folding_and_filtering(wsgi):
    out = raw(wsgi, b'GET /environ HTTP/1.1\r\nhost: x\r\nx-a: 1\r\nx-a: 2\r\ncookie: a=1\r\ncookie: b=2\r\n'
                    b'x_under: no\r\nproxy: no\r\nconnection: close\r\n\r\n')
    import json

    e = json.loads(out.split(b'\r\n\r\n', 1)[1])
    assert e['HTTP_X_A'] == '1, 2'
    assert e['HTTP_COOKIE'] == 'a=1; b=2'
    assert 'HTTP_X_UNDER' not in e
    assert 'HTTP_PROXY' not in e


def test_percent_decoded_path(wsgi):
    assert get(wsgi, '/a%20b%2Fc').json() == {'path': '/a b/c'}


def test_latin1(wsgi):
    r = get(wsgi, '/latin1', headers={'X-In': 'caf\u00e9'.encode('latin-1')})
    assert r.headers.raw[0] == (b'X-Latin', 'caf\u00e9'.encode('latin-1'))
    assert r.content == 'caf\u00e9'.encode('latin-1')


def test_echo(wsgi):
    body = b'z' * 100_000
    r = httpx.post(wsgi + '/echo', content=body)
    assert r.content == body


def test_echo_chunked(wsgi):
    r = httpx.post(wsgi + '/echo', content=iter([b'ab', b'cd']))
    assert r.content == b'abcd'
    r = httpx.post(wsgi + '/environ', content=iter([b'ab', b'cd']))
    assert r.json()['CONTENT_LENGTH'] == '4'


def test_expect_continue(wsgi):
    u = urlparse(wsgi)
    with socket.create_connection((u.hostname, u.port), timeout=5) as s:
        s.sendall(b'POST /echo HTTP/1.1\r\nhost: x\r\ncontent-length: 3\r\nexpect: 100-continue\r\n\r\n')
        assert s.recv(100).startswith(b'HTTP/1.1 100 Continue\r\n\r\n')
        s.sendall(b'abc')
        assert s.recv(1000).endswith(b'abc')


def test_generator_is_chunked(wsgi):
    r = get(wsgi, '/generator')
    assert r.text == 'part0;part1;part2;'
    assert r.headers['transfer-encoding'] == 'chunked'


def test_empty_generator(wsgi):
    r = get(wsgi, '/empty-generator')
    assert r.status_code == 204
    assert r.content == b''


def test_write_callable(wsgi):
    assert get(wsgi, '/write').text == 'written;returned'


def test_declared_length_is_enforced(wsgi):
    r = get(wsgi, '/declared')
    assert r.content == b'hello'
    out = raw(wsgi, b'GET /short HTTP/1.1\r\nhost: x\r\n\r\n')
    assert out.endswith(b'short')


def test_app_transfer_encoding_dropped(wsgi):
    out = raw(wsgi, b'GET /te HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
    assert out.lower().count(b'transfer-encoding') == 1
    assert out.endswith(b'1\r\na\r\n1\r\nb\r\n0\r\n\r\n')


def test_app_connection_close(wsgi):
    out = raw(wsgi, b'GET /close HTTP/1.1\r\nhost: x\r\n\r\nGET / HTTP/1.1\r\nhost: x\r\n\r\n')
    assert out.count(b'HTTP/1.1 200') == 1


def test_keep_alive_pipelined(wsgi):
    out = raw(wsgi, b'GET / HTTP/1.1\r\nhost: x\r\n\r\nPOST /echo HTTP/1.1\r\nhost: x\r\ncontent-length: 2\r\n\r\nhi'
                    b'GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
    assert out.count(b'HTTP/1.1 200') == 3
    assert b'\r\n\r\nhi' in out


def test_errors(wsgi):
    assert get(wsgi, '/error').status_code == 500
    assert get(wsgi, '/error-before-first').status_code == 500
    assert get(wsgi, '/no-start').status_code == 500
    out = raw(wsgi, b'GET /error-in-iter HTTP/1.1\r\nhost: x\r\n\r\n')
    assert out.startswith(b'HTTP/1.1 200')
    assert b'first' in out
    assert not out.endswith(b'0\r\n\r\n')
    assert get(wsgi, '/').status_code == 200


def test_exc_info(wsgi):
    r = get(wsgi, '/exc-info')
    assert r.status_code == 500
    assert r.text == 'recovered'


def test_start_response_twice(wsgi):
    assert get(wsgi, '/twice').text == 'refused'


def test_bad_header_value(wsgi):
    assert get(wsgi, '/bad-header').text == 'refused'


def test_close_called(wsgi):
    before = get(wsgi, '/closed').json()
    assert get(wsgi, '/close-called').text == 'body'
    assert get(wsgi, '/closed').json() == before + 1


def test_file_wrapper(wsgi):
    r = get(wsgi, '/file')
    assert r.content == b'x' * 200_000


def test_big_body(wsgi):
    r = get(wsgi, '/big')
    assert len(r.content) == 4 << 20


def test_head(wsgi):
    r = httpx.head(wsgi + '/')
    assert r.status_code == 200
    assert r.content == b''
    assert r.headers['content-length'] == '13'


def test_websocket_refused(wsgi):
    out = raw(wsgi, b'GET / HTTP/1.1\r\nhost: x\r\nupgrade: websocket\r\nconnection: upgrade\r\n'
                    b'sec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\nsec-websocket-version: 13\r\n\r\n')
    assert out.startswith(b'HTTP/1.1 501')


def test_body_stall_times_out(wsgi):
    u = urlparse(wsgi)
    with socket.create_connection((u.hostname, u.port), timeout=5) as s:
        s.sendall(b'POST /echo HTTP/1.1\r\nhost: x\r\ncontent-length: 10\r\n\r\nabc')
        assert s.recv(100).startswith(b'HTTP/1.1 408')


def test_forced_protocol():
    from tests.apps.wsgi_app import CallableApp

    with serve_thread(CallableApp(), protocol='wsgi') as url:
        assert httpx.get(url + '/').text == 'callable'


def test_cli(tmp_path):
    from tests.conftest import serve_process

    with serve_process('tests.apps.wsgi_app:app') as (url, _):
        assert httpx.get(url + '/').text == 'Hello, world!'


def test_many_keep_alive_clients_get_their_own_responses(wsgi):
    """More connections than a flush batch holds, each a run of requests:
    every response is sent, to its own client, in order, without waiting."""
    import threading
    import time

    errors = []

    def client(n):
        try:
            with httpx.Client(timeout=5.0) as c:
                for k in range(30):
                    body = f'{n}-{k}'.encode()
                    r = c.post(wsgi + '/echo', content=body)
                    if r.content != body:
                        errors.append((n, k, r.content))
        except Exception as e:  # noqa: BLE001
            errors.append((n, repr(e)))

    t = time.monotonic()
    threads = [threading.Thread(target=client, args=(n,)) for n in range(40)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(30)
    assert not errors, errors[:5]
    assert time.monotonic() - t < 20
