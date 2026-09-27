import socket
import time
from urllib.parse import urlparse

import httpx
import pytest


def raw(url: str, data: bytes, *, read_until_close: bool = True, timeout: float = 5.0) -> bytes:
    u = urlparse(url)
    with socket.create_connection((u.hostname, u.port), timeout=timeout) as s:
        s.sendall(data)
        chunks = []
        while True:
            try:
                b = s.recv(65536)
            except TimeoutError:
                break
            if not b:
                break
            chunks.append(b)
            if not read_until_close:
                break
        return b''.join(chunks)


def test_hello(basic):
    r = httpx.get(basic + '/')
    assert r.status_code == 200
    assert r.text == 'Hello, world!'
    assert r.headers['content-length'] == '13'
    assert r.headers['content-type'] == 'text/plain'
    assert r.headers['server'] == 'weft'
    assert r.headers['date'].endswith(' GMT')


def test_scope(basic):
    r = httpx.get(basic + '/scope?a=1&b=%20x', headers={'X-Custom': 'Yes'})
    s = r.json()
    assert s['type'] == 'http'
    assert s['asgi'] == {'version': '3.0', 'spec_version': '2.4'}
    assert s['http_version'] == '1.1'
    assert s['method'] == 'GET'
    assert s['scheme'] == 'http'
    assert s['path'] == '/scope'
    assert s['raw_path'] == '/scope'
    assert s['query_string'] == 'a=1&b=%20x'
    assert s['root_path'] == ''
    assert s['client'][0] == '127.0.0.1'
    assert s['server'] == ['127.0.0.1', int(basic.rsplit(':', 1)[1])]
    assert ['x-custom', 'Yes'] in s['headers']
    assert s['state'] == {'started': True}


def test_path_decoding(basic):
    out = raw(basic, b'GET /sc%6Fpe/../%E2%9C%93?q HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
    assert out.startswith(b'HTTP/1.1 404')
    r = httpx.get(basic + '/scope/%E2%9C%93%20x')
    assert r.status_code == 404
    out = raw(basic, b'GET /%73cope HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
    assert b'"path": "/scope"' in out
    assert b'"raw_path": "/%73cope"' in out


def test_post_echo(basic):
    r = httpx.post(basic + '/echo', content=b'abc123')
    assert r.content == b'abc123'


def test_post_large(basic):
    body = bytes(range(256)) * 20_000  # ~5 MB
    r = httpx.post(basic + '/echo', content=body, timeout=30)
    assert r.content == body


def test_chunked_request(basic):
    def gen():
        for i in range(50):
            yield b'y' * (1000 + i)

    r = httpx.post(basic + '/size', content=gen())
    assert int(r.text) == sum(1000 + i for i in range(50))


def test_chunked_request_raw(basic):
    out = raw(basic, b'POST /echo HTTP/1.1\r\nhost: x\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n'
                     b'4\r\nWiki\r\n5;ext\r\npedia\r\n0\r\nx-trailer: 1\r\n\r\n')
    assert out.endswith(b'Wikipedia')


def test_streaming_response(basic):
    r = httpx.get(basic + '/stream?5')
    assert r.headers['transfer-encoding'] == 'chunked'
    assert r.text == ''.join(f'chunk{i}\n' for i in range(5))


def test_http10_close_delimited(basic):
    out = raw(basic, b'GET /stream HTTP/1.0\r\n\r\n')
    head, body = out.split(b'\r\n\r\n', 1)
    assert b'transfer-encoding' not in head
    assert b'connection: close' in head
    assert body == b'chunk0\nchunk1\nchunk2\n'


def test_http10_keep_alive(basic):
    out = raw(basic, b'GET / HTTP/1.0\r\nconnection: keep-alive\r\n\r\n', read_until_close=False)
    assert b'connection: keep-alive' in out
    assert out.endswith(b'Hello, world!')


def test_big_response(basic):
    with httpx.stream('GET', basic + '/big?20000000', timeout=30) as r:
        total = sum(len(c) for c in r.iter_bytes())
    assert total == 20_000_000


def test_keep_alive_reuse(basic):
    with httpx.Client() as c:
        for _ in range(20):
            assert c.get(basic + '/').text == 'Hello, world!'
        assert c.post(basic + '/echo', content=b'z' * 10).content == b'z' * 10


def test_pipelining(basic):
    req = b'GET / HTTP/1.1\r\nhost: x\r\n\r\n'
    out = raw(basic, req * 3 + b'GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
    assert out.count(b'HTTP/1.1 200 OK') == 4
    assert out.count(b'Hello, world!') == 4


def test_head(basic):
    out = raw(basic, b'HEAD / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
    assert out.startswith(b'HTTP/1.1 200 OK')
    assert out.endswith(b'\r\n\r\n')


def test_no_content(basic):
    r = httpx.get(basic + '/nocontent')
    assert r.status_code == 204
    assert r.content == b''
    assert 'content-length' not in r.headers
    assert 'transfer-encoding' not in r.headers


def test_app_headers(basic):
    r = httpx.get(basic + '/headers')
    assert r.headers['x-one'] == '1'
    assert r.headers['x-two'] == '2'
    assert r.headers.get_list('server') == ['custom']


def test_app_error(basic):
    r = httpx.get(basic + '/error')
    assert r.status_code == 500
    assert httpx.get(basic + '/').status_code == 200


def test_no_response(basic):
    assert httpx.get(basic + '/noresponse').status_code == 500


def test_bad_send(basic):
    assert httpx.get(basic + '/bad-send').status_code == 500


def test_died_halfway(basic):
    with pytest.raises(httpx.RemoteProtocolError):
        httpx.get(basic + '/halfway')
    assert httpx.get(basic + '/').status_code == 200


def test_expect_continue(basic):
    u = urlparse(basic)
    with socket.create_connection((u.hostname, u.port), timeout=5) as s:
        s.sendall(b'POST /echo HTTP/1.1\r\nhost: x\r\ncontent-length: 5\r\nexpect: 100-continue\r\n\r\n')
        assert s.recv(1024) == b'HTTP/1.1 100 Continue\r\n\r\n'
        s.sendall(b'hello')
        out = b''
        while not out.endswith(b'hello'):
            out += s.recv(1024)
        assert out.startswith(b'HTTP/1.1 200 OK')


def test_bad_request(basic):
    assert raw(basic, b'NOT A REQUEST\r\n\r\n').startswith(b'HTTP/1.1 400')
    assert raw(basic, b'GET / HTTP/1.1\r\ncontent-length: 1\r\ntransfer-encoding: chunked\r\n\r\n').startswith(
        b'HTTP/1.1 400')


def test_head_too_large(basic):
    out = raw(basic, b'GET / HTTP/1.1\r\nx-big: ' + b'a' * 70_000 + b'\r\n\r\n')
    assert out.startswith(b'HTTP/1.1 431')


def test_disconnect_delivered(basic):
    u = urlparse(basic)
    before = int(httpx.get(basic + '/disconnects').text)
    with socket.create_connection((u.hostname, u.port)) as s:
        s.sendall(b'GET /wait-disconnect HTTP/1.1\r\nhost: x\r\n\r\n')
        time.sleep(0.1)
    for _ in range(50):
        if int(httpx.get(basic + '/disconnects').text) == before + 1:
            break
        time.sleep(0.05)
    assert int(httpx.get(basic + '/disconnects').text) == before + 1


def test_outgoing_socket_on_same_loop(basic):
    r = httpx.get(basic + '/proxy')
    assert r.text == 'Hello, world!'


def test_to_thread(basic):
    assert httpx.get(basic + '/thread').text == 'threaded'


def test_asyncio_sleep(basic):
    t = time.perf_counter()
    assert httpx.get(basic + '/sleep?0.2').text == 'slept'
    assert 0.19 < time.perf_counter() - t < 1.0


def test_concurrency(basic):
    import concurrent.futures

    def one(i):
        with httpx.Client() as c:
            return [c.get(basic + '/sleep?0.05').text for _ in range(5)]

    with concurrent.futures.ThreadPoolExecutor(32) as ex:
        results = list(ex.map(one, range(32)))
    assert all(r == ['slept'] * 5 for r in results)
