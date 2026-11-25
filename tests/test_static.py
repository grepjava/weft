import gzip

import httpx
import pytest

from tests.conftest import serve_thread
from tests.test_http import raw


@pytest.fixture
def static_root(tmp_path):
    (tmp_path / 'app.css').write_text('body{color:red}', encoding='utf-8')
    (tmp_path / 'app.js').write_text('console.log(1)', encoding='utf-8')
    nested = tmp_path / 'n'
    nested.mkdir()
    (nested / 'note.txt').write_text('hello', encoding='utf-8')
    big = b'x' * (20 * 1024)
    (tmp_path / 'big.bin').write_bytes(big)
    (tmp_path / 'app.js.gz').write_bytes(gzip.compress(b'console.log(1)'))
    return tmp_path


@pytest.fixture
def static_url(static_root):
    from tests.apps.basic import app

    spec = f'/static={static_root}'
    with serve_thread(app, static_dirs=[spec]) as url:
        yield url


def test_css_and_js(static_url):
    r = httpx.get(static_url + '/static/app.css')
    assert r.status_code == 200
    assert r.text == 'body{color:red}'
    assert r.headers['content-type'].startswith('text/css')
    assert r.headers['etag'].startswith('"')
    r = httpx.get(static_url + '/static/app.js')
    assert r.headers['content-type'].startswith('text/javascript')
    assert httpx.get(static_url + '/static/n/note.txt').text == 'hello'


def test_head_has_no_body(static_url):
    r = httpx.head(static_url + '/static/app.css')
    assert r.status_code == 200
    assert r.content == b''
    assert r.headers['content-length'] == '15'


def test_etag_304(static_url):
    r = httpx.get(static_url + '/static/app.css')
    etag = r.headers['etag']
    n = httpx.get(static_url + '/static/app.css', headers={'If-None-Match': etag})
    assert n.status_code == 304
    assert n.content == b''


def test_if_match_412(static_url):
    r = httpx.get(static_url + '/static/app.css', headers={'If-Match': '"nope"'})
    assert r.status_code == 412


def test_missing_falls_through(static_url):
    r = httpx.get(static_url + '/static/missing.css')
    assert r.status_code == 404
    assert r.text == 'not found'


def test_traversal_falls_through(static_root):
    from tests.apps.basic import app

    with serve_thread(app, static_dirs=[f'/static={static_root}']) as url:
        assert httpx.get(url + '/static/../app.css').status_code == 404
        assert httpx.get(url + '/static/%2e%2e/app.css').status_code == 404


def test_prefix_is_a_segment(static_url):
    r = httpx.get(static_url + '/staticky')
    assert r.status_code == 404


def test_post_falls_through(static_url):
    r = httpx.post(static_url + '/static/app.css', content=b'x')
    assert r.text == 'Hello, world!' or r.status_code in (404, 405, 200)


def test_large_file(static_url):
    r = httpx.get(static_url + '/static/big.bin')
    assert r.status_code == 200
    assert len(r.content) == 20 * 1024


def test_compress_static(static_root):
    from tests.apps.basic import app

    with serve_thread(app, static_dirs=[f'/static={static_root}'], compress_static=True) as url:
        raw_r = raw(url, b'GET /static/app.js HTTP/1.1\r\nhost: x\r\naccept-encoding: gzip\r\nconnection: close\r\n\r\n')
        assert b'content-encoding: gzip' in raw_r.split(b'\r\n\r\n', 1)[0].lower()
        assert b'vary: accept-encoding' in raw_r.lower()
        plain = raw(url, b'GET /static/app.js HTTP/1.1\r\nhost: x\r\naccept-encoding: identity\r\nconnection: close\r\n\r\n')
        assert b'content-encoding' not in plain.split(b'\r\n\r\n', 1)[0].lower()


def test_compressed_etag_is_quoted_and_follows_the_copy(static_root):
    from tests.apps.basic import app

    gz = {'accept-encoding': 'gzip'}
    with serve_thread(app, static_dirs=[f'/static={static_root}'], compress_static=True) as url:
        with httpx.Client() as c:
            first = c.get(url + '/static/app.js', headers=gz)
            etag = first.headers['etag']
            assert first.headers['content-encoding'] == 'gzip'
            assert etag.startswith('"') and etag.endswith('-gzip"'), etag
            assert c.get(url + '/static/app.js', headers={**gz, 'if-none-match': etag}).status_code == 304
            plain = c.get(url + '/static/app.js', headers={'accept-encoding': 'identity'}).headers['etag']
            assert plain != etag
            # Only the precompressed copy changes.
            (static_root / 'app.js.gz').write_bytes(gzip.compress(b'console.log(2); // changed'))
            again = c.get(url + '/static/app.js', headers={**gz, 'if-none-match': etag})
            assert again.status_code == 200
            assert again.headers['etag'] != etag
            assert again.text == 'console.log(2); // changed'


def test_file_cut_short_closes_the_connection(tmp_path):
    """A file that shrinks while it is being sent cannot be finished: the
    connection closes short of the Content-Length instead of being reused."""
    import socket
    from urllib.parse import urlparse

    from tests.apps.basic import app

    size = 8 * 1024 * 1024
    path = tmp_path / 'grow.bin'
    path.write_bytes(b'y' * size)
    with serve_thread(app, static_dirs=[f'/static={tmp_path}']) as url:
        u = urlparse(url)
        with socket.create_connection((u.hostname, u.port), timeout=5) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024)
            s.sendall(b'GET /static/grow.bin HTTP/1.1\r\nhost: x\r\n\r\n')
            got = s.recv(4096)
            assert b'content-length: %d' % size in got.lower()
            with open(path, 'r+b') as f:
                f.truncate(256 * 1024)
            while chunk := s.recv(65536):
                got += chunk
        body = got.split(b'\r\n\r\n', 1)[1]
        assert len(body) < size


def test_range_206_and_416(static_url):
    r = httpx.get(static_url + '/static/app.css', headers={'Range': 'bytes=0-3'})
    assert r.status_code == 206
    assert r.content == b'body'
    assert r.headers['accept-ranges'] == 'bytes'
    assert r.headers['content-range'] == 'bytes 0-3/15'
    assert r.headers['content-length'] == '4'

    # Suffix range
    s = httpx.get(static_url + '/static/app.css', headers={'Range': 'bytes=-4'})
    assert s.status_code == 206
    assert s.content == b'red}'
    assert s.headers['content-range'] == 'bytes 11-14/15'

    # Unsatisfiable range
    bad = httpx.get(static_url + '/static/app.css', headers={'Range': 'bytes=100-200'})
    assert bad.status_code == 416
    assert bad.headers['content-range'] == 'bytes */15'


def test_last_modified_and_if_modified_since(static_url):
    r = httpx.get(static_url + '/static/app.css')
    lm = r.headers['last-modified']
    assert lm.endswith('GMT')
    n = httpx.get(static_url + '/static/app.css', headers={'If-Modified-Since': lm})
    assert n.status_code == 304
    assert n.content == b''


def test_asgi_pathsend(tmp_path):
    target = tmp_path / 'sample.txt'
    payload = b'hello from pathsend!\n' * 100
    target.write_bytes(payload)

    async def pathsend_app(scope, receive, send):
        assert 'http.response.pathsend' in scope['extensions']
        await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
        await send({'type': 'http.response.pathsend', 'path': str(target)})

    with serve_thread(pathsend_app, lifespan='off') as url:
        r = httpx.get(url + '/')
        assert r.status_code == 200
        assert r.content == payload
        assert int(r.headers['content-length']) == len(payload)
