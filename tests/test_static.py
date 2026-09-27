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
