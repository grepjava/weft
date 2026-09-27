import httpx

from tests.apps import cache_app
from tests.conftest import serve_thread
from tests.test_http import raw


def test_fresh_is_cached():
    cache_app.HITS.clear()
    with serve_thread(cache_app.app, cache_size=8) as url:
        a = httpx.get(url + '/fresh')
        b = httpx.get(url + '/fresh')
        assert a.text == b.text
        assert a.text.startswith('fresh-1-')
        assert cache_app.HITS['/fresh'] == 1
        assert 'weft' in b.headers.get('cache-status', '')
        assert 'hit' in b.headers.get('cache-status', '')
        assert 'age' in b.headers


def test_private_not_cached():
    cache_app.HITS.clear()
    with serve_thread(cache_app.app, cache_size=8) as url:
        assert httpx.get(url + '/private').text == 'priv-1'
        assert httpx.get(url + '/private').text == 'priv-2'


def test_head_uses_get_copy():
    cache_app.HITS.clear()
    with serve_thread(cache_app.app, cache_size=8) as url:
        httpx.get(url + '/fresh')
        h = httpx.head(url + '/fresh')
        assert h.status_code == 200
        assert h.content == b''
        assert cache_app.HITS['/fresh'] == 1


def test_post_invalidates():
    cache_app.HITS.clear()
    with serve_thread(cache_app.app, cache_size=8) as url:
        first = httpx.get(url + '/item')
        assert httpx.get(url + '/item').text == first.text
        httpx.post(url + '/item')
        second = httpx.get(url + '/item')
        assert second.text != first.text
        assert cache_app.HITS['/item'] == 3


def test_404_is_cacheable():
    cache_app.HITS.clear()
    with serve_thread(cache_app.app, cache_size=8) as url:
        assert httpx.get(url + '/missing').status_code == 404
        assert httpx.get(url + '/missing').status_code == 404
        assert cache_app.HITS['/missing'] == 1


def test_compressed_from_one_copy():
    from urllib.parse import urlparse

    cache_app.HITS.clear()
    with serve_thread(cache_app.app, cache_size=8, compress=True) as url:
        httpx.get(url + '/fresh')
        u = urlparse(url)
        host = f'{u.hostname}:{u.port}'.encode()
        resp = raw(url, b'GET /fresh HTTP/1.1\r\nhost: ' + host + b'\r\naccept-encoding: gzip\r\nconnection: close\r\n\r\n')
        head = resp.split(b'\r\n\r\n', 1)[0].lower()
        assert b'cache-status: weft; hit' in head
        assert b'content-encoding: gzip' in head
        assert cache_app.HITS['/fresh'] == 1
