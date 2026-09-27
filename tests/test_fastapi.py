"""FastAPI end to end: the request shapes FastAPI and Starlette produce."""

import concurrent.futures

import httpx
import pytest

from tests.conftest import serve_thread


@pytest.fixture(scope='module')
def api():
    from tests.apps.fastapi_app import app

    with serve_thread(app) as url:
        yield url


def test_scope_and_lifespan(api):
    r = httpx.get(api + '/info?a=1&b=two', headers={'X-Custom': 'yes'})
    assert r.status_code == 200
    d = r.json()
    assert d['method'] == 'GET'
    assert d['path'] == '/info'
    assert d['query'] == {'a': '1', 'b': 'two'}
    assert d['headers']['x-custom'] == 'yes'
    assert d['client'][0] == '127.0.0.1'
    assert d['http_version'] == '1.1'
    assert d['scheme'] == 'http'
    assert d['lifespan_state'] == 'ready'
    assert d['app_started'] is True
    assert 'x-process-time' in r.headers
    assert int(r.headers['content-length']) == len(r.content)


def test_params_and_validation(api):
    assert httpx.get(api + '/').json() == {'hello': 'world'}
    assert httpx.get(api + '/users/42?verbose=true').json() == {'id': 42, 'name': 'user42 (verbose)', 'active': True}
    assert len(httpx.get(api + '/users?n=5').json()) == 5
    assert httpx.get(api + '/users/0').status_code == 422
    assert httpx.get(api + '/users/abc').status_code == 422
    assert httpx.get(api + '/users?n=9999').status_code == 422
    assert httpx.get(api + '/nope').status_code == 404
    assert httpx.post(api + '/users/1', json={}).status_code == 405


def test_json_body(api):
    item = {'name': 'widget', 'price': 9.5, 'tags': ['a', 'b']}
    r = httpx.post(api + '/items', json=item)
    assert r.status_code == 201
    assert r.json() == item
    assert httpx.post(api + '/items', json={'name': 'x'}).status_code == 422


def test_dependencies(api):
    assert httpx.get(api + '/dep', headers={'Authorization': 'Bearer ok'}).json() == {'user': 'Bearer ok', 'limit': 10}
    assert httpx.get(api + '/dep', headers={'Authorization': 'Bearer deny'}).status_code == 403


def test_sync_endpoint(api):
    assert httpx.get(api + '/sync').json() == {'sync': True, 'path': '/sync'}


def test_streaming(api):
    r = httpx.get(api + '/stream/100/1000')
    assert len(r.content) == 100_000
    assert r.headers['transfer-encoding'] == 'chunked'


def test_echo(api):
    body = b'q' * 3_000_000
    assert httpx.post(api + '/echo', content=body, timeout=30).content == body
    assert httpx.post(api + '/echo/stream', content=body, timeout=30).json() == {'size': len(body)}


def test_cors_preflight(api):
    r = httpx.options(api + '/items', headers={
        'Origin': 'https://example.com',
        'Access-Control-Request-Method': 'POST',
    })
    assert r.status_code == 200
    assert r.headers['access-control-allow-origin'] == 'https://example.com'


def test_error(api):
    assert httpx.get(api + '/boom').status_code == 500


def test_is_disconnected(api):
    assert httpx.get(api + '/sleep/10').json() == {'slept': 10, 'disconnected': False}


def test_concurrent_clients(api):
    def one(i):
        with httpx.Client() as c:
            return [c.get(api + f'/users/{i + 1}').json()['id'] for _ in range(10)]

    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        results = list(ex.map(one, range(16)))
    assert results == [[i + 1] * 10 for i in range(16)]
