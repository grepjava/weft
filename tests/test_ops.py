"""Server-side request handling: proxies, request IDs, trace context, queue
time, health checks, connection limits, the access log and drain delay."""

import json
import re
import socket
import threading
import time
from urllib.parse import urlparse

import httpx
import pytest

from tests.conftest import serve_thread
from tests.test_http import raw

TRACE = '4bf92f3577b34da6a3ce929d0e0e4736'
SPAN = '00f067aa0ba902b7'


def scope(url, **kw):
    return httpx.get(url + '/scope', **kw).json()


def headers_of(s):
    return {k: v for k, v in s['headers']}


@pytest.fixture(scope='module')
def ops():
    from tests.apps.basic import app

    with serve_thread(app, forwarded_allow_ips='127.0.0.1', request_id=True, request_start_header=True,
                      health_check_path='/healthz') as url:
        yield url


def test_forwarded_headers_from_trusted_proxy(ops):
    s = scope(ops, headers={'X-Forwarded-For': '203.0.113.7, 10.0.0.1', 'X-Forwarded-Proto': 'https'})
    assert s['client'] == ['10.0.0.1', 0]
    assert s['scheme'] == 'https'


def test_forwarded_chain_skips_trusted_hops(ops):
    s = scope(ops, headers={'X-Forwarded-For': '203.0.113.7, 127.0.0.1'})
    assert s['client'] == ['203.0.113.7', 0]


def test_rfc7239_forwarded(ops):
    s = scope(ops, headers={'Forwarded': 'for="[2001:db8::1]:4711";proto=https'})
    assert s['client'] == ['2001:db8::1', 0]
    assert s['scheme'] == 'https'


def test_forwarded_ignored_from_untrusted_peer():
    from tests.apps.basic import app

    with serve_thread(app, forwarded_allow_ips='10.0.0.0/8') as url:
        s = scope(url, headers={'X-Forwarded-For': '203.0.113.7', 'X-Forwarded-Proto': 'https'})
        assert s['client'][0] == '127.0.0.1'
        assert s['scheme'] == 'http'


def test_request_id_generated(ops):
    r = httpx.get(ops + '/scope')
    rid = r.headers['x-request-id']
    assert re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', rid)
    assert headers_of(r.json())['x-request-id'] == rid
    assert httpx.get(ops + '/').headers['x-request-id'] != rid


def test_request_id_from_trusted_proxy_kept(ops):
    r = httpx.get(ops + '/scope', headers={'X-Request-ID': 'abc-123'})
    assert r.headers['x-request-id'] == 'abc-123'
    assert [v for k, v in r.json()['headers'] if k == 'x-request-id'] == ['abc-123']


def test_request_id_invalid_replaced(ops):
    r = httpx.get(ops + '/scope', headers={'X-Request-ID': 'has space'})
    assert r.headers['x-request-id'] != 'has space'
    assert [v for k, v in r.json()['headers'] if k == 'x-request-id'] == [r.headers['x-request-id']]


def test_request_id_from_client_replaced():
    from tests.apps.basic import app

    with serve_thread(app, request_id=True) as url:
        r = httpx.get(url + '/scope', headers={'X-Request-ID': 'abc-123'})
        assert r.headers['x-request-id'] != 'abc-123'


def test_request_start(ops):
    before = time.time() * 1e6
    v = headers_of(scope(ops))['x-request-start']
    assert v.startswith('t=')
    assert abs(int(v[2:]) - before) < 5e6
    assert headers_of(scope(ops, headers={'X-Request-Start': 't=1'}))['x-request-start'] == 't=1'


def test_health_check(ops):
    r = httpx.get(ops + '/healthz?x=1')
    assert r.status_code == 200
    assert r.content == b''
    assert 'x-request-id' not in r.headers
    assert httpx.head(ops + '/healthz').status_code == 200
    # POST, and any other path, is still the application's.
    assert 'x-request-id' in httpx.post(ops + '/healthz').headers
    out = raw(ops, b'GET /healthz HTTP/1.1\r\nhost: x\r\n\r\nGET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
    assert out.count(b'HTTP/1.1 200') == 2


def test_wsgi_environ():
    from tests.apps.wsgi_app import app

    with serve_thread(app, forwarded_allow_ips='*', request_id=True) as url:
        r = httpx.get(url + '/environ', headers={'X-Forwarded-For': '198.51.100.2', 'X-Forwarded-Proto': 'https'})
        e = r.json()
        assert e['REMOTE_ADDR'] == '198.51.100.2'
        assert e['wsgi.url_scheme'] == 'https'
        assert e['HTTP_X_REQUEST_ID'] == r.headers['x-request-id']


def test_max_connections():
    from tests.apps.basic import app

    with serve_thread(app, max_connections=2) as url:
        u = urlparse(url)
        held = [socket.create_connection((u.hostname, u.port)) for _ in range(2)]
        try:
            time.sleep(0.2)
            out = raw(url, b'GET / HTTP/1.1\r\nhost: x\r\n\r\n')
            assert out.startswith(b'HTTP/1.1 503')
        finally:
            for s in held:
                s.close()
        time.sleep(0.2)
        assert httpx.get(url + '/').status_code == 200


def read_log(capfd, n, timeout=3.0):
    lines = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        lines += [ln for ln in capfd.readouterr().err.splitlines() if 'pid=' in ln or ln.startswith('{')]
        if len(lines) >= n:
            break
        time.sleep(0.05)
    return lines


def test_access_log_text(capfd):
    from tests.apps.basic import app

    with serve_thread(app, access_log=True, log_level='info', request_id=True, trace_context=True) as url:
        capfd.readouterr()
        r = httpx.get(url + '/stream?2', headers={'traceparent': f'00-{TRACE}-{SPAN}-01'})
        httpx.get(url + '/missing-page', headers={'traceparent': f'00-{"0" * 32}-{SPAN}-01'})
        lines = read_log(capfd, 2)
    assert len(lines) == 2, lines
    m = re.fullmatch(r'\[info\]  pid=\d+ GET /stream\?2 200 \d+us id=(\S+) trace=(\w+) span=(\w+)', lines[0])
    assert m, lines[0]
    assert m.group(1) == r.headers['x-request-id']
    assert (m.group(2), m.group(3)) == (TRACE, SPAN)
    assert 'trace=' not in lines[1]


def test_access_log_json(capfd):
    from tests.apps.wsgi_app import app

    with serve_thread(app, access_log_format='json', log_level='info') as url:
        capfd.readouterr()
        httpx.get(url + '/a"b\\c')
        httpx.get(url + '/error')
        lines = read_log(capfd, 2)
    first, second = (json.loads(ln) for ln in lines)
    assert first['method'] == 'GET'
    assert first['target'] == '/a%22b\\c'
    assert first['status'] == 404
    assert first['proto'] == 'HTTP/1.1'
    assert first['level'] == 'info' and isinstance(first['pid'], int)
    assert isinstance(first['duration_us'], int)
    assert second['status'] == 500


def test_access_log_off_at_warning(capfd):
    from tests.apps.basic import app

    with serve_thread(app, access_log=True) as url:
        capfd.readouterr()
        httpx.get(url + '/')
        assert read_log(capfd, 1, timeout=0.5) == []


def test_drain_delay():
    from tests.apps.basic import app
    from weft import worker
    from weft.config import Config
    from weft.server import bind

    sock = bind('127.0.0.1', 0, 128)
    url = f'http://127.0.0.1:{sock.getsockname()[1]}'
    config = Config(app=app, log_level='warning', drain_delay=1.0, health_check_path='/healthz')
    stop = threading.Event()
    t = threading.Thread(target=worker.run, args=(config, sock, stop), daemon=True)
    t.start()
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                assert httpx.get(url + '/healthz').status_code == 200
                break
            except httpx.ConnectError:
                assert time.monotonic() < deadline
                time.sleep(0.05)
        stop.set()
        time.sleep(0.3)
        assert httpx.get(url + '/healthz').status_code == 503
        r = httpx.get(url + '/')
        assert r.status_code == 200
        assert r.headers['connection'] == 'close'
        t.join(5)
        assert not t.is_alive()
    finally:
        stop.set()
        sock.close()


def test_rate_limit_429():
    from tests.apps.basic import app

    with serve_thread(app, rate_limit='1/s', rate_limit_burst=1, health_check_path='/healthz',
                      forwarded_allow_ips='*') as url:
        h = {'X-Forwarded-For': '198.51.100.1'}
        assert httpx.get(url + '/', headers=h).status_code == 200
        r = httpx.get(url + '/', headers=h)
        assert r.status_code == 429
        assert r.headers['retry-after'].isdigit()
        assert int(r.headers['retry-after']) >= 1
        assert r.text == 'Too Many Requests\n'
        # The probe is never refused.
        assert httpx.get(url + '/healthz', headers=h).status_code == 200


def test_rate_limit_keys_by_forwarded_client():
    from tests.apps.basic import app

    with serve_thread(app, rate_limit='1/s', rate_limit_burst=1, forwarded_allow_ips='127.0.0.1') as url:
        assert httpx.get(url + '/', headers={'X-Forwarded-For': '203.0.113.1'}).status_code == 200
        assert httpx.get(url + '/', headers={'X-Forwarded-For': '203.0.113.2'}).status_code == 200
        assert httpx.get(url + '/', headers={'X-Forwarded-For': '203.0.113.1'}).status_code == 429


def test_rate_limit_keep_alive():
    from tests.apps.basic import app

    with serve_thread(app, rate_limit='1/s', rate_limit_burst=1, forwarded_allow_ips='*') as url:
        h = b'x-forwarded-for: 198.51.100.9\r\n'
        out = raw(url, b'GET / HTTP/1.1\r\nhost: x\r\n' + h + b'\r\nGET / HTTP/1.1\r\nhost: x\r\n' + h
                       + b'connection: close\r\n\r\n')
        assert out.count(b'HTTP/1.1 200') == 1
        assert b'429 Too Many Requests' in out
