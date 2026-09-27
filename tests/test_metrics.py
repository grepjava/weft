import socket
import time
from urllib.parse import urlparse

import httpx

from tests.apps import cache_app
from tests.conftest import free_port, serve_process, serve_thread


def scrape(port, path='/metrics', timeout=10.0):
    s = socket.create_connection(('127.0.0.1', port), timeout=timeout)
    try:
        s.sendall(f'GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n'.encode())
        return read_http(s)
    finally:
        s.close()


def read_http(s):
    buf = b''
    while b'\r\n\r\n' not in buf:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
    head, _, rest = buf.partition(b'\r\n\r\n')
    status_line = head.split(b'\r\n', 1)[0]
    status = int(status_line.split()[1])
    headers = {}
    for line in head.split(b'\r\n')[1:]:
        if b':' in line:
            k, v = line.split(b':', 1)
            headers[k.decode().lower()] = v.strip().decode()
    need = int(headers.get('content-length', '0'))
    body = rest
    while len(body) < need:
        chunk = s.recv(4096)
        if not chunk:
            break
        body += chunk
    return status, headers, body[:need]


def values(body):
    out = {}
    for line in body.decode().splitlines():
        if not line or line.startswith('#'):
            continue
        name, _, value = line.rpartition(' ')
        out[name] = float(value)
    return out


def wait_scrape(port, timeout=15.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            status, headers, body = scrape(port)
            if status == 200 and body.startswith(b'# HELP'):
                return status, headers, body
            last = status
        except OSError as e:
            last = e
        time.sleep(0.05)
    raise TimeoutError(f'metrics port {port} did not answer ({last!r})')


def test_metrics_port_counts_and_is_not_the_app():
    metrics_port = free_port()
    from tests.apps.basic import app

    with serve_thread(app, metrics_port=metrics_port, metrics_host='127.0.0.1') as url:
        wait_scrape(metrics_port)
        for _ in range(7):
            assert httpx.get(url + '/').status_code == 200
        assert httpx.get(url + '/nope').status_code == 404

        status, headers, body = scrape(metrics_port)
        assert status == 200
        assert 'version=0.0.4' in headers.get('content-type', '')
        v = values(body)
        assert v.get('weft_requests_total{status="2xx"}') == 7
        assert v.get('weft_requests_total{status="4xx"}') == 1
        assert v.get('weft_workers') == 1
        assert v.get('weft_connections_accepted_total', 0) >= 1
        assert v.get('weft_request_duration_seconds_count') == 8
        assert v.get('weft_request_duration_seconds_bucket{le="+Inf"}') == 8
        assert 'weft_cache_hits_total' not in v

        _, _, other = scrape(metrics_port, path='/')
        assert other.startswith(b'# HELP')
        assert httpx.get(url + '/metrics').status_code == 404


def test_scrape_arrives_in_pieces_and_half_open_does_not_block():
    metrics_port = free_port()
    from tests.apps.basic import app

    with serve_thread(app, metrics_port=metrics_port, metrics_host='127.0.0.1') as url:
        wait_scrape(metrics_port)
        s = socket.create_connection(('127.0.0.1', metrics_port), timeout=10)
        s.settimeout(10)
        sent_whole = True
        try:
            for byte in b'GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n':
                s.sendall(bytes([byte]))
                time.sleep(0.01)
        except OSError:
            sent_whole = False
        assert sent_whole
        status, _, body = read_http(s)
        s.close()
        assert status == 200
        assert b'weft_requests_total' in body

        stalled = []
        for _ in range(12):
            half = socket.create_connection(('127.0.0.1', metrics_port), timeout=10)
            half.sendall(b'G')
            stalled.append(half)
        began = time.monotonic()
        assert scrape(metrics_port)[0] == 200
        assert httpx.get(url + '/').status_code == 200
        assert time.monotonic() - began < 3.0
        for half in stalled:
            half.close()


def test_without_metrics_port_the_app_still_serves():
    from tests.apps.basic import app

    with serve_thread(app) as url:
        assert httpx.get(url + '/').status_code == 200


def test_two_workers_share_one_scrape():
    metrics_port = free_port()
    with serve_process(
        'tests.apps.basic:app',
        '--workers',
        '2',
        '--metrics-port',
        str(metrics_port),
        '--metrics-host',
        '127.0.0.1',
    ) as (url, _proc):
        wait_scrape(metrics_port)
        pids = {httpx.get(url + '/pid').text for _ in range(20)}
        assert len(pids) >= 2
        _, _, body = scrape(metrics_port)
        v = values(body)
        assert v.get('weft_workers') == 2
        assert v.get('weft_requests_total{status="2xx"}', 0) >= 20
        port = int(urlparse(url).port)
        assert httpx.get(f'http://127.0.0.1:{port}/metrics').status_code == 404


def test_cache_counters_when_cache_is_on():
    metrics_port = free_port()
    cache_app.HITS.clear()
    with serve_thread(cache_app.app, cache_size=8, metrics_port=metrics_port, metrics_host='127.0.0.1') as url:
        wait_scrape(metrics_port)
        httpx.get(url + '/fresh')
        httpx.get(url + '/fresh')
        v = values(scrape(metrics_port)[2])
        assert v.get('weft_cache_misses_total', 0) >= 1
        assert v.get('weft_cache_hits_total', 0) >= 1
        assert v.get('weft_cache_stores_total', 0) >= 1
