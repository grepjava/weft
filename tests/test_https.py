import shutil
import socket
import ssl
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from tests.conftest import ROOT, free_port, serve_process, serve_thread, wait_connect


def _openssl() -> str:
    found = shutil.which('openssl')
    if found:
        return found
    mingw = Path(r'C:\msys64\mingw64\bin\openssl.exe')
    if mingw.is_file():
        return str(mingw)
    pytest.skip('openssl is needed to mint test certificates')


def _cert(dir: Path, cn: str, *sans: str) -> tuple[Path, Path]:
    cert, key = dir / f'{cn}.pem', dir / f'{cn}.key'
    ext = ['-addext', 'subjectAltName=' + ','.join(f'DNS:{n}' for n in (cn, *sans))]
    subprocess.run(
        [_openssl(), 'req', '-x509', '-newkey', 'rsa:2048', '-sha256', '-days', '1', '-nodes',
         '-batch', '-quiet', '-keyout', str(key), '-out', str(cert), '-subj', f'/CN={cn}', *ext],
        check=True, capture_output=True, text=True, timeout=30, stdin=subprocess.DEVNULL,
    )
    return cert, key


def _client():
    return httpx.Client(verify=False, timeout=5.0)


def test_https_get(tmp_path):
    from tests.apps.basic import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)]) as url:
        https = url.replace('http://', 'https://')
        r = _client().get(https + '/')
        assert r.status_code == 200
        assert r.text == 'Hello, world!'
        s = _client().get(https + '/scope').json()
        assert s['scheme'] == 'https'


def test_hsts(tmp_path):
    from tests.apps.basic import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], hsts=31536000) as url:
        r = _client().get(url.replace('http://', 'https://') + '/')
        assert r.headers['strict-transport-security'] == 'max-age=31536000'


def test_hsts_health(tmp_path):
    from tests.apps.basic import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], hsts=60, health_check_path='/ok') as url:
        r = _client().get(url.replace('http://', 'https://') + '/ok')
        assert r.status_code == 200
        assert r.headers['strict-transport-security'] == 'max-age=60'


def test_sni(tmp_path):
    from tests.apps.basic import app

    a_cert, a_key = _cert(tmp_path, 'alpha.test')
    b_cert, b_key = _cert(tmp_path, 'beta.test')
    with serve_thread(
        app,
        tls_certs=[str(a_cert), str(b_cert)],
        tls_keys=[str(a_key), str(b_key)],
    ) as url:
        port = int(url.rsplit(':', 1)[1])
        assert b'alpha.test' in _peer_der(port, 'alpha.test')
        assert b'beta.test' in _peer_der(port, 'beta.test')
        # unknown SNI still gets the first certificate
        assert b'alpha.test' in _peer_der(port, 'other.test')


def test_wsgi_https(tmp_path):
    from tests.apps.wsgi_app import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)]) as url:
        r = _client().get(url.replace('http://', 'https://') + '/')
        assert r.status_code == 200
        assert r.text == 'Hello, world!'
        env = _client().get(url.replace('http://', 'https://') + '/environ').json()
        assert env['wsgi.url_scheme'] == 'https'


def test_redirect_http(tmp_path):
    cert, key = _cert(tmp_path, 'localhost')
    http_port = free_port()
    with serve_process(
        'tests.apps.basic:app',
        '--tls-cert', str(cert),
        '--tls-key', str(key),
        '--redirect-http', str(http_port),
    ) as (url, _proc):
        https_port = int(url.rsplit(':', 1)[1])
        wait_connect(http_port)
        r = httpx.get(f'http://127.0.0.1:{http_port}/scope?x=1', follow_redirects=False, timeout=5.0)
        assert r.status_code == 301
        loc = r.headers['location']
        assert loc.startswith('https://')
        assert f':{https_port}' in loc or https_port == 443
        assert loc.endswith('/scope?x=1')


def test_cli_needs_a_key():
    from weft.config import Config

    with pytest.raises(SystemExit):
        Config(app='x:x', tls_certs=['a.pem']).prepare()
    with pytest.raises(SystemExit):
        Config(app='x:x', hsts=1).prepare()
    with pytest.raises(SystemExit):
        Config(app='x:x', redirect_http=80).prepare()


def _peer_der(port: int, server_name: str) -> bytes:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection(('127.0.0.1', port), timeout=5) as sock:
        with ctx.wrap_socket(sock, server_hostname=server_name) as tls:
            return tls.getpeercert(binary_form=True)


def test_cli_help_lists_tls():
    out = subprocess.check_output([sys.executable, '-m', 'weft', '--help'], cwd=ROOT, text=True)
    assert '--tls-cert' in out
    assert '--tls-key' in out
    assert '--hsts' in out
    assert '--redirect-http' in out
    assert '--metrics-port' in out
    assert '--metrics-host' in out
    assert '--http3' in out
    assert '--quic-port' in out
