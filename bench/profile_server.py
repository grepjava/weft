"""Profile a running Weft worker under load.

Starts one server configuration, drives it with `oha`, and samples the whole
process tree with `py-spy` (native frames included, so Rust and Python appear
in the same profile). Writes a folded stack file plus a speedscope JSON.

    python bench/profile_server.py --label opt --scenario hello --duration 20

Requires py-spy (`uv pip install py-spy`) and, on Linux, either root or
`kernel.yama.ptrace_scope=0`.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
APPS = ROOT / 'bench' / 'apps'
OUT = ROOT / 'bench' / 'results' / 'profiles'

PATHS = {
    'hello': ('GET', '/', []),
    'path-param': ('GET', '/users/42', []),
    'json-list-50': ('GET', '/users?n=50', []),
    'post-items': (
        'POST',
        '/items',
        ['-H', 'content-type: application/json', '-d', '{"name":"a","price":1.5,"tags":["x","y"]}'],
    ),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--label', required=True)
    ap.add_argument('--scenario', default='hello', choices=list(PATHS))
    ap.add_argument('--app', default='fastapi_app:app')
    ap.add_argument('--workers', type=int, default=1)
    ap.add_argument('--duration', type=int, default=20)
    ap.add_argument('--concurrency', type=int, default=64)
    ap.add_argument('--rate', type=int, default=None)
    ap.add_argument('--port', type=int, default=8399)
    ap.add_argument('--rate-hz', type=int, default=200, help='py-spy sampling rate')
    ap.add_argument('--cpu-affinity', default=None)
    ap.add_argument('--oha-affinity', default=None)
    args = ap.parse_args()

    for tool in ('py-spy', 'oha'):
        if shutil.which(tool) is None:
            print(f'{tool} not found on PATH', file=sys.stderr)
            return 2

    OUT.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env['PYTHONPATH'] = str(APPS)
    cmd = [
        sys.executable, '-m', 'weft', args.app,
        '--host', '127.0.0.1', '--port', str(args.port), '--workers', str(args.workers),
        '--log-level', 'warning',
    ]
    print('server:', ' '.join(cmd), flush=True)
    server = subprocess.Popen(cmd, env=env, cwd=str(APPS))
    try:
        deadline = time.monotonic() + 30
        import socket

        while time.monotonic() < deadline:
            try:
                with socket.create_connection(('127.0.0.1', args.port), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            print('server did not start', file=sys.stderr)
            return 1
        time.sleep(1.0)

        if args.cpu_affinity:
            import psutil

            for p in [psutil.Process(server.pid), *psutil.Process(server.pid).children(recursive=True)]:
                try:
                    p.cpu_affinity([int(c) for c in args.cpu_affinity.split(',')])
                except psutil.Error:
                    pass

        method, path, extra = PATHS[args.scenario]
        url = f'http://127.0.0.1:{args.port}{path}'
        oha = ['oha', '--no-tui', '--output-format', 'json', '-z', f'{args.duration + 4}s',
               '-c', str(args.concurrency), '-m', method, *extra, url]
        if args.rate:
            oha += ['-q', str(args.rate), '--latency-correction']
        load = subprocess.Popen(oha, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if args.oha_affinity:
            import psutil

            try:
                psutil.Process(load.pid).cpu_affinity([int(c) for c in args.oha_affinity.split(',')])
            except psutil.Error:
                pass
        time.sleep(2.0)  # let it reach steady state before sampling

        base = OUT / f'{args.label}-{args.scenario}'
        spy = [
            'py-spy', 'record', '--pid', str(server.pid), '--subprocesses', '--native',
            '--nonblocking', '--rate', str(args.rate_hz), '--duration', str(args.duration),
            '--format', 'speedscope', '--output', str(base.with_suffix('.speedscope.json')),
        ]
        print('py-spy:', ' '.join(spy), flush=True)
        subprocess.run(spy, check=False)
        spy[-3:] = ['--format', 'raw', '--output', str(base.with_suffix('.folded'))]
        subprocess.run(spy, check=False)

        out, _ = load.communicate(timeout=60)
        try:
            summary = json.loads(out).get('summary', {})
            print(f"load: {summary.get('requestsPerSec', 0):.0f} req/s over {args.duration + 4}s", flush=True)
        except Exception:  # noqa: BLE001
            pass
        print('wrote', base.with_suffix('.folded'), 'and', base.with_suffix('.speedscope.json'))
    finally:
        server.send_signal(signal.SIGTERM if os.name != 'nt' else signal.SIGTERM)
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
    return 0


if __name__ == '__main__':
    sys.exit(main())
