"""Interleaved A/B of Weft builds on the-benchmarker apps.

Each arm is a label and a Python interpreter with a Weft build installed.
Every round runs every arm once per scenario, in an order that alternates
between rounds, so the arms share one period (see docs on period confounds).

    python bench/ab.py --arm base=~/venvs/base/bin/python --arm exp=~/venvs/exp/bin/python \\
        --scenarios wsgi,asgi,fastapi --rounds 5 --duration 5

Linux only (taskset). Reports the median of each metric per arm and scenario,
and the median of the per-round ratios against the first arm. At saturation
compare req/s; with --rate, the server's CPU per request, which is steadier.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psutil

ROOT = Path(__file__).resolve().parent.parent
APPS = ROOT / 'bench' / 'apps'
SCENARIOS = {
    'wsgi': ('tb_wsgi:application', 'wsgi', '/'),
    'asgi': ('tb_asgi:app', 'asgi', '/'),
    'fastapi': ('tb_fastapi:app', 'asgi', '/'),
    'wsgi-user': ('tb_wsgi:application', 'wsgi', '/user/42'),
    'asgi-user': ('tb_asgi:app', 'asgi', '/user/42'),
}


def wait_port(port: int, proc: subprocess.Popen) -> None:
    for _ in range(200):
        if proc.poll() is not None:
            raise RuntimeError(f'server exited with {proc.returncode}')
        try:
            with socket.create_connection(('127.0.0.1', port), 0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError('server did not come up')


def cpu_seconds(pid: int) -> float:
    total = 0.0
    for p in [psutil.Process(pid), *psutil.Process(pid).children(recursive=True)]:
        try:
            t = p.cpu_times()
            total += t.user + t.system
        except psutil.Error:
            pass
    return total


def trial(args, label: str, python: str, scenario: str) -> dict:
    app, protocol, path = SCENARIOS[scenario]
    port = args.port
    cmd = ['taskset', '-c', args.server_cpus, python, '-m', 'weft', app, '--protocol', protocol,
           '--host', '127.0.0.1', '--port', str(port), '--workers', '1', '--log-level', 'error']
    env = {**os.environ, 'PYTHONPATH': str(APPS)}
    proc = subprocess.Popen(cmd, cwd=APPS, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_port(port, proc)
        oha = ['taskset', '-c', args.load_cpus, args.oha, '--no-tui', '--output-format', 'json',
               '-c', str(args.connections)]
        if args.rate:
            oha += ['-q', str(args.rate), '--latency-correction']
        url = f'http://127.0.0.1:{port}{path}'
        subprocess.run([*oha, '-z', f'{args.warmup}s', url], stdout=subprocess.DEVNULL, check=True)
        before = cpu_seconds(proc.pid)
        raw = subprocess.check_output([*oha, '-z', f'{args.duration}s', url], text=True)
        cpu = cpu_seconds(proc.pid) - before
        data = json.loads(raw)
        s = data['summary']
        ok = sum(n for code, n in data.get('statusCodeDistribution', {}).items() if code == '200')
        return {
            'arm': label, 'scenario': scenario,
            'rps': s['requestsPerSec'],
            'p50_ms': data['latencyPercentiles']['p50'] * 1000,
            'p99_ms': data['latencyPercentiles']['p99'] * 1000,
            'cpu_us_per_req': cpu / max(ok, 1) * 1e6,
            'non200': sum(data.get('statusCodeDistribution', {}).values()) - ok,
        }
    finally:
        proc.terminate()
        try:
            proc.wait(5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--arm', action='append', required=True, help='label=/path/to/python')
    ap.add_argument('--scenarios', default='wsgi,asgi')
    ap.add_argument('--rounds', type=int, default=5)
    ap.add_argument('--duration', type=int, default=5)
    ap.add_argument('--warmup', type=int, default=1)
    ap.add_argument('--connections', type=int, default=256)
    ap.add_argument('--rate', type=int, default=None,
                    help='fixed request rate instead of saturation; compare us/req then')
    ap.add_argument('--server-cpus', default='0')
    ap.add_argument('--load-cpus', default='2,3')
    ap.add_argument('--port', type=int, default=18791)
    ap.add_argument('--oha', default=os.path.expanduser('~/.cargo/bin/oha'))
    ap.add_argument('--out', default=None, help='write every trial as JSON here')
    args = ap.parse_args()
    arms = [(a.split('=', 1)[0], os.path.expanduser(a.split('=', 1)[1])) for a in args.arm]
    scenarios = args.scenarios.split(',')

    rows = []
    for r in range(args.rounds):
        for scenario in scenarios:
            order = arms if r % 2 == 0 else arms[::-1]
            for label, python in order:
                row = trial(args, label, python, scenario)
                row['round'] = r
                rows.append(row)
                print(json.dumps(row), flush=True)
    if args.out:
        Path(args.out).write_text(json.dumps(rows, indent=1))

    base = arms[0][0]
    print(f'\n{"scenario":10} {"arm":10} {"req/s":>9} {"p50 ms":>7} {"p99 ms":>7} {"us/req":>7}  vs {base} (median of per-round ratios)')
    for scenario in scenarios:
        for label, _ in arms:
            mine = [x for x in rows if x['arm'] == label and x['scenario'] == scenario]
            med = {k: statistics.median(x[k] for x in mine) for k in ('rps', 'p50_ms', 'p99_ms', 'cpu_us_per_req')}
            ratio = ''
            key = 'cpu_us_per_req' if args.rate else 'rps'
            if label != base:
                ratios = []
                for x in mine:
                    b = [y for y in rows if y['arm'] == base and y['scenario'] == scenario and y['round'] == x['round']]
                    if b:
                        ratios.append(x[key] / b[0][key])
                spread = f'{min(ratios):.3f}..{max(ratios):.3f}' if ratios else ''
                name = 'us/req' if args.rate else 'rps'
                ratio = f'  {name} x{statistics.median(ratios):.3f} [{spread}]' if ratios else ''
            print(f'{scenario:10} {label:10} {med["rps"]:9.0f} {med["p50_ms"]:7.2f} {med["p99_ms"]:7.2f} '
                  f'{med["cpu_us_per_req"]:7.1f}{ratio}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
