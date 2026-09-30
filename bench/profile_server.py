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


def interpreter_pid(pid: int) -> int:
    """The process running the server's Python. On Windows a venv's
    python.exe is a launcher that runs the base interpreter as its child."""
    if os.name != 'nt':
        return pid
    import psutil

    try:
        proc = psutil.Process(pid)
        if Path(proc.exe()).parent.parent.joinpath('pyvenv.cfg').exists():
            kids = [c for c in proc.children() if Path(c.exe()).name.lower().startswith('python')]
            if len(kids) == 1:
                return kids[0].pid
    except psutil.Error:
        pass
    return pid


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
    ap.add_argument('--python-only', action='store_true',
                    help='sample without pausing the process (py-spy --nonblocking), with no native frames')
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
        # Load covers the warmup and both captures, which run one after the
        # other, with a margin for py-spy attaching and writing out.
        load_s = 2 + 2 * args.duration + 6
        oha = ['oha', '--no-tui', '--output-format', 'json', '-z', f'{load_s}s',
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
        target_pid = interpreter_pid(server.pid)
        written = []
        for fmt, suffix in (('speedscope', '.speedscope.json'), ('raw', '.folded')):
            target = base.with_suffix(suffix)
            # py-spy refuses --native with --nonblocking (native frames need
            # the process paused while it is sampled), and on Windows with
            # --subprocesses. One worker runs in the server process itself.
            mode = ['--nonblocking'] if args.python_only else ['--native']
            if args.workers > 1:
                mode.append('--subprocesses')
            spy = [
                'py-spy', 'record', '--pid', str(target_pid), *mode,
                '--rate', str(args.rate_hz), '--duration', str(args.duration),
                '--format', fmt, '--output', str(target),
            ]
            print('py-spy:', ' '.join(spy), flush=True)
            if subprocess.run(spy, check=False).returncode == 0 and target.exists():
                written.append(target)
            else:
                print(f'py-spy failed to write {target}', file=sys.stderr)
        if load.poll() is not None:
            print('warning: the load ended before the captures did', file=sys.stderr)

        out, _ = load.communicate(timeout=load_s + 60)
        try:
            summary = json.loads(out).get('summary', {})
            print(f"load: {summary.get('requestsPerSec', 0):.0f} req/s over {load_s}s", flush=True)
        except Exception:  # noqa: BLE001
            pass
        if written:
            print('wrote', ' and '.join(str(w) for w in written))
        if len(written) < 2:
            return 1
    finally:
        server.send_signal(signal.SIGTERM if os.name != 'nt' else signal.SIGTERM)
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
    return 0


if __name__ == '__main__':
    sys.exit(main())
