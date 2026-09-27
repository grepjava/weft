"""Weft benchmark runner.

Starts one server configuration, waits for readiness, warms it up, runs a set
of `oha` scenarios with repeats while sampling the server process tree's CPU
time and RSS, and writes one JSON document per (scenario, repeat) plus a
`meta.json` describing the exact environment.

Usage (see bench/README.md):

    python bench/run.py --label weft-1w --server weft --workers 1 --app fastapi --scenarios core

Everything is recorded raw; `bench/report.py` aggregates.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import psutil

ROOT = Path(__file__).resolve().parent.parent
APPS = ROOT / 'bench' / 'apps'
RAW = ROOT / 'bench' / 'results' / 'raw'
VENVS = {
    'weft': ROOT / '.venv',
    'weft-ft': ROOT / '.venv-ft',
    'uvicorn': ROOT / '.venv',
    'peregrine': ROOT / '.venv',
}
# WEFT_BENCH_PY_<SERVER> (e.g. WEFT_BENCH_PY_WEFT_FT=/home/me/venvs/py314t/bin/python) overrides the
# interpreter for a server kind; used on Linux/WSL where the venvs live outside the checkout.
UPLOAD_FILE = APPS / 'static' / 'large.bin'
if not UPLOAD_FILE.exists():
    UPLOAD_FILE.parent.mkdir(exist_ok=True)
    UPLOAD_FILE.write_bytes(os.urandom(4 * 1024 * 1024))

APP_SPECS = {
    'fastapi': 'fastapi_app:app',
    'fastapi-mw': 'fastapi_mw:app',
    'asgi-min': 'asgi_min:app',
    'wsgi-min': 'wsgi_min:app',
}

# name -> (method, path, extra oha args, body-file, concurrency override)
SCENARIOS = {
    # tiny responses: server + framework overhead
    'hello': ('GET', '/', [], None, None),
    'plain': ('GET', '/plain', [], None, None),
    'json-validate': ('GET', '/json/validate?q=7&name=abc', [], None, None),
    # typical FastAPI shapes
    'path-param': ('GET', '/users/42', [], None, None),
    'json-list-50': ('GET', '/users?n=50', [], None, None),
    'json-list-200': ('GET', '/users?n=200', [], None, None),
    'depends': ('GET', '/dep?limit=5&offset=10', ['-H', 'authorization: Bearer token'], None, None),
    'sync-endpoint': ('GET', '/sync', [], None, None),
    'post-user': (
        'POST',
        '/users',
        ['-H', 'content-type: application/json', '-d', '{"id":7,"name":"u","email":"u@example.com","roles":["a","b"]}'],
        None,
        None,
    ),
    'post-items': (
        'POST',
        '/items',
        ['-H', 'content-type: application/json', '-d', '{"name":"a","price":1.5,"tags":["x","y"]}'],
        None,
        None,
    ),
    # body sizes
    'bytes-1k': ('GET', '/bytes/1k', [], None, None),
    'bytes-64k': ('GET', '/bytes/64k', [], None, None),
    'bytes-1m': ('GET', '/bytes/1m', [], None, 16),
    'stream-20x64k': ('GET', '/stream/20/65536', [], None, 32),
    'file-4m': ('GET', '/file', [], None, 16),
    'upload-4m': ('POST', '/upload/stream', ['-H', 'content-type: application/octet-stream'], UPLOAD_FILE, 16),
    'sse': ('GET', '/sse', [], None, 32),
    # the-benchmarker routes (asgi_min / wsgi_min), 256 connections
    'tb-root': ('GET', '/', [], None, 256),
    'tb-user': ('GET', '/user/123', [], None, 256),
    'tb-post': ('POST', '/user', [], None, 256),
    # io-wait simulation (NOT a database benchmark)
    'sleep-10ms': ('GET', '/sleep/10', [], None, 256),
    # client disconnect storm: every request is abandoned after 5 ms while the handler sleeps 10 ms
    'storm-5ms': ('GET', '/sleep/10', ['-t', '5ms'], None, 64),
    # database (only if PostgreSQL configured, see fastapi_app.py)
    'pg-select': ('GET', '/pg/select', [], None, 64),
}
SCENARIO_SETS = {
    'core': ['hello', 'plain', 'json-validate', 'post-items', 'bytes-1k', 'bytes-64k', 'bytes-1m'],
    # the FastAPI request shapes weft is tuned for
    'fastapi': ['hello', 'path-param', 'json-list-50', 'json-list-200', 'depends', 'sync-endpoint', 'post-items', 'post-user'],
    'bodies': ['bytes-1k', 'bytes-64k', 'bytes-1m', 'stream-20x64k', 'file-4m', 'upload-4m'],
    'stream': ['stream-20x64k', 'sse', 'file-4m', 'upload-4m'],
    'wait': ['sleep-10ms'],
    'pg': ['pg-select'],
    'benchmarker': ['tb-root', 'tb-user', 'tb-post'],
    'all': list(SCENARIOS),
}

def wait_port(port: int, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def proc_tree(pid: int) -> list[psutil.Process]:
    try:
        p = psutil.Process(pid)
        return [p, *p.children(recursive=True)]
    except psutil.Error:
        return []


def sample_tree(procs: list[psutil.Process]) -> tuple[float, int, int]:
    """(cpu seconds total, rss bytes total, thread count total)."""
    cpu = 0.0
    rss = 0
    threads = 0
    for p in procs:
        try:
            t = p.cpu_times()
            cpu += t.user + t.system
            rss += p.memory_info().rss
            threads += p.num_threads()
        except psutil.Error:
            pass
    return cpu, rss, threads


MEM_KEYS = ('rss', 'private', 'uss')


def sample_mem(procs: list[psutil.Process]) -> dict:
    """Summed memory of the process tree, bytes: rss (working set; shared pages
    counted once per process), private (committed private bytes, no shared
    pages), uss (unique set size: resident pages private to the process)."""
    out = dict.fromkeys(MEM_KEYS, 0)
    for p in procs:
        try:
            mi = p.memory_full_info()
            out['rss'] += mi.rss
            out['private'] += getattr(mi, 'private', 0) or 0
            out['uss'] += getattr(mi, 'uss', 0) or 0
        except psutil.Error:
            pass
    return out


def rust_meta() -> dict:
    out = {}
    for name, cmd in (('rustc', ['rustc', '--version']), ('cargo', ['cargo', '--version'])):
        try:
            out[name] = subprocess.check_output(cmd, text=True).strip()
        except Exception as exc:  # noqa: BLE001
            out[name] = f'unavailable: {exc}'
    return out


def git_meta(path: Path) -> dict:
    def run(*args):
        try:
            return subprocess.check_output(['git', *args], cwd=path, text=True).strip()
        except Exception:  # noqa: BLE001
            return None

    return {'commit': run('rev-parse', 'HEAD'), 'dirty': bool(run('status', '--porcelain'))}


def python_meta(python: Path) -> dict:
    code = (
        'import sys, sysconfig, json;'
        'gil = getattr(sys, "_is_gil_enabled", lambda: None)();'
        'print(json.dumps({"version": sys.version, "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),'
        ' "gil_enabled": gil, "executable": sys.executable}))'
    )
    return json.loads(subprocess.check_output([str(python), '-c', code], text=True))


def build_server_cmd(args, python: Path, port: int) -> tuple[list[str], dict]:
    env = dict(os.environ)
    env['PYTHONPATH'] = str(APPS)
    app = APP_SPECS[args.app]
    if args.server == 'uvicorn':
        cmd = [
            str(python), '-m', 'uvicorn', app, '--host', '127.0.0.1', '--port', str(port), '--workers', str(args.workers),
            '--loop', 'asyncio', '--http', 'httptools', '--log-level', 'warning', '--no-access-log',
        ]
    elif args.server == 'peregrine':
        cmd = [
            str(python), '-m', 'peregrine', app, '--host', '127.0.0.1', '--port', str(port),
            '--workers', str(args.workers), '--log-level', 'warning',
        ]
    else:
        cmd = [
            str(python), '-m', 'weft', app, '--host', '127.0.0.1', '--port', str(port),
            '--workers', str(args.workers), '--worker-mode', args.worker_mode, '--log-level', 'warning',
        ]
    for extra in args.server_arg or []:
        cmd += extra.split()
    return cmd, env


OHA_CPUS: list[int] | None = None


def run_oha(url: str, method: str, extra: list[str], body_file: Path | None, conc: int, duration: int,
            rate: int | None, http2: bool) -> dict:
    cmd = ['oha', '--no-tui', '--output-format', 'json', '-z', f'{duration}s', '-c', str(conc), '-m', method]
    if rate:
        cmd += ['-q', str(rate), '--latency-correction']
    if http2:
        cmd += ['--http2']
    if body_file:
        cmd += ['-D', str(body_file)]
    cmd += extra
    cmd.append(url)
    t0 = time.monotonic()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if OHA_CPUS:
        try:
            psutil.Process(proc.pid).cpu_affinity(OHA_CPUS)
        except psutil.Error:
            pass
    oha_ps = psutil.Process(proc.pid)
    oha_cpu = None
    import threading

    def _sample():
        nonlocal oha_cpu
        while proc.poll() is None:
            try:
                t = oha_ps.cpu_times()
                oha_cpu = t.user + t.system
            except psutil.Error:
                pass
            time.sleep(0.1)

    th = threading.Thread(target=_sample, daemon=True)
    th.start()
    stdout, stderr = proc.communicate()
    th.join(timeout=2)
    wall = time.monotonic() - t0
    if proc.returncode != 0:
        return {'error': stderr.strip()[-2000:], 'cmd': cmd, 'wall': wall}
    data = json.loads(stdout)
    data['cmd'] = cmd
    data['wall'] = wall
    data['oha_cpu_seconds'] = oha_cpu
    data['oha_cpu_cores'] = (oha_cpu / wall) if oha_cpu is not None else None
    return data


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--label', required=True)
    ap.add_argument('--server', choices=['weft', 'weft-ft', 'uvicorn', 'peregrine'], default='weft')
    ap.add_argument('--app', choices=list(APP_SPECS), default='fastapi',
                    help='fastapi_app, fastapi_mw (middleware), or the-benchmarker min apps')
    ap.add_argument('--workers', type=int, default=1)
    ap.add_argument('--worker-mode', choices=['process', 'thread'], default='process')
    ap.add_argument('--server-arg', action='append', help='extra raw server CLI args (quoted)')
    ap.add_argument('--scenarios', default='core', help='comma list or a set name')
    ap.add_argument('--concurrency', type=int, default=64)
    ap.add_argument('--duration', type=int, default=10)
    ap.add_argument('--warmup', type=int, default=3)
    ap.add_argument('--repeats', type=int, default=3)
    ap.add_argument('--rate', type=int, default=None, help='fixed arrival rate (req/s) instead of closed loop')
    ap.add_argument('--http2-client', action='store_true')
    ap.add_argument('--port', type=int, default=8321)
    ap.add_argument('--cpu-affinity', help='comma list of CPU ids to pin the server process tree to (Windows/Linux)')
    ap.add_argument('--oha-affinity', help='comma list of CPU ids to pin oha to')
    ap.add_argument('--retained-wait', type=float, default=0.0,
                    help='seconds to wait after each run before sampling retained memory (0 = skip)')
    args = ap.parse_args()

    if shutil.which('oha') is None:
        print('oha not found on PATH', file=sys.stderr)
        return 2

    override = os.environ.get('WEFT_BENCH_PY_' + args.server.upper().replace('-', '_'))
    python = Path(override) if override else VENVS[args.server] / 'Scripts' / 'python.exe'
    if not python.exists():
        python = VENVS[args.server] / 'bin' / 'python'
    scen_table = SCENARIOS
    names = SCENARIO_SETS.get(args.scenarios, args.scenarios.split(','))
    names = [n for n in names if n in scen_table]

    outdir = RAW / args.label
    outdir.mkdir(parents=True, exist_ok=True)

    cmd, env = build_server_cmd(args, python, args.port)
    print('server:', ' '.join(cmd))
    server = subprocess.Popen(cmd, env=env, cwd=str(APPS), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if not wait_port(args.port):
        server.kill()
        print(server.stdout.read())
        print('server did not start', file=sys.stderr)
        return 1
    time.sleep(1.0)
    tree = proc_tree(server.pid)
    global OHA_CPUS
    if args.oha_affinity:
        OHA_CPUS = [int(c) for c in args.oha_affinity.split(',')]
    if args.cpu_affinity:
        cpus = [int(c) for c in args.cpu_affinity.split(',')]
        for p in tree:
            try:
                p.cpu_affinity(cpus)
            except psutil.Error:
                pass

    meta = {
        'label': args.label,
        'args': vars(args),
        'server_cmd': cmd,
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
        'os': platform.platform(),
        'machine': platform.machine(),
        'cpu': platform.processor(),
        'cpu_count_logical': psutil.cpu_count(logical=True),
        'cpu_count_physical': psutil.cpu_count(logical=False),
        'memory_total': psutil.virtual_memory().total,
        'python_server': python_meta(python),
        'python_runner': sys.version,
        'rust': rust_meta(),
        'weft_git': git_meta(ROOT),
        'oha': subprocess.check_output(['oha', '--version'], text=True).strip(),
        'server_pids': [p.pid for p in tree],
        'server_threads_at_start': sample_tree(tree)[2],
        # idle memory: after startup and the 1 s settle, before any request
        'server_mem_idle': sample_mem(tree),
        'scenarios': names,
        'startup_inside_measurement': False,
        'notes': 'closed-loop (-c) unless --rate given; server startup excluded from measured intervals',
    }
    (outdir / 'meta.json').write_text(json.dumps(meta, indent=2, default=str))

    base = f'http://127.0.0.1:{args.port}'
    try:
        # warmup
        run_oha(base + '/', 'GET', [], None, args.concurrency, args.warmup, None, args.http2_client)
        for name in names:
            method, path, extra, body_file, conc_override = scen_table[name]
            conc = conc_override or args.concurrency
            for rep in range(args.repeats):
                tree = proc_tree(server.pid)
                cpu0, _, _ = sample_tree(tree)
                peak_rss = 0
                t0 = time.monotonic()
                # sample RSS in a thread-less way: run oha, then poll during by subprocess timing
                # (oha blocks; use a background sampler thread)
                import threading

                stop = threading.Event()
                samples = []

                mem_samples = []

                def sampler():
                    while not stop.is_set():
                        _, rss, thr = sample_tree(tree)
                        samples.append((rss, thr))
                        mem_samples.append(sample_mem(tree))
                        stop.wait(0.25)

                th = threading.Thread(target=sampler, daemon=True)
                th.start()
                res = run_oha(base + path, method, extra, body_file, conc, args.duration, args.rate, args.http2_client)
                stop.set()
                th.join()
                cpu1, rss_end, thr_end = sample_tree(tree)
                wall = time.monotonic() - t0
                peak_rss = max([s[0] for s in samples] + [rss_end])
                mem_end = sample_mem(tree)
                mem_all = mem_samples + [mem_end]
                mem_peak = {k: max(s[k] for s in mem_all) for k in MEM_KEYS}
                mem_mean = {k: sum(s[k] for s in mem_all) / len(mem_all) for k in MEM_KEYS}
                mem_retained = None
                if args.retained_wait > 0:
                    # memory still held after the burst, once the server is idle again
                    time.sleep(args.retained_wait)
                    mem_retained = sample_mem(tree)
                doc = {
                    'label': args.label,
                    'scenario': name,
                    'repeat': rep,
                    'concurrency': conc,
                    'rate': args.rate,
                    'duration_s': args.duration,
                    'server_cpu_seconds': cpu1 - cpu0,
                    'server_cpu_util_cores': (cpu1 - cpu0) / max(wall, 1e-6),
                    'server_rss_peak': peak_rss,
                    'server_rss_end': rss_end,
                    # memory (bytes, summed over the process tree): rss / private / uss
                    'server_mem_idle': meta['server_mem_idle'],
                    'server_mem_peak': mem_peak,
                    'server_mem_mean': mem_mean,
                    'server_mem_end': mem_end,
                    'server_mem_retained': mem_retained,
                    'retained_wait_s': args.retained_wait,
                    'server_threads': thr_end,
                    'oha': res,
                }
                (outdir / f'{name}-{rep}.json').write_text(json.dumps(doc, indent=1, default=str))
                summ = res.get('summary', {})
                codes = res.get('statusCodeDistribution', {})
                lat = res.get('latencyPercentiles', {})
                print(
                    f'{args.label:28} {name:16} rep{rep} rps={summ.get("requestsPerSec", 0):10.1f} '
                    f'p50={lat.get("p50", 0) * 1000:7.2f}ms p99={lat.get("p99", 0) * 1000:7.2f}ms '
                    f'codes={codes} cpu={doc["server_cpu_util_cores"]:.2f}cores rss={peak_rss / 1e6:.0f}MB '
                    f'priv={mem_peak["private"] / 1e6:.0f}MB uss={mem_peak["uss"] / 1e6:.0f}MB '
                    + (f'retained_priv={mem_retained["private"] / 1e6:.0f}MB ' if mem_retained else '')
                    +
                    f'oha={res.get("oha_cpu_cores") or 0:.2f}cores'
                    + (f' ERROR={res["error"][:200]}' if 'error' in res else '')
                )
    finally:
        for p in proc_tree(server.pid):
            try:
                p.terminate()
            except psutil.Error:
                pass
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
    return 0


if __name__ == '__main__':
    sys.exit(main())
