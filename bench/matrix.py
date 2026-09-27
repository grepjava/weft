"""Run the benchmark matrix (see docs/benchmarks.md) by invoking bench/run.py
for each configuration. Every configuration is a separate server process; the
load generator is `oha`.

    python bench/matrix.py --set baseline --duration 10 --repeats 3
    python bench/matrix.py --set placement
    python bench/matrix.py --set python-modes
    python bench/matrix.py --list

Configurations pin the server to a fixed CPU set (--cpus) and oha to a
disjoint set (--oha-cpus) so that the load generator does not steal CPU from
the server; pass empty strings to disable pinning.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PY = sys.executable

# label -> run.py arguments
CONFIGS = {
    # --- 1 worker: server overhead only ---
    'uvicorn-1w': ['--server', 'uvicorn', '--workers', '1'],
    'weft-1w': ['--server', 'weft', '--workers', '1'],
    'weft-ft-1w': ['--server', 'weft-ft', '--workers', '1'],
    # --- 4 workers ---
    'uvicorn-4w': ['--server', 'uvicorn', '--workers', '4'],
    'weft-4w': ['--server', 'weft', '--workers', '4'],
    # --- python modes: equal CPU budget (4 CPUs): 4 processes vs 4 free-threaded threads ---
    'weft-ft-4proc': ['--server', 'weft-ft', '--workers', '4'],
    'weft-ft-4thr': ['--server', 'weft-ft', '--workers', '4', '--worker-mode', 'thread'],
}
SETS = {
    'baseline': ['uvicorn-1w', 'weft-1w'],
    'fastapi': ['uvicorn-1w', 'weft-1w', 'weft-ft-1w'],
    'scaling': ['uvicorn-4w', 'weft-4w'],
    'python-modes': ['weft-ft-4proc', 'weft-ft-4thr'],
    # the-benchmarker min app: python bench/matrix.py --set benchmarker --app asgi-min --scenarios benchmarker --concurrency 256
    'benchmarker': ['weft-1w', 'uvicorn-1w'],
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--set', default='baseline')
    ap.add_argument('--only', help='comma list of labels')
    ap.add_argument('--suffix', default='', help='appended to labels (e.g. -optA)')
    ap.add_argument('--app', default='fastapi')
    ap.add_argument('--scenarios', default='core')
    ap.add_argument('--duration', default='10')
    ap.add_argument('--repeats', default='3')
    ap.add_argument('--concurrency', default='64')
    ap.add_argument('--rate', default=None)
    ap.add_argument('--cpus', default='0,1,2,3,4,5,6,7', help='server CPU set')
    ap.add_argument('--oha-cpus', default='12,13,14,15,16,17,18,19', help='load generator CPU set')
    ap.add_argument('--extra', action='append', default=[], help='extra run.py args')
    ap.add_argument('--list', action='store_true')
    args = ap.parse_args()
    if args.list:
        for k, v in CONFIGS.items():
            print(k, ' '.join(v))
        return 0
    labels = args.only.split(',') if args.only else SETS[args.set]
    for label in labels:
        cfg = [a for a in CONFIGS[label] if a != '']
        cmd = [PY, str(HERE / 'run.py'), '--label', label + args.suffix, '--app', args.app, '--scenarios', args.scenarios,
               '--duration', args.duration, '--repeats', args.repeats, '--concurrency', args.concurrency, *cfg]
        if args.rate:
            cmd += ['--rate', args.rate]
        if args.cpus:
            cmd += ['--cpu-affinity', args.cpus]
        if args.oha_cpus:
            cmd += ['--oha-affinity', args.oha_cpus]
        for e in args.extra:
            cmd += shlex.split(e)
        print('>>>', ' '.join(cmd), flush=True)
        env = {}
        import os

        env = {**os.environ, **env}
        subprocess.run(cmd, check=False, env=env)
    return 0


if __name__ == '__main__':
    sys.exit(main())
