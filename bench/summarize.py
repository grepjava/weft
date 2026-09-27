"""Compact table of raw benchmark results.

    python bench/summarize.py linux-base-          # every label starting with the prefix
    python bench/summarize.py linux-base- --scenarios hello,post-items
    python bench/summarize.py linux-base- --json out.json

Per (label, scenario): mean successful req/s over repeats, p50/p99 latency
(ms), server CPU cores in use, server CPU microseconds per request, non-2xx
and error counts. Reads bench/results/raw/<label>/<scenario>-<rep>.json.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


RAW = Path(__file__).resolve().parent / 'results' / 'raw'


def load(prefix: str, scenarios: set[str] | None) -> dict:
    rows: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for d in sorted(RAW.glob(prefix + '*')):
        if not d.is_dir():
            continue
        for f in sorted(d.glob('*.json')):
            if f.name == 'meta.json':
                continue
            data = json.loads(f.read_text())
            scen = data.get('scenario')
            if scenarios and scen not in scenarios:
                continue
            oha = data.get('oha') or {}
            summ = oha.get('summary') or {}
            if not summ:
                continue
            rps = summ.get('requestsPerSec', 0.0)
            codes = oha.get('statusCodeDistribution') or {}
            ok = sum(v for k, v in codes.items() if str(k).startswith('2'))
            total = summ.get('total', 0.0) or 1.0
            pct = oha.get('latencyPercentiles') or {}
            cpu_s = data.get('server_cpu_seconds') or 0.0
            n_req = sum(codes.values()) or 1
            rows[(d.name, scen)].append(
                {
                    'rps': rps * (ok / max(sum(codes.values()), 1)),
                    'p50_ms': (pct.get('p50') or 0.0) * 1000,
                    'p99_ms': (pct.get('p99') or 0.0) * 1000,
                    'cores': data.get('server_cpu_util_cores') or 0.0,
                    'us_per_req': cpu_s / n_req * 1e6,
                    'non2xx': sum(v for k, v in codes.items() if not str(k).startswith('2')),
                    'errors': sum((oha.get('errorDistribution') or {}).values()),
                    'total': total,
                }
            )
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('prefix')
    ap.add_argument('--scenarios', default=None)
    ap.add_argument('--json', default=None)
    args = ap.parse_args()
    scen = set(args.scenarios.split(',')) if args.scenarios else None
    rows = load(args.prefix, scen)
    if not rows:
        print('no results for prefix', args.prefix, file=sys.stderr)
        return 1
    out = []
    print(f"{'label':34} {'scenario':14} {'req/s':>8} {'p50ms':>7} {'p99ms':>7} {'cores':>6} {'us/req':>7} {'non2xx':>7} {'err':>5} {'n':>2}")
    for (label, scen), reps in sorted(rows.items()):
        m = {k: statistics.mean(r[k] for r in reps) for k in ('rps', 'p50_ms', 'p99_ms', 'cores', 'us_per_req')}
        non2xx = sum(r['non2xx'] for r in reps)
        errs = sum(r['errors'] for r in reps)
        print(f"{label:34} {scen:14} {m['rps']:8.0f} {m['p50_ms']:7.2f} {m['p99_ms']:7.2f} {m['cores']:6.2f} {m['us_per_req']:7.1f} {non2xx:7d} {errs:5d} {len(reps):2d}")
        out.append({'label': label, 'scenario': scen, **m, 'non2xx': non2xx, 'errors': errs, 'repeats': len(reps)})
    if args.json:
        Path(args.json).write_text(json.dumps(out, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
