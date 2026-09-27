"""Aggregate raw benchmark results (bench/results/raw/<label>/*.json) into a
Markdown table with mean and spread across repeats.

    python bench/report.py [--labels a,b,c] [--scenarios hello,plain] [--out bench/results/summary.md]
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path

RAW = Path(__file__).resolve().parent / 'results' / 'raw'


def load(labels: list[str] | None):
    rows = defaultdict(list)  # (label, scenario) -> [doc]
    metas = {}
    for d in sorted(RAW.iterdir()):
        if not d.is_dir() or (labels and d.name not in labels):
            continue
        mp = d / 'meta.json'
        if mp.exists():
            metas[d.name] = json.loads(mp.read_text())
        for f in d.glob('*-*.json'):
            doc = json.loads(f.read_text())
            if 'scenario' not in doc:  # e.g. ws-fanout results
                continue
            rows[(doc['label'], doc['scenario'])].append(doc)
    return rows, metas


def fmt(vals, scale=1.0, digits=1):
    vals = [v * scale for v in vals if v is not None]
    if not vals:
        return 'n/a'
    if len(vals) == 1:
        return f'{vals[0]:.{digits}f}'
    m = statistics.mean(vals)
    sd = statistics.stdev(vals)
    return f'{m:.{digits}f} ±{sd:.{digits}f}'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--labels')
    ap.add_argument('--scenarios')
    ap.add_argument('--out')
    args = ap.parse_args()
    labels = args.labels.split(',') if args.labels else None
    scen_filter = set(args.scenarios.split(',')) if args.scenarios else None
    rows, metas = load(labels)

    out = []
    out.append('| label | scenario | conc | rps | p50 ms | p99 ms | non-2xx | errors | server CPU cores | oha CPU cores | peak RSS MB | repeats |')
    out.append('|---|---|---|---|---|---|---|---|---|---|---|---|')
    for (label, scen), docs in sorted(rows.items()):
        if scen_filter and scen not in scen_filter:
            continue
        ok = [d for d in docs if 'error' not in d['oha']]
        rps = [d['oha']['summary']['requestsPerSec'] for d in ok]
        p50 = [d['oha']['latencyPercentiles']['p50'] for d in ok]
        p99 = [d['oha']['latencyPercentiles']['p99'] for d in ok]
        non2xx = sum(
            v for d in ok for k, v in d['oha'].get('statusCodeDistribution', {}).items() if not k.startswith('2')
        )
        # oha reports the in-flight request of every connection as 'aborted due to deadline'
        # when the timed run ends; that is an artifact of the run length, not a server error
        errs = sum(
            v for d in ok for k, v in d['oha'].get('errorDistribution', {}).items() if k != 'aborted due to deadline'
        )
        oha_cores = [d['oha'].get('oha_cpu_cores') for d in ok]
        cpu = [d['server_cpu_util_cores'] for d in ok]
        rss = [d['server_rss_peak'] for d in ok]
        conc = docs[0]['concurrency']
        out.append(
            f'| {label} | {scen} | {conc} | {fmt(rps, 1, 0)} | {fmt(p50, 1000, 2)} | {fmt(p99, 1000, 2)} | '
            f'{non2xx} | {errs} | {fmt(cpu, 1, 2)} | {fmt(oha_cores, 1, 2)} | {fmt(rss, 1e-6, 0)} | {len(ok)}/{len(docs)} |'
        )
    out.append('')
    for label, m in metas.items():
        py = m.get('python_server', {})
        out.append(
            f'- **{label}**: `{" ".join(m["server_cmd"][1:])}`; python {py.get("version", "?").split()[0]} '
            f'(free-threaded build: {py.get("free_threaded_build")}, GIL enabled: {py.get("gil_enabled")}); '
            f'{m["rust"].get("rustc")}; weft {m["weft_git"].get("commit", "?")[:10]}'
            f'{" (dirty)" if m["weft_git"].get("dirty") else ""}; {m["os"]}; {m["cpu_count_logical"]} logical CPUs; {m["oha"]}'
        )
    text = '\n'.join(out)
    if args.out:
        Path(args.out).write_text(text + '\n', encoding='utf-8')
    print(text)


if __name__ == '__main__':
    main()
