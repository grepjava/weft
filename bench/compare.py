"""Side-by-side view of labels for the same scenarios.

    python bench/compare.py --labels a,b,c [--strip -pre] [--metric rps|p50|p99|cpu|rss]
"""

from __future__ import annotations

import argparse
import statistics

from report import load


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--labels', required=True)
    ap.add_argument('--strip', default='')
    ap.add_argument('--metrics', default='rps,p99,cpu,rss')
    args = ap.parse_args()
    labels = args.labels.split(',')
    rows, _ = load(labels)

    def val(docs, metric):
        ok = [d for d in docs if 'error' not in d['oha']]
        if not ok:
            return 'n/a'
        if metric == 'rps':
            v = [d['oha']['summary']['requestsPerSec'] for d in ok]
            f = '{:.0f}'
        elif metric == 'p50':
            v = [d['oha']['latencyPercentiles']['p50'] * 1000 for d in ok]
            f = '{:.2f}'
        elif metric == 'p99':
            v = [d['oha']['latencyPercentiles']['p99'] * 1000 for d in ok]
            f = '{:.2f}'
        elif metric == 'cpu':
            v = [d['server_cpu_util_cores'] for d in ok]
            f = '{:.2f}'
        elif metric == 'rss':
            v = [d['server_rss_peak'] / 1e6 for d in ok]
            f = '{:.0f}'
        else:
            raise SystemExit(metric)
        m = statistics.mean(v)
        sd = statistics.stdev(v) if len(v) > 1 else 0
        return (f + ' +-' + f).format(m, sd)

    scenarios = sorted({s for (_, s) in rows})
    for metric in args.metrics.split(','):
        print(f'== {metric}')
        head = 'scenario'.ljust(16) + ''.join(l.replace(args.strip, '').ljust(24) for l in labels)
        print(head)
        for scen in scenarios:
            line = scen.ljust(16)
            for label in labels:
                line += val(rows.get((label, scen), []), metric).ljust(24)
            print(line)
        print()


if __name__ == '__main__':
    main()
