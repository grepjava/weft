"""Per-round view of a `bench/ab.py --out` file, to tell a steady effect from
one noisy round.

    python bench/ab_rounds.py results.json [--scenario wsgi]
"""

from __future__ import annotations

import argparse
import json


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('path')
    ap.add_argument('--scenario', default=None)
    args = ap.parse_args()
    rows = json.load(open(args.path, encoding='utf-8'))
    arms = list(dict.fromkeys(r['arm'] for r in rows))
    scenarios = [args.scenario] if args.scenario else list(dict.fromkeys(r['scenario'] for r in rows))
    for sc in scenarios:
        print(f'== {sc}')
        for rnd in sorted({r['round'] for r in rows}):
            cells = []
            for arm in arms:
                x = next((r for r in rows if r['scenario'] == sc and r['arm'] == arm and r['round'] == rnd), None)
                if x:
                    cells.append(f"{arm:>6} {x['rps']:8.0f}/s p50 {x['p50_ms']:5.2f} p99 {x['p99_ms']:6.2f} "
                                 f"{x['cpu_us_per_req']:5.1f}us")
            print(f'  r{rnd}  ' + ' | '.join(cells))


if __name__ == '__main__':
    main()
