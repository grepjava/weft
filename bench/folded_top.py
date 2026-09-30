"""Top frames of a py-spy raw (folded) profile, by self and inclusive samples.

    python bench/folded_top.py profile.folded [--top 40] [--grep weft]
"""

from __future__ import annotations

import argparse
import re
from collections import Counter


def clean(frame: str) -> str:
    # `name (file:line)` -> `name (file)`: lines split one function apart.
    return re.sub(r':\d+\)$', ')', frame.strip())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('path')
    ap.add_argument('--top', type=int, default=40)
    ap.add_argument('--grep', default=None, help='only frames matching this regex')
    args = ap.parse_args()
    self_c: Counter[str] = Counter()
    incl: Counter[str] = Counter()
    total = 0
    with open(args.path, encoding='utf-8', errors='replace') as f:
        for line in f:
            stack, _, n = line.rstrip('\n').rpartition(' ')
            if not n.isdigit():
                continue
            n = int(n)
            total += n
            frames = [clean(x) for x in stack.split(';') if x]
            if frames:
                self_c[frames[-1]] += n
            for fr in set(frames):
                incl[fr] += n
    pat = re.compile(args.grep) if args.grep else None
    for title, c in (('self', self_c), ('inclusive', incl)):
        print(f'\n== {title} (of {total} samples)')
        shown = 0
        for fr, n in c.most_common():
            if pat and not pat.search(fr):
                continue
            print(f'{100 * n / total:6.2f}%  {fr[:150]}')
            shown += 1
            if shown >= args.top:
                break


if __name__ == '__main__':
    main()
