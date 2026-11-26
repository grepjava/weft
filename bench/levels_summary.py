import collections
import json
import statistics
import sys

rows = [json.loads(line) for line in open(sys.argv[1] if len(sys.argv) > 1 else '/tmp/levels.jsonl') if line.strip()]
bad = [r for r in rows if 'rps' not in r]
rows = [r for r in rows if 'rps' in r]
g = collections.defaultdict(list)
for r in rows:
    g[(r['app'], r['server'], r['conns'], r['route'])].append(r['rps'])
servers = ['weft', 'uvicorn', 'granian']
routes = ['GET /', 'GET /user/42', 'POST /user']
for app in ('tb_fastapi', 'tb_asgi'):
    print(f'\n{app}: req/s, mean of the 3 routes (median over rounds per route)')
    print(f'{"server":>10}' + ''.join(f'{c:>10}' for c in (64, 256, 512)))
    level = {}
    for s in servers:
        cells = []
        for c in (64, 256, 512):
            per = [statistics.median(g[(app, s, c, rt)]) for rt in routes if g[(app, s, c, rt)]]
            v = sum(per) / len(per) if len(per) == 3 else float('nan')
            level[(s, c)] = v
            cells.append(f'{v:10.0f}')
        print(f'{s:>10}' + ''.join(cells))
    for s in servers[1:]:
        print(f'  weft vs {s}: ' + '  '.join(f'c{c} x{level[("weft", c)] / level[(s, c)]:.2f}' for c in (64, 256, 512)))
    print('  per route (weft / uvicorn / granian):')
    for c in (64, 256, 512):
        for rt in routes:
            vals = [statistics.median(g[(app, s, c, rt)]) if g[(app, s, c, rt)] else float('nan') for s in servers]
            spread = [f'{min(g[(app, s, c, rt)]):.0f}..{max(g[(app, s, c, rt)]):.0f}' for s in servers if g[(app, s, c, rt)]]
            print(f'    c{c:<4} {rt:<13}' + ' / '.join(f'{v:7.0f}' for v in vals) + '   [' + ', '.join(spread) + ']')
print('\nfailed starts:', bad)
print('non200 total:', sum(r['non200'] for r in rows), '| results:', len(rows))
