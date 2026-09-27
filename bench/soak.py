"""Stall detector: rounds of 60 concurrent GET / against a running server;
prints rounds where any request failed or the round took > 3 s, with the
distribution of serving threads.

    python bench/soak.py PORT [ROUNDS]
"""

import asyncio
import collections
import socket
import sys
import time

import httpx

port = int(sys.argv[1])
rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 40
for _ in range(100):
    try:
        socket.create_connection(('127.0.0.1', port), 1).close()
        break
    except OSError:
        time.sleep(0.1)


async def main():
    stalls = 0
    for rnd in range(rounds):
        async with httpx.AsyncClient(timeout=15) as client:
            t0 = time.perf_counter()
            res = await asyncio.gather(*[client.get(f'http://127.0.0.1:{port}/') for _ in range(60)], return_exceptions=True)
            dt = time.perf_counter() - t0
            errs = [r for r in res if isinstance(r, Exception)]
            if errs or dt > 3:
                tids = collections.Counter(r.json().get('tid') for r in res if not isinstance(r, Exception))
                print(
                    f'round {rnd}: {dt:.2f}s errors={len(errs)} {[type(e).__name__ for e in errs][:2]} per-thread={dict(tids)}',
                    flush=True,
                )
                stalls += 1
    print(f'port {port}: {rounds} rounds, {stalls} stalled/failed rounds', flush=True)


asyncio.run(main())
