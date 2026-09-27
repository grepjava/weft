"""FastAPI's own per-request cost with the server removed.

Drives the benchmark app's ASGI callable directly from an asyncio loop with an
in-memory receive/send pair, i.e. the cost a server can never remove:
Starlette's middleware stack, routing, dependency solving, pydantic
validation/serialization and json rendering. Requests per second here are the
single-core ceiling for any server; the gap between this and a server's
measured req/s is the server's overhead.

    python bench/app_floor.py                # fastapi_app
    python bench/app_floor.py --app fastapi_mw
    python bench/app_floor.py --eager        # asyncio.eager_task_factory
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import sys
import time
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent / 'apps'))

ROUTES = {
    'hello': ('GET', '/', b'', b''),
    'path-param': ('GET', '/users/42', b'', b''),
    'json-list-50': ('GET', '/users', b'n=50', b''),
    'depends': ('GET', '/dep', b'limit=5&offset=10', b''),
    'sync-endpoint': ('GET', '/sync', b'', b''),
    'post-items': ('POST', '/items', b'', b'{"name":"a","price":1.5,"tags":["x","y"]}'),
}


def make_scope(method: str, path: str, query: bytes, body: bytes, state: dict) -> dict:
    headers = [
        (b'host', b'127.0.0.1:8000'),
        (b'user-agent', b'oha/1.16'),
        (b'accept', b'*/*'),
        (b'authorization', b'Bearer token'),
    ]
    if body:
        headers.append((b'content-type', b'application/json'))
        headers.append((b'content-length', str(len(body)).encode()))
    return {
        'type': 'http',
        'asgi': {'version': '3.0', 'spec_version': '2.3'},
        'http_version': '1.1',
        'server': ('127.0.0.1', 8000),
        'client': ('127.0.0.1', 51234),
        'scheme': 'http',
        'method': method,
        'root_path': '',
        'path': path,
        'raw_path': path.encode(),
        'query_string': query,
        'headers': headers,
        'state': state.copy(),
        'extensions': {'http.response.pathsend': {}},
    }


async def run_lifespan(app, state: dict):
    started = asyncio.Event()
    stop = asyncio.Event()
    messages = [{'type': 'lifespan.startup'}]

    async def receive():
        if messages:
            return messages.pop(0)
        await stop.wait()
        return {'type': 'lifespan.shutdown'}

    async def send(msg):
        if msg['type'] == 'lifespan.startup.complete':
            started.set()

    task = asyncio.ensure_future(app({'type': 'lifespan', 'asgi': {'version': '3.0'}, 'state': state}, receive, send))
    await started.wait()
    return stop, task


async def bench(app, name: str, state: dict, seconds: float) -> dict:
    method, path, query, body = ROUTES[name]
    scope_proto = make_scope(method, path, query, body, state)
    sent_bytes = 0
    statuses: dict[int, int] = {}

    async def one():
        nonlocal sent_bytes
        msgs = [{'type': 'http.request', 'body': body, 'more_body': False}]

        async def receive():
            if msgs:
                return msgs.pop()
            await asyncio.sleep(3600)  # disconnect listener: never fires

        async def send(msg):
            nonlocal sent_bytes
            if msg['type'] == 'http.response.start':
                statuses[msg['status']] = statuses.get(msg['status'], 0) + 1
            elif msg['type'] == 'http.response.body':
                sent_bytes += len(msg.get('body', b''))

        scope = dict(scope_proto)
        scope['state'] = state.copy()
        await app(scope, receive, send)

    # warmup
    for _ in range(200):
        await one()
    n = 0
    t0 = time.perf_counter()
    cpu0 = time.process_time()
    deadline = t0 + seconds
    while time.perf_counter() < deadline:
        for _ in range(100):
            await one()
        n += 100
    wall = time.perf_counter() - t0
    cpu = time.process_time() - cpu0
    return {
        'scenario': name,
        'requests': n,
        'req_per_s': n / wall,
        'us_per_req_cpu': cpu / n * 1e6,
        'statuses': statuses,
        'resp_bytes_avg': sent_bytes / max(n + 200, 1),
    }


async def main_async(args):
    mod = importlib.import_module(args.app)
    app = mod.app
    if args.eager:
        asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
    state: dict = {}
    stop, task = await run_lifespan(app, state)
    results = []
    for name in args.scenarios.split(','):
        r = await bench(app, name, state, args.seconds)
        results.append(r)
        print(f"{r['scenario']:>14}  {r['req_per_s']:>9.0f} req/s  {r['us_per_req_cpu']:>7.1f} us cpu/req  "
              f"status={r['statuses']}  {r['resp_bytes_avg']:.0f} B", flush=True)
    stop.set()
    await task
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--app', default='fastapi_app')
    ap.add_argument('--scenarios', default=','.join(ROUTES))
    ap.add_argument('--seconds', type=float, default=3.0)
    ap.add_argument('--eager', action='store_true')
    ap.add_argument('--json', help='write results here')
    args = ap.parse_args()
    results = asyncio.run(main_async(args))
    if args.json:
        Path(args.json).write_text(json.dumps({'app': args.app, 'eager': args.eager, 'python': sys.version,
                                               'results': results}, indent=2))


if __name__ == '__main__':
    main()
