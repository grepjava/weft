"""WebSocket throughput: echo round trips and server push, weft against uvicorn.

The load generator speaks raw RFC 6455 over asyncio streams from several
processes, so that the client is not what gets measured.

    python bench/ws_bench.py --server weft --mode echo --size 64
    python bench/ws_bench.py --server uvicorn --mode push --size 1024
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import multiprocessing
import os
import struct
import subprocess
import sys
import time
from pathlib import Path

import psutil


ROOT = Path(__file__).resolve().parent.parent
APPS = ROOT / 'bench' / 'apps'
PY = {'weft': ROOT / '.venv', 'weft-ft': ROOT / '.venv-ft', 'uvicorn': ROOT / '.venv'}


def masked_frame(payload: bytes, opcode: int = 2) -> bytes:
    n = len(payload)
    if n < 126:
        head = struct.pack('!BB', 0x80 | opcode, 0x80 | n)
    elif n < 65536:
        head = struct.pack('!BBH', 0x80 | opcode, 0x80 | 126, n)
    else:
        head = struct.pack('!BBQ', 0x80 | opcode, 0x80 | 127, n)
    # An all-zero mask is a valid mask that leaves the payload as it is.
    return head + b'\0\0\0\0' + payload


async def handshake(port: int, path: str):
    r, w = await asyncio.open_connection('127.0.0.1', port)
    key = base64.b64encode(os.urandom(16)).decode()
    w.write(
        f'GET {path} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
        f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n'.encode()
    )
    head = await r.readuntil(b'\r\n\r\n')
    assert head.startswith(b'HTTP/1.1 101'), head[:80]
    return r, w


async def read_frame(r: asyncio.StreamReader) -> int:
    b = await r.readexactly(2)
    n = b[1] & 0x7F
    if n == 126:
        (n,) = struct.unpack('!H', await r.readexactly(2))
    elif n == 127:
        (n,) = struct.unpack('!Q', await r.readexactly(8))
    await r.readexactly(n)
    return n


async def echo_conn(port, frame, deadline, counts, i):
    r, w = await handshake(port, '/echo')
    n = 0
    while time.monotonic() < deadline:
        w.write(frame)
        await read_frame(r)
        n += 1
    counts[i] = n
    w.close()


async def push_conn(port, size, deadline, counts, i):
    r, w = await handshake(port, f'/push?{size}')
    n = 0
    while time.monotonic() < deadline:
        await read_frame(r)
        n += 1
    counts[i] = n
    w.close()


def client(port: int, mode: str, size: int, conns: int, start: float, duration: float, q) -> None:
    async def main():
        await asyncio.sleep(max(0.0, start - time.time()))
        deadline = time.monotonic() + duration
        counts = [0] * conns
        frame = masked_frame(b'x' * size)
        if mode == 'echo':
            tasks = [echo_conn(port, frame, deadline, counts, i) for i in range(conns)]
        else:
            tasks = [push_conn(port, size, deadline, counts, i) for i in range(conns)]
        await asyncio.gather(*tasks, return_exceptions=True)
        q.put(sum(counts))

    asyncio.run(main())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--server', choices=list(PY), default='weft')
    ap.add_argument('--mode', choices=['echo', 'push'], default='echo')
    ap.add_argument('--size', type=int, default=64)
    ap.add_argument('--procs', type=int, default=4, help='client processes')
    ap.add_argument('--conns', type=int, default=16, help='connections per client process')
    ap.add_argument('--duration', type=float, default=8.0)
    ap.add_argument('--repeats', type=int, default=3)
    ap.add_argument('--port', type=int, default=8331)
    ap.add_argument('--server-arg', action='append', default=[])
    args = ap.parse_args()

    python = PY[args.server] / 'Scripts' / 'python.exe'
    if not python.exists():
        python = PY[args.server] / 'bin' / 'python'
    if args.server == 'uvicorn':
        cmd = [str(python), '-m', 'uvicorn', 'ws_app:app', '--port', str(args.port), '--log-level', 'warning',
               '--loop', 'asyncio', '--ws', 'websockets', '--ws-max-size', str(1 << 24)]
    else:
        cmd = [str(python), '-m', 'weft', 'ws_app:app', '--port', str(args.port), '--log-level', 'warning']
    for extra in args.server_arg:
        cmd += extra.split()
    server = subprocess.Popen(cmd, cwd=APPS, env={**os.environ, 'PYTHONPATH': str(APPS)})
    try:
        time.sleep(2.0)
        ps = psutil.Process(server.pid)
        tree = [ps, *ps.children(recursive=True)]
        for rep in range(args.repeats):
            q = multiprocessing.Queue()
            start = time.time() + 1.0
            procs = [
                multiprocessing.Process(target=client, args=(args.port, args.mode, args.size, args.conns, start,
                                                             args.duration, q))
                for _ in range(args.procs)
            ]
            for p in procs:
                p.start()
            time.sleep(max(0.0, start - time.time()))
            cpu0 = sum(sum(p.cpu_times()[:2]) for p in tree)
            total = sum(q.get(timeout=args.duration + 30) for _ in procs)
            cpu1 = sum(sum(p.cpu_times()[:2]) for p in tree)
            for p in procs:
                p.join()
            rss = sum(p.memory_info().rss for p in tree)
            rate = total / args.duration
            print(f'{args.server:8} {args.mode:4} size={args.size:<6} rep{rep} msgs/s={rate:10.0f} '
                  f'server_cpu={(cpu1 - cpu0) / args.duration:.2f} cores rss={rss / 1e6:.0f}MB', flush=True)
    finally:
        for p in psutil.Process(server.pid).children(recursive=True):
            p.kill()
        server.kill()
    return 0


if __name__ == '__main__':
    sys.exit(main())
