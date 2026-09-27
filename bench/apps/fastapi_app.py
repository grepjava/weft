"""FastAPI benchmark/verification application used by the Weft test and bench suites.

Every benchmark runs against this app (or `fastapi_mw.py`, the same app behind
the middleware stack a typical deployment carries): Weft is tuned for FastAPI,
so there is deliberately no framework-less "raw ASGI" variant any more.
"""

import asyncio
import os
import threading
from typing import Annotated

from fastapi import Depends, FastAPI, Header, Path, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel

app = FastAPI()

BODY_1K = b'x' * 1024
BODY_64K = b'y' * 65536
BODY_1M = b'z' * (1024 * 1024)
STATIC_FILE = os.path.join(os.path.dirname(__file__), 'static', 'large.bin')


class Item(BaseModel):
    name: str
    price: float
    tags: list[str] = []


class User(BaseModel):
    id: int
    name: str
    email: str
    active: bool = True
    roles: list[str] = ['user']


def _user(uid: int) -> User:
    return User(id=uid, name=f'user{uid}', email=f'user{uid}@example.com', roles=['user', 'staff'])


@app.get('/')
async def root():
    return {'hello': 'world'}


@app.get('/pid')
async def pid():
    return {'pid': os.getpid(), 'tid': threading.get_native_id()}


@app.get('/plain', response_class=PlainTextResponse)
async def plain():
    return 'ok'


@app.get('/json/validate')
async def validate(q: Annotated[int, Query(ge=0, le=1000)] = 1, name: str = 'x'):
    return {'q': q, 'name': name, 'items': [{'i': i} for i in range(q % 10)]}


# --- typical FastAPI shapes: path params, response models, dependencies, sync endpoints


@app.get('/users/{uid}', response_model=User)
async def get_user(uid: Annotated[int, Path(ge=1)]):
    return _user(uid)


@app.get('/users', response_model=list[User])
async def list_users(n: Annotated[int, Query(ge=1, le=500)] = 50):
    # ~100 bytes of JSON per user: n=50 is a ~5 KB response, n=200 ~20 KB
    return [_user(i) for i in range(1, n + 1)]


async def _auth(authorization: Annotated[str | None, Header()] = None) -> str:
    return authorization or 'anonymous'


async def _pagination(limit: int = 20, offset: int = 0) -> dict:
    return {'limit': limit, 'offset': offset}


async def _ctx(user: Annotated[str, Depends(_auth)], page: Annotated[dict, Depends(_pagination)]) -> dict:
    return {'user': user, **page}


@app.get('/dep')
async def with_dependencies(ctx: Annotated[dict, Depends(_ctx)]):
    return ctx


@app.get('/sync')
def sync_endpoint():
    # `def` endpoints run in Starlette's threadpool (anyio.to_thread)
    return {'sync': True}


@app.post('/items')
async def create_item(item: Item) -> Item:
    return item


@app.post('/users', response_model=User, status_code=201)
async def create_user(user: User):
    return user


@app.get('/bytes/1k')
async def bytes_1k():
    return PlainTextResponse(BODY_1K)


@app.get('/bytes/64k')
async def bytes_64k():
    return PlainTextResponse(BODY_64K)


@app.get('/bytes/1m')
async def bytes_1m():
    return PlainTextResponse(BODY_1M)


@app.get('/stream/{chunks}/{size}')
async def stream(chunks: int, size: int):
    chunk = b'c' * size

    async def gen():
        for _ in range(chunks):
            yield chunk

    return StreamingResponse(gen(), media_type='application/octet-stream')


@app.get('/sse')
async def sse():
    async def gen():
        for i in range(5):
            yield f'data: {i}\n\n'.encode()
            await asyncio.sleep(0.01)

    return StreamingResponse(gen(), media_type='text/event-stream')


@app.get('/file')
async def file():
    return FileResponse(STATIC_FILE)


@app.post('/upload')
async def upload(request_body: bytes = b''):
    return {'size': len(request_body)}


@app.post('/upload/stream')
async def upload_stream(request: Request):
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
    return {'size': total}


@app.get('/sleep/{ms}')
async def sleep(ms: int):
    await asyncio.sleep(ms / 1000)
    return {'slept': ms}


# --- PostgreSQL (optional): WEFT_BENCH_PG_DSN, e.g. postgresql://user:password@127.0.0.1/db
PG_DSN = os.environ.get('WEFT_BENCH_PG_DSN')
# One pool *per event loop*. A module-global pool is the classic mistake with
# free-threaded worker threads: every worker's lifespan startup would overwrite
# it and all workers would share one pool bound to a single loop (measured:
# 2.5k req/s with 4 worker threads vs 7.7k with per-loop pools).
_pg_pools: dict = {}


def _pool():
    return _pg_pools.get(asyncio.get_running_loop())


@app.on_event('startup')
async def _pg_startup():
    # created inside the worker's own event loop, never at import time
    if PG_DSN:
        import asyncpg

        pool = await asyncpg.create_pool(PG_DSN, min_size=4, max_size=16)
        _pg_pools[asyncio.get_running_loop()] = pool
        async with pool.acquire() as conn:
            await conn.execute('CREATE TABLE IF NOT EXISTS weft_bench (id serial primary key, name text, value int)')
            n = await conn.fetchval('SELECT count(*) FROM weft_bench')
            if n < 1000:
                await conn.executemany(
                    'INSERT INTO weft_bench (name, value) VALUES ($1, $2)', [(f'row{i}', i) for i in range(1000)]
                )


@app.on_event('shutdown')
async def _pg_shutdown():
    pool = _pg_pools.pop(asyncio.get_running_loop(), None)
    if pool is not None:
        await pool.close()


@app.get('/pg/select')
async def pg_select(id: int = 42):
    pool = _pool()
    if pool is None:
        return PlainTextResponse('postgres not configured', status_code=503)
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT id, name, value FROM weft_bench WHERE id = $1', (id % 1000) + 1)
    return {'id': row['id'], 'name': row['name'], 'value': row['value']}


@app.websocket('/ws/echo')
async def ws_echo(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            msg = await ws.receive_text()
            await ws.send_text(msg)
    except WebSocketDisconnect:
        pass
