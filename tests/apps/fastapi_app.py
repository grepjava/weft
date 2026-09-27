"""FastAPI application exercised end-to-end over Weft by tests/test_fastapi.py.

It covers the request shapes FastAPI/Starlette produce that a server must get
right: scope details, path/query/header parameters and validation errors,
pydantic JSON bodies, dependencies, sync endpoints (threadpool), streaming
responses, BaseHTTPMiddleware (concurrent `receive` for disconnects),
CORSMiddleware preflight, websockets, lifespan state and client disconnects.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, Path, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.started = time.time()
    app.state.counter = 0
    yield {'lifespan_state': 'ready'}


app = FastAPI(lifespan=lifespan)


class Item(BaseModel):
    name: str
    price: float
    tags: list[str] = []


class User(BaseModel):
    id: int
    name: str
    active: bool = True


@app.middleware('http')
async def timing(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    response.headers['x-process-time'] = f'{(time.perf_counter() - t0) * 1000:.3f}'
    return response


app.add_middleware(CORSMiddleware, allow_origins=['https://example.com'], allow_methods=['*'], allow_headers=['*'])


@app.get('/')
async def root():
    return {'hello': 'world'}


@app.get('/info')
async def info(request: Request):
    return {
        'method': request.method,
        'path': request.url.path,
        'query': dict(request.query_params),
        'headers': dict(request.headers),
        'client': list(request.client) if request.client else None,
        'http_version': request.scope['http_version'],
        'scheme': request.url.scheme,
        'root_path': request.scope.get('root_path', ''),
        'lifespan_state': request.state.lifespan_state,
        'app_started': request.app.state.started > 0,
    }


@app.get('/users/{uid}', response_model=User)
async def get_user(uid: Annotated[int, Path(ge=1)], verbose: bool = False):
    return User(id=uid, name=f'user{uid}' + (' (verbose)' if verbose else ''))


@app.get('/users', response_model=list[User])
async def list_users(n: Annotated[int, Query(ge=1, le=500)] = 3):
    return [User(id=i, name=f'user{i}') for i in range(1, n + 1)]


@app.post('/items', status_code=201)
async def create_item(item: Item) -> Item:
    return item


async def _auth(authorization: Annotated[str | None, Header()] = None) -> str:
    if authorization == 'Bearer deny':
        from fastapi import HTTPException

        raise HTTPException(status_code=403, detail='denied')
    return authorization or 'anonymous'


@app.get('/dep')
async def with_dep(user: Annotated[str, Depends(_auth)], limit: int = 10):
    return {'user': user, 'limit': limit}


@app.get('/sync')
def sync_endpoint(request: Request):
    return {'sync': True, 'path': request.url.path}


@app.get('/plain', response_class=PlainTextResponse)
async def plain():
    return 'ok'


@app.get('/stream/{chunks}/{size}')
async def stream(chunks: int, size: int):
    chunk = b'c' * size

    async def gen():
        for _ in range(chunks):
            yield chunk
            await asyncio.sleep(0)

    return StreamingResponse(gen(), media_type='application/octet-stream')


@app.post('/echo')
async def echo(request: Request):
    body = await request.body()
    return PlainTextResponse(body)


@app.post('/echo/stream')
async def echo_stream(request: Request):
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
    return {'size': total}


@app.get('/sleep/{ms}')
async def sleep(ms: int, request: Request):
    await asyncio.sleep(ms / 1000)
    return {'slept': ms, 'disconnected': await request.is_disconnected()}


@app.get('/boom')
async def boom():
    raise RuntimeError('boom')


@app.websocket('/ws/echo')
async def ws_echo(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            msg = await ws.receive_text()
            await ws.send_text(msg)
    except WebSocketDisconnect:
        pass


@app.websocket('/ws/json/{room}')
async def ws_json(ws: WebSocket, room: str, token: str = 'none'):
    await ws.accept(subprotocol='chat' if 'chat' in ws.scope['subprotocols'] else None)
    try:
        while True:
            data = await ws.receive_json()
            await ws.send_json({'room': room, 'token': token, 'echo': data})
    except WebSocketDisconnect:
        pass


@app.websocket('/ws/bytes')
async def ws_bytes(ws: WebSocket):
    await ws.accept()
    async for data in ws.iter_bytes():
        await ws.send_bytes(data[::-1])


@app.websocket('/ws/private')
async def ws_private(ws: WebSocket):
    if ws.headers.get('authorization') != 'Bearer ok':
        await ws.close(code=1008)
        return
    await ws.accept()
    await ws.send_text('welcome')
    await ws.close()
