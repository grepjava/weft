"""ASGI app for shutdown tests: `/slow` takes a second, and every worker's
lifespan shutdown appends its pid to $WEFT_TEST_MARK."""

import asyncio
import os


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        while True:
            m = await receive()
            if m['type'] == 'lifespan.startup':
                await send({'type': 'lifespan.startup.complete'})
            elif m['type'] == 'lifespan.shutdown':
                with open(os.environ['WEFT_TEST_MARK'], 'a') as f:
                    f.write(f'{os.getpid()}\n')
                await send({'type': 'lifespan.shutdown.complete'})
                return
    if scope['path'] == '/slow':
        await asyncio.sleep(1.0)
    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
    await send({'type': 'http.response.body', 'body': b'done'})
