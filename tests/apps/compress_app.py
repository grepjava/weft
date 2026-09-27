"""ASGI app for --compress tests."""


BODY = b'Hello, compress! ' * 80  # > 1024


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        while True:
            m = await receive()
            if m['type'] == 'lifespan.startup':
                await send({'type': 'lifespan.startup.complete'})
            elif m['type'] == 'lifespan.shutdown':
                await send({'type': 'lifespan.shutdown.complete'})
                return
        return
    path = scope['path']
    if path == '/small':
        headers = [(b'content-type', b'text/plain'), (b'content-length', b'4')]
        await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
        await send({'type': 'http.response.body', 'body': b'tiny'})
        return
    if path == '/png':
        await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'image/png')]})
        await send({'type': 'http.response.body', 'body': BODY})
        return
    if path == '/encoded':
        headers = [(b'content-type', b'text/plain'), (b'content-encoding', b'gzip')]
        await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
        await send({'type': 'http.response.body', 'body': BODY})
        return
    if path == '/no-transform':
        headers = [(b'content-type', b'text/plain'), (b'cache-control', b'no-transform')]
        await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
        await send({'type': 'http.response.body', 'body': BODY})
        return
    if path == '/etag':
        headers = [(b'content-type', b'text/plain'), (b'etag', b'"v1"')]
        await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
        await send({'type': 'http.response.body', 'body': BODY})
        return
    if path == '/stream':
        await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
        await send({'type': 'http.response.body', 'body': BODY[:40], 'more_body': True})
        await send({'type': 'http.response.body', 'body': BODY[40:]})
        return
    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
    await send({'type': 'http.response.body', 'body': BODY})
