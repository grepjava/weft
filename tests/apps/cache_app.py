"""ASGI app for --cache-size tests. Counts how often each path ran."""

from collections import defaultdict

HITS = defaultdict(int)


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
    method = scope['method']
    if method != 'GET' or path in ('/fresh', '/private', '/item', '/missing', '/vary'):
        HITS[path] += 1
    if path == '/fresh':
        body = (b'fresh-%d-' % HITS[path]) + b'x' * 1200
        headers = [(b'content-type', b'text/plain'), (b'cache-control', b'max-age=60')]
        await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
        await send({'type': 'http.response.body', 'body': body})
        return
    if path == '/private':
        headers = [(b'content-type', b'text/plain'), (b'cache-control', b'private, max-age=60')]
        await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
        await send({'type': 'http.response.body', 'body': b'priv-%d' % HITS[path]})
        return
    if path == '/item':
        if method in ('POST', 'PUT', 'DELETE'):
            await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'ok'})
            return
        headers = [(b'content-type', b'text/plain'), (b'cache-control', b'max-age=60')]
        await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
        await send({'type': 'http.response.body', 'body': b'item-%d' % HITS[path]})
        return
    if path == '/vary':
        # Varies on Accept-Encoding, and says which one it was made for.
        ae = dict(scope['headers']).get(b'accept-encoding', b'-')
        headers = [(b'content-type', b'text/plain'), (b'cache-control', b'max-age=60'),
                   (b'vary', b'accept-encoding')]
        await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
        await send({'type': 'http.response.body', 'body': b'for:' + ae})
        return
    if path == '/missing':
        headers = [(b'content-type', b'text/plain'), (b'cache-control', b'max-age=30')]
        await send({'type': 'http.response.start', 'status': 404, 'headers': headers})
        await send({'type': 'http.response.body', 'body': b'gone'})
        return
    if path == '/hits':
        q = scope.get('query_string') or b'/fresh'
        body = b'%d' % HITS.get(q.decode(), 0)
        await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
        await send({'type': 'http.response.body', 'body': body})
        return
    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
    await send({'type': 'http.response.body', 'body': b'ok'})
