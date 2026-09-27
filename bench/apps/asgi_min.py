"""The-benchmarker's routes as a bare ASGI application."""

_START = {'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]}
_EMPTY = {'type': 'http.response.body', 'body': b''}


async def app(scope, receive, send):
    if scope['type'] != 'http':
        return
    path = scope['path']
    await send(_START)
    if path.startswith('/user/') and scope['method'] == 'GET':
        await send({'type': 'http.response.body', 'body': path[6:].encode()})
    else:
        await send(_EMPTY)
