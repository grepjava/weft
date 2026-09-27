"""An application whose lifespan startup fails, for --no-lifespan."""


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        await receive()
        await send({'type': 'lifespan.startup.failed', 'message': 'no database'})
        return
    if scope['type'] != 'http':
        return
    await send({'type': 'http.response.start', 'status': 200, 'headers': []})
    await send({'type': 'http.response.body', 'body': b'ok'})
