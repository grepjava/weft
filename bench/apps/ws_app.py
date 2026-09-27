"""Raw ASGI WebSocket endpoints for bench/ws_bench.py: the server, not a framework."""


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        while True:
            msg = await receive()
            await send({'type': msg['type'] + '.complete'})
            if msg['type'] == 'lifespan.shutdown':
                return
    if scope['type'] != 'websocket':
        await send({'type': 'http.response.start', 'status': 404, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})
        return
    await receive()
    await send({'type': 'websocket.accept'})
    if scope['path'] == '/push':
        size = int(scope['query_string'] or b'64')
        msg = {'type': 'websocket.send', 'bytes': b'x' * size}
        while True:
            await send(msg)
    while True:
        msg = await receive()
        if msg['type'] == 'websocket.disconnect':
            return
        # uvicorn leaves out the unused key.
        await send({'type': 'websocket.send', 'bytes': msg.get('bytes'), 'text': msg.get('text')})
