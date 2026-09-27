"""ASGI WebTransport endpoints matching Peregrine's extension."""


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        while True:
            m = await receive()
            if m['type'] == 'lifespan.startup':
                await send({'type': 'lifespan.startup.complete'})
            elif m['type'] == 'lifespan.shutdown':
                await send({'type': 'lifespan.shutdown.complete'})
                return

    if scope['type'] == 'http':
        await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
        await send({'type': 'http.response.body', 'body': b'Hello, world!'})
        return

    path = scope['path']
    message = await receive()
    assert message['type'] == 'webtransport.connect', message

    if path == '/wt-reject':
        await send({'type': 'webtransport.close', 'code': 403})
        return

    await send({'type': 'webtransport.accept'})

    if path == '/wt-close':
        await send({'type': 'webtransport.close', 'code': 7, 'reason': 'asked to'})
        return

    if path == '/wt-push':
        await send({'type': 'webtransport.stream.open', 'bidirectional': False})
        opened = await receive()
        assert opened['type'] == 'webtransport.stream.opened', opened
        await send({
            'type': 'webtransport.stream.send',
            'stream': opened['stream'],
            'data': b'push-uni',
            'end_stream': True,
        })
        await send({'type': 'webtransport.stream.open', 'bidirectional': True})
        opened = await receive()
        assert opened['type'] == 'webtransport.stream.opened', opened
        await send({
            'type': 'webtransport.stream.send',
            'stream': opened['stream'],
            'data': b'push-bidi',
            'end_stream': True,
        })

    buffers = {}
    while True:
        message = await receive()
        kind = message['type']
        if kind == 'webtransport.disconnect' or not kind.startswith('webtransport.'):
            return
        if kind == 'webtransport.datagram.receive':
            await send({'type': 'webtransport.datagram.send', 'data': b'echo:' + message['data']})
        elif kind == 'webtransport.stream.receive':
            stream = message['stream']
            buffers[stream] = buffers.get(stream, b'') + message['data']
            if message['more_data']:
                continue
            body = buffers.pop(stream)
            if stream % 4 == 0:
                await send({
                    'type': 'webtransport.stream.send',
                    'stream': stream,
                    'data': b'echo:' + body,
                    'end_stream': True,
                })
            else:
                await send({'type': 'webtransport.stream.open', 'bidirectional': False})
                opened = await receive()
                await send({
                    'type': 'webtransport.stream.send',
                    'stream': opened['stream'],
                    'data': b'echo:' + body,
                    'end_stream': True,
                })
