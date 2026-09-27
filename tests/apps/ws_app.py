"""Raw ASGI WebSocket endpoints for tests/test_websocket.py."""

import asyncio
import json

from weft import ClientDisconnected


#: What the last `/record` connection was told when it ended.
last: dict = {}


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        while True:
            msg = await receive()
            if msg['type'] == 'lifespan.startup':
                await send({'type': 'lifespan.startup.complete'})
            else:
                await send({'type': 'lifespan.shutdown.complete'})
                return
    if scope['type'] == 'http':
        body = json.dumps(last).encode()
        await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'application/json')]})
        await send({'type': 'http.response.body', 'body': body})
        return

    path = scope['path']
    assert (await receive())['type'] == 'websocket.connect'

    if path == '/echo':
        await send({'type': 'websocket.accept'})
        while True:
            msg = await receive()
            if msg['type'] == 'websocket.disconnect':
                return
            await send({'type': 'websocket.send', 'bytes': msg['bytes'], 'text': msg['text']})

    elif path == '/scope':
        await send({'type': 'websocket.accept'})
        info = {k: scope[k] for k in ('type', 'scheme', 'path', 'http_version', 'subprotocols', 'root_path')}
        info['query_string'] = scope['query_string'].decode()
        info['extensions'] = sorted(scope.get('extensions', {}))
        info['headers'] = {k.decode(): v.decode() for k, v in scope['headers']}
        info['client'] = list(scope['client'])
        await send({'type': 'websocket.send', 'text': json.dumps(info)})
        await send({'type': 'websocket.close'})

    elif path == '/subprotocol':
        await send({
            'type': 'websocket.accept',
            'subprotocol': scope['subprotocols'][-1],
            'headers': [(b'set-cookie', b'session=abc'), (b'connection', b'nonsense')],
        })
        await send({'type': 'websocket.close'})

    elif path == '/reject':
        await send({'type': 'websocket.close'})

    elif path == '/deny':
        await send({'type': 'websocket.http.response.start', 'status': 418, 'headers': [(b'x-deny', b'yes')]})
        await send({'type': 'websocket.http.response.body', 'body': b'no websockets for you'})

    elif path == '/noaccept':
        return

    elif path == '/crash':
        raise RuntimeError('before accept')

    elif path == '/crash-open':
        await send({'type': 'websocket.accept'})
        raise RuntimeError('after accept')

    elif path == '/close':
        await send({'type': 'websocket.accept'})
        await send({'type': 'websocket.close', 'code': 4001, 'reason': 'bye'})

    elif path == '/record':
        await send({'type': 'websocket.accept'})
        last.clear()
        count = 0
        while True:
            msg = await receive()
            if msg['type'] == 'websocket.disconnect':
                last.update(code=msg['code'], reason=msg.get('reason'), count=count)
                break
            count += 1
        try:
            await send({'type': 'websocket.send', 'text': 'too late'})
        except ClientDisconnected:
            last['send_after_close'] = 'ClientDisconnected'

    elif path == '/push':
        await send({'type': 'websocket.accept'})
        n = int(scope['query_string'] or b'100')
        for i in range(n):
            await send({'type': 'websocket.send', 'text': f'{i:06d}' + 'x' * 1000})
        await send({'type': 'websocket.close'})

    elif path == '/slow':
        await send({'type': 'websocket.accept'})
        await asyncio.sleep(0.5)
        got = []
        while True:
            msg = await receive()
            if msg['type'] == 'websocket.disconnect':
                return
            got.append(msg['text'])
            if msg['text'] == 'end':
                await send({'type': 'websocket.send', 'text': json.dumps(got)})

    elif path == '/idle':
        await send({'type': 'websocket.accept'})
        msg = await receive()
        last.clear()
        last.update(code=msg.get('code'), type=msg['type'])
