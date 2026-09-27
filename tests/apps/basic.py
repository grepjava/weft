"""A raw ASGI application covering what a server has to get right."""

import asyncio
import json
import os
import time


async def _body(receive):
    body = b''
    while True:
        m = await receive()
        if m['type'] == 'http.disconnect':
            return None
        body += m.get('body', b'')
        if not m.get('more_body'):
            return body


async def _respond(send, status=200, body=b'', headers=None, content_type=b'text/plain'):
    hdrs = [(b'content-type', content_type), *(headers or [])]
    await send({'type': 'http.response.start', 'status': status, 'headers': hdrs})
    await send({'type': 'http.response.body', 'body': body})


async def lifespan(scope, receive, send):
    while True:
        m = await receive()
        if m['type'] == 'lifespan.startup':
            scope['state']['started'] = True
            await send({'type': 'lifespan.startup.complete'})
        elif m['type'] == 'lifespan.shutdown':
            await send({'type': 'lifespan.shutdown.complete'})
            return


def _jsonable(v):
    if isinstance(v, bytes):
        return v.decode('latin-1')
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    return v


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        return await lifespan(scope, receive, send)
    path = scope['path']

    if path == '/':
        return await _respond(send, body=b'Hello, world!')

    if path == '/pid':
        return await _respond(send, body=str(os.getpid()).encode())

    if path == '/scope':
        return await _respond(send, body=json.dumps(_jsonable(scope)).encode(), content_type=b'application/json')

    if path == '/echo':
        body = await _body(receive)
        return await _respond(send, body=body or b'', content_type=b'application/octet-stream')

    if path == '/size':
        total = 0
        while True:
            m = await receive()
            total += len(m.get('body', b''))
            if not m.get('more_body'):
                break
        return await _respond(send, body=str(total).encode())

    if path == '/stream':
        n = int(scope['query_string'] or b'3')
        await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-type', b'text/plain')]})
        for i in range(n):
            await send({'type': 'http.response.body', 'body': b'chunk%d\n' % i, 'more_body': True})
        await send({'type': 'http.response.body', 'body': b''})
        return

    if path == '/big':
        size = int(scope['query_string'] or b'10000000')
        block = b'x' * 65536
        await send({'type': 'http.response.start', 'status': 200,
                    'headers': [(b'content-length', str(size).encode())]})
        sent = 0
        while sent < size:
            n = min(len(block), size - sent)
            sent += n
            await send({'type': 'http.response.body', 'body': block[:n], 'more_body': sent < size})
        return

    if path == '/nocontent':
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})
        return

    if path == '/error':
        raise ValueError('boom')

    if path == '/noresponse':
        return

    if path == '/halfway':
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': b'partial', 'more_body': True})
        raise ValueError('died halfway')

    if path == '/sleep':
        await asyncio.sleep(float(scope['query_string'] or b'0.05'))
        return await _respond(send, body=b'slept')

    if path == '/thread':
        await asyncio.to_thread(time.sleep, 0.01)
        return await _respond(send, body=b'threaded')

    if path == '/wait-disconnect':
        # Respond only once the client has gone: proves disconnect delivery.
        m = await receive()
        while m['type'] != 'http.disconnect':
            m = await receive()
        app.disconnected = getattr(app, 'disconnected', 0) + 1
        return

    if path == '/disconnects':
        return await _respond(send, body=str(getattr(app, 'disconnected', 0)).encode())

    if path == '/proxy':
        # Outgoing I/O from the application, on the same loop: a socket the
        # application registers is watched by the worker's reactor.
        port = scope['server'][1]
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(b'GET / HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n')
        await writer.drain()
        data = await reader.read()
        writer.close()
        await writer.wait_closed()
        return await _respond(send, body=data.split(b'\r\n\r\n', 1)[1])

    if path == '/headers':
        return await _respond(send, body=b'', headers=[(b'x-one', b'1'), (b'x-two', b'2'), (b'server', b'custom')])

    if path == '/interim':
        await send({'type': 'http.response.early_hint', 'links': [b'</a.css>; rel=preload; as=style',
                                                                  b'</b.js>; rel=preload; as=script']})
        await send({'type': 'http.response.informational', 'status': 104,
                    'headers': [(b'location', b'/uploads/7')]})
        body = await _body(receive)
        return await _respond(send, body=b'final:' + (body or b''))

    if path == '/interim-bad':
        # Each refused message, by the name of its exception.
        bad = [
            {'type': 'http.response.informational', 'status': 100},
            {'type': 'http.response.informational', 'status': 101},
            {'type': 'http.response.informational', 'status': 200},
            {'type': 'http.response.informational'},
            {'type': 'http.response.informational', 'status': 102, 'headers': [(b'content-length', b'0')]},
            {'type': 'http.response.informational', 'status': 102, 'headers': [(b'Connection', b'close')]},
            {'type': 'http.response.informational', 'status': 102, 'headers': [(b'x', b'a\r\nb')]},
            {'type': 'http.response.early_hint', 'links': [b'</a>\r\nx: y']},
            {'type': 'http.response.early_hint', 'links': ['</a>']},
        ]
        out = []
        for m in bad:
            try:
                await send(m)
                out.append('sent')
            except Exception as e:
                out.append(type(e).__name__)
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        try:
            await send({'type': 'http.response.early_hint', 'links': [b'</a>']})
            out.append('sent')
        except Exception as e:
            out.append(type(e).__name__)
        await send({'type': 'http.response.body', 'body': ','.join(out).encode()})
        return

    if path == '/extensions':
        return await _respond(send, body=json.dumps(scope.get('extensions')).encode())

    if path == '/respond-early':
        # Answers without reading the body.
        return await _respond(send, body=b'early')

    if path == '/bad-send':
        await send({'type': 'http.response.body', 'body': b'no start'})
        return

    return await _respond(send, status=404, body=b'not found')
