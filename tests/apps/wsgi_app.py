"""A WSGI application exercising PEP 3333."""

import io
import json
import sys


def _json(start_response, obj, status='200 OK'):
    body = json.dumps(obj).encode()
    start_response(status, [('Content-Type', 'application/json')])
    return [body]


def app(environ, start_response):
    path = environ['PATH_INFO']

    if path == '/':
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b'Hello, world!']

    if path == '/environ':
        keep = {k: v for k, v in environ.items() if isinstance(v, (str, int, bool, tuple))}
        keep['wsgi.errors_is_stderr'] = environ['wsgi.errors'] is sys.stderr
        keep['wsgi.version'] = list(environ['wsgi.version'])
        return _json(start_response, keep)

    if path == '/echo':
        body = environ['wsgi.input'].read()
        start_response('200 OK', [('Content-Type', 'application/octet-stream')])
        return [body]

    if path == '/generator':
        start_response('200 OK', [('Content-Type', 'text/plain')])

        def gen():
            yield b''
            for i in range(3):
                yield f'part{i};'.encode()

        return gen()

    if path == '/empty-generator':
        start_response('204 No Content', [])
        return iter(())

    if path == '/write':
        write = start_response('200 OK', [('Content-Type', 'text/plain')])
        write(b'written;')
        return [b'returned']

    if path == '/declared':
        start_response('200 OK', [('Content-Length', '5')])
        return [b'hello world']

    if path == '/short':
        start_response('200 OK', [('Content-Length', '100')])
        return [b'short']

    if path == '/te':
        start_response('200 OK', [('Transfer-Encoding', 'chunked')])
        return iter([b'a', b'b'])

    if path == '/close':
        start_response('200 OK', [('Connection', 'close')])
        return [b'bye']

    if path == '/error':
        raise RuntimeError('boom')

    if path == '/error-in-iter':
        start_response('200 OK', [])

        def gen():
            yield b'first'
            raise RuntimeError('boom')

        return gen()

    if path == '/error-before-first':
        start_response('200 OK', [])

        def gen():
            raise RuntimeError('boom')
            yield b''

        return gen()

    if path == '/exc-info':
        try:
            raise ValueError('bad')
        except ValueError:
            start_response('200 OK', [])
            start_response('500 Internal Server Error', [('Content-Type', 'text/plain')], sys.exc_info())
        return [b'recovered']

    if path == '/twice':
        start_response('200 OK', [])
        try:
            start_response('200 OK', [])
        except RuntimeError:
            return [b'refused']
        return [b'accepted']

    if path == '/no-start':
        return [b'x']

    if path == '/bad-header':
        try:
            start_response('200 OK', [('X-Bad', 'a\r\nb')])
        except ValueError:
            start_response('200 OK', [], sys.exc_info())
            return [b'refused']
        return [b'accepted']

    if path == '/latin1':
        start_response('200 OK', [('X-Latin', 'caf\u00e9')])
        return [environ.get('HTTP_X_IN', '').encode('latin-1')]

    if path == '/close-called':
        class Body:
            def __iter__(self):
                yield b'body'

            def close(self):
                CLOSED.append(True)

        start_response('200 OK', [])
        return Body()

    if path == '/closed':
        return _json(start_response, len(CLOSED))

    if path == '/file':
        start_response('200 OK', [('Content-Type', 'application/octet-stream')])
        return environ['wsgi.file_wrapper'](io.BytesIO(b'x' * 200_000), 8192)

    if path == '/sleep':
        import time

        time.sleep(1)
        start_response('200 OK', [])
        return [b'slept']

    if path == '/big':
        start_response('200 OK', [])
        return [b'y' * (4 << 20)]

    return _json(start_response, {'path': path}, '404 Not Found')


CLOSED = []


class CallableApp:
    def __call__(self, environ, start_response):
        start_response('200 OK', [])
        return [b'callable']
