"""The-benchmarker's routes as a bare WSGI application."""


def app(environ, start_response):
    path = environ['PATH_INFO']
    if path.startswith('/user/') and environ['REQUEST_METHOD'] == 'GET':
        body = path[6:].encode()
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [body]
    start_response('200 OK', [('Content-Type', 'text/plain')])
    return [b'']
