"""Python-only checks for weft.contrib and weft.webtransport.

No server binary: routing, converters, session plumbing, and the upload store.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import tempfile
import threading
import time
import uuid

import pytest

from weft.contrib.asgi import (
    AltSvcMiddleware,
    WebTransportRouter,
    _Route,
    http_version,
    is_http3,
    session_from,
    supports_webtransport,
)
from weft.contrib.django import WebTransportRouter as DjangoRouter, http_version as django_http_version
from weft.contrib.fastapi import WebTransportEndpoint, WebTransportRouter as FastAPIRouter
from weft.webtransport import (
    DATAGRAM_QUEUE,
    INCOMING_QUEUE,
    STREAM_QUEUE,
    WebTransportSession,
    _as_bytes,
)


SCOPE = {'type': 'webtransport', 'path': '/probe', 'headers': []}


class Channel:
    def __init__(self, scripted=()):
        self.inbox = asyncio.Queue()
        self.sent = []
        for message in scripted:
            self.inbox.put_nowait(message)

    async def receive(self):
        return await self.inbox.get()

    async def send(self, message):
        self.sent.append(message)


# -- Starlette-style routes ------------------------------------------------


def test_starlette_name_matches_a_segment():
    assert _Route('/room/{name}', None).match('/room/lobby') == {'name': 'lobby'}


def test_starlette_name_does_not_cross_a_slash():
    assert _Route('/room/{name}', None).match('/room/a/b') is None


def test_starlette_int_converts():
    assert _Route('/n/{count:int}', None).match('/n/42') == {'count': 42}


def test_starlette_int_rejects_non_integer():
    assert _Route('/n/{count:int}', None).match('/n/abc') is None


def test_starlette_int_rejects_signed_value():
    assert _Route('/n/{count:int}', None).match('/n/-1') is None


def test_starlette_uuid_converts():
    ident = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
    matched = _Route('/u/{id:uuid}', None).match('/u/' + ident)
    assert matched is not None
    assert isinstance(matched['id'], uuid.UUID)


def test_starlette_path_keeps_slashes():
    assert _Route('/files/{rest:path}', None).match('/files/a/b/c') == {'rest': 'a/b/c'}


def test_starlette_slug_accepts():
    assert _Route('/s/{name:slug}', None).match('/s/hello-world') == {'name': 'hello-world'}


def test_starlette_slug_rejects_space():
    assert _Route('/s/{name:slug}', None).match('/s/hello world') is None


def test_starlette_unknown_converter_is_refused():
    with pytest.raises(ValueError):
        _Route('/x/{n:nope}', None)


def test_starlette_duplicate_path_parameter_is_refused():
    with pytest.raises(ValueError):
        _Route('/a/{n}/b/{n}', None)


def test_starlette_int_does_not_steal_str_route():
    router = WebTransportRouter()
    router.add_route('/n/{count:int}', lambda s: 'int')
    router.add_route('/n/{name}', lambda s: 'str')
    int_route, int_params = router._lookup('/n/7')
    str_route, str_params = router._lookup('/n/seven')
    assert int_params == {'count': 7}
    assert str_params == {'name': 'seven'}
    assert int_route is not None and str_route is not None
    assert int_route.handler is not str_route.handler


def test_fastapi_endpoint_subclass_is_wrapped():
    class Endpoint(WebTransportEndpoint):
        pass

    fastapi = FastAPIRouter()
    fastapi.add_route('/chat', Endpoint)
    assert fastapi.routes
    assert not isinstance(fastapi.routes[0].handler, type)


# -- Django-style routes ---------------------------------------------------


def test_django_leading_slash_optional():
    router = DjangoRouter()
    router.add_route('chat/<str:room>/', lambda s: 'room')
    _, params = router._lookup('/chat/lobby/')
    assert params == {'room': 'lobby'}


def test_django_int_converts():
    router = DjangoRouter()
    router.add_route('n/<int:count>/', lambda s: 'count')
    _, params = router._lookup('/n/42/')
    assert params == {'count': 42}
    assert router._lookup('/n/abc/')[1] is None


def test_django_uuid_converts():
    router = DjangoRouter()
    router.add_route('u/<uuid:ident>/', lambda s: 'uuid')
    ident = '12345678-1234-1234-1234-123456789abc'
    _, params = router._lookup('/u/' + ident + '/')
    assert params is not None
    assert isinstance(params['ident'], uuid.UUID)


def test_django_path_keeps_slashes():
    router = DjangoRouter()
    router.add_route('files/<path:rest>', lambda s: 'path')
    _, params = router._lookup('/files/a/b/c')
    assert params == {'rest': 'a/b/c'}
    assert router._lookup('/files/')[1] is None


def test_django_without_trailing_slash_misses_exact_pattern():
    router = DjangoRouter()
    router.add_route('chat/<str:room>/', lambda s: 'room')
    assert router._lookup('/chat/lobby')[1] is None


# -- Trailing slashes ------------------------------------------------------


def test_trailing_slash_still_reaches_starlette_handler():
    seen = []

    async def echo(session):
        seen.append(session.path)
        await session.close(code=0)

    async def run():
        router = WebTransportRouter()
        router.add_route('/wt/echo', echo)
        channel = Channel([{'type': 'webtransport.connect'}])
        await router(
            {'type': 'webtransport', 'path': '/wt/echo/', 'headers': []},
            channel.receive,
            channel.send,
        )
        return channel

    channel = asyncio.run(run())
    assert seen[:1] == ['/wt/echo/']
    assert any(m.get('type') == 'webtransport.close' for m in channel.sent)


def test_django_without_trailing_slash_still_matches():
    seen = []

    async def echo(session):
        seen.append(session.path)
        await session.close(code=0)

    async def run():
        django = DjangoRouter()
        django.add_route('wt/room/<str:name>/', echo)
        channel = Channel([{'type': 'webtransport.connect'}])
        await django(
            {'type': 'webtransport', 'path': '/wt/room/lobby', 'headers': []},
            channel.receive,
            channel.send,
        )
        return channel

    asyncio.run(run())
    assert seen[:1] == ['/wt/room/lobby']


# -- Helpers ---------------------------------------------------------------


def test_http_version_helpers():
    assert http_version({'http_version': '3'}) == '3'
    assert is_http3({'http_version': '3'}) is True
    assert supports_webtransport({'extensions': {'webtransport': {}}})
    assert not supports_webtransport({'http_version': '1.1'})


def test_helpers_unwrap_starlette_request():
    class Request:
        def __init__(self, scope):
            self.scope = scope

    assert http_version(Request({'http_version': '2'})) == '2'
    assert supports_webtransport(Request({'extensions': {'webtransport': {}}}))
    assert not supports_webtransport(object())


def test_django_http_version_from_meta_and_scope():
    class WSGIRequest:
        def __init__(self):
            self.META = {'SERVER_PROTOCOL': 'HTTP/3'}

    assert django_http_version(WSGIRequest()) == '3'

    class ASGIRequest:
        def __init__(self):
            self.scope = {'http_version': '3'}
            self.META = {'SERVER_PROTOCOL': 'HTTP/1.1'}

    assert django_http_version(ASGIRequest()) == '3'


# -- Alt-Svc ---------------------------------------------------------------


def test_altsvc_added_to_http11_not_http3():
    async def run():
        captured = []

        async def app(scope, receive, send):
            await send(
                {
                    'type': 'http.response.start',
                    'status': 200,
                    'headers': [(b'content-type', b'text/plain')],
                }
            )
            await send({'type': 'http.response.body', 'body': b'ok'})

        async def capture(message):
            captured.append(message)

        wrapped = AltSvcMiddleware(app, port=443)
        await wrapped({'type': 'http', 'http_version': '1.1'}, None, capture)
        headers = dict(captured[0]['headers'])
        assert headers.get(b'alt-svc') == b'h3=":443"; ma=86400'

        captured.clear()
        await wrapped({'type': 'http', 'http_version': '3'}, None, capture)
        headers = dict(captured[0]['headers'])
        assert b'alt-svc' not in headers

        captured.clear()

        async def already(scope, receive, send):
            await send(
                {
                    'type': 'http.response.start',
                    'status': 200,
                    'headers': [(b'alt-svc', b'h3=":9999"')],
                }
            )
            await send({'type': 'http.response.body', 'body': b'ok'})

        wrapped = AltSvcMiddleware(already, port=443)
        await wrapped({'type': 'http', 'http_version': '1.1'}, None, capture)
        values = [v for k, v in captured[0]['headers'] if k.lower() == b'alt-svc']
        assert values == [b'h3=":9999"']

    asyncio.run(run())


# -- Endpoint / router lifecycle -------------------------------------------


def test_on_connect_raising_does_not_call_on_disconnect():
    async def run():
        channel = Channel([{'type': 'webtransport.connect'}])
        session = session_from(SCOPE, channel.receive, channel.send)
        disconnected = []

        class BoomConnect(WebTransportEndpoint):
            async def on_connect(self):
                raise RuntimeError('refused to start')

            async def on_disconnect(self, code, reason):
                disconnected.append('no-accept')

        with pytest.raises(RuntimeError, match='refused to start'):
            await BoomConnect(session).dispatch()
        assert disconnected == []

    asyncio.run(run())


def test_on_disconnect_runs_after_accepted_session():
    async def run():
        channel = Channel([{'type': 'webtransport.connect'}])
        session = WebTransportSession(SCOPE, channel.receive, channel.send)
        disconnected = []

        class AcceptThenBoom(WebTransportEndpoint):
            async def on_connect(self):
                await self.session.accept()
                raise RuntimeError('after accept')

            async def on_disconnect(self, code, reason):
                disconnected.append('after-accept')

        with pytest.raises(RuntimeError, match='after accept'):
            await AcceptThenBoom(session).dispatch()
        assert disconnected == ['after-accept']
        await session._stop()

    asyncio.run(run())


def test_on_disconnect_does_not_hide_original_error():
    async def run():
        channel = Channel([{'type': 'webtransport.connect'}])
        session = WebTransportSession(SCOPE, channel.receive, channel.send)

        class BothBoom(WebTransportEndpoint):
            async def on_connect(self):
                await self.session.accept()
                raise RuntimeError('after accept')

            async def on_disconnect(self, code, reason):
                raise RuntimeError('disconnect failed')

        with pytest.raises(RuntimeError, match='after accept'):
            await BothBoom(session).dispatch()
        await session._stop()

    asyncio.run(run())


def test_router_closes_session_the_handler_abandoned():
    async def run():
        seen = []

        async def boom(session):
            await session.accept()
            seen.append('accepted')
            raise RuntimeError('handler failed')

        router = WebTransportRouter()
        router.add_route('/boom', boom)
        channel = Channel([{'type': 'webtransport.connect'}])
        with pytest.raises(RuntimeError, match='handler failed'):
            await router(
                {'type': 'webtransport', 'path': '/boom', 'headers': []},
                channel.receive,
                channel.send,
            )
        assert seen == ['accepted']
        assert any(m.get('type') == 'webtransport.close' for m in channel.sent)

        class CloseFails(Channel):
            async def send(self, message):
                self.sent.append(message)
                if message.get('type') == 'webtransport.close':
                    raise RuntimeError('close failed')

        channel = CloseFails([{'type': 'webtransport.connect'}])

        async def boom_then_close_fails(session):
            await session.accept()
            raise RuntimeError('handler failed')

        router = WebTransportRouter()
        router.add_route('/boom', boom_then_close_fails)
        with pytest.raises(RuntimeError, match='handler failed'):
            await router(
                {'type': 'webtransport', 'path': '/boom', 'headers': []},
                channel.receive,
                channel.send,
            )

    asyncio.run(run())


# -- Session backpressure --------------------------------------------------


def test_per_stream_backpressure():
    async def run():
        channel = Channel([{'type': 'webtransport.connect'}])
        session = WebTransportSession(SCOPE, channel.receive, channel.send)
        await session.accept()
        for _ in range(STREAM_QUEUE):
            channel.inbox.put_nowait(
                {
                    'type': 'webtransport.stream.receive',
                    'stream': 0,
                    'data': b'x',
                    'more_data': True,
                }
            )
        channel.inbox.put_nowait(
            {
                'type': 'webtransport.stream.receive',
                'stream': 4,
                'data': b'fast',
                'more_data': False,
            }
        )
        slow = await asyncio.wait_for(session.accept_stream(), 5)
        fast = await asyncio.wait_for(session.accept_stream(), 5)
        assert slow.id == 0
        assert fast.id == 4
        assert await asyncio.wait_for(fast.read(), 5) == b'fast'
        assert any(m.get('type') == 'webtransport.stream.pause' and m.get('stream') == 0 for m in channel.sent)
        n = 0
        while n < STREAM_QUEUE:
            chunk = await asyncio.wait_for(slow.receive(), 5)
            if chunk is None:
                break
            n += 1
        assert any(m.get('type') == 'webtransport.stream.resume' and m.get('stream') == 0 for m in channel.sent)
        await session._stop()

    asyncio.run(run())


def test_full_datagram_queue_does_not_stall_streams():
    async def run():
        channel = Channel([{'type': 'webtransport.connect'}])
        session = WebTransportSession(SCOPE, channel.receive, channel.send)
        await session.accept()
        for i in range(DATAGRAM_QUEUE + 8):
            channel.inbox.put_nowait(
                {
                    'type': 'webtransport.datagram.receive',
                    'data': b'%d' % i,
                }
            )
        channel.inbox.put_nowait(
            {
                'type': 'webtransport.stream.receive',
                'stream': 0,
                'data': b'ok',
                'more_data': False,
            }
        )
        stream = await asyncio.wait_for(session.accept_stream(), 5)
        assert await asyncio.wait_for(stream.read(), 5) == b'ok'
        await session._stop()

    asyncio.run(run())


def test_full_incoming_queue_does_not_stall_accepted_stream():
    async def run():
        channel = Channel([{'type': 'webtransport.connect'}])
        session = WebTransportSession(SCOPE, channel.receive, channel.send)
        await session.accept()
        for i in range(INCOMING_QUEUE):
            channel.inbox.put_nowait(
                {
                    'type': 'webtransport.stream.receive',
                    'stream': i * 4,
                    'data': b'a',
                    'more_data': True,
                }
            )
        overflow = INCOMING_QUEUE * 4
        channel.inbox.put_nowait(
            {
                'type': 'webtransport.stream.receive',
                'stream': overflow,
                'data': b'held',
                'more_data': False,
            }
        )
        channel.inbox.put_nowait(
            {
                'type': 'webtransport.stream.receive',
                'stream': 0,
                'data': b'b',
                'more_data': False,
            }
        )
        first = await asyncio.wait_for(session.accept_stream(), 5)
        assert await asyncio.wait_for(first.read(), 5) == b'ab'
        assert any(m.get('type') == 'webtransport.stream.pause' and m.get('stream') == overflow for m in channel.sent)
        await session._stop()

    asyncio.run(run())


# -- Bytes coercion --------------------------------------------------------


def test_as_bytes():
    assert _as_bytes(b'hi') == b'hi'
    assert _as_bytes(bytearray(b'hi')) == b'hi'
    assert _as_bytes('hi') == b'hi'
    with pytest.raises(TypeError):
        _as_bytes(3)


# -- Resumable uploads -----------------------------------------------------


def test_upload_structured_fields():
    from weft.contrib import uploads as u

    assert (u._sf_boolean('?1'), u._sf_boolean(' ?0 ')) == (True, False)
    assert [u._sf_boolean(t) for t in ('1', '?2', 'true', '')] == [None] * 4
    assert u._sf_integer(' 42 ') == 42
    assert [u._sf_integer(t) for t in ('-1', '1.0', '1' * 16, '', '0x1')] == [None] * 5
    digest = hashlib.sha256(b'x').digest()
    field = u._digest_field(digest)
    assert u._sha256_of_field('md5=:AA==:, ' + field) == digest
    assert u._sha256_of_field('sha-256=abc') is None
    assert u._wants_sha256('sha-256=5')
    assert not u._wants_sha256('sha-256=0')
    assert not u._wants_sha256('sha-512=3')
    assert u.UploadLimits(max_size=10, min_append_size=2, max_age=60).field(30) == (
        'max-size=10, min-append-size=2, max-age=30'
    )
    with pytest.raises(ValueError):
        u.UploadLimits(max_size=1, min_size=2)
    assert [u._answer_parts(a) for a in (None, 202, b'x', (201, b'y'), (200, [], b'z'))] == [
        (204, [], b''),
        (202, [], b''),
        (200, [], b'x'),
        (201, [], b'y'),
        (200, [], b'z'),
    ]


def test_upload_store_round_trip():
    from weft.contrib import uploads as u

    directory = tempfile.mkdtemp(prefix='weft-uploads-unit-')
    try:
        store = u.FileUploadStore(directory)
        info = store.create(length=5, content_type='text/plain', metadata={'user': 'ada'})
        handle = store.acquire(info.id)
        assert store.acquire(info.id) is None
        handle.append(b'abc')
        handle.release()
        again = store.info(info.id)
        assert (again.offset, again.length, again.metadata) == (3, 5, {'user': 'ada'})
        store.remember(info.id, 201, 'text/plain', '/elsewhere', b'body\nwith newline', 7)
        answer = store.answer(info.id)
        assert (
            answer.status,
            answer.content_type,
            answer.location,
            answer.body,
            answer.created_at,
        ) == (201, 'text/plain', '/elsewhere', b'body\nwith newline', 7)
        store.delete(info.id)
        assert store.info(info.id) is None
        assert store.answer(info.id) is not None
        assert store.remove_expired(0) == 0
        assert os.listdir(directory) == []
        assert store.info('../../etc/passwd') is None

        for name in ('a' * 32 + '.data', 'b' * 32 + '.info.1.2.tmp', 'c' * 32 + '.data'):
            open(os.path.join(directory, name), 'wb').close()
        old = time.time() - 3600
        os.utime(os.path.join(directory, 'a' * 32 + '.data'), (old, old))
        os.utime(os.path.join(directory, 'b' * 32 + '.info.1.2.tmp'), (old, old))
        store.remove_expired(60)
        assert os.listdir(directory) == ['c' * 32 + '.data']
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_uploads_asgi():
    from weft.contrib import uploads as u

    def scope(method, path, headers=(), root_path=''):
        return {
            'type': 'http',
            'method': method,
            'path': path,
            'root_path': root_path,
            'headers': [(k.encode(), v.encode()) for k, v in headers],
            'extensions': {'http.response.informational': {}},
        }

    async def call(app, s, body=b'', fail_on=None):
        sent = []
        messages = [{'type': 'http.request', 'body': body, 'more_body': False}]

        async def receive():
            return messages.pop(0) if messages else {'type': 'http.disconnect'}

        async def send(message):
            if message['type'] == fail_on:
                raise RuntimeError('the send failed')
            sent.append(message)

        await app(s, receive, send)
        starts = [m for m in sent if m['type'] == 'http.response.start']
        return (
            starts[-1]['status'] if starts else None,
            {k.lower(): v for k, v in starts[-1]['headers']} if starts else {},
            sent,
        )

    async def run():
        directory = tempfile.mkdtemp(prefix='weft-uploads-asgi-')
        try:
            store = u.FileUploadStore(directory)
            hashed_on = []
            real = u._sha256_of_file

            def recording(path):
                hashed_on.append(threading.get_ident())
                return real(path)

            u._sha256_of_file = recording

            async def finished(upload):
                return 201, [], b'done'

            app = u.ResumableUploads(None, '/files', store=store, on_complete=finished)

            status, headers, _ = await call(
                app,
                scope(
                    'POST',
                    '/files',
                    [('upload-complete', '?1'), ('want-repr-digest', 'sha-256=1')],
                ),
                b'abc',
            )
            assert status == 201
            assert hashed_on and hashed_on[0] != threading.get_ident()
            u._sha256_of_file = real

            with pytest.raises(RuntimeError, match='the send failed'):
                await call(
                    app,
                    scope(
                        'POST',
                        '/files',
                        [('upload-complete', '?0'), ('upload-draft-interop-version', '9')],
                    ),
                    b'abc',
                    fail_on='http.response.informational',
                )
            ids = [n[:-5] for n in os.listdir(directory) if n.endswith('.info') and not store.info(n[:-5]).complete]
            assert len(ids) == 1
            handle = store.acquire(ids[0])
            assert handle is not None

            status, headers, _ = await call(app, scope('HEAD', '/uploads/' + ids[0]))
            assert (status, headers.get(b'retry-after')) == (503, b'1')
            status, _, _ = await call(app, scope('DELETE', '/uploads/' + ids[0]))
            assert status == 503
            handle.append(b'abc')
            asyncio.get_running_loop().call_later(0.1, handle.release)
            status, headers, _ = await call(app, scope('HEAD', '/uploads/' + ids[0]))
            assert (status, headers.get(b'upload-offset')) == (204, b'3')

            status, headers, _ = await call(
                app,
                scope('POST', '/files', [('upload-complete', '?0')], root_path='/api/'),
                b'x',
            )
            assert headers.get(b'location', b'').startswith(b'/api/uploads/')

            status, headers, _ = await call(
                app,
                scope('POST', '/api/files', [('upload-complete', '?1')], root_path='/api'),
                b'mounted',
            )
            assert status == 201

            async def finished_and_removed(upload):
                upload.remove()
                return 201, [], b'kept'

            app_rm = u.ResumableUploads(None, '/files', store=store, on_complete=finished_and_removed)
            with pytest.raises(RuntimeError, match='the send failed'):
                await call(
                    app_rm,
                    scope('POST', '/files', [('upload-complete', '?1')]),
                    b'xyz',
                    fail_on='http.response.start',
                )
            kept_ids = [
                n[:-5]
                for n in os.listdir(directory)
                if n.endswith('.done') and (a := store.answer(n[:-5])) is not None and a.body == b'kept'
            ]
            assert len(kept_ids) == 1
            status, _, sent = await call(app_rm, scope('GET', '/uploads/' + kept_ids[0]))
            assert status == 201
            assert b''.join(m.get('body', b'') for m in sent if m['type'] == 'http.response.body') == b'kept'
            status, _, _ = await call(app_rm, scope('GET', '/api/uploads/' + kept_ids[0], root_path='/api'))
            assert status == 201

            from starlette.responses import Response

            async def finished_starlette(upload):
                upload.remove()
                return Response(b'starlette-kept', status_code=201, media_type='text/plain')

            app_st = u.ResumableUploads(None, '/files', store=store, on_complete=finished_starlette)
            with pytest.raises(RuntimeError, match='the send failed'):
                await call(
                    app_st,
                    scope('POST', '/files', [('upload-complete', '?1')]),
                    b'st-body',
                    fail_on='http.response.start',
                )
            starlette_ids = [
                n[:-5]
                for n in os.listdir(directory)
                if n.endswith('.done') and (a := store.answer(n[:-5])) is not None and a.body == b'starlette-kept'
            ]
            assert len(starlette_ids) == 1
            status, headers, sent = await call(app_st, scope('GET', '/uploads/' + starlette_ids[0]))
            assert status == 201
            assert headers.get(b'content-type', b'').startswith(b'text/plain')
            assert b''.join(m.get('body', b'') for m in sent if m['type'] == 'http.response.body') == b'starlette-kept'
        finally:
            u._sha256_of_file = real
            shutil.rmtree(directory, ignore_errors=True)

    asyncio.run(run())
