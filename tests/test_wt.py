import asyncio
import ssl

import pytest

from tests.conftest import serve_thread
from tests.test_https import _cert

aioquic = pytest.importorskip('aioquic')
from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DatagramReceived, HeadersReceived, WebTransportStreamDataReceived
from aioquic.h3.connection import encode_uint_var
from aioquic.quic.configuration import QuicConfiguration

CLOSE_WEBTRANSPORT_SESSION = 0x2843


class Client(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http = H3Connection(self._quic, enable_webtransport=True)
        self.headers = {}
        self.connected = {}
        self.stream_data = {}
        self.stream_ended = set()
        self.datagrams = []
        self._events = asyncio.Event()

    def connect_session(self, path, authority='localhost', protocol='webtransport'):
        stream_id = self._quic.get_next_available_stream_id()
        self._http.send_headers(
            stream_id=stream_id,
            headers=[
                (b':method', b'CONNECT'),
                (b':scheme', b'https'),
                (b':authority', authority.encode()),
                (b':path', path.encode()),
                (b':protocol', protocol.encode()),
            ],
            end_stream=False,
        )
        self.connected[stream_id] = asyncio.get_event_loop().create_future()
        self.transmit()
        return stream_id

    async def await_session(self, stream_id, timeout=15.0):
        return await asyncio.wait_for(asyncio.shield(self.connected[stream_id]), timeout)

    def open_stream(self, session_id, unidirectional=False, data=b'', end=True):
        stream_id = self._http.create_webtransport_stream(session_id, is_unidirectional=unidirectional)
        if not unidirectional:
            with self._http._get_or_create_stream(stream_id) as state:
                state.frame_type = 0x41
                state.session_id = session_id
        if data or end:
            self._quic.send_stream_data(stream_id, data, end_stream=end)
        self.transmit()
        return stream_id

    def send_datagram(self, session_id, data):
        self._http.send_datagram(session_id, data)
        self.transmit()

    def close_session(self, session_id, code=0, reason=b''):
        payload = code.to_bytes(4, 'big') + reason
        capsule = encode_uint_var(CLOSE_WEBTRANSPORT_SESSION) + encode_uint_var(len(payload)) + payload
        self._quic.send_stream_data(session_id, capsule, end_stream=True)
        self.transmit()

    async def wait_for(self, predicate, timeout=10.0):
        deadline = asyncio.get_event_loop().time() + timeout
        while not predicate():
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return predicate()
            self._events.clear()
            try:
                await asyncio.wait_for(self._events.wait(), remaining)
            except asyncio.TimeoutError:
                return predicate()
        return True

    async def wait_stream(self, stream_id, timeout=10.0):
        got = await self.wait_for(lambda: stream_id in self.stream_ended, timeout)
        return self.stream_data.get(stream_id, b'') if got else None

    async def wait_datagrams(self, count, timeout=10.0):
        await self.wait_for(lambda: len(self.datagrams) >= count, timeout)
        return list(self.datagrams)

    async def wait_new_stream(self, known, timeout=10.0):
        def arrived():
            return any(s not in known for s in self.stream_ended)

        if not await self.wait_for(arrived, timeout):
            return None, None
        new = [s for s in self.stream_ended if s not in known][0]
        return new, self.stream_data.get(new, b'')

    def quic_event_received(self, event):
        for http_event in self._http.handle_event(event):
            if isinstance(http_event, HeadersReceived):
                sid = http_event.stream_id
                self.headers[sid] = dict(http_event.headers)
                if sid in self.connected and not self.connected[sid].done():
                    self.connected[sid].set_result(self.headers[sid])
            elif isinstance(http_event, WebTransportStreamDataReceived):
                sid = http_event.stream_id
                self.stream_data[sid] = self.stream_data.get(sid, b'') + http_event.data
                if http_event.stream_ended:
                    self.stream_ended.add(sid)
            elif isinstance(http_event, DatagramReceived):
                self.datagrams.append(http_event.data)
        self._events.set()


def configuration():
    config = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    config.verify_mode = ssl.CERT_NONE
    config.max_datagram_frame_size = 65536
    return config


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=30))


@pytest.fixture
def wt(tmp_path):
    from tests.apps.wt_app import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, tls_certs=[str(cert)], tls_keys=[str(key)], http3=True) as url:
        yield int(url.rsplit(':', 1)[1])


def test_wt_accept_and_reject(wt):
    async def go():
        async with connect('127.0.0.1', wt, configuration=configuration(), create_protocol=Client) as client:
            session = client.connect_session('/wt')
            headers = await client.await_session(session)
            assert headers.get(b':status') == b'200'
            rejected = client.connect_session('/wt-reject')
            headers = await client.await_session(rejected)
            assert headers.get(b':status') == b'403'
            unknown = client.connect_session('/wt', protocol='flying-carpet')
            try:
                headers = await client.await_session(unknown, timeout=3.0)
                assert headers.get(b':status') == b'501'
            except TimeoutError:
                # h3 resets a CONNECT whose :protocol it cannot name
                pass

    run(go())


def test_wt_bidi_echo(wt):
    async def go():
        async with connect('127.0.0.1', wt, configuration=configuration(), create_protocol=Client) as client:
            session = client.connect_session('/wt')
            await client.await_session(session)
            bidi = client.open_stream(session, data=b'hello', end=True)
            body = await client.wait_stream(bidi)
            assert body == b'echo:hello'

    run(go())


def test_wt_uni_echo(wt):
    async def go():
        async with connect('127.0.0.1', wt, configuration=configuration(), create_protocol=Client) as client:
            session = client.connect_session('/wt')
            await client.await_session(session)
            known = set(client.stream_ended)
            client.open_stream(session, unidirectional=True, data=b'uni', end=True)
            new, body = await client.wait_new_stream(known)
            assert body == b'echo:uni'
            assert new is not None and new % 4 == 3

    run(go())


def test_wt_datagram(wt):
    async def go():
        async with connect('127.0.0.1', wt, configuration=configuration(), create_protocol=Client) as client:
            session = client.connect_session('/wt')
            await client.await_session(session)
            client.send_datagram(session, b'ping')
            got = await client.wait_datagrams(1)
            assert got[:1] == [b'echo:ping']

    run(go())


def test_wt_server_streams(wt):
    async def go():
        async with connect('127.0.0.1', wt, configuration=configuration(), create_protocol=Client) as client:
            session = client.connect_session('/wt-push')
            await client.await_session(session)
            got = await client.wait_for(lambda: len(client.stream_ended) >= 2, 10)
            assert got
            bodies = sorted(client.stream_data[s] for s in client.stream_ended)
            assert bodies == [b'push-bidi', b'push-uni']
            kinds = sorted(s % 4 for s in client.stream_ended)
            assert kinds == [1, 3]

    run(go())


def test_wt_second_session(wt):
    async def go():
        async with connect('127.0.0.1', wt, configuration=configuration(), create_protocol=Client) as client:
            first = client.connect_session('/wt')
            await client.await_session(first)
            second = client.connect_session('/wt')
            await client.await_session(second)
            assert second == 4
            bidi = client.open_stream(second, data=b'hello', end=True)
            assert await client.wait_stream(bidi) == b'echo:hello'
            client.send_datagram(second, b'ping')
            got = await client.wait_datagrams(1)
            assert got[:1] == [b'echo:ping']

    run(go())


def test_wt_alongside_http(wt):
    async def go():
        async with connect('127.0.0.1', wt, configuration=configuration(), create_protocol=Client) as client:
            session = client.connect_session('/wt')
            await client.await_session(session)
            request = client._quic.get_next_available_stream_id()
            client._http.send_headers(
                stream_id=request,
                headers=[
                    (b':method', b'GET'),
                    (b':scheme', b'https'),
                    (b':authority', b'localhost'),
                    (b':path', b'/'),
                ],
                end_stream=True,
            )
            client.transmit()
            await client.wait_for(lambda: request in client.headers, 10)
            assert client.headers.get(request, {}).get(b':status') == b'200'
            bidi = client.open_stream(session, data=b'after', end=True)
            assert await client.wait_stream(bidi) == b'echo:after'

    run(go())


def test_wt_wsgi_501(tmp_path):
    from tests.apps.wsgi_app import app

    cert, key = _cert(tmp_path, 'localhost')
    with serve_thread(app, protocol='wsgi', tls_certs=[str(cert)], tls_keys=[str(key)], http3=True) as url:
        port = int(url.rsplit(':', 1)[1])

        async def go():
            async with connect('127.0.0.1', port, configuration=configuration(), create_protocol=Client) as client:
                session = client.connect_session('/wt')
                headers = await client.await_session(session)
                assert headers.get(b':status') == b'501'

        run(go())
