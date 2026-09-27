"""The ASGI lifespan protocol.

Kept in Python: it runs twice per event loop, once at startup and once at
shutdown, and is pure coordination.
"""

from __future__ import annotations

import asyncio

from .log import logger


class Lifespan:
    def __init__(self, app, mode: str = 'auto'):
        self.app = app
        self.mode = mode
        self.state: dict = {}
        self.scope = {
            'type': 'lifespan',
            'asgi': {'version': '3.0', 'spec_version': '2.0'},
            'state': self.state,
        }
        self.unsupported = False
        self.error: str | None = None
        self.task: asyncio.Task | None = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self._started = asyncio.Event()
        self._stopped = asyncio.Event()

    async def _receive(self):
        return await self._queue.get()

    async def _send(self, message):
        kind = message['type']
        if kind == 'lifespan.startup.complete':
            self._started.set()
        elif kind == 'lifespan.startup.failed':
            self.error = message.get('message') or 'startup failed'
            self._started.set()
        elif kind == 'lifespan.shutdown.complete':
            self._stopped.set()
        elif kind == 'lifespan.shutdown.failed':
            self.error = message.get('message') or 'shutdown failed'
            self._stopped.set()
        else:
            raise RuntimeError(f'Unexpected ASGI lifespan message type {kind!r}')

    async def _main(self):
        try:
            await self.app(self.scope, self._receive, self._send)
        except BaseException as exc:
            if not self._started.is_set() and self.mode == 'auto':
                # An application without lifespan support raises as soon as it
                # sees the scope type: an opt-out, not a failure.
                self.unsupported = True
                logger.info("ASGI 'lifespan' protocol appears unsupported.")
            else:
                self.error = self.error or f'{type(exc).__name__}: {exc}'
                logger.error('Exception in ASGI lifespan', exc_info=exc)
        finally:
            self._started.set()
            self._stopped.set()

    async def startup(self) -> bool:
        """Runs startup; False means the server must not start."""
        self.task = asyncio.get_running_loop().create_task(self._main())
        await self._queue.put({'type': 'lifespan.startup'})
        await self._started.wait()
        if self.error is not None:
            logger.error('Application startup failed: %s', self.error)
            return False
        return True

    async def shutdown(self, timeout: float):
        if self.unsupported or self.task is None:
            return
        if not self.task.done():
            await self._queue.put({'type': 'lifespan.shutdown'})
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout)
            except TimeoutError:
                logger.warning('Lifespan shutdown did not complete within %.1fs', timeout)
        if self.error is not None:
            logger.error('Application shutdown failed: %s', self.error)
        if not self.task.done():
            # It said what it had to say but left the coroutine parked.
            self.task.cancel()
            await asyncio.wait({self.task}, timeout=timeout)
