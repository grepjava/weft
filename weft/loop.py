"""The asyncio event loop whose selector is the server.

`WeftEventLoop` is a stock `asyncio.SelectorEventLoop` with one substitution:
its selector is backed by a native `Worker`, whose `select()` runs the Tokio
runtime that accepts, parses and writes HTTP. Connections are served inside
that call on this thread, and descriptors the application registers (its own
sockets, the loop's wakeup socket) are watched by the same reactor, so one
wait covers both.

Everything else, from timers and callbacks to tasks, subprocesses (on Unix)
and signal handling, is asyncio's own code, unchanged.
"""

from __future__ import annotations

import asyncio
import selectors
import signal
import sys
import threading

from ._weft import Worker


__all__ = ['WeftEventLoop', 'WeftSelector', 'new_event_loop']

_VALID_EVENTS = selectors.EVENT_READ | selectors.EVENT_WRITE


class WeftSelector(selectors._BaseSelectorImpl):
    """A selector whose readiness comes from a native worker."""

    def __init__(self, worker: Worker):
        super().__init__()
        self._worker = worker

    def register(self, fileobj, events, data=None):
        key = super().register(fileobj, events, data)
        try:
            self._worker.register(key.fd, events)
        except BaseException:
            super().unregister(fileobj)
            raise
        return key

    def unregister(self, fileobj):
        key = super().unregister(fileobj)
        try:
            self._worker.unregister(key.fd)
        except (OSError, RuntimeError):
            # the descriptor may already be closed, or the worker with it
            pass
        return key

    def modify(self, fileobj, events, data=None):
        if not events or events & ~_VALID_EVENTS:
            raise ValueError(f'Invalid events: {events!r}')
        try:
            key = self._fd_to_key[self._fileobj_lookup(fileobj)]
        except KeyError:
            raise KeyError(f'{fileobj!r} is not registered') from None
        if events != key.events:
            self._worker.modify(key.fd, events)
            key = key._replace(events=events, data=data)
            self._fd_to_key[key.fd] = key
        elif data != key.data:
            key = key._replace(data=data)
            self._fd_to_key[key.fd] = key
        return key

    def select(self, timeout=None):
        fd_to_key = self._fd_to_key
        ready = []
        for fd, events in self._worker.select(timeout):
            key = fd_to_key.get(fd)
            if key is not None:
                events &= key.events
                if events:
                    ready.append((key, events))
        return ready


class WeftEventLoop(asyncio.SelectorEventLoop):
    """`SelectorEventLoop` driven by a native worker."""

    def __init__(self, worker: Worker | None = None):
        self.worker = worker if worker is not None else Worker()
        super().__init__(selector=WeftSelector(self.worker))

    def _write_to_self(self):
        # call_soon_threadsafe: wake the worker directly instead of writing
        # to the self-pipe and having the reactor report it readable.
        self.worker.wakeup()

    def run_forever(self):
        # On Windows nothing interrupts a blocked selector when Ctrl+C
        # arrives, so route signals through the self-pipe, which the worker
        # watches, the way the proactor loop does.
        restore = None
        if sys.platform == 'win32' and threading.current_thread() is threading.main_thread() and self._csock:
            try:
                restore = signal.set_wakeup_fd(self._csock.fileno())
            except (ValueError, OSError):
                restore = None
        try:
            super().run_forever()
        finally:
            if restore is not None:
                try:
                    signal.set_wakeup_fd(restore)
                except (ValueError, OSError):
                    pass

    def close(self):
        super().close()
        try:
            self.worker.close()
        except RuntimeError:
            pass


def new_event_loop() -> WeftEventLoop:
    return WeftEventLoop()
