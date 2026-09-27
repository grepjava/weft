"""WSGI support: `wsgi.file_wrapper`, and telling WSGI applications from ASGI ones."""

from __future__ import annotations

import inspect


class FileWrapper:
    """`environ['wsgi.file_wrapper']`: iterates a file-like object in blocks."""

    __slots__ = ('filelike', 'blksize')

    def __init__(self, filelike, blksize: int = 64 * 1024):
        self.filelike = filelike
        self.blksize = blksize

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        data = self.filelike.read(self.blksize)
        if data:
            return data
        raise StopIteration

    def close(self) -> None:
        close = getattr(self.filelike, 'close', None)
        if close is not None:
            close()


def _positional(fn) -> int | None:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return None
    n = 0
    for p in sig.parameters.values():
        if p.kind is p.VAR_POSITIONAL:
            return None
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD):
            n += 1
    return n


def detect(app) -> str:
    """``'asgi'`` or ``'wsgi'``: a coroutine function, or three parameters,
    is ASGI; two is WSGI. Anything unclear is taken to be ASGI."""
    call = app if inspect.isroutine(app) or inspect.isclass(app) else getattr(app, '__call__', app)
    if inspect.iscoroutinefunction(call) or inspect.iscoroutinefunction(app):
        return 'asgi'
    return 'wsgi' if _positional(app) == 2 else 'asgi'
