from ._alloc import tune as _tune_alloc


# mimalloc reads its options on the extension's first allocation.
_tune_alloc()

from ._weft import ClientDisconnected, __version__  # noqa: E402
from .config import Config  # noqa: E402
from .loop import WeftEventLoop, new_event_loop  # noqa: E402


def run(app, **options) -> int:
    """Serves `app` (an object, or ``"module:attribute"``) until stopped."""
    from .server import run as _run

    return _run(Config(app=app, **options))


__all__ = ['ClientDisconnected', 'Config', 'WeftEventLoop', '__version__', 'new_event_loop', 'run']
