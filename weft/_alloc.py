"""Allocator defaults applied before the native extension is loaded.

Weft links mimalloc as its global allocator, which by default commits the
whole of each arena it reserves up front. That is the right trade for a
process that will go on to use the memory, and the wrong one for a web
server worker: a FastAPI application touches a few tens of megabytes and then
runs at a steady state for hours, so the eagerly committed remainder is
resident for the life of the process without ever being written to.

Turning eager commit off makes mimalloc commit arena pages on demand instead.
Measured on the FastAPI benchmark app (Linux, one worker), idle USS drops
from 79.0 MB to 65.2 MB - 13.8 MB, about 17% - for no change in throughput.
Pages that are actually used are committed exactly as before; only the
untouched tail of each arena stops being resident.

These are set as *defaults*: an explicit `MIMALLOC_*` in the environment
always wins, and `WEFT_ALLOC_TUNE=0` disables the mechanism entirely. They
must be applied before `weft._weft` is imported, because mimalloc reads its
options when it initialises, which happens on the extension's first
allocation.
"""

import os

#: mimalloc options set unless the environment already specifies them.
_DEFAULTS = {
    # commit arena pages on demand rather than reserving them committed
    'MIMALLOC_ARENA_EAGER_COMMIT': '0',
}


def tune() -> None:
    """Apply allocator defaults. Must run before the extension is imported."""
    if os.environ.get('WEFT_ALLOC_TUNE') == '0':
        return
    for key, value in _DEFAULTS.items():
        os.environ.setdefault(key, value)
