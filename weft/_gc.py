"""Garbage-collector tuning applied once a worker is ready to serve.

Everything a FastAPI application allocates while it is being imported and
started up - Starlette's routing table and middleware stack, pydantic's
compiled validators, the dependency graph FastAPI builds per route - is
long-lived and can never become garbage. The cyclic collector nonetheless
re-traverses all of it on every generation-2 pass, which on a large app is
tens of thousands of objects walked for nothing, repeatedly, while requests
are being served.

`gc.freeze()` moves everything currently tracked into a permanent generation
that is never traversed again. The cycles a request legitimately creates (a
Task and its coroutine, Starlette's exception-handler closures) are allocated
afterwards, stay in the young generations and are still collected exactly as
before. Nothing an application can observe changes; only the collector's
workload does.

`WEFT_GC_TUNE=0` disables this. `WEFT_GC_THRESHOLD=<n>` additionally raises the
generation-0 threshold (default CPython value is 700), trading peak memory for
fewer collections; it is opt-in because it is a trade rather than a win.
"""

import gc
import os

from .log import logger


def tune() -> None:
    """Freeze the startup object graph. Safe to call more than once."""
    if os.environ.get('WEFT_GC_TUNE') == '0':
        return
    try:
        # settle anything still collectable from import/startup, then take it
        # out of the collector's view for good
        gc.collect()
        gc.freeze()
    except Exception:  # noqa: BLE001 - never let tuning break a worker
        logger.debug('GC tuning skipped', exc_info=True)
        return

    threshold = os.environ.get('WEFT_GC_THRESHOLD')
    if threshold:
        try:
            gen0 = int(threshold)
        except ValueError:
            logger.warning('Ignoring invalid WEFT_GC_THRESHOLD=%s', threshold)
            return
        if gen0 > 0:
            _, gen1, gen2 = gc.get_threshold()
            gc.set_threshold(gen0, gen1, gen2)
