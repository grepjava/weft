import logging
import os


logger = logging.getLogger('weft')

LEVELS = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG,
}


def configure(level: str = 'info') -> None:
    """Gives the `weft` logger a stderr handler unless one is configured."""
    logger.setLevel(LEVELS.get(level, logging.INFO))
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(f'[%(asctime)s] [{os.getpid()}] %(levelname)s %(message)s'))
    logger.addHandler(handler)
    logger.propagate = False


def report_app_error(exc: BaseException) -> None:
    logger.error('Exception in ASGI application', exc_info=exc)
