import asyncio
import logging
import os
import signal
import sys

from .context import AbstractAsyncContextManager

__all__ = (
    'start_server',
)


def start_server(server_ctxmgr: AbstractAsyncContextManager,
                 num_procs: int=1,
                 term_signals=None,
                 logger=None,
                 loop=None):

    loop = asyncio.get_event_loop()
    term_ev = asyncio.Event()
    if not logger:
        logger = logging.getLogger(__name__)
    if not term_signals:
        term_signals = (signal.SIGINT, signal.SIGTERM)

    def _handle_term_signal():
        if term_ev.is_set():
            logger.warning('forced shutdown')
            sys.exit(1)
        else:
            term_ev.set()
            loop.stop()

    async def _server():
        for sig in term_signals:
            loop.add_signal_handler(sig, _handle_term_signal)
        try:
            async with server_ctxmgr(loop):
                yield
        finally:
            await loop.shutdown_asyncgens()

    try:
        server = _server()
        loop.run_until_complete(server.__anext__())
        loop.run_forever()
        try:
            loop.run_until_complete(server.__anext__())
        except StopAsyncIteration:
            pass
        else:
            raise RuntimeError('should not happen')
    finally:
        loop.close()
