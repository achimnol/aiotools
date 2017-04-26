import asyncio
import logging
import multiprocessing as mp
import os
import signal
import sys
from typing import Any, Iterable, Optional

from .context import AbstractAsyncContextManager

__all__ = (
    'start_server',
)


def _worker_main(server_ctxmgr, stop_signals, proc_idx, args=None):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    log = logging.getLogger(__name__)

    if args is None:
        args = tuple()

    def _handle_term_signal():
        loop.stop()

    async def _server():
        for signum in stop_signals:
            signal.signal(signum, signal.SIG_IGN)
            loop.add_signal_handler(signum, _handle_term_signal)
        async with server_ctxmgr(loop, proc_idx, args):
            yield

    try:
        server = _server()
        loop.run_until_complete(server.__anext__())
        try:
            loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            pass
        try:
            loop.run_until_complete(server.__anext__())
        except StopAsyncIteration:
            loop.run_until_complete(loop.shutdown_asyncgens())
        else:
            raise RuntimeError('should not happen')  # pragma: no cover
    finally:
        loop.close()


def start_server(server_ctxmgr: AbstractAsyncContextManager,
                 stop_signals: Iterable[signal.Signals]=(signal.SIGINT, signal.SIGTERM),
                 num_proc: int=1,
                 args: Optional[Iterable[Any]]=None):

    if num_proc > 1:
        children = []

        def _main_sig_handler(signum, frame):
            # propagate signal to children
            for p in children:
                p.terminate()

        for signum in stop_signals:
            signal.signal(signum, _main_sig_handler)

        for i in range(num_proc):
            p = mp.Process(target=_worker_main,
                           args=(server_ctxmgr, stop_signals, i, args))
            p.start()
            children.append(p)
        for i in range(num_proc):
            children[i].join()
    else:
        _worker_main(server_ctxmgr, stop_signals, 0, args)
