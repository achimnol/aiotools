import asyncio
import logging
import multiprocessing as mp
import os
import signal
import sys

from .context import AbstractAsyncContextManager

__all__ = (
    'start_server',
)


def _worker_main(server_ctxmgr, proc_idx, args=None):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    term_ev = asyncio.Event(loop=loop)
    log = logging.getLogger(__name__)
    term_signals = (signal.SIGINT, signal.SIGTERM)
    if args is None:
        args = tuple()

    def _handle_term_signal():
        if term_ev.is_set():
            log.warning('forced shutdown')
            sys.exit(1)
        else:
            print('first shutdown')
            term_ev.set()
            loop.stop()

    async def _server():
        for sig in term_signals:
            loop.add_signal_handler(sig, _handle_term_signal)
        async with server_ctxmgr(loop, proc_idx, args):
            yield

    try:
        server = _server()
        loop.run_until_complete(server.__anext__())
        try:
            loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            # Emulate real signals.
            print('emulated interrupt')
            term_ev.set()
        try:
            loop.run_until_complete(server.__anext__())
        except StopAsyncIteration:
            loop.run_until_complete(loop.shutdown_asyncgens())
        else:
            raise RuntimeError('should not happen')  # pragma: no cover
    finally:
        loop.close()


def start_server(server_ctxmgr: AbstractAsyncContextManager,
                 num_proc: int=1,
                 args=None):

    if num_proc > 1:
        children = []

        def _main_sig_handler(signum, frame):
            # propagate signal to children
            for p in children:
                p.terminate()

        signal.signal(signal.SIGINT, _main_sig_handler)
        signal.signal(signal.SIGTERM, _main_sig_handler)

        for i in range(num_proc):
            p = mp.Process(target=_worker_main,
                           args=(server_ctxmgr, i, args))
            p.start()
            children.append(p)
        for i in range(num_proc):
            children[i].join()
    else:
        _worker_main(server_ctxmgr, 0, args)
