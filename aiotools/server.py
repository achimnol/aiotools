import asyncio
from contextlib import AbstractContextManager, contextmanager
import multiprocessing as mp
import os
import signal
import sys
from typing import Any, Callable, Iterable, Optional

from .context import AbstractAsyncContextManager

__all__ = (
    'start_server',
)


def _worker_main(worker_actxmgr, stop_signals, proc_idx, args):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    interrupted = False

    def _handle_term_signal():
        nonlocal interrupted
        if not interrupted:
            loop.stop()
            interrupted = True

    async def _work():
        for signum in stop_signals:
            signal.signal(signum, signal.SIG_IGN)
            loop.add_signal_handler(signum, _handle_term_signal)
        async with worker_actxmgr(loop, proc_idx, args):
            yield

    try:
        task = _work()
        loop.run_until_complete(task.__anext__())
        try:
            loop.run_forever()
        except (SystemExit, KeyboardInterrupt):
            pass
        try:
            loop.run_until_complete(task.__anext__())
        except StopAsyncIteration:
            loop.run_until_complete(loop.shutdown_asyncgens())
        else:
            raise RuntimeError('should not happen')  # pragma: no cover
    finally:
        loop.close()


def _extra_main(main_ctxmgr, stop_signals, proc_idx, args):

    def _handle_term_signal(signum, frame):
        sys.exit(0)

    for signum in stop_signals:
        signal.signal(signum, _handle_term_signal)

    try:
        main_ctxmgr(proc_idx, args)
    except SystemExit:
        pass


def start_server(worker_actxmgr: AbstractAsyncContextManager,
                 main_ctxmgr: Optional[AbstractContextManager]=None,
                 extra_procs: Iterable[Callable]=tuple(),
                 stop_signals: Iterable[signal.Signals]=(signal.SIGINT, ),
                 num_workers: int=1,
                 args: Iterable[Any]=tuple()):

    @contextmanager
    def noop_main_ctxmgr():
        yield

    if main_ctxmgr is None:
        main_ctxmgr = noop_main_ctxmgr

    children = []

    def _main_sig_handler(signum, frame):
        # propagate signal to children
        for p in children:
            os.kill(p.pid, signal.SIGINT)

    for signum in stop_signals:
        signal.signal(signum, _main_sig_handler)

    with main_ctxmgr() as main_args:
        if main_args is None:
            main_args = tuple()
        if not isinstance(main_args, tuple):
            main_args = (main_args, )
        for i in range(num_workers):
            p = mp.Process(target=_worker_main, daemon=True,
                           args=(worker_actxmgr, stop_signals, i, main_args + args))
            p.start()
            children.append(p)
        for i, f in enumerate(extra_procs):
            p = mp.Process(target=_extra_main, daemon=True,
                           args=(f, stop_signals, num_workers + i, main_args + args))
            p.start()
            children.append(p)
        for child in children:
            child.join()
