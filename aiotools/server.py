'''
Based on :doc:`aiotools.context`, this module provides an automated lifecycle
management for multi-process servers with explicit initialization steps and
graceful shutdown steps.
'''

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
                 stop_signals: Iterable[signal.Signals]=(
                     signal.SIGINT,
                     signal.SIGTERM),
                 num_workers: int=1,
                 args: Iterable[Any]=tuple()):
    '''
    Starts a multi-process server where each process has their own individual
    asyncio event loop.  Their lifecycles are automantically managed -- if the
    main program receives one of the signals specified in ``stop_signals`` it
    will initiate the shutdown routines on each worker that stops the event
    loop gracefully.

    Args:
        worker_actxmgr: An asynchronous context manager that dicates the
                        initialization and shutdown steps of each worker.
                        It should accept three arguments:

                        * ``loop``: the asyncio event loop created and set
                          by aiotools
                        * ``pidx``: the 0-based index of the worker process
                          (use this for per-worker logging)
                        * ``args``: a concatenated tuple of values yielded by
                          **main_ctxmgr** and the user-defined arguments in
                          **args**.

        main_ctxmgr: An optional context manager that performs global
                     initialization and shutdown steps of the whole program.
                     It may yield one or more values to be passed to worker
                     processes along with **args** passed to this function.
                     There is no arguments passed to those functions since
                     you can directly access ``sys.argv`` to parse command
                     line arguments and/or read user configuratinos.

        extra_procs: An iterable of functions that consist of extra processes
                     whose lifecycles are synchronized with other workers.
                     Normally you would put synchronous I/O loops into here
                     which can be interrupted by SIGINT.
                     It should accept the same set of arguments as
                     **worker_actxmgr** does.

        stop_signals: A list of UNIX signals that the main program to
                      recognize as termination signals.
                      Note that worker processes *only* receive SIGINT
                      regardless of this argument.

        num_workers: The number of children workers.

        args: The user-defined arguments passed to workers and extra
              processes.  If **main_ctxmgr** yields one or more values,
              they are *prepended* to this user arguments when passed to
              workers and extra processes.

    Returns:
        None

    .. versionchanged:: 0.3.2

       The name of argument ``num_proc`` is changed to ``num_workers``.

    .. versionadded:: 0.3.2

       The argument ``extra_procs`` and ``main_ctxmgr``.
    '''

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
