'''
Based on :doc:`aiotools.context`, this module provides an automated lifecycle
management for multi-process servers with explicit initialization steps and
graceful shutdown steps.
'''

import asyncio
from contextlib import AbstractContextManager, contextmanager
import multiprocessing as mp
import threading
import os
import signal
from typing import Any, Callable, Iterable, Optional

from .context import AbstractAsyncContextManager

__all__ = (
    'start_server',
)

# for threaded mode
_children_loops = []
_children_lock = threading.Lock()


def _worker_main(worker_actxmgr, threaded, proc_idx, args):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    interrupted = False
    if threaded:
        with _children_lock:
            _children_loops.append(loop)

    def _handle_term_signal():
        nonlocal interrupted
        if not interrupted:
            loop.stop()
            interrupted = True

    async def _work():
        if not threaded:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            loop.add_signal_handler(signal.SIGINT, _handle_term_signal)
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


def _extra_main(extra_func, threaded, intr_event, proc_idx, args):
    if not threaded:

        _interrupted = False

        def raise_kbdintr(signum, frame):
            nonlocal _interrupted
            if not _interrupted:
                _interrupted = True
                raise KeyboardInterrupt

        signal.signal(signal.SIGINT, raise_kbdintr)
        intr_event = None
    try:
        extra_func(intr_event, proc_idx, args)
    except SystemExit:
        pass


def start_server(worker_actxmgr: AbstractAsyncContextManager,
                 main_ctxmgr: Optional[AbstractContextManager]=None,
                 extra_procs: Iterable[Callable]=tuple(),
                 stop_signals: Iterable[signal.Signals]=(
                     signal.SIGINT,
                     signal.SIGTERM),
                 num_workers: int=1,
                 use_threading: bool=False,
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
                        It should accept the following three arguments:

                        * **loop**: the asyncio event loop created and set
                          by aiotools
                        * **pidx**: the 0-based index of the worker
                          (use this for per-worker logging)
                        * **args**: a concatenated tuple of values yielded by
                          **main_ctxmgr** and the user-defined arguments in
                          **args**.

                        aiotools automatically installs an interruption handler
                        that calls ``loop.stop()`` to the given event loop,
                        regardless of using either threading or
                        multiprocessing.

        main_ctxmgr: An optional context manager that performs global
                     initialization and shutdown steps of the whole program.
                     It may yield one or more values to be passed to worker
                     processes along with **args** passed to this function.
                     There is no arguments passed to those functions since
                     you can directly access ``sys.argv`` to parse command
                     line arguments and/or read user configurations.

        extra_procs: An iterable of functions that consist of extra processes
                     whose lifecycles are synchronized with other workers.

                     You should write the shutdown steps of them differently
                     depending on the value of **use_threading** argument.

                     If it is ``False`` (default), they will get
                     :class:`KeyboardInterrupt` like normal Python codes in the
                     main thread upon arrival of a stop signal to the main
                     program.

                     If it is ``True``, they should check their **intr_event**
                     argument periodically because there is no way to install
                     signal handlers in Python threads (only the main thread
                     can install signal handlers).

                     It should accept the following three arguments:

                     * **intr_event**: :class:`threading.Event` object that
                       signals the interruption of the main thread (only
                       available when **use_threading** is ``True``; otherwise
                       it is set to ``None``)
                     * **pidx**: same to **worker_actxmgr** argument
                     * **args**: same to **worker_actxmgr** argument

        stop_signals: A list of UNIX signals that the main program to
                      recognize as termination signals.

        num_workers: The number of children workers.

        use_threading: Use :mod:`threading` instead of :mod:`multiprocessing`.
                       In this case, the GIL may become the performance
                       bottleneck.  Set this ``True`` only when you know what
                       you are going to do.  Note that this changes the way
                       to write user-defined functions passed as **extra_procs**.

        args: The user-defined arguments passed to workers and extra
              processes.  If **main_ctxmgr** yields one or more values,
              they are *prepended* to this user arguments when passed to
              workers and extra processes.

    Returns:
        None

    .. versionchanged:: 0.3.2

       The name of argument **num_proc** is changed to **num_workers**.
       Even if **num_workers** is 1, a child is created instead of
       doing everything at the main thread.

    .. versionadded:: 0.3.2

       The argument ``extra_procs`` and ``main_ctxmgr``.

    .. versionadded:: 0.4.0

       Now supports use of threading instead of multiprocessing via
       **use_threading** option.
    '''

    @contextmanager
    def noop_main_ctxmgr():
        yield

    def create_child(*args, **kwargs):
        if use_threading:
            return threading.Thread(*args, **kwargs)
        else:
            return mp.Process(*args, **kwargs)

    assert stop_signals

    if main_ctxmgr is None:
        main_ctxmgr = noop_main_ctxmgr

    children = []
    _children_loops.clear()
    intr_event = threading.Event()

    mainloop = asyncio.new_event_loop()
    asyncio.set_event_loop(mainloop)

    # to make subprocess working in child threads
    try:
        asyncio.get_child_watcher()
    except NotImplementedError:
        pass  # for uvloop

    _main_stopped = False

    def _main_sig_handler():
        nonlocal _main_stopped
        if _main_stopped:
            return
        _main_stopped = True

        # propagate signal to children
        if use_threading:
            with _children_lock:
                for l in _children_loops:
                    l.call_soon_threadsafe(l.stop)
            intr_event.set()
        else:
            for p in children:
                os.kill(p.pid, signal.SIGINT)
        mainloop.stop()

    for signum in stop_signals:
        signal.signal(signum, signal.SIG_IGN)
        mainloop.add_signal_handler(signum, _main_sig_handler)

    with main_ctxmgr() as main_args:
        if main_args is None:
            main_args = tuple()
        if not isinstance(main_args, tuple):
            main_args = (main_args, )
        for i in range(num_workers):
            p = create_child(target=_worker_main, daemon=True,
                             args=(worker_actxmgr, use_threading,
                                   i, main_args + args))
            p.start()
            children.append(p)
        for i, f in enumerate(extra_procs):
            p = create_child(target=_extra_main, daemon=True,
                             args=(f, use_threading, intr_event,
                                   num_workers + i, main_args + args))
            p.start()
            children.append(p)
        try:
            mainloop.run_forever()
        finally:
            for child in children:
                child.join()
            mainloop.close()
