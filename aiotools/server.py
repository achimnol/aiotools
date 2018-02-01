'''
Based on :doc:`aiotools.context`, this module provides an automated lifecycle
management for multi-process servers with explicit initialization steps and
graceful shutdown steps.
'''

import asyncio
from contextlib import AbstractContextManager, contextmanager
import logging
import multiprocessing as mp
import threading
import os
import signal
import struct
import sys
import time
from typing import Any, Callable, Iterable, Optional

from .context import AbstractAsyncContextManager

__all__ = (
    'start_server',
)

log = logging.getLogger(__name__)

# for threaded mode
_children_loops = []
_children_lock = threading.Lock()


def _worker_main(worker_actxmgr, threaded, intr_pipe, proc_idx, args):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    interrupted = asyncio.Event(loop=loop)

    def handle_intr_signal():
        if interrupted.is_set():
            pass
        else:
            interrupted.set()
            loop.stop()

    if not threaded:
        loop.add_signal_handler(signal.SIGINT, handle_intr_signal)
        # Allow the worker to be interrupted during initialization
        # (in case of initialization failures in other workers)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
    else:
        with _children_lock:
            _children_loops.append(loop)
    try:
        ctx = worker_actxmgr(loop, proc_idx, args)
        try:
            loop.run_until_complete(ctx.__aenter__())
        except Exception:
            log.exception(f'Worker {proc_idx}: Error during initialization')
            wfd = intr_pipe if threaded else intr_pipe.fileno()
            os.write(wfd, struct.pack('i', proc_idx))
            # this sleep is necessary to prevent hang-up in Linux
            time.sleep(0.2)
        else:
            try:
                loop.run_forever()
            except Exception:
                loop.run_until_complete(ctx.__aexit__(*sys.exc_info()))
            else:
                loop.run_until_complete(ctx.__aexit__(None, None, None))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def _extra_main(extra_func, threaded, intr_event, proc_idx, args):
    interrupted = threading.Event()
    if not threaded:

        def raise_kbdintr(signum, frame):
            if interrupted.is_set():
                pass
            else:
                interrupted.set()
                raise KeyboardInterrupt

        # restore signal handler.
        signal.signal(signal.SIGINT, raise_kbdintr)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
        intr_event = None

    try:
        if not interrupted.is_set():
            extra_func(intr_event, proc_idx, args)
    except (SystemExit, KeyboardInterrupt):
        pass
    finally:
        if not threaded:
            # same as in _worker_main()
            signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})


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
    sigblock_mask = {signal.SIGINT}
    for signum in stop_signals:
        sigblock_mask.add(signum)

    # temporarily block signals and register signal handlers to mainloop
    signal.pthread_sigmask(signal.SIG_BLOCK, sigblock_mask)

    old_loop = asyncio.get_event_loop()
    mainloop = asyncio.new_event_loop()
    asyncio.set_event_loop(mainloop)

    # to make subprocess working in child threads
    try:
        asyncio.get_child_watcher()
    except NotImplementedError:
        pass  # for uvloop

    # build a main-to-worker interrupt channel using signals
    def handle_stop_signal():
        if use_threading:
            with _children_lock:
                for l in _children_loops:
                    l.call_soon_threadsafe(l.stop)
            intr_event.set()
        else:
            os.killpg(0, signal.SIGINT)
        mainloop.stop()

    for signum in stop_signals:
        mainloop.add_signal_handler(signum, handle_stop_signal)

    # build a reliable worker-to-main interrupt channel using a pipe
    # (workers have no idea whether the main interrupt is enabled/disabled)
    def handle_child_interrupt(fd):
        child_idx = struct.unpack('i', os.read(fd, 4))[0]  # noqa
        log.debug(f'Child {child_idx} has interrupted the main program.')
        # self-interrupt to initiate the main-to-worker interrupts
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
        os.kill(0, signal.SIGINT)

    if use_threading:
        child_intr_pipe = os.pipe()
        rfd = child_intr_pipe[0]
    else:
        child_intr_pipe = mp.Pipe()
        rfd = child_intr_pipe[0].fileno()
    mainloop.add_reader(rfd, handle_child_interrupt, rfd)

    # start
    with main_ctxmgr() as main_args:

        # retrieve args generated by the user-defined main
        if main_args is None:
            main_args = tuple()
        if not isinstance(main_args, tuple):
            main_args = (main_args, )

        # spawn managed async workers
        for i in range(num_workers):
            p = create_child(target=_worker_main, daemon=True,
                             args=(worker_actxmgr, use_threading,
                                   child_intr_pipe[1], i, main_args + args))
            p.start()
            children.append(p)

        # spawn extra workers
        for i, f in enumerate(extra_procs):
            p = create_child(target=_extra_main, daemon=True,
                             args=(f, use_threading, intr_event,
                                   num_workers + i, main_args + args))
            p.start()
            children.append(p)
        try:
            # unblock the stop signals for user/external interrupts.
            signal.pthread_sigmask(signal.SIG_UNBLOCK, sigblock_mask)

            # run!
            mainloop.run_forever()

            # if interrupted, wait for workers to finish.
            for child in children:
                child.join()
        finally:
            mainloop.close()
            asyncio.set_event_loop(old_loop)
