"""
Based on :doc:`aiotools.context`, this module provides an automated lifecycle
management for multi-process servers with explicit initialization steps and
graceful shutdown steps.

.. function:: server(func)

    A decorator wrapper for :class:`AsyncServerContextManager`.

    Usage example:

    .. code:: python

       @aiotools.server
       async def myserver(loop, pidx, args):
           await do_init(args)
           stop_sig = yield
           if stop_sig == signal.SIGINT:
               await do_graceful_shutdown()
           else:
               await do_forced_shutdown()

       aiotools.start_server(myserver, ...)
"""

import asyncio
from contextlib import (
    AbstractContextManager, ContextDecorator,
)
import functools
import inspect
import logging
import multiprocessing as mp, multiprocessing.synchronize
import threading
import os
import signal
import struct
import sys
import time
from typing import (
    Any, Optional, Union,
    Callable, Iterable,
    Tuple, Sequence,
    Set,
)

from .context import AbstractAsyncContextManager

if sys.version_info < (3, 8, 0):
    from typing_extensions import Literal
else:
    from typing import Literal  # type: ignore

__all__ = (
    'main',
    'start_server',
    'AsyncServerContextManager',
    'ServerMainContextManager',
    'InterruptedBySignal',
)

log = logging.getLogger(__name__)

# for threaded mode
_children_ctxs = []
_children_loops = []
_children_ffs = []
_children_lock = threading.Lock()


class InterruptedBySignal(BaseException):
    """
    A new :class:`BaseException` that represents interruption by an arbitrary UNIX
    signal.

    Since this is a :class:`BaseException` instead of :class:`Exception`, it behaves
    like :class:`KeyboardInterrupt` and :class:`SystemExit` exceptions (i.e.,
    bypassing except clauses catching the :class:`Exception` type only)

    The first argument of this exception is the signal number received.
    """
    pass


class AsyncServerContextManager(AbstractAsyncContextManager):
    """
    A modified version of :func:`contextlib.asynccontextmanager`.

    The implementation detail is mostly taken from the ``contextlib`` standard
    library, with a minor change to inject ``self.yield_return`` into the wrapped
    async generator.
    """

    yield_return: Optional[signal.Signals]

    def __init__(self, func: Callable[..., Any], args, kwargs):
        if not inspect.isasyncgenfunction(func):
            raise RuntimeError('Context manager function must be '
                               'an async-generator')
        self._agen = func(*args, **kwargs)
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.yield_return = None

    async def __aenter__(self):
        try:
            return (await self._agen.__anext__())
        except StopAsyncIteration:
            raise RuntimeError("async-generator didn't yield") from None

    async def __aexit__(self, exc_type, exc_value, tb):
        if exc_type is None:
            try:
                # Here is the modified part.
                await self._agen.asend(self.yield_return)
            except StopAsyncIteration:
                return
            else:
                raise RuntimeError("async-generator didn't stop") from None
        else:
            if exc_value is None:
                exc_value = exc_type()
            try:
                await self._agen.athrow(exc_type, exc_value, tb)
                raise RuntimeError("async-generator didn't stop after athrow()")
            except StopAsyncIteration as exc_new_value:
                return exc_new_value is not exc_value
            except RuntimeError as exc_new_value:
                if exc_new_value is exc_value:
                    return False
                if isinstance(exc_value, (StopIteration, StopAsyncIteration)):
                    if exc_new_value.__cause__ is exc_value:
                        return False
                raise
            except (BaseException, asyncio.CancelledError) as exc:
                if exc is not exc_value:
                    raise


class ServerMainContextManager(AbstractContextManager,
                               ContextDecorator):
    """
    A modified version of :func:`contextlib.contextmanager`.

    The implementation detail is mostly taken from the ``contextlib`` standard
    library, with a minor change to inject ``self.yield_return`` into the wrapped
    generator.
    """

    yield_return: Optional[signal.Signals]

    def __init__(self, func, args, kwargs):
        self.gen = func(*args, **kwargs)
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.yield_return = None

    def __enter__(self):
        del self.args, self.kwargs, self.func
        try:
            return next(self.gen)
        except StopIteration:
            raise RuntimeError("generator didn't yield") from None

    def __exit__(self, type, value, traceback):
        if type is None:
            try:
                self.gen.send(self.yield_return)
            except StopIteration:
                return False
            else:
                raise RuntimeError("generator didn't stop")
        else:
            if value is None:
                value = type()
            try:
                self.gen.throw(type, value, traceback)
            except StopIteration as exc:
                return exc is not value
            except RuntimeError as exc:
                if exc is value:
                    return False
                if type is StopIteration and exc.__cause__ is value:
                    return False
                raise
            except:  # noqa
                if sys.exc_info()[1] is value:
                    return False
                raise
            raise RuntimeError("generator didn't stop after throw()")


# This is a dirty hack to implement "module callable".
# NOTE: only works in Python 3.5 or higher.

def _server_ctxmgr(func):
    @functools.wraps(func)
    def helper(*args, **kwargs):
        return AsyncServerContextManager(func, args, kwargs)
    return helper


class _ServerModule(sys.modules[__name__].__class__):  # type: ignore
    def __call__(self, func):
        return _server_ctxmgr(func)


sys.modules[__name__].__class__ = _ServerModule


def _main_ctxmgr(func):
    """
    A decorator wrapper for :class:`ServerMainContextManager`

    Usage example:

    .. code:: python

       @aiotools.main
       def mymain():
           server_args = do_init()
           stop_sig = yield server_args
           if stop_sig == signal.SIGINT:
               do_graceful_shutdown()
           else:
               do_forced_shutdown()

       aiotools.start_server(..., main_ctxmgr=mymain, ...)
    """
    @functools.wraps(func)
    def helper(*args, **kwargs):
        return ServerMainContextManager(func, args, kwargs)
    return helper


main = _main_ctxmgr


def _worker_main(
        worker_actxmgr: Callable[
            [asyncio.AbstractEventLoop, int, Sequence[Any]],
            AsyncServerContextManager],
        threaded: bool,
        stop_signals: Set[signal.Signals],
        intr_pipe: Union[threading.Event, mp.synchronize.Event],
        proc_idx: int,
        args: Sequence[Any]):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    interrupted = asyncio.Event()
    ctx = worker_actxmgr(loop, proc_idx, args)
    forever_future = loop.create_future()

    def handle_stop_signal(signum):
        if interrupted.is_set():
            pass
        else:
            interrupted.set()
            ctx.yield_return = signum
            forever_future.cancel()

    if not threaded:
        for signum in stop_signals:
            loop.add_signal_handler(
                signum,
                functools.partial(handle_stop_signal, signum))
        # Allow the worker to be interrupted during initialization
        # (in case of initialization failures in other workers)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
    else:
        with _children_lock:
            _children_ctxs.append(ctx)
            _children_loops.append(loop)
            _children_ffs.append(forever_future)

    async def _wrapped_worker():
        err_ctx = 'enter'
        try:
            async with ctx:
                err_ctx = 'body'
                try:
                    await forever_future
                except asyncio.CancelledError:
                    pass
                finally:
                    err_ctx = 'exit'
        except Exception:
            if err_ctx != 'body':
                err_ctx_str = 'initialization' if err_ctx == 'enter' else 'shutdown'
                log.exception(f'Worker {proc_idx}: '
                              f'Error during context manager {err_ctx_str}')
                wfd = intr_pipe if threaded else intr_pipe.fileno()
                os.write(wfd, struct.pack('i', proc_idx))
                # FIXME: this sleep is required to prevent hang-up in Linux
                time.sleep(0.2)
            raise

    try:
        loop.run_until_complete(_wrapped_worker())
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()


def _extra_main(extra_func, threaded, stop_signals, intr_event, proc_idx, args):
    interrupted: Union[threading.Event, mp.synchronize.Event]
    if threaded:
        interrupted = threading.Event()
    else:
        interrupted = mp.Event()
    if not threaded:

        # Since signals only work for the main thread in Python,
        # extra processes in use_threading=True mode should check
        # the intr_event by themselves (probably in their loops).

        def raise_stop(signum, frame):
            if interrupted.is_set():
                pass
            else:
                interrupted.set()
                if signum == signal.SIGINT:
                    raise KeyboardInterrupt
                elif signum == signal.SIGTERM:
                    raise SystemExit
                else:
                    raise InterruptedBySignal(signum)

        # restore signal handler.
        for signum in stop_signals:
            signal.signal(signum, raise_stop)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
        intr_event = None

    try:
        if not interrupted.is_set():
            extra_func(intr_event, proc_idx, args)
    except (SystemExit, KeyboardInterrupt, InterruptedBySignal):
        log.warning(f'extra_proc[{proc_idx}] did not handle stop signals.')
    finally:
        if not threaded:
            # same as in _worker_main()
            signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)


def start_server(
        worker_actxmgr: Callable[
            [asyncio.AbstractEventLoop, int, Sequence[Any]],
            AsyncServerContextManager],
        main_ctxmgr: Optional[Callable[[], ServerMainContextManager]] = None,
        extra_procs: Iterable[Callable] = tuple(),
        stop_signals: Iterable[signal.Signals] = (
            signal.SIGINT,
            signal.SIGTERM),
        num_workers: int = 1,
        use_threading: bool = False,
        start_method: Literal['spawn', 'fork', 'forkserver'] = None,
        args: Iterable[Any] = tuple()):
    """
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
                     a :class:`BaseException` depending on the received stop signal
                     number, either :class:`KeyboardInterrupt` (for SIGINT),
                     :class:`SystemExit` (for SIGTERM), or
                     :class:`InterruptedBySignal` (otherwise).

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

        start_method: Change the start method when :mod:`multiprocessing` is used.
                      The default is same to what :mod:`multiprocessing` uses.
                      Only effective when **use_threading** is ``False``.

                      Note that if there are other libraries that rely on
                      :mod:`multiprocessing` called before aiotools, you need to
                      invoke :func:`multiprocessing.set_start_method()` earlier than
                      both aiotools and such libraries *without* setting this
                      argument as it changes the global context of
                      :mod:`multiprocessing`.

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

    .. versionchanged:: 0.8.0

       Now **worker_actxmgr** must be an instance of
       :class:`AsyncServerContextManager` or async generators decorated by
       ``@aiotools.server``.

       Now **main_ctxmgr** must be an instance of :class:`ServerMainContextManager`
       or plain generators decorated by ``@aiotools.main``.

       The usage is same to asynchronous context managers, but optionally you can
       distinguish the received stop signal by retrieving the return value of the
       ``yield`` statement.

       In **extra_procs** in non-threaded mode, stop signals are converted into
       either one of :class:`KeyboardInterrupt`, :class:`SystemExit`, or
       :class:`InterruptedBySignal` exception.

    .. versionadded:: 0.8.4

       **start_method** argument can be set to change the subprocess spawning
       implementation.
    """

    @_main_ctxmgr
    def noop_main_ctxmgr():
        yield

    def create_child(*args, **kwargs):
        if use_threading:
            return threading.Thread(*args, **kwargs)
        else:
            return mp.Process(*args, **kwargs)

    assert stop_signals

    if hasattr(asyncio, 'get_running_loop'):
        # only for Python 3.7+
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # there should be none.
            pass
        else:
            raise RuntimeError(
                'aiotools.start_server() cannot be called inside '
                'a running event loop.')

    if main_ctxmgr is None:
        main_ctxmgr = noop_main_ctxmgr
    if not use_threading and start_method is not None:
        mp.set_start_method(start_method)

    children = []
    _children_ctxs.clear()
    _children_loops.clear()
    _children_ffs.clear()
    intr_event: Union[threading.Event, mp.synchronize.Event]
    if use_threading:
        intr_event = threading.Event()
    else:
        intr_event = mp.Event()
    sigblock_mask = frozenset(stop_signals)

    main_ctx = main_ctxmgr()

    # temporarily block signals and register signal handlers to mainloop
    signal.pthread_sigmask(signal.SIG_BLOCK, sigblock_mask)

    mainloop = asyncio.new_event_loop()
    asyncio.set_event_loop(mainloop)

    # to make subprocess working in child threads
    try:
        asyncio.get_child_watcher()
    except NotImplementedError:
        pass  # for uvloop

    # build a main-to-worker interrupt channel using signals
    def handle_stop_signal(signum: signal.Signals) -> None:
        main_ctx.yield_return = signum  # type: ignore
        if use_threading:
            with _children_lock:
                for c in _children_ctxs:
                    c.yield_return = signum
                for child_loop, ff in zip(_children_loops, _children_ffs):
                    child_loop.call_soon_threadsafe(ff.cancel)
            intr_event.set()
        else:
            os.killpg(0, signum)
        mainloop.stop()

    for signum in stop_signals:
        mainloop.add_signal_handler(
            signum,
            functools.partial(handle_stop_signal, signum))

    # build a reliable worker-to-main interrupt channel using a pipe
    # (workers have no idea whether the main interrupt is enabled/disabled)
    def handle_child_interrupt(fd: int) -> None:
        child_idx = struct.unpack('i', os.read(fd, 4))[0]  # noqa
        log.debug(f'Child {child_idx} has interrupted the main program.')
        # self-interrupt to initiate the main-to-worker interrupts
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
        os.kill(0, signal.SIGINT)

    child_intr_pipe: Union[Tuple[int, int],
                           Tuple[mp.connection.Connection, mp.connection.Connection]]
    if use_threading:
        child_intr_pipe = os.pipe()
        rfd = child_intr_pipe[0]
    else:
        child_intr_pipe = mp.Pipe()
        rfd = child_intr_pipe[0].fileno()
    mainloop.add_reader(rfd, handle_child_interrupt, rfd)

    # start
    with main_ctx as main_args:

        # retrieve args generated by the user-defined main
        if main_args is None:
            main_args = tuple()
        if not isinstance(main_args, tuple):
            main_args = (main_args, )

        # spawn managed async workers
        for i in range(num_workers):
            p = create_child(target=_worker_main, daemon=True,
                             args=(worker_actxmgr, use_threading, stop_signals,
                                   child_intr_pipe[1], i,
                                   main_args + args))
            p.start()
            children.append(p)

        # spawn extra workers
        for i, f in enumerate(extra_procs):
            p = create_child(target=_extra_main, daemon=True,
                             args=(f, use_threading, stop_signals,
                                   intr_event, num_workers + i,
                                   main_args + args))
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
            try:
                mainloop.run_until_complete(mainloop.shutdown_asyncgens())
            finally:
                mainloop.close()
                asyncio.set_event_loop(None)
