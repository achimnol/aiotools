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
try:
    import contextvars
    _cv_available = True
except ImportError:
    _cv_available = False
import functools
import inspect
import logging
import os
import signal
import struct
import sys
import threading
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from .compat import all_tasks, current_task, get_running_loop
from .context import AbstractAsyncContextManager
from .fork import AbstractChildProcess, afork

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type: ignore  # noqa

__all__ = (
    'main',
    'start_server',
    'process_index',
    'AsyncServerContextManager',
    'ServerMainContextManager',
    'InterruptedBySignal',
)

log = logging.getLogger(__name__)

if _cv_available:
    process_index: 'contextvars.ContextVar[int]'
    process_index = contextvars.ContextVar('process_index')
else:
    # Unsupported in Python 3.6
    process_index = None  # type: ignore  # noqa


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


def setup_child_watcher():
    try:
        asyncio.get_child_watcher()
        if hasattr(asyncio, 'PidfdChildWatcher'):
            asyncio.set_child_watcher(asyncio.PidfdChildWatcher())
    except NotImplementedError:
        pass  # for uvloop


async def cancel_all_tasks():
    loop = get_running_loop()
    cancelled_tasks = []
    for task in all_tasks():
        if not task.done() and task is not current_task():
            task.cancel()
            cancelled_tasks.append(task)
    await asyncio.gather(*cancelled_tasks, return_exceptions=True)
    for task in cancelled_tasks:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler({
                'message': 'unhandled exception during loop shutdown',
                'exception': task.exception(),
                'task': task,
            })


def _worker_main(
    worker_actxmgr: Callable[
        [asyncio.AbstractEventLoop, int, Sequence[Any]],
        AsyncServerContextManager,
    ],
    stop_signals: Set[signal.Signals],
    intr_pipe_wfd: int,
    proc_idx: int,
    args: Sequence[Any],
) -> int:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    setup_child_watcher()
    interrupted = asyncio.Event()
    ctx = worker_actxmgr(loop, proc_idx, args)
    if _cv_available:
        process_index.set(proc_idx)
    forever_future = loop.create_future()

    def handle_stop_signal(signum):
        if interrupted.is_set():
            pass
        else:
            interrupted.set()
            ctx.yield_return = signum
            forever_future.cancel()

    for signum in stop_signals:
        loop.add_signal_handler(
            signum,
            functools.partial(handle_stop_signal, signum),
        )
    # Allow the worker to be interrupted during initialization
    # (in case of initialization failures in other workers)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)

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
                os.write(intr_pipe_wfd, struct.pack('i', proc_idx))
            raise

    try:
        loop.run_until_complete(_wrapped_worker())
    finally:
        try:
            loop.run_until_complete(cancel_all_tasks())
            loop.run_until_complete(loop.shutdown_asyncgens())
            try:
                loop.run_until_complete(loop.shutdown_default_executor())
            except (AttributeError, NotImplementedError):  # for uvloop
                pass
        finally:
            loop.close()
        return 0


def _extra_main(
    extra_func,
    stop_signals: Set[signal.Signals],
    proc_idx: int,
    args: Sequence[Any],
) -> int:
    interrupted = threading.Event()
    if _cv_available:
        process_index.set(proc_idx)

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
        # same as in _worker_main()
        signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
        return 0


def start_server(
    worker_actxmgr: Callable[
        [asyncio.AbstractEventLoop, int, Sequence[Any]],
        AsyncServerContextManager],
    main_ctxmgr: Optional[Callable[[], ServerMainContextManager]] = None,
    extra_procs: Iterable[Callable] = tuple(),
    stop_signals: Iterable[signal.Signals] = (
        signal.SIGINT,
        signal.SIGTERM
    ),
    num_workers: int = 1,
    args: Iterable[Any] = tuple()
) -> None:
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
                     They should set up their own signal handlers.

                     It should accept the following three arguments:

                     * **intr_event**: Always ``None``, kept for legacy
                     * **pidx**: same to **worker_actxmgr** argument
                     * **args**: same to **worker_actxmgr** argument

        stop_signals: A list of UNIX signals that the main program to
                      recognize as termination signals.

        num_workers: The number of children workers.

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

    .. deprecated:: 1.2.0

       The **start_method** and **use_threading** arguments, in favor of our new
       :func:`afork()` function which provides better synchronization and pid-fd
       support.

    .. versionchanged:: 1.2.0

       The **extra_procs** will be always separate processes since **use_threading**
       is deprecated and thus **intr_event** arguments are now always ``None``.
    """

    @_main_ctxmgr
    def noop_main_ctxmgr():
        yield

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

    children: List[AbstractChildProcess] = []
    sigblock_mask = frozenset(stop_signals)

    main_ctx = main_ctxmgr()

    # temporarily block signals and register signal handlers to main_loop
    signal.pthread_sigmask(signal.SIG_BLOCK, sigblock_mask)

    main_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(main_loop)
    main_future = main_loop.create_future()

    # to make subprocess working in child threads
    setup_child_watcher()

    # build a main-to-worker interrupt channel using signals
    def handle_stop_signal(signum: signal.Signals) -> None:
        main_ctx.yield_return = signum  # type: ignore
        for child in children:
            child.send_signal(signum)
        # instead of main_loop.stop(), we use a future here
        # NOT to interrupt the cleanup routines in
        # an arbitrary timing upon async child failures.
        main_future.cancel()

    for signum in stop_signals:
        main_loop.add_signal_handler(
            signum,
            functools.partial(handle_stop_signal, signum),
        )

    # build a reliable worker-to-main interrupt channel using a pipe
    # (workers have no idea whether the main interrupt is enabled/disabled)
    def handle_child_interrupt(fd: int) -> None:
        child_idx = struct.unpack('i', os.read(fd, 4))[0]  # noqa
        log.debug(f'Child {child_idx} has interrupted the main program.')
        # self-interrupt to initiate the main-to-worker interrupts
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
        os.kill(0, signal.SIGINT)

    child_intr_pipe: Tuple[int, int] = os.pipe()
    rfd = child_intr_pipe[0]
    main_loop.add_reader(rfd, handle_child_interrupt, rfd)

    # start
    with main_ctx as main_args:

        # retrieve args generated by the user-defined main
        if main_args is None:
            main_args = tuple()
        if not isinstance(main_args, tuple):
            main_args = (main_args, )

        # spawn managed async workers
        for i in range(num_workers):
            try:
                p = main_loop.run_until_complete(afork(functools.partial(
                    _worker_main,
                    worker_actxmgr,
                    stop_signals,
                    child_intr_pipe[1],
                    i,
                    main_args + args,
                )))
            except RuntimeError as e:
                if 'loop stopped' in e.args[0]:
                    log.warning(
                        "skipping spawning of child[worker]:%d due to "
                        "async failure(s) of other child process",
                        i,
                    )
                    continue
                raise
            children.append(p)

        # spawn extra workers
        for i, f in enumerate(extra_procs):
            try:
                p = main_loop.run_until_complete(afork(functools.partial(
                    _extra_main,
                    f,
                    stop_signals,
                    num_workers + i,
                    main_args + args,
                )))
            except RuntimeError as e:
                if 'loop stopped' in e.args[0]:
                    log.warning(
                        "skipping spawning of child[extra]:%d due to "
                        "async failure(s) of other child process",
                        num_workers + i,
                    )
                    continue
                raise
            children.append(p)
        try:
            # unblock the stop signals for user/external interrupts.
            signal.pthread_sigmask(signal.SIG_UNBLOCK, sigblock_mask)

            # run!
            try:
                main_loop.run_until_complete(main_future)
            except asyncio.CancelledError:
                pass

            # if interrupted, wait for workers to finish.
            main_loop.run_until_complete(
                asyncio.gather(*[child.wait() for child in children])
            )
        finally:
            try:
                main_loop.run_until_complete(cancel_all_tasks())
                main_loop.run_until_complete(main_loop.shutdown_asyncgens())
                try:
                    main_loop.run_until_complete(
                        main_loop.shutdown_default_executor()
                    )
                except (AttributeError, NotImplementedError):  # for uvloop
                    pass
            finally:
                main_loop.close()
                asyncio.set_event_loop(None)
