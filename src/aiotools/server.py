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
import functools
import inspect
import logging
import multiprocessing as mp
import multiprocessing.connection as mpconn
import multiprocessing.resource_tracker
import os
import signal
import struct
import sys
import threading
from collections.abc import (
    AsyncGenerator,
    Callable,
    Collection,
    Generator,
    Mapping,
    Sequence,
)
from contextlib import AbstractContextManager, ContextDecorator
from contextvars import ContextVar
from types import TracebackType
from typing import Any, Optional, ParamSpec, TypeVar

from .compat import all_tasks, current_task, get_running_loop
from .context import AbstractAsyncContextManager
from .fork import AbstractChildProcess, MPContext, _has_pidfd, afork

__all__ = (
    "main",
    "start_server",
    "process_index",
    "AsyncServerContextManager",
    "ServerMainContextManager",
    "InterruptedBySignal",
)

log = logging.getLogger(__name__)

process_index: ContextVar[int] = ContextVar("process_index")

TYield = TypeVar("TYield")
PArgs = ParamSpec("PArgs")


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


class AsyncServerContextManager(AbstractAsyncContextManager[TYield]):
    """
    A modified version of :func:`contextlib.asynccontextmanager`.

    The implementation detail is mostly taken from the ``contextlib`` standard
    library, with a minor change to inject ``self.yield_return`` into the wrapped
    async generator.
    """

    yield_return: Optional[signal.Signals]

    def __init__(
        self,
        func: Callable[..., AsyncGenerator[Any, signal.Signals]],
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
    ) -> None:
        if not inspect.isasyncgenfunction(func):
            raise RuntimeError("Context manager function must be an async-generator")
        self._agen = func(*args, **kwargs)
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.yield_return = None

    async def __aenter__(self) -> TYield:
        try:
            return await self._agen.__anext__()
        except StopAsyncIteration:
            raise RuntimeError("async-generator didn't yield") from None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Optional[bool]:
        if exc_type is None:
            assert self.yield_return is not None
            try:
                # Here is the modified part.
                await self._agen.asend(self.yield_return)
            except StopAsyncIteration:
                return None
            else:
                raise RuntimeError("async-generator didn't stop") from None
        else:
            if exc_value is None:
                exc_value = exc_type()
            try:
                await self._agen.athrow(exc_type, exc_value, traceback)
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
        return None


class ServerMainContextManager(AbstractContextManager[TYield], ContextDecorator):
    """
    A modified version of :func:`contextlib.contextmanager`.

    The implementation detail is mostly taken from the ``contextlib`` standard
    library, with a minor change to inject ``self.yield_return`` into the wrapped
    generator.
    """

    yield_return: Optional[signal.Signals]

    def __init__(
        self,
        func: Callable[..., Generator[Any, signal.Signals]],
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
    ) -> None:
        self.gen = func(*args, **kwargs)
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.yield_return = None

    def __enter__(self) -> TYield:
        del self.args, self.kwargs, self.func
        try:
            return next(self.gen)
        except StopIteration:
            raise RuntimeError("generator didn't yield") from None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Optional[bool]:
        if exc_type is None:
            assert self.yield_return is not None
            try:
                self.gen.send(self.yield_return)
            except StopIteration:
                return False
            else:
                raise RuntimeError("generator didn't stop")
        else:
            if exc_value is None:
                exc_value = exc_type()
            try:
                self.gen.throw(exc_type, exc_value, traceback)
            except StopIteration as exc:
                return exc is not exc_value
            except RuntimeError as exc:
                if exc is exc_value:
                    return False
                if exc_type is StopIteration and exc.__cause__ is exc_value:
                    return False
                raise
            except:  # noqa
                if sys.exc_info()[1] is exc_value:
                    return False
                raise
            raise RuntimeError("generator didn't stop after throw()")
        return None


# This is a dirty hack to implement "module callable".
# NOTE: only works in Python 3.5 or higher.


def _server_ctxmgr(
    func: Callable[
        [asyncio.AbstractEventLoop, int, Sequence[Any]],
        AsyncGenerator[None, signal.Signals],
    ],
):
    @functools.wraps(func)
    def helper(*args, **kwargs) -> AsyncServerContextManager:
        return AsyncServerContextManager(func, args, kwargs)

    return helper


class _ServerModule(sys.modules[__name__].__class__):  # type: ignore
    def __call__(self, func):
        return _server_ctxmgr(func)


sys.modules[__name__].__class__ = _ServerModule


def _main_ctxmgr(func: Callable[[], Generator[TYield, signal.Signals]]):
    """
    A decorator wrapper for :class:`ServerMainContextManager`

    Usage example:

    .. code:: python

       @aiotools.main_context
       def mymain() -> Generator[TServerArgs, signal.Signals]:
           server_args = do_init()
           stop_sig = yield server_args
           if stop_sig == signal.SIGINT:
               do_graceful_shutdown()
           else:
               do_forced_shutdown()

       aiotools.start_server(..., main_ctxmgr=mymain, ...)
    """

    @functools.wraps(func)
    def helper(*args, **kwargs) -> ServerMainContextManager[TYield]:
        return ServerMainContextManager(func, args, kwargs)

    return helper


main = _main_ctxmgr


def setup_child_watcher(loop: asyncio.AbstractEventLoop) -> None:
    if sys.version_info < (3, 12, 0):
        # see python/cpython#94597 (issue) and python/cpython#98215 (pr)
        try:
            watcher_cls = getattr(asyncio, "PidfdChildWatcher", None)
            if _has_pidfd and watcher_cls:
                watcher = watcher_cls()
                asyncio.set_child_watcher(watcher)
            else:
                # Just get the default child watcher.
                watcher = asyncio.get_child_watcher()
            if not watcher.is_active():
                watcher.attach_loop(loop)
        except NotImplementedError:
            pass  # for uvloop


async def cancel_all_tasks() -> None:
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
                "message": "unhandled exception during loop shutdown",
                "exception": task.exception(),
                "task": task,
            })


def _worker_main(
    worker_actxmgr: Callable[
        [asyncio.AbstractEventLoop, int, Sequence[Any]],
        AsyncServerContextManager,
    ],
    stop_signals: Collection[signal.Signals],
    intr_write_pipe: mpconn.Connection,
    proc_idx: int,
    run_to_completion: bool,
    args: Sequence[Any],
    *,
    prestart_hook: Optional[Callable[[int], None]] = None,
) -> int:
    process_index.set(proc_idx)
    if prestart_hook:
        prestart_hook(proc_idx)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    setup_child_watcher(loop)
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

    for signum in stop_signals:
        loop.add_signal_handler(
            signum,
            functools.partial(handle_stop_signal, signum),
        )
    # Allow the worker to be interrupted during initialization
    # (in case of initialization failures in other workers)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)

    async def _wrapped_worker():
        err_ctx = "enter"
        try:
            async with ctx:
                err_ctx = "body"
                try:
                    if not run_to_completion:
                        await forever_future
                    else:
                        ctx.yield_return = sorted(stop_signals)[0]
                except asyncio.CancelledError:
                    pass
                finally:
                    err_ctx = "exit"
        except Exception:
            if err_ctx != "body":
                err_ctx_str = "initialization" if err_ctx == "enter" else "shutdown"
                log.exception(
                    f"Worker {proc_idx}: Error during context manager {err_ctx_str}"
                )
                try:
                    intr_write_pipe.send_bytes(struct.pack("i", proc_idx))
                except BrokenPipeError:
                    pass
            raise
        finally:
            intr_write_pipe.close()

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
    extra_func: Callable[[Optional[threading.Event], int, Sequence[Any]], None],
    stop_signals: Collection[signal.Signals],
    proc_idx: int,
    args: Sequence[Any],
    *,
    prestart_hook: Optional[Callable[[int], None]] = None,
) -> int:
    process_index.set(proc_idx)
    if prestart_hook:
        prestart_hook(proc_idx)
    interrupted = threading.Event()

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
        log.warning(f"extra_proc[{proc_idx}] did not handle stop signals.")
    finally:
        # same as in _worker_main()
        signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
        return 0


def start_server(
    worker_actxmgr: Callable[
        [asyncio.AbstractEventLoop, int, Sequence[Any]], AsyncServerContextManager
    ],
    main_ctxmgr: Optional[Callable[[], ServerMainContextManager]] = None,
    extra_procs: Collection[Callable] = tuple(),
    stop_signals: Collection[signal.Signals] = (signal.SIGINT, signal.SIGTERM),
    num_workers: int = 1,
    args: Sequence[Any] = tuple(),
    *,
    wait_timeout: Optional[float] = None,
    mp_context: Optional[MPContext] = None,
    prestart_hook: Optional[Callable[[int], None]] = None,
    ignore_child_interrupts: bool = False,
    run_to_completion: bool = False,
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

        wait_timeout: The timeout in seconds before forcibly killing all
                      remaining child processes after sending initial stop
                      signals.

        mp_context: The multiprocessing context to use for creating child
                    processes.  If not specified, the default context is
                    used.

        prestart_hook: A function to be called once before creating the
                       event loop in the children.  The function should
                       accept an int argument representing the process index.

        ignore_child_interrupts: By default, any unhandled exceptions in the
                                 child functions are translated as active
                                 SIGINT to shutdown.
                                 This flag makes the main process to ignore them,
                                 which is useful to gather all worker's results
                                 even when some of them raises unhandled exceptions.

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

    .. versionadded:: 1.5.5

       The **wait_timeout** argument.

    .. versionadded:: 1.9.0

        The **mp_context**, **prestart_hook**, and **ignore_child_interrupts* arguments.
    """

    @_main_ctxmgr
    def noop_main_ctxmgr():
        yield

    assert stop_signals

    if hasattr(asyncio, "get_running_loop"):
        # only for Python 3.7+
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # there should be none.
            pass
        else:
            raise RuntimeError(
                "aiotools.start_server() cannot be called inside a running event loop."
            )

    if main_ctxmgr is None:
        main_ctxmgr = noop_main_ctxmgr

    children: list[AbstractChildProcess] = []
    sigblock_mask = frozenset(stop_signals)

    main_ctx = main_ctxmgr()

    # temporarily block signals and register signal handlers to main_loop
    signal.pthread_sigmask(signal.SIG_BLOCK, sigblock_mask)

    main_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(main_loop)
    main_future = main_loop.create_future()

    # to make subprocess working in child threads
    setup_child_watcher(main_loop)

    # Build a main-to-worker interrupt channel using signals
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

    # Build a reliable worker-to-main interrupt channel using a pipe.
    # This channel is used when the worker main functions raise an unhandled exception,
    # so that the main program can be interrupted immediately.
    def handle_child_interrupt(read_pipe: mpconn.Connection) -> None:
        try:
            child_idx = struct.unpack("i", read_pipe.recv_bytes(4))[0]
        except EOFError:
            # read_pipe is already closed
            return
        if not ignore_child_interrupts and not run_to_completion:
            # self-interrupt to initiate the main-to-worker interrupts
            log.debug(f"Child {child_idx} has interrupted the main program.")
            signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
            os.kill(0, signal.SIGINT)

    read_pipe, write_pipe = mp.Pipe()
    main_loop.add_reader(read_pipe.fileno(), handle_child_interrupt, read_pipe)

    # start
    try:
        with main_ctx as main_args:
            # retrieve args generated by the user-defined main
            if main_args is None:
                main_args = tuple()
            if not isinstance(main_args, tuple):
                main_args = (main_args,)

            # spawn managed async workers
            for i in range(num_workers):
                try:
                    p = main_loop.run_until_complete(
                        afork(
                            functools.partial(
                                _worker_main,
                                worker_actxmgr,
                                stop_signals,
                                write_pipe,
                                i,
                                run_to_completion,
                                (*main_args, *args),
                                prestart_hook=prestart_hook,
                            ),
                            mp_context=mp_context,
                        )
                    )
                except RuntimeError as e:
                    if "loop stopped" in e.args[0]:
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
                    p = main_loop.run_until_complete(
                        afork(
                            functools.partial(
                                _extra_main,
                                f,
                                stop_signals,
                                num_workers + i,
                                (*main_args, *args),
                                prestart_hook=prestart_hook,
                            ),
                            mp_context=mp_context,
                        )
                    )
                except RuntimeError as e:
                    if "loop stopped" in e.args[0]:
                        log.warning(
                            "skipping spawning of child[extra]:%d due to "
                            "async failure(s) of other child process",
                            num_workers + i,
                        )
                        continue
                    raise
                children.append(p)

            write_pipe.close()

            # unblock the stop signals for user/external interrupts.
            signal.pthread_sigmask(signal.SIG_UNBLOCK, sigblock_mask)

            # run!
            try:
                if not run_to_completion:
                    main_loop.run_until_complete(main_future)
                else:
                    main_ctx.yield_return = sorted(stop_signals)[0]
            except asyncio.CancelledError:
                pass
            finally:
                # If interrupted or complete, wait for workers to finish.
                try:
                    worker_results = main_loop.run_until_complete(
                        asyncio.wait_for(
                            asyncio.gather(
                                *[child.wait() for child in children],
                                return_exceptions=True,
                            ),
                            wait_timeout,
                        )
                    )
                    for child, result in zip(children, worker_results):
                        if isinstance(result, Exception):
                            log.error(
                                "Waiting for a child process [%d] has failed by an error.",
                                child.pid,
                                exc_info=result,
                            )
                except asyncio.TimeoutError:
                    log.warning(
                        "Timeout during waiting for child processes; killing all",
                    )
                    for child in children:
                        child.send_signal(signal.SIGKILL)
    finally:
        main_loop.remove_reader(read_pipe.fileno())
        read_pipe.close()
        try:
            main_loop.run_until_complete(cancel_all_tasks())
            main_loop.run_until_complete(main_loop.shutdown_asyncgens())
            try:
                main_loop.run_until_complete(main_loop.shutdown_default_executor())
            except (AttributeError, NotImplementedError):  # for uvloop
                pass
        finally:
            main_loop.close()
            asyncio.set_event_loop(None)
