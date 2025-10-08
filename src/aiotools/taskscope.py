from __future__ import annotations

import asyncio
import contextvars
from asyncio import events, exceptions, tasks
from contextvars import Context
from types import TracebackType
from typing import (
    Any,
    Self,
    TypeGuard,
    TypeVar,
)

from .taskcontext import ErrorCallback, LoopExceptionHandler, TaskContext
from .types import CoroutineLike

T = TypeVar("T")

__all__ = ("TaskScope",)

_has_callgraph = hasattr(asyncio, "future_add_to_awaited_by")


class TaskScope(TaskContext):
    """
    TaskScope is an asynchronous context manager which implements structured
    concurrency, i.e., "scoped" cancellation, over a set of child tasks.
    It terminates when all child tasks make conclusion (either results or exceptions).

    :class:`TaskScope` subclasses :class:`~aiotools.taskcontext.TaskContext`, but it
    mandates use of ``async with`` blocks to clarify which task is the parent of the
    child tasks spawned via :meth:`create_task()`.

    The key difference to :class:`asyncio.TaskGroup` is that it allows
    customization of the exception handling logic for unhandled child
    task exceptions, instead cancelling all pending child tasks upon any
    unhandled child task exceptions and collecting them as an :exc:`ExceptionGroup`.

    Since :class:`TaskScope` may be used for a long-running server context, unhandled
    child exceptions are NOT stored at all, but passed to the exception handler
    directly and immediately.  If you want to collect results and exceptions, please
    use :func:`~aiotools.utils.as_completed_safe()` or
    :func:`~aiotools.utils.gather_safe()`.

    Refer :class:`TaskContext` for the descriptions about the constructor arguments.

    Based on this customizability, :class:`~aiotools.supervisor.Supervisor` is a mere
    alias of :class:`TaskScope` with ``exception_handler=None``.

    .. versionadded:: 2.0

    .. versionchanged:: 2.1

       In Python 3.14 or higher, it also updates :doc:`the asyncio call graph
       <python:library/asyncio-graph>` so that the task awaiter could be tracked down
       via TaskScope, like :class:`asyncio.TaskGroup`.
    """

    _tasks: set[asyncio.Task[Any]]
    _on_completed_fut: asyncio.Future[Any] | None
    _base_error: BaseException | None
    _entered: bool
    _exiting: bool
    _aborting: bool

    def __init__(
        self,
        *,
        exception_handler: ErrorCallback
        | LoopExceptionHandler
        | None = LoopExceptionHandler.TOKEN,
        context: contextvars.Context | None = None,
    ) -> None:
        super().__init__(exception_handler=exception_handler, context=context)
        # status flags
        self._entered = False
        self._exiting = False
        self._aborting = False
        # taskscope-specifics
        self._base_error = None
        self._on_completed_fut = None

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError(
                f"{type(self).__name__} {self!r} has been already entered"
            )
        self._entered = True

        if self._loop is None:
            self._loop = events.get_running_loop()

        self._parent_task = tasks.current_task(self._loop)
        if self._parent_task is None:
            raise RuntimeError(
                f"{type(self).__name__} {self!r} cannot determine the parent task"
            )

        return self

    async def __aexit__(
        self,
        et: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        assert self._loop is not None
        assert self._parent_task is not None
        self._exiting = True

        if exc is not None and self._is_base_error(exc) and self._base_error is None:
            self._base_error = exc

        propagate_cancellation_error = exc if et is exceptions.CancelledError else None

        if et is not None:
            if not self._aborting:
                # Our parent task is being cancelled:
                #
                #    async with TaskGroup() as g:
                #        g.create_task(...)
                #        await ...  # <- CancelledError
                #
                # or there's an exception in "async with":
                #
                #    async with TaskGroup() as g:
                #        g.create_task(...)
                #        1 / 0
                #
                self.abort()

        prop_ex = await self._wait_completion()
        assert not self._tasks
        if prop_ex is not None:
            propagate_cancellation_error = prop_ex
        self._exited = True

        if self._base_error is not None:
            raise self._base_error

        # Propagate CancelledError as the child exceptions are handled separately.
        if propagate_cancellation_error:
            raise propagate_cancellation_error

        return None

    async def _wait_completion(self) -> BaseException | None:
        # We use while-loop here because "self._on_completed_fut"
        # can be cancelled multiple times if our parent task
        # is being cancelled repeatedly (or even once, when
        # our own cancellation is already in progress)
        assert self._loop is not None
        propagate_cancellation_error = None
        while self._tasks:
            if self._on_completed_fut is None:
                self._on_completed_fut = self._loop.create_future()
            try:
                await self._on_completed_fut
            except exceptions.CancelledError as ex:
                if not self._aborting:
                    # Our parent task is being cancelled:
                    #
                    #    async def wrapper():
                    #        async with TaskGroup() as g:
                    #            g.create_task(foo)
                    #
                    # "wrapper" is being cancelled while "foo" is
                    # still running.
                    propagate_cancellation_error = ex
                    self.abort(msg=ex.args[0] if ex.args else None)
            self._on_completed_fut = None
        return propagate_cancellation_error

    async def aclose(self) -> None:
        """
        Triggers cancellation and waits for completion.
        """
        self.abort(r"{self!r} is closed")
        await self._wait_completion()

    def create_task(
        self,
        coro: CoroutineLike[T],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> tasks.Task[T]:
        """
        Create a new task in this scope and return it.
        Similar to :func:`asyncio.create_task()`.
        """
        if not self._entered:
            raise RuntimeError(f"{type(self).__name__} {self!r} has not been entered")
        if self._exiting and not self._tasks:
            raise RuntimeError(f"{type(self).__name__} {self!r} is finished")
        assert self._parent_task is not None
        task = self._create_task(coro, name=name, context=context)
        if _has_callgraph:
            asyncio.future_add_to_awaited_by(task, self._parent_task)  # type: ignore[attr-defined]
        return task

    # Since Python 3.8 Tasks propagate all exceptions correctly,
    # except for KeyboardInterrupt and SystemExit which are
    # still considered special.

    def _is_base_error(self, exc: BaseException) -> TypeGuard[BaseException]:
        assert isinstance(exc, BaseException)
        return isinstance(exc, (SystemExit, KeyboardInterrupt))

    def abort(self, msg: str | None = None) -> None:
        """
        Triggers cancellation and immediately returns *without* waiting for completion.
        """
        self._aborting = True
        for t in self._tasks:
            if not t.done():
                t.cancel(msg=msg)

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        assert self._loop is not None
        assert self._parent_task is not None
        self._tasks.discard(task)
        if _has_callgraph:
            asyncio.future_discard_from_awaited_by(task, self._parent_task)  # type: ignore[attr-defined]
        if self._on_completed_fut is not None and not self._tasks:
            if not self._on_completed_fut.done():
                self._on_completed_fut.set_result(True)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return

        # Instead of adding to ExceptionGroup, call the configured exception handler.
        self._handle_task_exception(task)

        is_base_error = self._is_base_error(exc)
        if is_base_error and self._base_error is None:
            self._base_error = exc

        if self._parent_task.done():
            # Not sure if this case is possible, but we want to handle
            # it anyways.
            self._loop.call_exception_handler({
                "message": (
                    f"Task {task!r} has errored out but its parent "
                    f"task {self._parent_task} is already completed"
                ),
                "exception": exc,
                "task": task,
            })
            return

        # If parent task *is not* being cancelled, we should just keep
        # running, unlike TaskGroup:
        #
        #    async def foo():
        #        try:
        #            async with TaskScope() as ts:
        #                ts.create_task(crash_soon())
        #                await something  # <- this should keep running
        #        except Exception:
        #            # Ignore any exceptions raised in the TaskScope
        #            pass
        #        await something_else     # <- unaffected as well
