from __future__ import annotations

import asyncio
import contextvars
from asyncio import TimerHandle, events, exceptions, tasks
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import Context
from types import TracebackType
from typing import (
    Any,
    Self,
    TypeGuard,
    TypeVar,
)

from .cancel import ShieldScope
from .taskcontext import ErrorCallback, LoopExceptionHandler, TaskContext
from .types import CoroutineLike

T = TypeVar("T")

__all__ = (
    "TaskScope",
    "move_on_after",
)

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

    When ``shield=True``, the scope is protected from outside cancellations,
    while re-raising the requested cancellations when terminated.
    From inside, you may simply raise a new :exc:`asyncio.CancelledError` from the
    context manager body to self-cancel the shielded scope.

    The ``timeout`` argument enforces timeout even when the scope is shielded,
    unlike the vanilla :func:`asyncio.timeout()`.

    Refer :class:`TaskContext` for the descriptions about the constructor arguments.

    Based on this customizability, :class:`~aiotools.supervisor.Supervisor` is a mere
    alias of :class:`TaskScope` with ``exception_handler=None``.

    .. versionadded:: 2.0

    .. versionchanged:: 2.1

       In Python 3.14 or higher, it also updates :doc:`the asyncio call graph
       <python:library/asyncio-graph>` so that the task awaiter could be tracked down
       via TaskScope, like :class:`asyncio.TaskGroup`.

    .. versionchanged:: 2.1

       Added the ``shield`` and ``timeout`` options.
    """

    _tasks: set[asyncio.Task[Any]]
    _on_completed_fut: asyncio.Future[Any] | None
    _base_error: BaseException | None
    _entered: bool
    _exiting: bool
    _aborting: bool
    _shield: bool
    _shield_scope: ShieldScope
    _timeout: float | None
    _timeout_expired: bool
    _timeout_handler: TimerHandle | None

    def __init__(
        self,
        *,
        shield: bool = False,
        timeout: float | None = None,
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
        self._shield = shield
        self._shield_scope = ShieldScope()
        self._timeout = timeout
        self._timeout_expired = False
        self._timeout_handler = None

    def _on_timeout(self) -> None:
        self._timeout_expired = True
        self._timeout_handler = None
        if self._shield:
            self._shield_scope.__exit__()  # no longer shielded
        assert self._parent_task is not None
        self._parent_task.cancel()  # interrupt the taskscope body
        self.abort("timeout")  # trigger cancellation of child tasks

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

        self._cancelling = self._parent_task.cancelling()

        if self._timeout is not None:
            when = self._loop.time() + self._timeout
            self._timeout_handler = self._loop.call_at(when, self._on_timeout)
        if self._shield:
            self._shield_scope.__enter__()

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

        propagate_cancellation_error: BaseException | None
        if self._parent_task.cancelling() > self._cancelling:
            # If we have received more cancellation requests than the starting point,
            # raise up the cancellation.
            # e.g., When there are timeout-based cancellation from outside while the
            #       TaskScope itself was shielded during its execution.
            propagate_cancellation_error = exceptions.CancelledError()
        else:
            propagate_cancellation_error = (
                exc if et is exceptions.CancelledError else None
            )

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

        # If the intrinsic timeout is set and expired,
        # raise up TimeoutError.
        if self._timeout_expired:
            prop_exc = propagate_cancellation_error
            propagate_cancellation_error = asyncio.TimeoutError()
            propagate_cancellation_error.__context__ = prop_exc
            propagate_cancellation_error.__cause__ = prop_exc

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
                    #        async with TaskScope() as ts:
                    #            ts.create_task(foo)
                    #
                    # "wrapper" is being cancelled while "foo" is
                    # still running.
                    propagate_cancellation_error = ex
                    self.abort(msg=ex.args[0] if ex.args else None)
            self._on_completed_fut = None

        if self._timeout_handler is not None and not self._timeout_handler.cancelled():
            self._timeout_handler.cancel()
        if self._shield:
            self._shield_scope.__exit__()

        return propagate_cancellation_error

    async def aclose(self) -> None:
        """
        Triggers cancellation of the scope and all its children, then waits for completion.
        This method ignores the shield option.
        """
        self.abort(f"{self!r} is closed")
        await self._wait_completion()

    def create_task(
        self,
        coro: CoroutineLike[T],
        *,
        name: str | None = None,
        context: Context | None = None,
        **kwargs: Any,
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
        task = self._create_task(coro, name=name, context=context, **kwargs)
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
        Triggers cancellation of the scope and all its children and immediately returns *without*
        waiting for completion.
        This method ignores the shield option.
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


@asynccontextmanager
async def move_on_after(
    timeout: float | None = None, shield: bool = False
) -> AsyncIterator[TaskScope]:
    """
    A shortcut to create a :class:`TaskScope` with a timeout while ignoring and continuing after timeout.

    .. code-block:: python

        prior_work()
        async with move_on_after(3.0) as ts:
            ts.create_task(...)
            await some_work()
        # after 3.0 seconds, any remaining coroutines/tasks within the scope is cancelled,
        # and the control resumes here.
        after_work()

    .. versionadded:: 2.1
    """
    try:
        async with TaskScope(timeout=timeout, shield=shield) as ts:
            yield ts
    except asyncio.TimeoutError:
        pass
