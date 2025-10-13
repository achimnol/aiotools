from __future__ import annotations

import asyncio
import contextvars
from asyncio import TimerHandle, events, exceptions, tasks
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from contextvars import Context
from dataclasses import dataclass
from types import TracebackType
from typing import (
    Any,
    Final,
    Self,
    TypeGuard,
    TypeVar,
)

from .taskcontext import ErrorCallback, LoopExceptionHandler, TaskContext
from .types import CoroutineLike, OptExcInfo

T = TypeVar("T")

__all__ = (
    "TaskScope",
    "ShieldScope",
    "move_on_after",
)


@dataclass(slots=True)
class _TaskState:
    task_scope: TaskScope | None


_task_states: dict[asyncio.Task[Any], _TaskState] = {}
_has_callgraph: Final = hasattr(asyncio, "future_add_to_awaited_by")


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

    You may nest and mix multiple TaskScope and even :class:`asyncio.TaskGroup`
    within a single task or via subtask chains.  In such cases, cancelling the
    outmost parent task will be shielded by the topmost TaskScope with ``shield=True``
    or :class:`ShieldScope`.

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

    .. versionchanged:: 2.2

       Now nesting and mixing TaskScope and TaskGroup is fully supported and
       behaves consistently, by deferring the cancellation at the topmost
       shielded scope.
    """

    _tasks: set[asyncio.Task[Any]]
    _on_completed_fut: asyncio.Future[Any] | None
    _base_error: BaseException | None
    _entered: bool
    _exiting: bool
    _aborting: bool
    _shield: bool
    _timeout: float | None
    _timeout_expired: bool
    _timeout_handler: TimerHandle | None
    _parent_scope: TaskScope | None
    _child_scopes: set[TaskScope]
    _cancel_requests: list[str | None]
    _host_cancel: Callable[[str | None], bool] | None
    _host_cancelling: Callable[[], int] | None
    _host_uncancel: Callable[[], int] | None

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
        self._parent_scope = None
        self._child_scopes = set()
        self._shield = shield
        self._timeout = timeout
        self._timeout_expired = False
        self._timeout_handler = None
        self._loop = events.get_running_loop()
        self._cancel_requests = []

    def _on_timeout(self) -> None:
        self._timeout_expired = True
        self._timeout_handler = None
        assert self._host_task is not None
        assert self._host_cancel is not None
        self.abort("timeout")  # trigger cancellation of child tasks
        self._host_cancel("timeout")  # interrupt the taskscope body

    def _cancel(self, msg: str | None = None) -> bool:
        if self._shield:
            self._cancel_requests.append(msg)
        else:
            assert self._host_cancel is not None
            self._host_cancel(msg)  # may already be hooked
        return True

    def _cancelling(self) -> int:
        if self._shield:
            return len(self._cancel_requests)
        else:
            assert self._host_cancelling is not None
            return self._host_cancelling()  # may already be hooked

    def _uncancel(self) -> int:
        if self._shield:
            self._cancel_requests.pop()
            return len(self._cancel_requests)
        else:
            assert self._host_uncancel is not None
            return self._host_uncancel()  # may already be hooked

    def _add_to_parent_scope(self) -> None:
        """
        Keeps track of the parent/child relationship of task scopes within the host task
        by adding the current scope to the parent scope.
        """
        assert self._host_task is not None
        if (task_state := _task_states.get(self._host_task, None)) is None:
            task_state = _TaskState(task_scope=self)
            _task_states[self._host_task] = task_state
        else:
            self._parent_scope = task_state.task_scope
            # replace the "current" taskscope with myself.
            task_state.task_scope = self
            if self._parent_scope is not None:
                self._parent_scope._child_scopes.add(self)

    def _remove_from_parent_scope(self) -> None:
        """
        Keeps track of the parent/child relationship of task scopes within the host task
        by removing the current scope from the parent scope.
        """
        assert self._host_task is not None
        task_state = _task_states.get(self._host_task)
        if task_state is None or task_state.task_scope is not self:
            raise RuntimeError(
                "Exiting task scope not owned by the current task is not allowed"
            )
        if self._parent_scope is not None:
            self._parent_scope._child_scopes.remove(self)
        # restore the "current" taskscope.
        task_state.task_scope = self._parent_scope

    def _hook_task_cancel_methods(self) -> None:
        assert self._host_task is not None
        self._host_cancel, self._host_task.cancel = (  # type: ignore[method-assign]
            self._host_task.cancel,
            self._cancel,
        )
        self._host_cancelling, self._host_task.cancelling = (  # type: ignore[method-assign]
            self._host_task.cancelling,
            self._cancelling,
        )
        self._host_uncancel, self._host_task.uncancel = (  # type: ignore[method-assign]
            self._host_task.uncancel,
            self._uncancel,
        )

    def _restore_task_cancel_methods(self) -> None:
        assert self._host_task is not None
        self._host_task.cancel = self._host_cancel  # type: ignore[method-assign,assignment]
        self._host_task.cancelling = self._host_cancelling  # type: ignore[method-assign,assignment]
        self._host_task.uncancel = self._host_uncancel  # type: ignore[method-assign,assignment]
        self._host_cancel = None
        self._host_cancelling = None
        self._host_uncancel = None

    def _enter_scope(self) -> None:
        if self._entered:
            raise RuntimeError(
                f"{type(self).__name__} {self!r} has been already entered"
            )
        self._host_task = asyncio.current_task()
        if self._host_task is None:
            raise RuntimeError(
                f"{type(self).__name__} {self!r} cannot determine the parent task"
            )
        assert self._host_task is not None

        self._add_to_parent_scope()
        self._prior_cancel_request_count = self._host_task.cancelling()
        self._hook_task_cancel_methods()
        self._entered = True

        if self._timeout is not None:
            when = self._loop.time() + self._timeout
            self._timeout_handler = self._loop.call_at(when, self._on_timeout)

    def _exit_scope_prep(
        self,
        et: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> BaseException | None:
        assert self._host_task is not None
        assert self._host_cancelling is not None
        self._exiting = True

        if exc is not None and self._is_base_error(exc) and self._base_error is None:
            self._base_error = exc

        propagate_cancellation_error: BaseException | None = None
        cancelling = len(self._cancel_requests) + self._host_cancelling()
        if et is None and cancelling > self._prior_cancel_request_count:
            # If we have received more cancellation requests than the starting point,
            # raise up the cancellation.
            # e.g., When there are timeout-based cancellation from outside while the
            #       TaskScope itself was shielded during its execution.
            propagate_cancellation_error = exceptions.CancelledError()
        else:
            if (
                et is not None and issubclass(et, exceptions.CancelledError)
                # > and not self._shield
            ):
                propagate_cancellation_error = exc

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

        return propagate_cancellation_error

    def _exit_scope_conclude(
        self, propagated_cancellation: BaseException | None
    ) -> bool:
        self._exited = True

        # BaseExceptions other than CancelledError have higher priority.
        if self._base_error is not None:
            raise self._base_error

        # If the intrinsic timeout is set and expired, raise TimeoutError instead.
        # Preserve the original exception as the context/cause of it.
        if self._timeout_expired:
            prop_exc = propagated_cancellation
            timeout_error = asyncio.TimeoutError()
            timeout_error.__context__ = prop_exc
            timeout_error.__cause__ = prop_exc
            raise timeout_error

        # Propagate CancelledError as the child exceptions are handled separately.
        if propagated_cancellation:
            if (
                isinstance(propagated_cancellation, asyncio.CancelledError)
                and self._find_topmost_shield()
            ):
                # Suppress cancellation error when delegating it to the topmost shield.
                return True
            raise propagated_cancellation

        return False

    def _find_topmost_shield(self) -> bool:
        scope = self._parent_scope
        while scope is not None:
            if scope._shield:
                return True
            scope = scope._parent_scope
        return False

    def _exit_scope_cleanup(self) -> None:
        assert self._host_task is not None

        if self._timeout_handler is not None and not self._timeout_handler.cancelled():
            self._timeout_handler.cancel()
            self._timeout_handler = None

        self._remove_from_parent_scope()
        self._restore_task_cancel_methods()

        # Synchronize upward the cancellation requests made while shielded.
        for msg in self._cancel_requests:
            self._host_task.cancel(msg)

        # Remove heavy objects that may have reference cycles.
        self._host_task = None
        self._base_error = None

    async def __aenter__(self) -> Self:
        self._enter_scope()
        return self

    async def __aexit__(self, *exc_info: OptExcInfo) -> bool | None:
        try:
            prop_ex = self._exit_scope_prep(*exc_info)  # type: ignore[arg-type]
            child_ex = await self._wait_completion()
            assert not self._tasks
            if child_ex is not None:
                prop_ex = child_ex
            return self._exit_scope_conclude(prop_ex)
        finally:
            self._exit_scope_cleanup()
            prop_ex = None
            child_ex = None

    async def _wait_completion(self) -> BaseException | None:
        # We use while-loop here because "self._on_completed_fut"
        # can be cancelled multiple times if our parent task
        # is being cancelled repeatedly (or even once, when
        # our own cancellation is already in progress)
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

        return propagate_cancellation_error

    def abort(self, msg: str | None = None) -> None:
        """
        Triggers cancellation of the scope and all its children and immediately returns *without*
        waiting for completion.
        This method ignores the shield option.
        """
        if not self._aborting:
            self._aborting = True
            for t in self._tasks:
                t.cancel(msg=msg)

    async def aclose(self) -> None:
        """
        Triggers cancellation of the scope and all its children, then waits for completion.
        This method ignores the shield option.

        Calling this method will cancel the host task and the task body will observe a
        :exc:`asyncio.CancelledError`.
        """
        assert self._host_task is not None
        if not self._aborting:
            self.abort(f"{self!r} is closed")
            self._host_task.cancel()  # interrupt the taskscope body
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
        assert self._host_task is not None
        task = self._create_task(coro, name=name, context=context, **kwargs)
        if _has_callgraph:
            asyncio.future_add_to_awaited_by(task, self._host_task)  # type: ignore[attr-defined]
        return task

    def _is_base_error(self, exc: BaseException) -> TypeGuard[BaseException]:
        # Since Python 3.8 Tasks propagate all exceptions correctly,
        # except for KeyboardInterrupt and SystemExit which are
        # still considered special.
        # Discussion: https://github.com/python/cpython/issues/135736
        assert isinstance(exc, BaseException)
        return isinstance(exc, (SystemExit, KeyboardInterrupt))

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        assert self._host_task is not None
        self._tasks.discard(task)
        if _has_callgraph:
            asyncio.future_discard_from_awaited_by(task, self._host_task)  # type: ignore[attr-defined]
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

        if self._host_task.done():
            # Not sure if this case is possible, but we want to handle
            # it anyways.
            self._loop.call_exception_handler({
                "message": (
                    f"Task {task!r} has errored out but its parent "
                    f"task {self._host_task} is already completed"
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


class ShieldScope(TaskScope):
    """
    A context-manager to make the codes within the scope to be shielded from cancellation,
    delaying any cancellation attempts in the middle to be re-raised afterwards.
    You may use it as an async context manager as well.

    See https://github.com/python/cpython/issues/99714#issuecomment-1817941789
    for the original ideation.

    To self-cancel from the inside of ShieldScope, you may simply raise
    :exc:`asyncio.CancelledError`.

    .. code-block:: python

        async def work():
            try:
                await some_job()
            finally:
                with ShieldScope():
                    await cleanup()  # ensured to run regardless of cancellation timing

        task = asyncio.create_task(work())
        ...
        await cancel_and_wait(task)

    It may be used as either an async context manager or a context manager.
    As an async context manager, it is equivalent to ``TaskScope(shield=True)``.
    As a (sync) coontext manager, you cannot spawn child tasks because it
    does not wait for children's completion when exiting the context scope.
    You may use it as a simple block marker to wrap the code lines to be shielded.

    When there are multiple nested ShieldScope combined with TaskScope,
    the cancellation is deferred to the point of exit of the topmost
    ``ShieldScope`` (or ``TaskScop(shield=True)``) block where the parent scope
    is not shielded or there is no parent scope.

    .. versionadded:: 2.1

    .. versionchanged:: 2.2

       Moved from ``aiotools.cancel`` module to ``aiotools.taskscope`` module
       to subclass :class:`TaskScope` without circular imports.
       The root import path ``aiotools.ShieldScope`` is preserved.

    .. versionchanged:: 2.2

       When used as a synchronous context manager, spawning child tasks is explicitly
       prohibited.
    """

    def __init__(self, timeout: float | None = None) -> None:
        super().__init__(shield=True, timeout=timeout)
        self._disable_subtask = False

    def __enter__(self) -> None:
        # When used in a synchronous context, users SHOULD NOT spawn child tasks using this.
        self._disable_subtask = True
        self._enter_scope()

    def __exit__(self, *exc_info: OptExcInfo) -> bool:
        try:
            prop_ex = self._exit_scope_prep(*exc_info)  # type: ignore[arg-type]
            # skip waiting for child tasks when used in a synchronous context
            return self._exit_scope_conclude(prop_ex)
        finally:
            self._exit_scope_cleanup()
            prop_ex = None

    def create_task(
        self,
        coro: CoroutineLike[T],
        *,
        name: str | None = None,
        context: Context | None = None,
        **kwargs: Any,
    ) -> tasks.Task[T]:
        if self._disable_subtask:
            raise RuntimeError("Cannot spawn child tasks in a synchronous ShieldScope.")
        return super().create_task(coro, name=name, context=context, **kwargs)
