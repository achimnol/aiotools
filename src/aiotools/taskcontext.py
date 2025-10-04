from __future__ import annotations

import asyncio
import contextvars
import enum
import warnings
from contextvars import Context
from typing import (
    Any,
    Callable,
    Optional,
    TypeAlias,
    TypedDict,
    TypeVar,
)

from .cancel import cancel_and_wait
from .types import CoroutineLike

__all__ = (
    "ErrorArg",
    "ErrorCallback",
    "TaskContext",
)


class DefaultErrorHandler(enum.Enum):
    TOKEN = 0


class ErrorArg(TypedDict):
    # Intentionally designed as a typed dict to match with
    # the signature of asyncio stdlib's error callback handler.
    message: str
    exception: BaseException
    task: asyncio.Task[Any]


ErrorCallback: TypeAlias = Callable[[ErrorArg], None]
T = TypeVar("T")


class TaskContext:
    """
    TaskContext keeps the references to the child tasks during its lifetime,
    so that they can be terminated safely when shutdown is explicitly requested.

    This is the loosest form of child task managers among TaskScope, TaskGroup, and
    Supervisor, as it does not enforce structured concurrency but just provides a
    reference set to child tasks.

    You may replace existing patterns using :class:`weakref.WeakSet` to keep track
    of child tasks for a long-running server application with TaskContext.

    If ``delegate_errors`` is not set (the default behavior), it will run
    :meth:`loop.call_exception_handler() <asyncio.loop.call_exception_handler>`
    with the context argument consisting of the ``message``, ``task`` (the child task
    that raised the exception), and ``exception`` (the exception object) fields.

    If it is set *None*, it will silently ignore the exception.

    If it is set as a callable function, it will invoke the specified callback
    function using the context argument same to that used when calling
    :meth:`loop.call_exception_handler() <asyncio.loop.call_exception_handler>`.

    If you provide ``context``, it will be passed to :meth:`create_task()` by default.

    .. versionadded:: 2.0
    """

    # We need to keep track of strong references as the event loop keeps weak references to
    # fire-and-forget tasks until Python 3.13. In Python 3.14, it will be changed as
    # per-thread doubly-linked lists to support free-threaded (nogil) setups to avoid
    # lock contention in the weakref subsystem.
    _tasks: set[asyncio.Task[Any]]
    _parent_task: Optional[asyncio.Task[Any]]
    _loop: Optional[asyncio.AbstractEventLoop]

    def __init__(
        self,
        delegate_errors: Optional[
            ErrorCallback | DefaultErrorHandler | None
        ] = DefaultErrorHandler.TOKEN,
        context: Optional[contextvars.Context] = None,
    ) -> None:
        self._loop = None
        self._tasks = set()
        self._parent_task = None
        self._delegate_errors = delegate_errors
        self._default_context = context
        # status flags
        self._entered = False
        self._exited = False
        self._aborting = False

    def __del__(self) -> None:
        loc: str = "<loc>"  # TODO: implement
        if self._entered and not self._exited:
            warnings.warn(
                f"TaskContext initialized at {loc} is not properly "
                "terminated until it is garbage-collected.",
                category=ResourceWarning,
            )

    def __repr__(self) -> str:
        info = [""]
        if self._tasks:
            info.append(f"tasks={len(self._tasks)}")
        if self._aborting:
            info.append("cancelling")
        elif self._entered:
            info.append("entered")

        info_str = " ".join(info)
        return f"<{type(self).__name__} {info_str}>"

    async def shutdown(self) -> None:
        """
        Triggers cancellation and waits for completion.
        """
        self._aborting = True
        try:
            # NOTE: unhandled exceptions are captured by our own task-done handler.
            await asyncio.gather(
                (cancel_and_wait(t) for t in self._tasks),
                return_exceptions=True,
            )
        finally:
            self._exited = True

    def create_task(
        self,
        coro: CoroutineLike[T],
        *,
        name: Optional[str] = None,
        context: Optional[Context] = None,
    ) -> asyncio.Task[T]:
        """
        Create a new task in this scope and return it.
        Similar to :func:`asyncio.create_task()`.
        """
        if not self._entered:
            self._entered = True
            self._loop = asyncio.get_running_loop()
        parent_task = asyncio.current_task()
        assert parent_task is not None
        if self._parent_task is not None and parent_task is not self._parent_task:
            raise RuntimeError(
                "{type(self).__name__} must be used within a single parent task."
            )
        self._parent_task = parent_task
        return self._create_task(coro, name=name, context=context)

    def _create_task(
        self,
        coro: CoroutineLike[T],
        *,
        name: Optional[str] = None,
        context: Optional[Context] = None,
    ) -> asyncio.Task[T]:
        assert self._loop is not None
        if self._aborting:
            raise RuntimeError(f"{type(self).__name__} {self!r} is shutting down")
        task: asyncio.Task[T] = self._loop.create_task(
            coro,
            name=name,
            context=self._default_context if context is None else context,
        )
        # optimization: Immediately call the done callback if the task is
        # already done (e.g. if the coro was able to complete eagerly),
        # and skip scheduling a done callback
        if task.done():
            self._on_task_done(task)
        else:
            self._tasks.add(task)
            task.add_done_callback(self._on_task_done)
        return task

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        self._handle_task_exception(task)

    def _handle_task_exception(self, task: asyncio.Task[Any]) -> None:
        assert self._loop is not None
        exc = task.exception()
        assert exc is not None
        match self._delegate_errors:
            case None:
                pass  # deliberately set to ignore errors
            case func if callable(func):
                func({
                    "message": (
                        f"Task {task!r} has errored inside the parent "
                        f"task {self._parent_task}"
                    ),
                    "exception": exc,
                    "task": task,
                })
            case DefaultErrorHandler():
                self._loop.call_exception_handler({
                    "message": (
                        f"Task {task!r} has errored inside the parent "
                        f"task {self._parent_task}"
                    ),
                    "exception": exc,
                    "task": task,
                })
            case _:
                raise RuntimeError(f"Invalid error handler: {self._delegate_errors!r}")
