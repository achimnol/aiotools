__all__ = ["ErrorArg", "ErrorCallback", "TaskContext"]

import asyncio
import enum
import warnings
from contextvars import Context
from typing import (
    Any,
    Callable,
    Coroutine,
    Generator,
    Optional,
    TypeAlias,
    TypedDict,
    TypeVar,
)


class DefaultErrorHandler(enum.Enum):
    TOKEN = 0


class ErrorArg(TypedDict):
    message: str
    exception: BaseException
    task: asyncio.Task[Any]


ErrorCallback: TypeAlias = Callable[[ErrorArg], None]
T = TypeVar("T")


async def cancel_and_wait(
    task: asyncio.Task[Any],
    msg: str | None = None,
) -> None:
    """
    See the discussion in https://github.com/python/cpython/issues/103486
    """
    task.cancel(msg)
    try:
        await task
    except asyncio.CancelledError:
        parent_task = asyncio.current_task()
        if parent_task is not None and parent_task.cancelling() == 0:
            raise
        else:
            return  # this is the only non-exceptional return
    else:
        raise RuntimeError("Cancelled task did not end with an exception")


class TaskContext:
    """
    TaskContext keeps the references to the child tasks during its lifetime,
    so that they can be terminated safely when shutdown is explicitly requested.
    """

    _tasks: set[asyncio.Task[Any]]
    _parent_task: Optional[asyncio.Task[Any]]
    _loop: Optional[asyncio.AbstractEventLoop]

    def __init__(
        self,
        delegate_errors: Optional[
            ErrorCallback | DefaultErrorHandler
        ] = DefaultErrorHandler.TOKEN,
    ) -> None:
        self._loop = None
        self._delegate_errors = delegate_errors
        self._tasks = set()
        self._parent_task = None
        # status flags
        self._entered = False
        self._aborting = False
        self._exited = False

    def __del__(self) -> None:
        loc: str = "<loc>"  # TODO: implement
        if self._entered and not self._exited:
            warnings.warn(
                f"TaskContext initialized at {loc} is not properly "
                f"terminated until it is garbage-collected.",
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
        # Trigger cancellation and wait.
        self._aborting = True
        try:
            for t in {*self._tasks}:
                if not t.done():
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass
        finally:
            self._exited = True

    def create_task(
        self,
        coro: Generator[None, None, T] | Coroutine[Any, None, T],
        *,
        name: Optional[str] = None,
        context: Optional[Context] = None,
    ) -> asyncio.Task[T]:
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
        coro: Generator[None, None, T] | Coroutine[Any, None, T],
        *,
        name: Optional[str] = None,
        context: Optional[Context] = None,
    ) -> asyncio.Task[T]:
        """
        Create a new task in this context and return it.
        Similar to :func:`asyncio.create_task()`.
        """
        assert self._loop is not None
        if self._aborting:
            raise RuntimeError(f"{type(self).__name__} {self!r} is shutting down")
        task: asyncio.Task[T] = self._loop.create_task(
            coro,
            name=name,
            context=context,
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
                func(
                    {
                        "message": f"Task {task!r} has errored inside the parent "
                        f"task {self._parent_task}",
                        "exception": exc,
                        "task": task,
                    }
                )
            case DefaultErrorHandler():
                self._loop.call_exception_handler(
                    {
                        "message": f"Task {task!r} has errored inside the parent "
                        f"task {self._parent_task}",
                        "exception": exc,
                        "task": task,
                    }
                )
