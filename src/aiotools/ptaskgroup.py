import asyncio
import enum
import itertools
import logging
import sys
import traceback
from types import TracebackType
from typing import (
    Any,
    Coroutine,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)
try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol  # type: ignore  # noqa
import weakref

from . import compat

__all__ = (
    'PersistentTaskGroup',
)

TResult = TypeVar('TResult')

_ptaskgroup_idx = itertools.count()
_log = logging.getLogger(__name__)
_has_task_name = (sys.version_info >= (3, 8, 0))


class UndefinedResult(enum.Enum):
    UNDEFINED = 0


UNDEFINED = UndefinedResult.UNDEFINED


class ExceptionHandler(Protocol):
    async def __call__(self, exc: BaseException) -> None:
        ...


async def _default_exc_handler(exc: BaseException) -> None:
    traceback.print_exc()


class PersistentTaskGroup(Generic[TResult]):
    """
    Provides an abstraction of long-running task group for server applications.

    If used as async context manager, it propagates cancellation from the parent task
    into the child tasks.
    It exits the context scope when all tasks finish, just like
    :class:`asyncio.TaskGroup`.

    If used without async context manager, it keeps running until
    :method:`shutdown()` is called explicitly.
    """

    _base_error: Optional[BaseException]
    _exc_handler: ExceptionHandler
    _errors: Optional[List[BaseException]]
    _tasks: "weakref.WeakSet[asyncio.Task[TResult]]"
    _on_completed_fut: Optional[asyncio.Future]

    def __init__(
        self,
        *,
        name: str = None,
        exception_handler: ExceptionHandler = None,
    ) -> None:
        self._entered = False
        self._exiting = False
        self._aborting = False
        self._errors = []
        self._base_error = None
        self._name = name or f"PTaskGroup-{next(_ptaskgroup_idx)}"
        self._parent_cancel_requested = False
        self._unfinished_tasks = 0
        self._on_completed_fut = None
        self._parent_task = compat.current_task()
        self._tasks = weakref.WeakSet()
        if exception_handler is None:
            self._exc_handler = _default_exc_handler
        else:
            self._exc_handler = exception_handler

    @property
    def name(self) -> str:
        return self._name

    def create_task(
        self,
        coro: Coroutine[Any, Any, TResult],
        *,
        name: str = None,
    ) -> "asyncio.Task[TResult]":
        if not self._entered:
            # When used as object attribute, auto-enter.
            self._entered = True
        if self._exiting and self._unfinished_tasks == 0:
            raise RuntimeError(f"{self!r} has already finished")
        loop = compat.get_running_loop()
        if _has_task_name:
            t = loop.create_task(coro, name=name)
        else:
            t = loop.create_task(coro)
        _log.debug("%r is spawned in %r.", t, self)
        self._unfinished_tasks += 1
        t.add_done_callback(self._on_task_done)
        self._tasks.add(t)
        return t

    def _is_base_error(self, exc: BaseException) -> bool:
        assert isinstance(exc, BaseException)
        return isinstance(exc, (SystemExit, KeyboardInterrupt))

    async def _wait_completion(self) -> Optional[BaseException]:
        loop = compat.get_running_loop()
        propagate_cancellation_error = None
        while self._unfinished_tasks:
            if self._on_completed_fut is None:
                self._on_completed_fut = loop.create_future()
            try:
                await self._on_completed_fut
            except asyncio.CancelledError as ex:
                if not self._aborting:
                    propagate_cancellation_error = ex
                    self._trigger_shutdown()
            self._on_completed_fut = None

        assert self._unfinished_tasks == 0
        self._on_completed_fut = None
        return propagate_cancellation_error

    def _trigger_shutdown(self) -> None:
        self._aborting = True
        for t in self._tasks:
            if not t.done():
                t.cancel()

    async def shutdown(self) -> None:
        self._trigger_shutdown()
        await self._wait_completion()

    def _on_exc_handler_done(self, task: asyncio.Task[None]) -> None:
        self._unfinished_tasks -= 1
        assert self._unfinished_tasks >= 0

        # TODO: how to handle exceptions in the exception handler?
        # TODO: prevent infinite recursion

        if self._on_completed_fut is not None and not self._unfinished_tasks:
            if not self._on_completed_fut.done():
                self._on_completed_fut.set_result(True)

    def _on_task_done(self, task: asyncio.Task[TResult]) -> None:
        self._unfinished_tasks -= 1
        assert self._unfinished_tasks >= 0
        assert self._parent_task is not None
        assert self._errors is not None
        set_completed = False

        if self._on_completed_fut is not None and not self._unfinished_tasks:
            if not self._on_completed_fut.done():
                set_completed = True

        try:
            if task.cancelled():
                _log.debug("%r in %r has been cancelled.", task, self)
                return

            exc = task.exception()
            if exc is None:
                return

            # Spawn our exception handler for non-base exceptions
            # and swallow the error.
            if not self._is_base_error(exc):
                t = asyncio.create_task(self._exc_handler(exc))
                t.add_done_callback(self._on_exc_handler_done)
                self._tasks.add(t)
                self._unfinished_tasks += 1
                set_completed = False
                return

            # Now the exception is BaseException.
            self._errors.append(exc)
            if self._base_error is None:
                self._base_error = exc

            self._trigger_shutdown()
            if not self._parent_task.cancelling():
                self._parent_cancel_requested = True
        finally:
            if self._on_completed_fut is not None and set_completed:
                self._on_completed_fut.set_result(True)

    async def __aenter__(self) -> "PersistentTaskGroup":
        self._parent_task = compat.current_task()
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        self._exiting = True
        assert self._errors is not None
        propagate_cancellation_error: Optional[
            Union[Type[BaseException], BaseException]
        ] = None

        if (exc_val is not None and
                self._is_base_error(exc_val) and
                self._base_error is None):
            self._base_error = exc_val

        if exc_type is asyncio.CancelledError:
            if self._parent_cancel_requested:
                self._parent_task.uncancel()
            else:
                propagate_cancellation_error = exc_type
        if exc_type is not None and not self._aborting:
            if exc_type is asyncio.CancelledError:
                propagate_cancellation_error = exc_type
            self._trigger_shutdown()

        prop_ex = await self._wait_completion()
        if prop_ex is not None:
            propagate_cancellation_error = prop_ex
        if propagate_cancellation_error is not None:
            raise propagate_cancellation_error

        if exc_type is not None and exc_type is not asyncio.CancelledError:
            # If there are any unhandled errors, let's add them to
            # the bubbled up exception group.
            # Normally, they should have been swallowed and logged
            # by the fallback exception handler.
            self._errors.append(exc_val)

        if self._errors:
            # Bubble up errors
            errors = self._errors
            self._errors = None
            me = BaseExceptionGroup('unhandled errors in a TaskGroup', errors)
            raise me from None

        return None

    def __repr__(self) -> str:
        info = ['']
        if self._tasks:
            info.append(f'tasks={len(self._tasks)}')
        if self._unfinished_tasks:
            info.append(f'unfinished={self._unfinished_tasks}')
        if self._errors:
            info.append(f'errors={len(self._errors)}')
        if self._aborting:
            info.append('cancelling')
        elif self._entered:
            info.append('entered')
        info_str = ' '.join(info)
        return f'<PersistentTaskGroup{info_str}>'
