import asyncio
import itertools
import logging
import sys
import traceback
from types import TracebackType
from typing import (
    Any,
    Callable,
    Coroutine,
    List,
    Optional,
    Type,
)
import weakref

from .. import compat
from .common import patch_task
from .types import AsyncExceptionHandler, TaskGroupError

__all__ = (
    'PersistentTaskGroup',
)

_ptaskgroup_idx = itertools.count()
_log = logging.getLogger(__name__)
_has_task_name = (sys.version_info >= (3, 8, 0))


async def _default_exc_handler(exc_type, exc_obj, exc_tb) -> None:
    traceback.print_exc()


class PersistentTaskGroup:
    """
    Provides an abstraction of long-running task groups for server applications.
    The main use case is to implement a dispatcher of async event handlers, to group
    RPC/API request handlers, etc. with safe and graceful shutdown.
    Here "long-running" means that all tasks should keep going even when sibling
    tasks fail with unhandled errors and such errors must be reported immediately.
    Here "safety" means that all spawned tasks should be reclaimed before exit or
    shutdown.

    When used as an async context manager, it works similarly to
    :func:`asyncio.gather()` with ``return_exceptions=True`` option.  It exits the
    context scope when all tasks finish, just like :class:`asyncio.TaskGroup`, but
    it does NOT abort when there are unhandled exceptions from child tasks; just
    keeps sibling tasks running and reporting errors as they occur (see below).

    When *not* used as an async context maanger (e.g., used as attributes of
    long-lived objects), it persists running until :method:`shutdown()` is called
    explicitly.  Note that it is the user's responsibility to call
    :method:`shutdown()` because ``PersistentTaskGroup`` does not provide the
    ``__del__()`` method.

    Regardless how it is executed, it lets all spawned tasks run to their completion
    and calls the exception handler to report any unhandled exceptions immediately.
    If there are exceptions occurred again in the exception handlers, then it uses
    :method:`AbstractEventLoop.call_exception_handler()` as the last resort.

    Since the exception handling and reporting takes places immediately, it
    eliminates potential arbitrary report delay due to other tasks or the execution
    method.  This resolves a critical debugging pain when only termination of the
    application displays accumulated errors, as sometimes we don't want to terminate
    but just inspect what is happening.
    """

    _base_error: Optional[BaseException]
    _exc_handler: AsyncExceptionHandler
    _errors: Optional[List[BaseException]]
    _tasks: "weakref.WeakSet[asyncio.Task]"
    _on_completed_fut: Optional[asyncio.Future]

    def __init__(
        self,
        *,
        name: str = None,
        exception_handler: AsyncExceptionHandler = None,
    ) -> None:
        self._entered = False
        self._exiting = False
        self._aborting = False
        self._errors = []
        self._base_error = None
        self._name = name or f"{next(_ptaskgroup_idx)}"
        self._parent_cancel_requested = False
        self._unfinished_tasks = 0
        self._on_completed_fut = None
        self._parent_task = compat.current_task()
        self._tasks = weakref.WeakSet()
        if exception_handler is None:
            self._exc_handler = _default_exc_handler
        else:
            self._exc_handler = exception_handler

    # TODO: task statistics and enumeration (for aiomonitor)
    # TODO: two-phase shutdown

    def get_name(self) -> str:
        return self._name

    def create_task(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        name: str = None,
    ) -> "asyncio.Task":
        if not self._entered:
            # When used as object attribute, auto-enter.
            self._entered = True
        if self._exiting and self._unfinished_tasks == 0:
            raise RuntimeError(f"{self!r} has already finished")
        return self._create_task_with_name(coro, name=name, cb=self._on_task_done)

    def _create_task_with_name(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        name: str = None,
        cb: Callable[[asyncio.Task], Any],
    ) -> "asyncio.Task":
        loop = compat.get_running_loop()
        if _has_task_name and name:
            child_task = loop.create_task(self._task_wrapper(coro), name=name)
        else:
            child_task = loop.create_task(self._task_wrapper(coro))
        _log.debug("%r is spawned in %r.", child_task, self)
        self._unfinished_tasks += 1
        child_task.add_done_callback(cb)
        self._tasks.add(child_task)
        return child_task

    def _is_base_error(self, exc: BaseException) -> bool:
        assert isinstance(exc, BaseException)
        return isinstance(exc, (SystemExit, KeyboardInterrupt))

    async def _wait_completion(self) -> Optional[bool]:
        loop = compat.get_running_loop()
        propagate_cancellation_error = None
        while self._unfinished_tasks:
            if self._on_completed_fut is None:
                self._on_completed_fut = loop.create_future()
            try:
                await self._on_completed_fut
            except asyncio.CancelledError:
                if not self._aborting:
                    propagate_cancellation_error = True
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

    async def _task_wrapper(self, coro: Coroutine) -> Any:
        loop = compat.get_running_loop()
        task = compat.current_task()
        try:
            return await coro
        except Exception:
            # Swallow unhandled exceptions by our own and
            # prevent abortion of the task group bu them.
            # Wrapping corotuines directly has advantage for
            # exception handlers to access full traceback
            # and there is no need to implement separate
            # mechanism to wait for exception handler tasks.
            try:
                await self._exc_handler(*sys.exc_info())
            except Exception as exc:
                # If there are exceptions inside the exception handler
                # we report it as soon as possible using the event loop's
                # exception handler, instead of postponing
                # to the timing when PersistentTaskGroup terminates.
                loop.call_exception_handler({
                    'message': f"Got an unhandled exception "
                               f"in the exception handler of Task {task!r}",
                    'exception': exc,
                    'task': task,
                })

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._unfinished_tasks -= 1
        assert self._unfinished_tasks >= 0
        assert self._parent_task is not None
        assert self._errors is not None

        if self._on_completed_fut is not None and not self._unfinished_tasks:
            if not self._on_completed_fut.done():
                self._on_completed_fut.set_result(True)

        if task.cancelled():
            _log.debug("%r in %r has been cancelled.", task, self)
            return

        exc = task.exception()
        if exc is None:
            return

        # Now the exception is BaseException.
        self._errors.append(exc)
        if self._base_error is None:
            self._base_error = exc

        self._trigger_shutdown()
        if not self._parent_task.__cancel_requested__:  # type: ignore
            self._parent_cancel_requested = True

    async def __aenter__(self) -> "PersistentTaskGroup":
        self._parent_task = compat.current_task()
        patch_task(self._parent_task)
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
        propagate_cancelation = False

        if (exc_val is not None and
                self._is_base_error(exc_val) and
                self._base_error is None):
            self._base_error = exc_val

        if exc_type is asyncio.CancelledError:
            if self._parent_cancel_requested:
                # Only if we did request task to cancel ourselves
                # we mark it as no longer cancelled.
                self._parent_task.__cancel_requested__ = False  # type: ignore
            else:
                propagate_cancelation = True

        if exc_type is not None and not self._aborting:
            if exc_type is asyncio.CancelledError:
                propagate_cancelation = True
            self._trigger_shutdown()

        prop_ex = await self._wait_completion()
        if prop_ex is not None:
            propagate_cancelation = prop_ex

        if propagate_cancelation:
            # The wrapping task was cancelled; since we're done with
            # closing all child tasks, just propagate the cancellation
            # request now.
            raise asyncio.CancelledError()

        if exc_val is not None and exc_type is not asyncio.CancelledError:
            # If there are any unhandled errors, let's add them to
            # the bubbled up exception group.
            # Normally, they should have been swallowed and logged
            # by the fallback exception handler.
            self._errors.append(exc_val)

        if self._errors:
            # Bubble up errors
            errors = self._errors
            self._errors = None
            me = TaskGroupError(
                'unhandled errors in a PersistentTaskGroup',
                errors,
            )
            raise me from None

        return None

    def __repr__(self) -> str:
        info = ['']
        if self._name:
            info.append(f'name={self._name}')
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
        return f'<PersistentTaskGroup({info_str})>'
