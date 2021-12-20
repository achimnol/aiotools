from __future__ import annotations

import asyncio
import itertools
import logging
import traceback
from types import TracebackType
from typing import (
    Any,
    Coroutine,
    Optional,
    Type,
    TypeVar,
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

TAny = TypeVar('TAny')

_ptaskgroup_idx = itertools.count()
_log = logging.getLogger(__name__)


class PersistentTaskGroupExceptionHandler(Protocol):
    async def __call__(self, exc: Exception) -> None:
        ...


async def _default_exc_handler(exc: Exception) -> None:
    traceback.print_exc()


class PersistentTaskGroup:

    _exc_handler: PersistentTaskGroupExceptionHandler
    _tasks: weakref.WeakSet[asyncio.Task[Any]]

    def __init__(
        self,
        *,
        name: str = None,
        exception_handler: PersistentTaskGroupExceptionHandler = None,
    ) -> None:
        self._name = name or f"PTaskGroup-{next(_ptaskgroup_idx)}"
        self._tasks = weakref.WeakSet()
        if exception_handler is None:
            self._exc_handler = _default_exc_handler
        else:
            self._exc_handler = exception_handler

    @property
    def name(self) -> str:
        return self._name

    async def create_task(
        self,
        coro: Coroutine[Any, Any, TAny],
        *,
        name: str = None,
    ) -> asyncio.Task[Optional[TAny]]:

        # TODO: functools.wraps equivalent for coro?
        async def wrapped_task() -> Optional[TAny]:
            current_task = compat.current_task()
            assert current_task is not None
            _log.debug("%r is spawned in %r.", current_task, self)
            try:
                return await coro
            except asyncio.CancelledError:
                _log.debug("%r in %r has been cancelled.", current_task, self)
            except BaseException:
                # TODO: implement
                raise
            except Exception as exc:
                await self._exc_handler(exc)
            # As our fallback handler handled the exception, the task should
            # terminate silently with no explicit result.
            # TODO: Add support for ExceptionGroup in Python 3.11, for the cases
            #       with nested sub-tasks and sub-taskgroups.
            return None

        t = asyncio.create_task(wrapped_task(), name=name)
        self._tasks.add(t)
        return t

    async def shutdown(self) -> None:
        remaining_tasks = {*self._tasks}
        cancelled_tasks = set()
        for t in remaining_tasks:
            if t.cancelled():
                continue
            if not t.done():
                t.cancel()
                cancelled_tasks.add(t)
        await asyncio.gather(*cancelled_tasks)

    async def __aenter__(self) -> None:
        pass

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        await self.shutdown()
        return False

    def __repr__(self) -> str:
        return f"<PersistentTaskGroup {self.name}>"
