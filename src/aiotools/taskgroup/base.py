import asyncio
from contextvars import ContextVar

from .types import TaskGroupError

__all__ = (
    'TaskGroup',
    'current_taskgroup',
)

current_taskgroup: ContextVar['TaskGroup'] = ContextVar('current_taskgroup')


class TaskGroup(asyncio.TaskGroup):

    async def __aenter__(self):
        self._current_taskgroup_token = current_taskgroup.set(self)
        return await super().__aenter__()

    async def __aexit__(self, et, exc, tb):
        try:
            return await super().__aexit__(et, exc, tb)
        except BaseExceptionGroup as eg:
            # Just wrap the exception group as TaskGroupError for backward
            # compatibility.  In Python 3.11 or higher, TaskGroupError
            # also inherits BaseExceptionGroup, so the standard except*
            # clauses and ExceptionGroup methods will work as expected.
            # New codes should migrate to directly using asyncio.TaskGroup.
            raise TaskGroupError(eg.message, eg.exceptions) from None
        finally:
            current_taskgroup.reset(self._current_taskgroup_token)
