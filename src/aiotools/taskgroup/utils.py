"""
A set of helper utilities to utilize taskgroups in better ways.
"""

import asyncio

from ..compat import get_running_loop
from . import PersistentTaskGroup

__all__ = ("as_completed_safe", )


async def as_completed_safe(coros, timeout=None):
    """
    This is a safer version of :func:`asyncio.as_completed()` which uses
    :class:`PersistentTaskGroup` as an underlying coroutine lifecycle keeper.
    """
    async with PersistentTaskGroup() as tg:
        tasks = []
        for coro in coros:
            t = tg.create_task(coro)
            tasks.append(t)
        await asyncio.sleep(0)
        try:
            for result in asyncio.as_completed(tasks, timeout=timeout):
                yield result
        except GeneratorExit:
            # This happens when as_completed() is timeout.
            raise asyncio.TimeoutError()
