"""
A set of helper utilities to utilize taskgroups in better ways.
"""

import asyncio

from . import PersistentTaskGroup

__all__ = ("as_completed_safe", )


async def as_completed_safe(coros):
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
        for result in asyncio.as_completed(tasks):
            yield result
