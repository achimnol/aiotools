"""
A set of helper utilities to utilize taskgroups in better ways.
"""

import asyncio

from . import PersistentTaskGroup

__all__ = ("as_completed_safe", )


async def as_completed_safe(coros, timeout=None):
    """
    This is a safer version of :func:`asyncio.as_completed()` which uses
    :class:`PersistentTaskGroup` as an underlying coroutine lifecycle keeper.

    Upon a timeout, it raises :class:`asyncio.TimeoutError` immediately
    and cancels all remaining tasks or coroutines.

    This requires Python 3.11 or higher to work properly with timeouts.

    .. versionadded:: 1.6
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
