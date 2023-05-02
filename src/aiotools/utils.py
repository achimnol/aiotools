"""
A set of helper utilities to utilize taskgroups in better ways.
"""

import asyncio
from contextlib import aclosing
from dataclasses import dataclass, field
from typing import Any

from .supervisor import Supervisor

__all__ = (
    "as_completed_safe",
    "gather_safe",
    "race",
    "GroupResult",
)


async def as_completed_safe(coros):
    """
    This is a safer version of :func:`asyncio.as_completed()` which uses
    :class:`Supervisor` as an underlying coroutine lifecycle keeper.

    This requires Python 3.11 or higher to work properly with timeouts.

    .. versionadded:: 1.6
    """
    q = asyncio.Queue()
    tasks = set()

    def result_callback(t: asyncio.Task) -> None:
        tasks.discard(t)
        q.put_nowait(t)

    async with Supervisor() as supervisor:
        for coro in coros:
            t = supervisor.create_task(coro)
            t.add_done_callback(result_callback)
            tasks.add(t)
        while True:
            if not tasks:
                return
            try:
                yield await q.get()
            except (GeneratorExit, asyncio.CancelledError):
                # GeneratorExit: injected when aclose() is called.
                #                (i.e., the async-for body raises an exception)
                # CancelledError: injected when a timeout occurs
                #                 (i.e., the outer scope cancels the inner)
                await supervisor.shutdown()
                raise


@dataclass
class GroupResult:
    results: list[Any] = field(default_factory=list)
    cancelled: int = 0


async def gather_safe(coros, group_result: GroupResult) -> GroupResult:
    errors: list[BaseException] = []
    ongoing_cancelled_count = 0

    def result_callback(t: asyncio.Task) -> None:
        nonlocal errors, ongoing_cancelled_count
        try:
            group_result.results.append(t.result())
        except asyncio.CancelledError:
            ongoing_cancelled_count += 1
        except Exception as e:
            errors.append(e)

    try:
        async with Supervisor() as supervisor:
            for coro in coros:
                t = supervisor.create_task(coro)
                t.add_done_callback(result_callback)
        return group_result
    except asyncio.CancelledError as e:
        errors.append(e)
    finally:
        group_result.cancelled = ongoing_cancelled_count
        if errors:
            raise BaseExceptionGroup("unhandled exceptions in gather_safe()", errors)
    raise RuntimeError("should not reach here")


async def race(coros):
    async with aclosing(as_completed_safe(coros)) as ag:
        async for aresult in ag:
            try:
                result = await aresult
                return result
            except Exception:
                raise
        else:
            raise RuntimeError("No coroutines were given to race()")
