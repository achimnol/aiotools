"""
A set of helper utilities to utilize taskgroups in better ways.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any

from ..supervisor import Supervisor

__all__ = (
    "as_completed_safe",
    "gather_safe",
)


async def as_completed_safe(coros, timeout=None):
    """
    This is a safer version of :func:`asyncio.as_completed()` which uses
    :class:`Supervisor` as an underlying coroutine lifecycle keeper.

    Upon a timeout, it raises :class:`asyncio.TimeoutError` immediately
    and cancels all remaining tasks or coroutines.

    This requires Python 3.11 or higher to work properly with timeouts.

    .. versionadded:: 1.6
    """
    q = asyncio.Queue()
    tasks = set()

    def result_callback(t: asyncio.Task) -> None:
        tasks.discard(t)
        q.put_nowait(t)

    async with (
        asyncio.timeout(timeout),
        Supervisor() as supervisor,
        # asyncio.TaskGroup() as supervisor,
    ):
        for coro in coros:
            t = supervisor.create_task(coro)
            t.add_done_callback(result_callback)
            tasks.add(t)
        await asyncio.sleep(0)
        while True:
            if not tasks:
                return
            try:
                yield await q.get()
            except GeneratorExit as e:
                # It is raised when "async for" body raises an exception
                # and calls aclose() to the async generator.
                raise asyncio.CancelledError()


@dataclass
class GroupResult:
    results: list[Any] = field(default_factory=list)
    cancelled: int = 0


async def gather_safe(coros, group_result: GroupResult) -> GroupResult:
    errors = []
    ongoing_cancelled_count = 0

    def result_callback(t: asyncio.Task) -> None:
        nonlocal errors, ongoing_cancelled_count
        try:
            group_result.results.append(t.result())
            print("collecting result:", t.result())
        except asyncio.CancelledError:
            print("collecting cancellation")
            ongoing_cancelled_count += 1
        except Exception as e:
            print("collecting error:", e)
            errors.append(e)

    try:
        async with Supervisor() as supervisor:
            for coro in coros:
                t = supervisor.create_task(coro)
                t.add_done_callback(result_callback)
        return group_result
    except asyncio.CancelledError as e:
        print("gather_safe cancelled")
        errors.append(e)
    finally:
        print(f"{ongoing_cancelled_count=}")
        print(f"{errors=}")
        group_result.cancelled = ongoing_cancelled_count
        if errors:
            raise BaseExceptionGroup("unhandled exceptions in gather_safe()", errors)
