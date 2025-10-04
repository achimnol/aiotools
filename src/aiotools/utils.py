"""
A set of higher-level coroutine aggregation utilities based on :class:`TaskScope`.
"""

from __future__ import annotations

import asyncio
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Iterable,
    Sequence,
)
from contextlib import aclosing
from contextvars import Context
from typing import Any, Optional, TypeVar

from .taskscope import TaskScope
from .types import CoroutineLike

__all__ = (
    "cancel_and_wait",
    "as_completed_safe",
    "gather_safe",
    "race",
)

T = TypeVar("T")


async def cancel_and_wait(
    task: asyncio.Task[Any],
    msg: str | None = None,
) -> None:
    """
    Safely cancels and waits until the given task is concluded as cancelled.
    If the task is already terminated, it does nothing.

    When cancellation is triggered from the task itself or its inner subtasks,
    or the caller of this function is cancelled during waiting,
    those cancellations are transparently raised up.

    See the discussion in https://github.com/python/cpython/issues/103486 for details.
    """
    if task.done():
        return
    # After our cancellation, cancelling() should be incremented by 1.
    # If its incremented by more than 1, it means cancel was requested externally.
    # In that case CancelledError should be raised to also end waiting task.
    cancelling_expected = task.cancelling() + 1
    task.cancel(msg)
    try:
        await task
    except asyncio.CancelledError:
        if task.cancelling() != cancelling_expected:
            raise
        else:
            return  # this is the only non-exceptional return
    else:
        raise RuntimeError("Cancelled task did not end with an exception")


async def as_completed_safe(
    coros: Iterable[CoroutineLike[T]],
    *,
    context: Optional[Context] = None,
) -> AsyncGenerator[Awaitable[T], None]:
    """
    This is a safer version of :func:`asyncio.as_completed()` which uses
    :class:`aiotools.TaskScope` as an underlying coroutine lifecycle keeper.

    This requires Python 3.11 or higher to work properly with timeouts.

    .. versionadded:: 1.6

    .. versionchanged:: 2.0

       It now uses :class:`aiotools.TaskScope` internally and handles
       timeouts in a bettery way.
    """
    q: asyncio.Queue[asyncio.Task[Any]] = asyncio.Queue()
    remaining = 0

    def result_callback(t: asyncio.Task[Any]) -> None:
        q.put_nowait(t)

    async with TaskScope(context=context) as ts:
        for coro in coros:
            t = ts.create_task(coro)
            t.add_done_callback(result_callback)
            remaining += 1
        while remaining:
            try:
                t = await q.get()
                remaining -= 1
                try:
                    yield t
                finally:
                    q.task_done()
            except (GeneratorExit, BaseException):
                # GeneratorExit: injected when aclose() is called.
                #                (i.e., the async-for body raises an exception)
                # CancelledError: injected when a timeout occurs
                #                 (i.e., the outer scope cancels the inner)
                # BaseException: injected when the process is going to terminate
                await ts.shutdown()
                raise


async def gather_safe(
    coros: Iterable[CoroutineLike[T]],
    *,
    context: Optional[Context] = None,
) -> list[T | BaseException]:
    """
    A safer version of :func:`asyncio.gather()`.  It wraps the passed coroutines
    with a :class:`TaskScope` to ensure the termination of them when returned.

    Additionally, it supports manually setting the context of each subtask.

    Note that if it is cancelled from an outer scope (e.g., timeout), there
    is no way to retrieve partially completed or failed results.
    If you need to process them anyway, you must store the results in a
    separate place in the passed coroutines or use :func:`as_completed_safe()`
    instead.

    .. versionadded:: 2.0
    """
    tasks: list[Awaitable[T]] = []
    async with TaskScope(context=context) as ts:
        for coro in coros:
            t = ts.create_task(coro)
            tasks.append(t)
        return await asyncio.gather(*tasks, return_exceptions=True)


async def race(
    coros: Iterable[CoroutineLike[T]],
    *,
    continue_on_error: bool = False,
    context: Optional[Context] = None,
) -> tuple[T, Sequence[Exception]]:
    """
    Returns the first result and cancelling all remaining coroutines safely.
    Passing an empty iterable of coroutines is not allowed.

    If ``continue_on_error`` is set False (default), it will raise the first
    exception immediately, cancelling all remaining coroutines.  This behavior is
    same to Javascript's ``Promise.race()``.  The second item of the returned tuple
    is always empty.

    If ``continue_on_error`` is set True, it will keep running until it encounters
    the first successful result.  Then it returns the exceptions as a list in the
    second item of the returned tuple.  If all coroutines fail, it will raise an
    :exc:`ExceptionGroup` to indicate the explicit failure of the entire operation.

    You may use this function to implement a "happy eyeball" algorithm.

    .. versionadded:: 2.0
    """
    async with aclosing(as_completed_safe(coros, context=context)) as ag:
        errors: list[Exception] = []
        async for aresult in ag:
            try:
                result = await aresult
                return result, errors
            except Exception as e:
                if continue_on_error:
                    errors.append(e)
                    continue
                raise
        else:
            if errors:
                raise ExceptionGroup("All coroutines have failed in race()", errors)
            raise ValueError("No coroutines were given to race()")
