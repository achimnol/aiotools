"""
A set of helper utilities to utilize taskgroups in better ways.
"""

from __future__ import annotations

import asyncio
from contextlib import aclosing
from contextvars import Context
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Sequence,
)

from .supervisor import Supervisor

__all__ = (
    "as_completed_safe",
    "gather_safe",
    "race",
)

T = TypeVar("T")


async def as_completed_safe(
    coros: Iterable[Awaitable[Any]],
    *,
    context: Optional[Context] = None,
) -> AsyncGenerator[Any, None]:
    """
    This is a safer version of :func:`asyncio.as_completed()` which uses
    :class:`Supervisor` as an underlying coroutine lifecycle keeper.

    This requires Python 3.11 or higher to work properly with timeouts.

    .. versionadded:: 1.6
    """
    q: asyncio.Queue[asyncio.Task[Any]] = asyncio.Queue()
    remaining = 0

    def result_callback(t: asyncio.Task[Any]) -> None:
        q.put_nowait(t)

    async with Supervisor() as supervisor:
        for coro in coros:
            t = supervisor.create_task(coro, context=context)
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
                await supervisor.shutdown()
                raise


async def gather_safe(
    coros: Iterable[Awaitable[Any]],
    *,
    context: Optional[Context] = None,
) -> List[Any | Exception]:
    """
    A safer version of :func:`asyncio.gather()`.  It wraps the passed coroutines
    with a :class:`Supervisor` to ensure the termination of them when returned.

    Additionally, it supports manually setting the context of each subtask.
    """
    tasks = []
    async with Supervisor() as supervisor:
        for coro in coros:
            t = supervisor.create_task(coro, context=context)
            tasks.append(t)
        # To ensure safety, the Python version must be 3.7 or higher.
        return await asyncio.gather(*tasks, return_exceptions=True)


async def race(
    coros: Iterable[Awaitable[T]],
    *,
    continue_on_error: bool = False,
    context: Optional[Context] = None,
) -> Tuple[T, Sequence[Exception]]:
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
    exc:`ExceptionGroup` to indicate the explicit failure of the entire operation.
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
