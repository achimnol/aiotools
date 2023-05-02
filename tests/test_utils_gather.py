import asyncio
from contextvars import ContextVar, copy_context
from typing import TypeVar

import pytest

from aiotools import (
    gather_safe,
    VirtualClock,
)

T = TypeVar("T")
cancelled = ContextVar("cancelled", default=0)


async def do_job(delay: float, result: T) -> T:
    try:
        await asyncio.sleep(delay)
        return result
    except asyncio.CancelledError:
        await asyncio.sleep(0.1)
        cancelled.set(cancelled.get() + 1)
        raise


async def fail_job(delay: float) -> None:
    try:
        await asyncio.sleep(delay)
        1 / 0
    except asyncio.CancelledError:
        await asyncio.sleep(0.1)
        cancelled.set(cancelled.get() + 1)
        raise


@pytest.mark.asyncio
async def test_gather_safe() -> None:
    context = copy_context()
    with VirtualClock().patch_loop():

        async def _inner() -> None:
            results = await gather_safe([
                do_job(0.3, 3),
                do_job(0.2, 2),
                do_job(0.1, 1),
            ], context=context)
            assert results == [3, 2, 1]

        await asyncio.create_task(_inner(), context=context)


@pytest.mark.asyncio
async def test_gather_safe_partial_failure() -> None:
    context = copy_context()
    with VirtualClock().patch_loop():

        async def _inner() -> None:
            results = await gather_safe([
                do_job(0.3, 3),
                fail_job(0.2),
                do_job(0.1, 1),
            ], context=context)
            assert results[0] == 3
            assert isinstance(results[1], ZeroDivisionError)
            assert results[2] == 1
            assert cancelled.get() == 0

        await asyncio.create_task(_inner(), context=context)


@pytest.mark.asyncio
async def test_gather_safe_timeout():
    context = copy_context()
    with VirtualClock().patch_loop():

        async def _inner() -> None:
            with pytest.raises(asyncio.TimeoutError):
                async with asyncio.timeout(0.35):
                    await gather_safe([
                        do_job(0.1, 1),
                        fail_job(0.2),
                        fail_job(0.25),
                        do_job(0.3, 3),
                        # timeout occurs here
                        do_job(0.4, 4),  # cancelled
                        fail_job(0.5),   # cancelled
                        fail_job(0.6),   # cancelled
                    ], context=context)
            # There is no way to retrieve partial result in this case.
            # If you want to combine gather_safe() and timeouts, your
            # tasks should take care of any potential exceptions and results
            # by on their own, instead of relying on the return value of
            # the gather_safe() call.
            # We track how many were cancelled indirectly using a contextvar.
            assert cancelled.get() == 3

        await asyncio.create_task(_inner(), context=context)
