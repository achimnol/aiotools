import asyncio
from contextvars import Context, ContextVar, copy_context
from typing import TypeVar

import pytest

from aiotools import VirtualClock, race

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
async def test_race_partial_failure() -> None:
    context = copy_context()
    with VirtualClock().patch_loop():

        async def _inner1() -> None:
            result, errors = await race(
                [
                    do_job(0.1, 1),  # returns here
                    fail_job(0.3),  # cancelled
                    do_job(0.5, 5),  # cancelled
                ],
                context=context,
            )
            assert result == 1
            assert len(errors) == 0  # should be empty
            assert cancelled.get() == 2

        # Fix the context used by all sub-tasks
        await asyncio.create_task(_inner1(), context=context)

        async def _inner2() -> None:
            with pytest.raises(ZeroDivisionError):
                await race(
                    [
                        fail_job(0.3),  # raises here
                        do_job(0.4, 1),  # cancelled
                        do_job(0.5, 5),  # cancelled
                    ],
                    context=context,
                )
                assert cancelled.get() == 2

        # Fix the context used by all sub-tasks
        await asyncio.create_task(_inner2(), context=context)


@pytest.mark.asyncio
async def test_race_continue_on_error() -> None:
    context = copy_context()
    with VirtualClock().patch_loop():

        async def _inner() -> None:
            result, errors = await race(
                [
                    fail_job(0.3),  # collected
                    fail_job(0.35),  # collected
                    do_job(0.4, 4),  # returns here
                    fail_job(0.45),  # cancelled
                    do_job(0.5, 5),  # cancelled
                ],
                continue_on_error=True,
                context=context,
            )
            assert result == 4
            assert len(errors) == 2
            assert isinstance(errors[0], ZeroDivisionError)
            assert isinstance(errors[1], ZeroDivisionError)
            assert cancelled.get() == 2

        await asyncio.create_task(_inner(), context=context)


@pytest.mark.asyncio
async def test_race_empty_coro_list() -> None:
    with pytest.raises(ValueError):
        await race([])


@pytest.mark.asyncio
async def test_race_all_failure() -> None:
    context = Context()
    with VirtualClock().patch_loop():

        async def _inner() -> None:
            with pytest.raises(ZeroDivisionError):
                await race(
                    [
                        fail_job(0.1),  # raises here
                        fail_job(0.3),  # cancelled
                        fail_job(0.5),  # cancelled
                    ],
                    context=context,
                )
            assert cancelled.get() == 2

        await asyncio.create_task(_inner(), context=context)


@pytest.mark.asyncio
async def test_race_all_failure_with_continue_on_error() -> None:
    context = Context()
    with VirtualClock().patch_loop():

        async def _inner() -> None:
            try:
                await race(
                    [
                        fail_job(0.1),  # collected
                        fail_job(0.3),  # collected
                        fail_job(0.5),  # collected
                    ],
                    continue_on_error=True,
                    context=context,
                )
            except* ZeroDivisionError as e:
                assert len(e.exceptions) == 3
            else:
                pytest.fail("The collected exceptions were not raised")
            assert cancelled.get() == 0

        await asyncio.create_task(_inner(), context=context)
