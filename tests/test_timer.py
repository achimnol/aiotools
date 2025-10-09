from __future__ import annotations

import asyncio

import pytest

import aiotools


@pytest.mark.asyncio
async def test_timer() -> None:
    """
    Test the timer functionality.
    """
    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():
        count = 0

        async def counter(interval: float) -> None:
            assert interval == 0.1
            nonlocal count
            await asyncio.sleep(0)
            count += 1

        count = 0
        timer = aiotools.create_timer(counter, 0.1)
        await asyncio.sleep(0.22)
        await aiotools.cancel_and_wait(timer)
        assert count == 3

        count = 0
        timer = aiotools.create_timer(counter, 0.1, aiotools.TimerDelayPolicy.CANCEL)
        await asyncio.sleep(0.22)
        await aiotools.cancel_and_wait(timer)
        # should have same results
        assert count == 3


@pytest.mark.asyncio
async def test_timer_unhandled_tick_exception() -> None:
    """
    Test if the timer task is NOT cancelled when the timer-fired tick tasks
    raises an unhandled exception.
    """

    tick_count = 0

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async def failing_tick(interval: float) -> None:
            nonlocal tick_count
            tick_count += 1
            await asyncio.sleep(0.1)
            raise ZeroDivisionError()

        timer = aiotools.create_timer(failing_tick, 1)
        await asyncio.sleep(5)

        # the tick task must be executed more than once.
        assert tick_count > 1
        # the timer task should be alive.
        assert not timer.done()

        await aiotools.cancel_and_wait(timer)


@pytest.mark.asyncio
async def test_timer_leak_default() -> None:
    """
    Test if the timer-fired tasks are claned up properly
    even when each timer-fired task takes longer than the timer interval.
    (In this case they will accumulate indefinitely!)
    """
    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():
        spawn_count = 0
        cancel_count = 0
        done_count = 0

        async def delayed(interval: float) -> None:
            nonlocal spawn_count, cancel_count, done_count
            spawn_count += 1
            try:
                await asyncio.sleep(5)
                done_count += 1
            except asyncio.CancelledError:
                cancel_count += 1
                raise

        task_count = len(aiotools.compat.all_tasks())
        timer = aiotools.create_timer(delayed, 1)
        await asyncio.sleep(9.9)
        await aiotools.cancel_and_wait(timer)
        assert task_count + 1 >= len(aiotools.compat.all_tasks())
        assert spawn_count == done_count + cancel_count
        assert spawn_count == 10
        assert cancel_count == 5


@pytest.mark.asyncio
async def test_timer_leak_cancel() -> None:
    """
    Test the effect of TimerDelayPolicy.CANCEL which always
    cancels any pending previous tasks on each interval.
    """
    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():
        spawn_count = 0
        cancel_count = 0
        done_count = 0

        async def delayed(interval: float) -> None:
            nonlocal spawn_count, cancel_count, done_count
            spawn_count += 1
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                cancel_count += 1
                raise
            else:
                done_count += 1

        task_count = len(aiotools.compat.all_tasks())
        timer = aiotools.create_timer(
            delayed,
            0.01,
            aiotools.TimerDelayPolicy.CANCEL,
        )
        await asyncio.sleep(0.1)
        await aiotools.cancel_and_wait(timer)
        assert task_count + 1 >= len(aiotools.compat.all_tasks())
        assert spawn_count == cancel_count + done_count
        assert cancel_count == 10
        assert done_count == 0


@pytest.mark.asyncio
async def test_timer_leak_nocancel() -> None:
    """
    Test the effect of TimerDelayPolicy.CANCEL which always
    cancels any pending previous tasks on each interval.
    """
    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():
        spawn_count = 0
        cancel_count = 0
        done_count = 0

        async def delayed(interval: float) -> None:
            nonlocal spawn_count, cancel_count, done_count
            spawn_count += 1
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                cancel_count += 1
                raise
            else:
                done_count += 1

        task_count = len(aiotools.compat.all_tasks())
        timer = aiotools.create_timer(
            delayed,
            0.01,
            aiotools.TimerDelayPolicy.CANCEL,
        )
        await asyncio.sleep(0.096)
        await aiotools.cancel_and_wait(timer)
        assert task_count + 1 >= len(aiotools.compat.all_tasks())
        assert spawn_count == cancel_count + done_count
        assert cancel_count == 0
        assert done_count == 10


@pytest.mark.asyncio
async def test_timer_stuck_forever() -> None:
    # See achimnol/aiotools#69
    # (https://github.com/achimnol/aiotools/issues/69)
    vclock = aiotools.VirtualClock()
    vclock.vtime = 1709591365
    with vclock.patch_loop():
        await asyncio.sleep(1)
