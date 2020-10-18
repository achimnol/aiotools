import asyncio

import pytest

import aiotools


@pytest.mark.asyncio
async def test_timer():
    """
    Test the timer functionality.
    """
    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():
        count = 0

        async def counter(interval):
            assert interval == 0.1
            nonlocal count
            await asyncio.sleep(0)
            count += 1

        count = 0
        timer = aiotools.create_timer(counter, 0.1)
        await asyncio.sleep(0.22)
        timer.cancel()
        await timer
        assert count == 3

        count = 0
        timer = aiotools.create_timer(counter, 0.1, aiotools.TimerDelayPolicy.CANCEL)
        await asyncio.sleep(0.22)
        timer.cancel()
        await timer
        # should have same results
        assert count == 3


@pytest.mark.asyncio
async def test_timer_leak_default():
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

        async def delayed(interval):
            nonlocal spawn_count, cancel_count, done_count
            spawn_count += 1
            try:
                await asyncio.sleep(5)
                done_count += 1
            except asyncio.CancelledError:
                cancel_count += 1

        task_count = len(aiotools.compat.all_tasks())
        timer = aiotools.create_timer(delayed, 1)
        await asyncio.sleep(9.9)
        timer.cancel()
        await timer
        assert task_count + 1 >= len(aiotools.compat.all_tasks())
        assert spawn_count == done_count + cancel_count
        assert spawn_count == 10
        assert cancel_count == 5


@pytest.mark.asyncio
async def test_timer_leak_cancel():
    """
    Test the effect of TimerDelayPolicy.CANCEL which always
    cancels any pending previous tasks on each interval.
    """
    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():
        spawn_count = 0
        cancel_count = 0
        done_count = 0

        async def delayed(interval):
            nonlocal spawn_count, cancel_count, done_count
            spawn_count += 1
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                cancel_count += 1
            else:
                done_count += 1

        task_count = len(aiotools.compat.all_tasks())
        timer = aiotools.create_timer(
            delayed, 0.01, aiotools.TimerDelayPolicy.CANCEL,
        )
        await asyncio.sleep(0.1)
        timer.cancel()
        await timer
        await asyncio.sleep(0)
        assert task_count + 1 >= len(aiotools.compat.all_tasks())
        assert spawn_count == cancel_count + done_count
        assert cancel_count == 10
        assert done_count == 0


@pytest.mark.asyncio
async def test_timer_leak_nocancel():
    """
    Test the effect of TimerDelayPolicy.CANCEL which always
    cancels any pending previous tasks on each interval.
    """
    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():
        spawn_count = 0
        cancel_count = 0
        done_count = 0

        async def delayed(interval):
            nonlocal spawn_count, cancel_count, done_count
            spawn_count += 1
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                cancel_count += 1
            else:
                done_count += 1

        task_count = len(aiotools.compat.all_tasks())
        timer = aiotools.create_timer(
            delayed, 0.01, aiotools.TimerDelayPolicy.CANCEL,
        )
        await asyncio.sleep(0.096)
        timer.cancel()
        await timer
        await asyncio.sleep(0)
        assert task_count + 1 >= len(aiotools.compat.all_tasks())
        assert spawn_count == cancel_count + done_count
        assert cancel_count == 0
        assert done_count == 10
