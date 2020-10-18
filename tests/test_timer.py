import pytest

import asyncio
import aiotools


@pytest.mark.asyncio
async def test_timer():
    '''
    Test the timer functionality.
    '''
    count = 0

    async def counter(interval):
        assert interval == 0.1
        nonlocal count
        count += 1

    count = 0
    timer = aiotools.create_timer(counter, 0.1)
    await asyncio.sleep(0.3)
    timer.cancel()
    await timer
    assert count == 3

    count = 0
    timer = aiotools.create_timer(counter, 0.1, aiotools.TimerDelayPolicy.CANCEL)
    await asyncio.sleep(0.3)
    timer.cancel()
    await timer
    # should have same results
    assert count == 3


@pytest.mark.asyncio
async def test_timer_leak_default():
    '''
    Test if the timer-fired tasks are claned up properly
    even when each timer-fired task takes longer than the timer interval.
    (In this case they will accumulate indefinitely!)
    '''
    spawn_count = 0
    cancel_count = 0
    done_count = 0

    async def delayed(interval):
        nonlocal spawn_count, cancel_count, done_count
        spawn_count += 1
        try:
            await asyncio.sleep(0.05)
            done_count += 1
        except asyncio.CancelledError:
            cancel_count += 1

    task_count = len(aiotools.compat.all_tasks())
    timer = aiotools.create_timer(delayed, 0.01)
    await asyncio.sleep(0.1)
    timer.cancel()
    await timer
    assert task_count + 1 >= len(aiotools.compat.all_tasks())
    assert spawn_count == done_count + cancel_count
    assert 9 <= spawn_count <= 10
    assert 4 <= cancel_count <= 5


@pytest.mark.asyncio
async def test_timer_leak_cancel():
    '''
    Test the effect of TimerDelayPolicy.CANCEL which always
    cancels any pending previous tasks on each interval.
    '''
    spawn_count = 0
    cancel_count = 0
    done_count = 0

    async def delayed(interval):
        nonlocal spawn_count, cancel_count, done_count
        spawn_count += 1
        try:
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            cancel_count += 1
        else:
            done_count += 1

    task_count = len(aiotools.compat.all_tasks())
    timer = aiotools.create_timer(delayed, 0.01, aiotools.TimerDelayPolicy.CANCEL)
    await asyncio.sleep(0.1)
    timer.cancel()
    await timer
    assert task_count + 1 >= len(aiotools.compat.all_tasks())
    assert spawn_count == cancel_count
    assert done_count == 0


@pytest.mark.asyncio
async def test_timer_leak_nocancel():
    '''
    Test the effect of TimerDelayPolicy.CANCEL which always
    cancels any pending previous tasks on each interval.
    '''
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
    timer = aiotools.create_timer(delayed, 0.01, aiotools.TimerDelayPolicy.CANCEL)
    await asyncio.sleep(0.096)
    timer.cancel()
    await timer
    assert task_count + 1 >= len(aiotools.compat.all_tasks())
    assert spawn_count == done_count
    assert cancel_count == 0
