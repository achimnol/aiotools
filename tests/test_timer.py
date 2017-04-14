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

    timer = aiotools.create_timer(counter, 0.1)
    await asyncio.sleep(0.3)
    timer.cancel()
    await timer
    assert count == 3


@pytest.mark.asyncio
async def test_timer_leak():
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

    task_count = len(asyncio.Task.all_tasks())
    timer = aiotools.create_timer(delayed, 0.01)
    await asyncio.sleep(0.1)
    timer.cancel()
    await timer
    assert task_count + 1 >= len(asyncio.Task.all_tasks())
    assert spawn_count == done_count + cancel_count
    assert 9 <= spawn_count <= 10
    assert 4 <= cancel_count <= 5
