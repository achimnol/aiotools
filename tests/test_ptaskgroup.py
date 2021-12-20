import aiotools
import asyncio
import sys

import pytest


@pytest.mark.asyncio
async def test_ptaskgroup_all_done():

    count = 0

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async def subtask():
            nonlocal count
            await asyncio.sleep(0.1)
            count += 1

        async with aiotools.PersistentTaskGroup() as tg:
            assert tg.name.startswith("PTaskGroup-")
            for idx in range(10):
                t = tg.create_task(subtask(), name=f"Task-{idx}")
                if sys.version_info >= (3, 8):
                    assert t.get_name() == f"Task-{idx}"
                del t  # to prevent ref-leak after loop
            assert len(tg._tasks) == 10
            # all done
            await asyncio.sleep(0.2)
            assert count == 10
            assert len(tg._tasks) == 0

        assert count == 10


@pytest.mark.asyncio
async def test_ptaskgroup_cancel_after_schedule():

    count = 0

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async def subtask():
            nonlocal count
            await asyncio.sleep(0.1)
            count += 1

        async with aiotools.PersistentTaskGroup() as tg:
            for _ in range(10):
                tg.create_task(subtask())
            await asyncio.sleep(0)
            assert len(tg._tasks) == 10
            # all cancelled after scheduled

        assert count == 0
        assert len(tg._tasks) == 10
        await asyncio.sleep(0)
        # after cancellation, all refs should be gone
        assert len(tg._tasks) == 0


@pytest.mark.asyncio
async def test_ptaskgroup_cancel_before_schedule():

    count = 0

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async def subtask():
            nonlocal count
            await asyncio.sleep(0.1)
            count += 1

        async with aiotools.PersistentTaskGroup() as tg:
            for _ in range(10):
                tg.create_task(subtask())
            assert len(tg._tasks) == 10
            # all cancelled before scheduled

        assert count == 0
        assert len(tg._tasks) == 10
        await asyncio.sleep(0)
        # after cancellation, all refs should be gone
        assert len(tg._tasks) == 0


@pytest.mark.asyncio
async def test_ptaskgroup_exc_handler():

    count = 0
    error_count = 0

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async def subtask():
            nonlocal count
            await asyncio.sleep(0.1)
            1 / 0
            count += 1

        async def handler(e):
            nonlocal error_count
            assert isinstance(e, ZeroDivisionError)
            error_count += 1

        async with aiotools.PersistentTaskGroup(exception_handler=handler) as tg:
            for _ in range(10):
                tg.create_task(subtask())
            assert len(tg._tasks) == 10
            await asyncio.sleep(1.0)

        assert count == 0
        assert error_count == 10
        # after handlign error, all refs should be gone
        assert len(tg._tasks) == 0
