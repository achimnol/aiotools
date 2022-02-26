import aiotools
import asyncio
import sys

import pytest


@pytest.mark.asyncio
async def test_ptaskgroup_all_done():

    count = 0

    async def subtask():
        nonlocal count
        await asyncio.sleep(0.1)
        count += 1

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async with aiotools.PersistentTaskGroup() as tg:
            assert tg.name.startswith("PTaskGroup-")
            for idx in range(10):
                t = tg.create_task(subtask(), name=f"Task-{idx}")
                if sys.version_info >= (3, 8):
                    assert t.get_name() == f"Task-{idx}"
                del t  # prevent ref-leak after loop
            assert len(tg._tasks) == 10
            # all done
            await asyncio.sleep(0.2)
            assert count == 10
            assert len(tg._tasks) == 0

        assert count == 10


@pytest.mark.asyncio
async def test_ptaskgroup_as_obj_attr():

    count = 0

    async def subtask():
        nonlocal count
        await asyncio.sleep(0.1)
        count += 1

    class LongLivedObject:

        def __init__(self):
            self.tg = aiotools.PersistentTaskGroup()

        async def work(self):
            self.tg.create_task(subtask())

        async def aclose(self):
            await self.tg.shutdown()

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        obj = LongLivedObject()
        for idx in range(10):
            await obj.work()
        assert len(obj.tg._tasks) == 10

        # shutdown after all done
        await asyncio.sleep(0.2)
        await obj.aclose()

        assert count == 10
        assert len(obj.tg._tasks) == 0

        count = 0
        obj = LongLivedObject()
        for idx in range(10):
            await obj.work()
        assert len(obj.tg._tasks) == 10

        # shutdown immediately
        await obj.aclose()

        assert count == 0
        assert len(obj.tg._tasks) == 0


@pytest.mark.asyncio
async def test_ptaskgroup_shutdown_from_different_task():

    count = 0

    async def subtask(idx):
        nonlocal count
        await asyncio.sleep(0.1 * idx)
        count += 1

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        tg = aiotools.PersistentTaskGroup()

        async def _main_task():
            async with tg:
                for idx in range(10):
                    tg.create_task(subtask(idx))
                assert len(tg._tasks) == 10

        async def _canceller_task():
            await asyncio.sleep(0.49)
            await tg.shutdown()

        async with asyncio.TaskGroup() as outer_tg:
            outer_tg.create_task(_main_task())
            outer_tg.create_task(_canceller_task())

        assert count == 5
        assert len(tg._tasks) == 0


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

        # shutdown after exit (all done) is no-op.
        assert count == 10
        assert len(tg._tasks) == 0
        await tg.shutdown()
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
            # let's abort immediately.
            await tg.shutdown()

        assert count == 0
        assert len(tg._tasks) == 0


@pytest.mark.asyncio
async def test_ptaskgroup_exc_handler():

    count = 0
    error_count = 0

    async def subtask():
        nonlocal count
        await asyncio.sleep(0.1)
        1 / 0
        count += 1

    async def handler(e):
        nonlocal error_count
        assert isinstance(e, ZeroDivisionError)
        error_count += 1

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async with aiotools.PersistentTaskGroup(exception_handler=handler) as tg:
            for _ in range(10):
                tg.create_task(subtask())
            assert len(tg._tasks) == 10
            await asyncio.sleep(1.0)

        assert count == 0
        assert error_count == 10
        assert len(tg._tasks) == 0


@pytest.mark.asyncio
async def test_ptaskgroup_cancel_with_await():

    count = 0

    async def subtask():
        nonlocal count
        try:
            await asyncio.sleep(0.1)
            count += 1   # should not be executed
        except asyncio.CancelledError:
            await asyncio.sleep(0.1)
            count += 10  # should be executed

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async with aiotools.PersistentTaskGroup() as tg:
            for _ in range(10):
                tg.create_task(subtask())
            assert len(tg._tasks) == 10
            # shutdown just after starting child tasks
            await asyncio.sleep(0.01)
            await tg.shutdown()

        # ensure that awaits in all cancellation handling blocks have been executed
        assert count == 100
        assert len(tg._tasks) == 0
