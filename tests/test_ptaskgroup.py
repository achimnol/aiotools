import asyncio
import sys
from unittest import mock

import pytest

import aiotools


# NOTE: Until pytest-asyncio support ExceptionGroup,
#       assertion failures inside PersistentTaskGroup/TaskGroup blocks
#       may be represented as sub-task errors instead of
#       being logged explicitly by pytest.


@pytest.mark.asyncio
async def test_ptaskgroup_naming():

    async def subtask():
        pass

    async with aiotools.PersistentTaskGroup(name="XYZ") as tg:
        t = tg.create_task(subtask(), name="ABC")
        assert tg.get_name() == "XYZ"
        if hasattr(t, 'get_name'):
            assert t.get_name() == "ABC"


@pytest.mark.asyncio
async def test_ptaskgroup_all_done():

    done_count = 0

    async def subtask():
        nonlocal done_count
        await asyncio.sleep(0.1)
        done_count += 1

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async with aiotools.PersistentTaskGroup() as tg:
            for idx in range(10):
                tg.create_task(subtask())
            assert tg._unfinished_tasks == 10
            # wait until all is done
            await asyncio.sleep(0.2)
            assert done_count == 10
            assert len(tg._tasks) == 0
            assert tg._unfinished_tasks == 0

        assert done_count == 10
        with pytest.raises(RuntimeError):
            tg.create_task(subtask())


@pytest.mark.asyncio
async def test_ptaskgroup_as_obj_attr():

    done_count = 0

    async def subtask():
        nonlocal done_count
        await asyncio.sleep(0.1)
        done_count += 1

    class LongLivedObject:

        def __init__(self):
            self.tg = aiotools.PersistentTaskGroup()
            assert not self.tg._entered

        async def work(self):
            self.tg.create_task(subtask())
            assert self.tg._entered

        async def aclose(self):
            await self.tg.shutdown()

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        obj = LongLivedObject()
        for idx in range(10):
            await obj.work()
        assert obj.tg._unfinished_tasks == 10

        # shutdown after all done
        await asyncio.sleep(0.2)
        await obj.aclose()

        assert done_count == 10
        assert len(obj.tg._tasks) == 0
        assert obj.tg._unfinished_tasks == 0

        done_count = 0
        obj = LongLivedObject()
        for idx in range(10):
            await obj.work()
        assert obj.tg._unfinished_tasks == 10

        # shutdown immediately
        await obj.aclose()

        assert done_count == 0
        assert len(obj.tg._tasks) == 0
        assert obj.tg._unfinished_tasks == 0


@pytest.mark.asyncio
async def test_ptaskgroup_shutdown_from_different_task():

    done_count = 0
    exec_after_termination = False

    async def subtask(idx):
        nonlocal done_count
        await asyncio.sleep(0.1 * idx)
        done_count += 1

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        outer_myself = aiotools.compat.current_task()
        tg = aiotools.PersistentTaskGroup()
        assert tg._parent_task is outer_myself

        async def _main_task():
            nonlocal exec_after_termination
            myself = aiotools.compat.current_task()
            async with tg:
                # The parent task is overriden when
                # using "async with", to keep consistency with
                # the original asyncio.TaskGroup.
                assert tg._parent_task is myself

                for idx in range(10):
                    tg.create_task(subtask(idx))

            # The code below must be executed even when
            # tg is shutdown from other tasks.
            exec_after_termination = True

        async def _stop_task():
            await asyncio.sleep(0.49)
            await tg.shutdown()

        async with aiotools.TaskGroup() as outer_tg:
            outer_tg.create_task(_main_task())
            outer_tg.create_task(_stop_task())

        assert done_count == 5
        assert exec_after_termination
        assert tg._parent_task is not outer_myself
        assert tg._unfinished_tasks == 0


@pytest.mark.asyncio
async def test_ptaskgroup_cancel_after_schedule():

    done_count = 0

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async def subtask():
            nonlocal done_count
            await asyncio.sleep(0.1)
            done_count += 1

        async with aiotools.PersistentTaskGroup() as tg:
            for _ in range(10):
                tg.create_task(subtask())
            await asyncio.sleep(0)

        # shutdown after exit (all done) is no-op.
        assert done_count == 10
        assert len(tg._tasks) == 0
        assert tg._unfinished_tasks == 0
        await tg.shutdown()
        assert len(tg._tasks) == 0
        assert tg._unfinished_tasks == 0


@pytest.mark.asyncio
async def test_ptaskgroup_cancel_before_schedule():

    done_count = 0

    async def subtask():
        nonlocal done_count
        await asyncio.sleep(0.1)
        done_count += 1

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async with aiotools.PersistentTaskGroup() as tg:
            for _ in range(10):
                tg.create_task(subtask())
            # let's abort immediately.
            await tg.shutdown()

        assert done_count == 0
        assert len(tg._tasks) == 0


@pytest.mark.skipif(
    sys.version_info < (3, 7, 0),
    reason='Requires Python 3.7 or higher',
    # In Python 3.6, this test hangs indefinitely.
    # We don't fix this -- 3.6 is EoL as of December 2021.
)
@pytest.mark.asyncio
async def test_ptaskgroup_await_result():

    done_count = 0

    async def subtask():
        nonlocal done_count
        await asyncio.sleep(0.1)
        done_count += 1
        return "a"

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        results = []

        async with aiotools.PersistentTaskGroup() as tg:

            ret = await tg.create_task(subtask())
            results.append(ret)

            ret = await asyncio.shield(tg.create_task(subtask()))
            results.append(ret)

            a = tg.create_task(subtask())
            try:
                ret = await a
                results.append(ret)
            finally:
                del a

            a = asyncio.shield(tg.create_task(subtask()))
            try:
                ret = await a
                results.append(ret)
            finally:
                del a

        # task callbacks has an extra ref, and they need to finish before
        # we task objetcs get garbage-collected.
        await asyncio.sleep(0)
        assert results == ["a", "a", "a", "a"]
        assert done_count == 4
        assert tg._unfinished_tasks == 0
        assert len(tg._tasks) == 0


@pytest.mark.skipif(
    sys.version_info < (3, 7, 0),
    reason='Requires Python 3.7 or higher',
    # In Python 3.6, this test hangs indefinitely.
    # We don't fix this -- 3.6 is EoL as of December 2021.
)
@pytest.mark.asyncio
async def test_ptaskgroup_await_exception():

    done_count = 0
    error_count = 0

    async def subtask():
        nonlocal done_count
        await asyncio.sleep(0.1)
        1 / 0
        done_count += 1

    async def handler(exc_type, exc_obj, exc_tb):
        nonlocal error_count
        assert issubclass(exc_type, ZeroDivisionError)
        error_count += 1

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async with aiotools.PersistentTaskGroup(exception_handler=handler) as tg:

            with pytest.raises(ZeroDivisionError):
                await tg.create_task(subtask())

            with pytest.raises(ZeroDivisionError):
                await asyncio.shield(tg.create_task(subtask()))

            with pytest.raises(ZeroDivisionError):
                a = tg.create_task(subtask())
                try:
                    await a
                finally:
                    del a

            with pytest.raises(ZeroDivisionError):
                # WARNING: This pattern leaks the reference to the task.
                a = asyncio.shield(tg.create_task(subtask()))
                try:
                    await a
                finally:
                    del a

        # task callbacks has an extra ref, and they need to finish before
        # we task objetcs get garbage-collected.
        await asyncio.sleep(0)
        assert done_count == 0
        assert error_count == 4
        assert tg._unfinished_tasks == 0
        assert len(tg._tasks) == 0


@pytest.mark.asyncio
async def test_ptaskgroup_exc_handler_swallow():

    done_count = 0
    error_count = 0

    async def subtask():
        nonlocal done_count
        await asyncio.sleep(0.1)
        1 / 0
        done_count += 1

    async def handler(exc_type, exc_obj, exc_tb):
        nonlocal error_count
        assert issubclass(exc_type, ZeroDivisionError)
        error_count += 1

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        try:
            async with aiotools.PersistentTaskGroup(exception_handler=handler) as tg:
                for _ in range(10):
                    tg.create_task(subtask())
        except ExceptionGroup as eg:
            # All non-base exceptions must be swallowed by
            # our exception handler.
            assert len(eg.subgroup(ZeroDivisionError).exceptions) == 0

        assert done_count == 0
        assert error_count == 10
        assert len(tg._tasks) == 0
        assert tg._unfinished_tasks == 0


@pytest.mark.skipif(
    sys.version_info < (3, 8, 0),
    reason='Requires Python 3.8 or higher',
)
@pytest.mark.asyncio
async def test_ptaskgroup_error_in_exc_handlers():

    done_count = 0
    error_count = 0

    async def subtask():
        nonlocal done_count
        await asyncio.sleep(0.1)
        1 / 0
        done_count += 1

    async def handler(exc_type, exc_obj, exc_tb):
        nonlocal error_count
        assert issubclass(exc_type, ZeroDivisionError)
        error_count += 1
        raise ValueError("something wrong in exception handler")

    loop = aiotools.compat.get_running_loop()
    vclock = aiotools.VirtualClock()
    with vclock.patch_loop(), \
         mock.patch.object(
             loop,
             'call_exception_handler',
             mock.MagicMock(),
         ):
        # Errors in exception handlers are covered by the event loop's exception
        # handler, so that they can be reported as soon as possible when they occur.
        #
        # In asyncio.TaskGroup, they are propagated as an exception group when
        # the task group terminates, but in PersistentTaskGroup it results in delayed
        # propagation because it needs to wait until other tasks to finish and
        # may not terminate at all until the application terminates if used as object
        # attributes instead of an async context manager.

        try:
            async with aiotools.PersistentTaskGroup(exception_handler=handler) as tg:
                for _ in range(10):
                    tg.create_task(subtask())
        except ValueError:
            assert False, "should not reach here"
        except ExceptionGroup:
            assert False, "should not reach here"

        # Check if the event loop exception handler is called.
        loop.call_exception_handler.assert_called()
        calls = loop.call_exception_handler.mock_calls
        for idx in range(10):
            assert isinstance(calls[idx].args[0]['exception'], ValueError)
        loop.call_exception_handler.reset_mock()  # to clean up task refs
        del calls  # to clean up task refs
        assert done_count == 0
        assert error_count == 10
        assert len(tg._tasks) == 0
        assert tg._unfinished_tasks == 0

        done_count = 0
        error_count = 0
        try:
            async with aiotools.PersistentTaskGroup(exception_handler=handler) as tg:
                for _ in range(10):
                    tg.create_task(subtask())
        except ExceptionGroup:
            assert False, "should not reach here"

        # Check if the event loop exception handler is called.
        loop.call_exception_handler.assert_called()
        calls = loop.call_exception_handler.mock_calls
        for idx in range(10):
            assert isinstance(calls[idx].args[0]['exception'], ValueError)
        loop.call_exception_handler.reset_mock()  # to clean up task refs
        del calls  # to clean up task refs
        assert done_count == 0
        assert error_count == 10
        assert len(tg._tasks) == 0
        assert tg._unfinished_tasks == 0


@pytest.mark.asyncio
async def test_ptaskgroup_cancel_with_await():

    done_count = 0

    async def subtask():
        nonlocal done_count
        try:
            await asyncio.sleep(0.1)
            done_count += 1   # should not be executed
        except asyncio.CancelledError:
            await asyncio.sleep(0.1)
            done_count += 10  # should be executed

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async with aiotools.PersistentTaskGroup() as tg:
            for _ in range(10):
                tg.create_task(subtask())
            # Shutdown just after starting child tasks.
            # Even in this case, awaits in the tasks' cancellation blocks
            # should be executed until their completion.
            await asyncio.sleep(0.01)
            await tg.shutdown()

        # ensure that awaits in all cancellation handling blocks have been executed
        assert done_count == 100
        assert len(tg._tasks) == 0
        assert tg._unfinished_tasks == 0


@pytest.mark.skipif(
    sys.version_info < (3, 7, 0),
    reason='Requires Python 3.7 or higher',
)
@pytest.mark.asyncio
async def test_ptaskgroup_current():

    names = []

    async def subtask():
        await asyncio.sleep(1)
        names.append(aiotools.current_ptaskgroup.get().get_name())

    async def job():
        names.append(aiotools.current_ptaskgroup.get().get_name())
        async with aiotools.PersistentTaskGroup(name="inner") as tg:
            for _ in range(10):
                tg.create_task(subtask())

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async with aiotools.PersistentTaskGroup(name="outer") as tg:
            tg.create_task(job())
            tg.create_task(job())
            tg.create_task(job())

    assert names == ["outer"] * 3 + ["inner"] * 30


@pytest.mark.asyncio
async def test_ptaskgroup_enumeration():

    async def subtask():
        await asyncio.sleep(1)

    async def job():
        async with aiotools.PersistentTaskGroup() as tg:
            for _ in range(10):
                tg.create_task(subtask())

    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():

        async with aiotools.PersistentTaskGroup() as tg:
            tg.create_task(job())
            tg.create_task(job())
            tg.create_task(job())
            await asyncio.sleep(0.1)
            all_tgs = aiotools.PersistentTaskGroup.all_ptaskgroups()

        assert len(all_tgs) == 4
