import asyncio
import sys
import warnings

import pytest

from aiotools import (
    TaskGroup,
    TaskGroupError,
    VirtualClock,
)


@pytest.mark.asyncio
async def test_taskgroup_naming():

    async def subtask():
        pass

    async with TaskGroup(name="XYZ") as tg:
        t = tg.create_task(subtask(), name="ABC")
        assert tg.get_name() == "XYZ"
        if hasattr(t, 'get_name'):
            assert t.get_name() == "ABC"


@pytest.mark.asyncio
async def test_delayed_subtasks():
    with VirtualClock().patch_loop():
        async with TaskGroup() as tg:
            t1 = tg.create_task(asyncio.sleep(3, 'a'))
            t2 = tg.create_task(asyncio.sleep(2, 'b'))
            t3 = tg.create_task(asyncio.sleep(1, 'c'))
        assert t1.done()
        assert t2.done()
        assert t3.done()
        assert t1.result() == 'a'
        assert t2.result() == 'b'
        assert t3.result() == 'c'


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.version_info < (3, 7),
    reason='contextvars is available only in Python 3.7 or later',
)
async def test_contextual_taskgroup():

    from aiotools import current_taskgroup

    refs = []

    async def check_tg(delay):
        await asyncio.sleep(delay)
        refs.append(current_taskgroup.get())

    with VirtualClock().patch_loop():
        async with TaskGroup() as outer_tg:
            ot1 = outer_tg.create_task(check_tg(0.1))
            async with TaskGroup() as inner_tg:
                it1 = inner_tg.create_task(check_tg(0.2))
            ot2 = outer_tg.create_task(check_tg(0.3))
        assert ot1.done()
        assert ot2.done()
        assert it1.done()
        assert refs == [outer_tg, inner_tg, outer_tg]

        with pytest.raises(LookupError):
            # outside of any taskgroup, this is an error.
            current_taskgroup.get()


@pytest.mark.skipif(
    sys.version_info < (3, 7),
    reason='contextvars is available only in Python 3.7 or later',
)
@pytest.mark.filterwarnings('ignore::RuntimeWarning')
@pytest.mark.asyncio
async def test_contextual_taskgroup_spawning():

    from aiotools import current_taskgroup

    total_jobs = 0

    async def job():
        nonlocal total_jobs
        await asyncio.sleep(0)
        total_jobs += 1

    async def spawn_job():
        await asyncio.sleep(0)
        tg = current_taskgroup.get()
        tg.create_task(job())

    async def inner_tg_job():
        await asyncio.sleep(0)
        async with TaskGroup() as tg:
            tg.create_task(job())

    with VirtualClock().patch_loop():

        total_jobs = 0
        async with TaskGroup() as tg:
            t = tg.create_task(spawn_job())
        assert t.done()
        del t
        assert total_jobs == 1

        total_jobs = 0
        async with TaskGroup() as tg:
            tg.create_task(inner_tg_job())
            tg.create_task(spawn_job())
            tg.create_task(inner_tg_job())
            tg.create_task(spawn_job())
            # Give the subtasks chances to run.
            await asyncio.sleep(1)
        assert total_jobs == 4


@pytest.mark.asyncio
async def test_taskgroup_cancellation():
    with VirtualClock().patch_loop():

        async def do_job(delay, result):
            # NOTE: replacing do_job directly with asyncio.sleep
            #       results future-pending-after-loop-closed error,
            #       because asyncio.sleep() is not a task but a future.
            await asyncio.sleep(delay)
            return result

        with pytest.raises(asyncio.CancelledError):
            async with TaskGroup() as tg:
                t1 = tg.create_task(do_job(0.3, 'a'))
                t2 = tg.create_task(do_job(0.6, 'b'))
                await asyncio.sleep(0.5)
                raise asyncio.CancelledError

        assert t1.done()
        assert t2.cancelled()
        assert t1.result() == 'a'


@pytest.mark.asyncio
async def test_subtask_cancellation():

    results = []

    async def do_job():
        await asyncio.sleep(1)
        results.append('a')

    async def do_cancel():
        await asyncio.sleep(0.5)
        raise asyncio.CancelledError

    with VirtualClock().patch_loop():
        async with TaskGroup() as tg:
            t1 = tg.create_task(do_job())
            t2 = tg.create_task(do_cancel())
            t3 = tg.create_task(do_job())
        assert t1.done()
        assert t2.cancelled()
        assert t3.done()
        assert results == ['a', 'a']


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cancel_msg",
    (
        ["MISSING", None, "msg"]
        if sys.version_info >= (3, 9, 0)
        else ["MISSING"]
    ),
)
async def test_cancel_parent_task(cancel_msg):

    results = []

    async def do_job():
        await asyncio.sleep(1)
        results.append('a')

    async def parent():
        async with TaskGroup() as tg:
            tg.create_task(do_job())

    with VirtualClock().patch_loop():
        parent_task = asyncio.ensure_future(parent())
        await asyncio.sleep(0.1)
        if cancel_msg == "MISSING":
            parent_task.cancel()
        else:
            parent_task.cancel(cancel_msg)
        await asyncio.gather(parent_task, return_exceptions=True)
        assert results == []


@pytest.mark.asyncio
async def test_taskgroup_distinguish_inner_error_and_outer_cancel():

    async def do_error():
        await asyncio.sleep(0.5)
        raise ValueError("bad stuff")

    with VirtualClock().patch_loop():

        with pytest.raises(TaskGroupError) as eg:
            async with TaskGroup() as tg:
                t1 = tg.create_task(do_error())
                # The following sleep is cancelled due to do_error(),
                # raising CancelledError!
                await asyncio.sleep(1)
                # We need to preserve the source exception instead of the outer
                # cancellation.  __aexit__() should not treat CancelledError as a
                # base error to handle such cases.

        assert t1.done()
        assert not t1.cancelled()
        assert isinstance(t1.exception(), ValueError)
        assert len(eg.value.__errors__) == 1
        assert isinstance(eg.value.__errors__[0], ValueError)


@pytest.mark.asyncio
async def test_taskgroup_error():
    with VirtualClock().patch_loop():

        async def do_job(delay, result):
            await asyncio.sleep(delay)
            if result == 'x':
                raise ZeroDivisionError('oops')
            else:
                return 99

        with pytest.raises(TaskGroupError) as e:
            async with TaskGroup() as tg:
                t1 = tg.create_task(do_job(0.3, 'a'))
                t2 = tg.create_task(do_job(0.5, 'x'))
                t3 = tg.create_task(do_job(0.7, 'a'))

        assert len(e.value.__errors__) == 1
        assert type(e.value.__errors__[0]).__name__ == 'ZeroDivisionError'

        assert t1.done()
        assert await t1 == 99
        assert t1.result() == 99
        assert t1.exception() is None

        assert t2.done()
        with pytest.raises(ZeroDivisionError):
            await t2
        with pytest.raises(ZeroDivisionError):
            t2.result()
        assert type(t2.exception()).__name__ == 'ZeroDivisionError'

        assert t3.cancelled()


@pytest.mark.asyncio
async def test_taskgroup_error_weakref():
    with VirtualClock().patch_loop():

        results = []

        async def do_job(delay, result):
            await asyncio.sleep(delay)
            if result == 'x':
                results.append('x')
                raise ZeroDivisionError('oops')
            else:
                results.append('o')
                return 99

        with pytest.raises(TaskGroupError) as e:
            async with TaskGroup() as tg:
                # We don't keep the reference to the tasks,
                # but they should behave the same way
                # regardless of usage of WeakSet in the implementation.
                tg.create_task(do_job(0.3, 'a'))
                tg.create_task(do_job(0.5, 'x'))
                tg.create_task(do_job(0.7, 'a'))

        assert len(e.value.__errors__) == 1
        assert type(e.value.__errors__[0]).__name__ == 'ZeroDivisionError'
        assert results == ['o', 'x']


@pytest.mark.asyncio
async def test_taskgroup_memoryleak_with_persistent_tg():

    with VirtualClock().patch_loop(), \
         warnings.catch_warnings():
        warnings.simplefilter("ignore")

        async def do_job(delay):
            await asyncio.sleep(delay)
            return 1

        async with TaskGroup() as tg:
            for count in range(1000):
                await asyncio.sleep(1)
                tg.create_task(do_job(10))
                if count == 100:
                    # 10 ongoing tasks + 1 just spawned task
                    assert len(tg._tasks) == 11
            await asyncio.sleep(10.1)
            assert len(tg._tasks) == 0
