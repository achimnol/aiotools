import asyncio
import sys

import async_timeout
import pytest

from aiotools import (
    aclosing,
    as_completed_safe,
    VirtualClock,
)
from aiotools.taskgroup.utils import gather_safe, GroupResult
from aiotools.supervisor import Supervisor


async def do_job(delay, idx):
    await asyncio.sleep(delay)
    return idx


async def fail_job(delay):
    await asyncio.sleep(delay)
    1 / 0


@pytest.mark.asyncio
async def test_as_completed_safe():
    with VirtualClock().patch_loop():
        results = []
        async with aclosing(as_completed_safe([
            do_job(0.3, 1),
            do_job(0.2, 2),
            do_job(0.1, 3),
        ])) as ag:
            async for result in ag:
                results.append(await result)

        assert results == [3, 2, 1]


@pytest.mark.asyncio
async def test_supervisor_partial_failure():
    with VirtualClock().patch_loop():
        results = []
        errors = []
        tasks = []
        async with Supervisor() as supervisor:
            tasks.append(supervisor.create_task(do_job(0.1, 1)))
            tasks.append(supervisor.create_task(fail_job(0.2)))
            tasks.append(supervisor.create_task(do_job(0.3, 3)))
        for t in tasks:
            try:
                results.append(t.result())
            except Exception as e:
                errors.append(e)

        assert results == [1, 3]
        assert len(errors) == 1
        assert isinstance(errors[0], ZeroDivisionError)


@pytest.mark.asyncio
async def test_as_completed_safe_partial_failure():
    with VirtualClock().patch_loop():
        results = []
        errors = []
        async with aclosing(as_completed_safe([
            do_job(0.1, 1),
            fail_job(0.2),
            do_job(0.3, 3),
        ])) as ag:
            async for result in ag:
                try:
                    results.append(await result)
                except Exception as e:
                    errors.append(e)

        assert results == [1, 3]
        assert len(errors) == 1
        assert isinstance(errors[0], ZeroDivisionError)


@pytest.mark.asyncio
async def test_gather_safe():
    with VirtualClock().patch_loop():
        group_result = GroupResult()
        await gather_safe([
            do_job(0.3, 3),
            do_job(0.2, 2),
            do_job(0.1, 1),
        ], group_result)
        assert group_result.results == [1, 2, 3]
        assert group_result.cancelled == 0


@pytest.mark.asyncio
async def test_gather_safe_partial_failure():
    with VirtualClock().patch_loop():
        group_result = GroupResult()
        try:
            await gather_safe([
                do_job(0.3, 3),
                fail_job(0.2),
                do_job(0.1, 1),
            ], group_result)
        except* ZeroDivisionError:
            assert group_result.results == [1, 3]
            assert group_result.cancelled == 0
        else:
            pytest.fail("inner-exception was not re-raised")


@pytest.mark.asyncio
async def test_gather_safe_timeout():
    detected_exc_groups = set()
    with VirtualClock().patch_loop():
        group_result = GroupResult()
        print("ready")
        try:
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
                ], group_result)
        except* asyncio.TimeoutError as e:
            detected_exc_groups.add("timeout")
            # assert len(e.exceptions) == 1
            # we should be able to access the partial results
            assert group_result.results == [1, 3]
            assert group_result.cancelled == 3
        except* ZeroDivisionError as e:
            detected_exc_groups.add("zerodiv")
            # assert len(e.exceptions) == 2
            # we should be able to access the partial results
            assert group_result.results == [1, 3]
            assert group_result.cancelled == 3
        else:
            pytest.fail("inner exception was not re-raised")
        print("all done")
        assert detected_exc_groups == {"timeout", "zerodiv"}
        assert group_result.results == [1, 3]
        assert group_result.cancelled == 3


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason='timeout supoport requires Python 3.11 or higher',
)
async def test_as_completed_safe_timeout_intrinsic():
    executed = 0
    cancelled = 0
    loop_count = 0

    async def do_job(delay, idx):
        nonlocal cancelled, executed
        try:
            await asyncio.sleep(delay)
            executed += 1
            return idx
        except asyncio.CancelledError:
            cancelled += 1
            raise

    with VirtualClock().patch_loop():
        results = []
        timeouts = 0
        try:
            async with aclosing(as_completed_safe([
                do_job(0.1, 1),
                # timeout occurs here
                do_job(0.2, 2),
                do_job(10.0, 3),
            ], timeout=0.15)) as ag:
                async for result in ag:
                    results.append(await result)
                    loop_count += 1
        except asyncio.TimeoutError:
            timeouts += 1

        assert loop_count == 1
        assert executed == 1
        assert cancelled == 2
        assert results == [1]
        assert timeouts == 1


@pytest.mark.asyncio
@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason='timeout supoport requires Python 3.11 or higher',
)
async def test_as_completed_safe_timeout_extlib():

    executed = 0
    cancelled = 0
    loop_count = 0

    async def do_job(delay, idx):
        nonlocal cancelled, executed
        try:
            await asyncio.sleep(delay)
            executed += 1
            return idx
        except asyncio.CancelledError:
            cancelled += 1
            raise

    with VirtualClock().patch_loop():
        results = []
        timeouts = 0
        try:
            async with async_timeout.timeout(0.15):
                async with aclosing(as_completed_safe([
                    do_job(0.1, 1),
                    # timeout occurs here
                    do_job(0.2, 2),
                    do_job(10.0, 3),
                ])) as ag:
                    async for result in ag:
                        results.append(await result)
                        loop_count += 1
        except asyncio.TimeoutError:
            timeouts += 1

        assert loop_count == 1
        assert executed == 1
        assert cancelled == 2
        assert results == [1]
        assert timeouts == 1


@pytest.mark.asyncio
async def test_as_completed_safe_cancel_from_body():
    executed = 0
    cancelled = 0
    loop_count = 0

    async def do_job(delay, idx):
        nonlocal cancelled, executed
        try:
            await asyncio.sleep(delay)
            executed += 1
            return idx
        except asyncio.CancelledError:
            cancelled += 1
            raise

    with VirtualClock().patch_loop():
        results = []
        with pytest.raises(asyncio.CancelledError):
            async with aclosing(as_completed_safe([
                do_job(0.1, 1),
                do_job(0.2, 2),
                # cancellation occurs here
                do_job(10.0, 3),
            ])) as ag:
                async for result in ag:
                    results.append(await result)
                    loop_count += 1
                    if loop_count == 2:
                        raise asyncio.CancelledError()

        assert loop_count == 2
        assert executed == 2
        assert cancelled == 1
        assert results == [1, 2]


@pytest.mark.asyncio
async def test_as_completed_safe_error_from_body():
    executed = 0
    cancelled = 0
    loop_count = 0

    async def do_job(delay, idx):
        nonlocal cancelled, executed
        try:
            await asyncio.sleep(delay)
            executed += 1
            return idx
        except asyncio.CancelledError:
            cancelled += 1
            raise

    with VirtualClock().patch_loop():
        results = []
        with pytest.raises(asyncio.CancelledError):
            async with aclosing(as_completed_safe([
                do_job(0.1, 1),
                do_job(0.2, 2),
                # cancellation occurs here
                do_job(10.0, 3),
            ])) as ag:
                async for result in ag:
                    results.append(await result)
                    loop_count += 1
                    if loop_count == 2:
                        raise ZeroDivisionError()

        assert loop_count == 2
        assert executed == 2
        assert cancelled == 1
        assert results == [1, 2]


@pytest.mark.asyncio
async def test_as_completed_safe_error_from_body_without_aclosing():
    executed = 0
    cancelled = 0
    loop_count = 0

    async def do_job(delay, idx):
        nonlocal cancelled, executed
        try:
            await asyncio.sleep(delay)
            executed += 1
            return idx
        except asyncio.CancelledError:
            cancelled += 1
            raise

    with VirtualClock().patch_loop():
        results = []
        # This is "unsafe" because it cannot guarantee the completion of
        # the internal supervisor.
        with pytest.raises(ZeroDivisionError):
            async for result in as_completed_safe([
                do_job(0.1, 1),
                do_job(0.2, 2),
                # cancellation occurs here
                do_job(10.0, 3),
            ]):
                results.append(await result)
                loop_count += 1
                if loop_count == 2:
                    raise ZeroDivisionError()

        assert loop_count == 2
        assert executed == 2
        assert cancelled == 0  # should be one but without aclosing() it isn't.
        assert results == [1, 2]

        # It is expected that this test generates:
        # "Task was destroyed but it is pending!" 


@pytest.mark.asyncio
async def test_as_completed_safe_error_from_body_aclose_afterwards():
    executed = 0
    cancelled = 0
    loop_count = 0

    async def do_job(delay, idx):
        nonlocal cancelled, executed
        try:
            await asyncio.sleep(delay)
            executed += 1
            return idx
        except asyncio.CancelledError:
            cancelled += 1
            raise

    with VirtualClock().patch_loop():
        results = []
        ag = as_completed_safe([
            do_job(0.1, 1),
            do_job(0.2, 2),
            # inner error occurs here
            do_job(10.0, 3),
        ])
        try:
            async for result in ag:
                results.append(await result)
                loop_count += 1
                if loop_count == 2:
                    raise ZeroDivisionError()
        except ZeroDivisionError:
            with pytest.raises(asyncio.CancelledError):
                await ag.aclose()
        else:
            pytest.fail("The inner exception should have been propagated out")

        assert loop_count == 2
        assert executed == 2
        assert cancelled == 1
        assert results == [1, 2]
