import asyncio
from typing import TypeVar

import pytest

from aiotools import (
    gather_safe,
    timeout,
    GroupResult,
    VirtualClock,
)

T = TypeVar("T")


async def do_job(delay: float, result: T) -> T:
    await asyncio.sleep(delay)
    return result


async def fail_job(delay: float) -> None:
    await asyncio.sleep(delay)
    1 / 0


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


@pytest.mark.xfail
@pytest.mark.asyncio
async def test_gather_safe_timeout_vanilla():
    detected_exc_groups = set()
    with VirtualClock().patch_loop():
        group_result = GroupResult()
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
        except* asyncio.TimeoutError:
            detected_exc_groups.add("timeout")
            # we should be able to access the partial results
            assert group_result.results == [1, 3]
            assert group_result.cancelled == 3
        except* ZeroDivisionError:
            detected_exc_groups.add("zerodiv")
            # we should be able to access the partial results
            assert group_result.results == [1, 3]
            assert group_result.cancelled == 3
        else:
            pytest.fail("inner exception was not re-raised")
        # This will fail because the vanilla asyncio.timeout()
        # does not look into BaseExceptionGroup to check whether it contains
        # CancelledError or not, and thus does not raise TimeoutError
        # while still cancelling at the moment when the timerout expires.
        assert detected_exc_groups == {"timeout", "zerodiv"}
        assert group_result.results == [1, 3]
        assert group_result.cancelled == 3


@pytest.mark.asyncio
async def test_gather_safe_timeout_custom():
    detected_exc_groups = set()
    with VirtualClock().patch_loop():
        group_result = GroupResult()
        try:
            async with timeout(0.35):  # our custom timeout
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
        except* asyncio.TimeoutError:
            detected_exc_groups.add("timeout")
            # we should be able to access the partial results
            assert group_result.results == [1, 3]
            assert group_result.cancelled == 3
        except* ZeroDivisionError:
            detected_exc_groups.add("zerodiv")
            # we should be able to access the partial results
            assert group_result.results == [1, 3]
            assert group_result.cancelled == 3
        else:
            pytest.fail("inner exception was not re-raised")
        assert detected_exc_groups == {"timeout", "zerodiv"}
        assert group_result.results == [1, 3]
        assert group_result.cancelled == 3
