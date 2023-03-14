import asyncio

import async_timeout
import pytest

from aiotools import (
    aclosing,
    as_completed_safe,
    VirtualClock,
)


@pytest.mark.asyncio
async def test_as_completed_safe():

    async def do_job(delay, idx):
        await asyncio.sleep(delay)
        return idx

    async def fail_job(delay):
        await asyncio.sleep(delay)
        1 / 0

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

        results = []
        errors = []


@pytest.mark.asyncio
async def test_as_completed_safe_timeout_intrinsic():

    executed = 0
    loop_count = 0

    with VirtualClock().patch_loop():

        async def do_job(delay, idx):
            nonlocal executed
            await asyncio.sleep(delay)
            executed += 1
            return idx

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
        assert results == [1]
        assert timeouts == 1


@pytest.mark.asyncio
async def test_as_completed_safe_timeout_extlib():

    executed = 0
    loop_count = 0

    with VirtualClock().patch_loop():

        async def do_job(delay, idx):
            nonlocal executed
            await asyncio.sleep(delay)
            executed += 1
            return idx

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
        assert results == [1]
        assert timeouts == 1
