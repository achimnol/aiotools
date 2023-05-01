import asyncio
from typing import TypeVar

import pytest

from aiotools import (
    aclosing,
    as_completed_safe,
    timeout,
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
async def test_as_completed_safe():
    results = []
    with VirtualClock().patch_loop():
        async with aclosing(as_completed_safe([
            do_job(0.3, 1),
            do_job(0.2, 2),
            do_job(0.1, 3),
        ])) as ag:
            async for result in ag:
                results.append(await result)
    assert results == [3, 2, 1]


@pytest.mark.asyncio
async def test_as_completed_safe_partial_failure():
    results = []
    errors = []
    with VirtualClock().patch_loop():
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
async def test_as_completed_safe_timeout_vanilla():
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
            async with (
                asyncio.timeout(0.15),
                aclosing(as_completed_safe([
                    do_job(0.1, 1),
                    # timeout occurs here
                    do_job(0.2, 2),
                    do_job(0.3, 3),
                ])) as ag,
            ):
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
async def test_as_completed_safe_timeout_custom():

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
            async with (
                timeout(0.15),
                aclosing(as_completed_safe([
                    do_job(0.1, 1),
                    # timeout occurs here
                    do_job(0.2, 2),
                    do_job(0.3, 3),
                ])) as ag,
            ):
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
                do_job(0.3, 3),
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
        with pytest.raises(ZeroDivisionError):
            async with aclosing(as_completed_safe([
                do_job(0.1, 1),
                do_job(0.2, 2),
                # cancellation occurs here
                do_job(0.3, 3),
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
                do_job(0.3, 3),
            ]):
                results.append(await result)
                loop_count += 1
                if loop_count == 2:
                    raise ZeroDivisionError()

    assert loop_count == 2
    assert executed == 2
    assert cancelled == 0  # should be one but without aclosing() it isn't.
    assert results == [1, 2]

    # Expected: "Task was destroyed but it is pending!" is observed here.


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
            # body error occurs here
            do_job(0.3, 3),
        ])
        try:
            async for result in ag:
                results.append(await result)
                loop_count += 1
                if loop_count == 2:
                    raise ZeroDivisionError()
        except ZeroDivisionError:
            await ag.aclose()
        else:
            pytest.fail("The inner exception should have been propagated out")

    assert loop_count == 2
    assert executed == 2
    assert cancelled == 1
    assert results == [1, 2]
