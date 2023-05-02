import asyncio
from contextvars import ContextVar, copy_context
from typing import TypeVar

import pytest

from aiotools import (
    aclosing,
    as_completed_safe,
    timeout,
    VirtualClock,
)

T = TypeVar("T")
cancelled = ContextVar("cancelled", default=0)


async def do_job(delay: float, result: T) -> T:
    try:
        await asyncio.sleep(delay)
        return result
    except asyncio.CancelledError:
        await asyncio.sleep(0.1)
        cancelled.set(cancelled.get() + 1)
        raise


async def fail_job(delay: float) -> None:
    try:
        await asyncio.sleep(delay)
        1 / 0
    except asyncio.CancelledError:
        await asyncio.sleep(0.1)
        cancelled.set(cancelled.get() + 1)
        raise


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
            fail_job(0.4),
        ])) as ag:
            async for result in ag:
                try:
                    results.append(await result)
                except Exception as e:
                    errors.append(e)
    assert results == [1, 3]
    assert len(errors) == 2
    assert isinstance(errors[0], ZeroDivisionError)
    assert isinstance(errors[1], ZeroDivisionError)


@pytest.mark.asyncio
async def test_as_completed_safe_immediate_failures():
    context = copy_context()
    cancelled.set(0)
    with VirtualClock().patch_loop():

        async def _inner() -> None:
            results = []
            errors = []
            async with aclosing(as_completed_safe([
                # All these jobs fail at the same tick.
                # Still, we should be able to retrieve all errors.
                fail_job(0),
                fail_job(0),
                fail_job(0),
            ], context=context)) as ag:
                async for result in ag:
                    try:
                        results.append(await result)
                    except Exception as e:
                        errors.append(e)
            assert results == []
            assert cancelled.get() == 0
            assert len(errors) == 3
            assert isinstance(errors[0], ZeroDivisionError)
            assert isinstance(errors[1], ZeroDivisionError)
            assert isinstance(errors[2], ZeroDivisionError)

        await asyncio.create_task(_inner(), context=context)


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
            await asyncio.sleep(0.1)  # should await cancellation
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
async def test_as_completed_safe_timeout_in_middle():
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
            await asyncio.sleep(0.1)  # should await cancellation
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
                    await asyncio.sleep(0.1)  # timeout occurs here
                    loop_count += 1
        except asyncio.TimeoutError:
            timeouts += 1

    assert loop_count == 0
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
            await asyncio.sleep(0.1)  # should await cancellation
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
            await asyncio.sleep(0.1)  # should await cancellation
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
            await asyncio.sleep(0.1)  # should await cancellation
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
            await asyncio.sleep(0.1)  # not awaited without aclose()
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
            await asyncio.sleep(0.1)  # should await cancellation
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
