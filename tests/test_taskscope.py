from __future__ import annotations

import asyncio
import sys
from typing import Any, TypeVar, cast

import pytest

from aiotools import (
    TaskScope,
    VirtualClock,
    cancel_and_wait,
    move_on_after,
)

T = TypeVar("T")


async def fail_after(delay: float) -> None:
    await asyncio.sleep(delay)
    raise ZeroDivisionError()


@pytest.mark.asyncio
async def test_taskscope_keep_running() -> None:
    results: list[str] = []

    async def parent() -> None:
        async with TaskScope() as ts:
            ts.create_task(fail_after(0.2))
            await asyncio.sleep(0.5)
            results.append("context-final")  # kept running
        await asyncio.sleep(0.5)
        results.append("parent-final")  # unaffected

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent())
        await parent_task
        assert results == ["context-final", "parent-final"]


@pytest.mark.skipif(
    sys.version_info < (3, 14),
    reason="asyncio callgraph requires Python 3.14 or higher",
)
@pytest.mark.asyncio
async def test_taskscope_call_graph_support() -> None:
    graph = None

    async def child() -> None:
        nonlocal graph
        print()
        asyncio.print_call_graph()  # type: ignore[attr-defined]
        graph = asyncio.capture_call_graph()  # type: ignore[attr-defined]
        await asyncio.sleep(1.0)

    async def parent() -> None:
        async with TaskScope() as ts:
            ts.create_task(child(), name="child1")

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent(), name="parent")
        await parent_task

        assert graph is not None
        assert len(graph.awaited_by) == 1, "TaskScope should chain the task awaiters."
        parent_task = cast(asyncio.Task[None], graph.awaited_by[0].future)
        assert parent_task.get_name() == "parent", (
            "the awaiter name should be 'parent', not 'Task-1' or something else."
        )


@pytest.mark.skipif(
    sys.version_info < (3, 14),
    reason="per-task eagerness control is available in Python 3.14 or higher",
)
@pytest.mark.asyncio
async def test_taskscope_create_task_passing_kwargs() -> None:
    """
    Passing arbitrary kwargs to create_task() methods is now allowed in Python 3.14.

    In Python 3.14, we now have "eager_start=True" option to control individual
    task's eagerness, so let's make a kwargs-passing test using it.
    """
    result_holder: list[str] = []

    async def eager_task() -> None:
        # No await - completes synchronously
        result_holder.append("done")

    async with TaskScope() as ts:
        task = ts.create_task(eager_task(), eager_start=True)
        # The task is already done if it is eagerly scheduled.
        assert result_holder == ["done"]
        assert task.done()


@pytest.mark.parametrize("shield", [False, True])
@pytest.mark.asyncio
async def test_taskscope_partial_failure(shield: bool) -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []
    with VirtualClock().patch_loop():
        async with TaskScope(shield=shield) as ts:
            tasks.append(ts.create_task(asyncio.sleep(0.1, 1)))
            tasks.append(ts.create_task(fail_after(0.2)))
            tasks.append(ts.create_task(asyncio.sleep(0.3, 3)))
            await asyncio.sleep(0.1)
            results.append(4)
            await asyncio.sleep(0.15)  # after child failure
            results.append(5)  # should run as-is
        for t in tasks:
            try:
                results.append(t.result())
            except asyncio.CancelledError:
                cancelled += 1
            except Exception as e:
                errors.append(e)

    # Task #3 should have run to its completion.
    assert results == [4, 5, 1, 3]
    assert len(errors) == 1
    assert isinstance(errors[0], ZeroDivisionError)
    assert cancelled == 0


@pytest.mark.parametrize("shield", [False, True])
@pytest.mark.asyncio
async def test_taskscope_child_self_cancellation(shield: bool) -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []

    async def cancel_after(delay: float) -> None:
        await asyncio.sleep(delay)
        raise asyncio.CancelledError

    with VirtualClock().patch_loop():
        async with TaskScope(shield=shield) as ts:
            tasks.append(ts.create_task(asyncio.sleep(0.1, 1)))
            tasks.append(ts.create_task(cancel_after(0.2)))
            tasks.append(ts.create_task(asyncio.sleep(0.3, 3)))
            await asyncio.sleep(0.1)
            results.append(4)
            await asyncio.sleep(0.15)  # after child cancellation
            results.append(5)  # should run as-is

        for t in tasks:
            try:
                results.append(t.result())
            except asyncio.CancelledError:
                cancelled += 1
            except Exception as e:
                errors.append(e)

    # Task #3 should have run to its completion.
    assert results == [4, 5, 1, 3]
    assert errors == []
    assert cancelled == 1


@pytest.mark.asyncio
async def test_taskscope_external_timeout_when_not_shielded() -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []
    with VirtualClock().patch_loop():
        with pytest.raises(TimeoutError):
            async with (
                # from TaskScope's POV, this timeout context is an outside canceller.
                asyncio.timeout(0.25),
                TaskScope() as ts,
            ):
                tasks.append(ts.create_task(asyncio.sleep(0.1, 1)))
                tasks.append(ts.create_task(asyncio.sleep(0.2, 2)))
                tasks.append(ts.create_task(asyncio.sleep(0.3, 3)))  # timeout
                await asyncio.sleep(0.23)
                results.append(4)  # executed before timeout
                await asyncio.sleep(0.03)
                results.append(5)  # executed after timeout

        for t in tasks:
            try:
                results.append(t.result())
            except asyncio.CancelledError:
                cancelled += 1
            except Exception as e:
                errors.append(e)

    # Since TaskScope is not shielded, the timeout cancellation is applied transparently.
    assert results == [4, 1, 2]
    assert errors == []
    assert cancelled == 1  # cancelled due to timeout


@pytest.mark.asyncio
async def test_taskscope_external_timeout_when_shielded() -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []
    with VirtualClock().patch_loop():
        # It should raise timeout but AFTER the child tasks have run to completion.
        with pytest.raises(TimeoutError):
            async with (
                # from TaskScope's POV, this timeout context is an outside canceller.
                asyncio.timeout(0.25),
                TaskScope(shield=True) as ts,
            ):
                tasks.append(ts.create_task(asyncio.sleep(0.1, 1)))
                tasks.append(ts.create_task(asyncio.sleep(0.2, 2)))
                tasks.append(ts.create_task(asyncio.sleep(0.3, 3)))  # timeout
                await asyncio.sleep(0.23)
                results.append(4)  # executed before timeout
                await asyncio.sleep(0.03)
                # executed after timeout, but shielded from outside cancellation
                results.append(5)

        for t in tasks:
            try:
                results.append(t.result())
            except asyncio.CancelledError:
                cancelled += 1
            except Exception as e:
                errors.append(e)

    # Since TaskScope is shielded, the timeout cancellation is absorbed,
    # while it raises TimeoutError afterwards since the timeout context has been expired.
    assert results == [4, 5, 1, 2, 3]
    assert errors == []
    assert cancelled == 0  # nothing is cancelled


@pytest.mark.parametrize("shield", [False, True])
@pytest.mark.asyncio
async def test_taskscope_intrinsic_timeout(shield: bool) -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []
    with VirtualClock().patch_loop():
        with pytest.raises(TimeoutError):
            async with TaskScope(timeout=0.25, shield=shield) as ts:
                tasks.append(ts.create_task(asyncio.sleep(0.1, 1)))
                tasks.append(ts.create_task(asyncio.sleep(0.2, 2)))
                tasks.append(ts.create_task(asyncio.sleep(0.3, 3)))  # timeout
                await asyncio.sleep(0.23)
                results.append(4)  # executed before timeout
                await asyncio.sleep(0.03)
                results.append(5)  # executed after timeout
        for t in tasks:
            try:
                results.append(t.result())
            except asyncio.CancelledError:
                cancelled += 1
            except Exception as e:
                errors.append(e)

    assert results == [4, 1, 2]
    assert errors == []
    assert cancelled == 1  # cancelled due to timeout


@pytest.mark.asyncio
async def test_taskscope_intrinsic_timeout_shielded_from_outside_cancellation() -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []

    async def some_work() -> None:
        async with TaskScope(timeout=0.25, shield=True) as ts:
            tasks.append(ts.create_task(asyncio.sleep(0.1, 1)))
            tasks.append(ts.create_task(asyncio.sleep(0.2, 2)))
            tasks.append(ts.create_task(asyncio.sleep(0.3, 3)))  # timeout
            await asyncio.sleep(0.23)
            results.append(4)  # executed before timeout
            await asyncio.sleep(0.03)
            results.append(5)  # executed after timeout

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(some_work())
        await asyncio.sleep(0.05)
        # TaskScope is shielded, so its children and body should continue with its own timeout.
        # However, since its timeout mechanism is triggered,
        # the result of parent_task is an exception (TimeoutError).
        with pytest.raises(TimeoutError):
            await cancel_and_wait(parent_task)
        assert parent_task.done()
        assert isinstance(parent_task.exception(), TimeoutError)

    for t in tasks:
        try:
            results.append(t.result())
        except asyncio.CancelledError:
            cancelled += 1
        except Exception as e:
            errors.append(e)

    assert results == [4, 1, 2]
    assert errors == []
    assert cancelled == 1  # cancelled due to timeout


@pytest.mark.asyncio
async def test_taskscope_move_on_after() -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []
    with VirtualClock().patch_loop():
        async with move_on_after(0.25) as ts:
            tasks.append(ts.create_task(asyncio.sleep(0.1, 1)))
            tasks.append(ts.create_task(asyncio.sleep(0.2, 2)))
            tasks.append(ts.create_task(asyncio.sleep(0.3, 3)))  # timeout
            await asyncio.sleep(0.23)
            results.append(4)  # executed before timeout
            await asyncio.sleep(0.03)
            results.append(5)  # executed after timeout

        # This collection code should be executed as move_on_after() has ignored TimeoutError.
        for t in tasks:
            try:
                results.append(t.result())
            except asyncio.CancelledError:
                cancelled += 1
            except Exception as e:
                errors.append(e)

    assert results == [4, 1, 2]
    assert errors == []
    assert cancelled == 1  # cancelled due to timeout
