from __future__ import annotations

import asyncio
import sys
from typing import Any, TypeVar, cast

import pytest

from aiotools import TaskScope, VirtualClock

T = TypeVar("T")


async def do_job(delay: float, result: T) -> T:
    await asyncio.sleep(delay)
    return result


async def fail_job(delay: float) -> None:
    await asyncio.sleep(delay)
    raise ZeroDivisionError()


@pytest.mark.asyncio
async def test_taskscope_keep_running() -> None:
    results: list[str] = []

    async def parent() -> None:
        async with TaskScope() as ts:
            ts.create_task(fail_job(0.2))
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


@pytest.mark.asyncio
async def test_taskscope_partial_failure() -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    tasks: list[asyncio.Task[Any]] = []
    with VirtualClock().patch_loop():
        async with TaskScope() as ts:
            tasks.append(ts.create_task(do_job(0.1, 1)))
            tasks.append(ts.create_task(fail_job(0.2)))
            tasks.append(ts.create_task(do_job(0.3, 3)))
        for t in tasks:
            try:
                results.append(t.result())
            except Exception as e:
                errors.append(e)
    assert results == [1, 3]
    assert len(errors) == 1
    assert isinstance(errors[0], ZeroDivisionError)


@pytest.mark.asyncio
async def test_taskscope_timeout_before_failure() -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []
    with VirtualClock().patch_loop():
        with pytest.raises(TimeoutError):
            async with (
                asyncio.timeout(0.15),
                TaskScope() as ts,
            ):
                tasks.append(ts.create_task(do_job(0.1, 1)))
                # timeout here
                tasks.append(ts.create_task(fail_job(0.2)))
                tasks.append(ts.create_task(do_job(0.3, 3)))
        for t in tasks:
            try:
                results.append(t.result())
            except asyncio.CancelledError:
                cancelled += 1
            except Exception as e:
                errors.append(e)
    assert results == [1]
    assert len(errors) == 0
    assert cancelled == 2


@pytest.mark.asyncio
async def test_taskscope_timeout_after_failure() -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []
    with VirtualClock().patch_loop():
        with pytest.raises(TimeoutError):
            async with (
                asyncio.timeout(0.25),
                TaskScope() as ts,
            ):
                tasks.append(ts.create_task(do_job(0.1, 1)))
                tasks.append(ts.create_task(fail_job(0.2)))
                # timeout here
                tasks.append(ts.create_task(do_job(0.3, 3)))
        for t in tasks:
            try:
                results.append(t.result())
            except asyncio.CancelledError:
                cancelled += 1
            except Exception as e:
                errors.append(e)
    assert results == [1]
    assert len(errors) == 1
    assert isinstance(errors[0], ZeroDivisionError)
    assert cancelled == 1
