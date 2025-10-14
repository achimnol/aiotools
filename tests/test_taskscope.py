from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, Protocol, TypeVar, cast

import anyio
import pytest
from _pytest.mark.structures import ParameterSet

from aiotools import (
    ShieldScope,
    TaskScope,
    VirtualClock,
    cancel_and_wait,
    move_on_after,
)

T = TypeVar("T")


# @pytest.fixture(scope="module")
# def event_loop_policy() -> _AbstractEventLoopPolicy:
#     import uvloop
#
#     return uvloop.EventLoopPolicy()


class CancellableContextManager(AbstractContextManager[Any], Protocol):
    def cancel(self, msg: str | None = None) -> None: ...


shield_scope_factories: list[ParameterSet] = [
    pytest.param(lambda: anyio.CancelScope(shield=True), id="anyio.CancelScope"),
    pytest.param(lambda: ShieldScope(), id="aiotools.ShieldScope"),
]


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


@pytest.mark.xfail(
    sys.version_info < (3, 13),
    reason="Use TaskScope(timeout=...) in Python 3.12 or older due to compatibility issues with asyncio.timeout()",
)
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


@pytest.mark.asyncio
async def test_taskscope_cancelled_from_outside() -> None:
    results: list[str] = []

    async def failing_child(delay: float) -> None:
        await asyncio.sleep(delay)
        raise ZeroDivisionError

    async def succeeding_child(delay: float, result: str) -> None:
        await asyncio.sleep(delay)
        results.append(result)

    async def parent() -> None:
        results.append("parent-begin")
        async with TaskScope() as ts:
            results.append("context-begin")
            ts.create_task(failing_child(0.2))  # concluded after cancel
            ts.create_task(succeeding_child(0.08, "s1"))  # concluded before cancel
            ts.create_task(succeeding_child(0.12, "s2"))  # concluded after cancel
            await asyncio.sleep(0.5)
            results.append("context-end")  # skipped as taskscope is cancelled
        await asyncio.sleep(0.5)
        results.append("parent-end")  # skipped as parent is cancelled

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent())
        await asyncio.sleep(0.1)
        await cancel_and_wait(parent_task)
        assert results == ["parent-begin", "context-begin", "s1"]


@pytest.mark.asyncio
async def test_taskscope_aclose() -> None:
    results: list[str] = []

    async def failing_child(delay: float) -> None:
        await asyncio.sleep(delay)
        raise ZeroDivisionError

    async def succeeding_child(delay: float, result: str) -> None:
        try:
            await asyncio.sleep(delay)
            results.append(result)
        except asyncio.CancelledError:
            await asyncio.sleep(0.7)
            results.append(f"cancelled-{result}")
            raise

    async def close_scope(delay: float, ts: TaskScope) -> None:
        await asyncio.sleep(delay)
        results.append("aclose-trigger")
        await ts.aclose()
        results.append("aclose-done")

    async def parent() -> None:
        results.append("parent-begin")
        async with TaskScope() as ts:
            results.append("context-begin")
            ts.create_task(failing_child(0.2))  # concluded after cancel
            ts.create_task(succeeding_child(0.08, "s1"))  # concluded before cancel
            ts.create_task(succeeding_child(0.12, "s2"))  # concluded after cancel
            asyncio.create_task(close_scope(0.1, ts))  # self-close
            await asyncio.sleep(0.11)
            results.append("context-end")  # skipped as scope is cancelled
        await asyncio.sleep(0.5)
        results.append("parent-end")  # skipped as parent is cancelled

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent())
        await asyncio.sleep(0.1)
        with pytest.raises(asyncio.CancelledError):
            # cancellation triggered by a separate call to aclose()
            await parent_task
        assert results == [
            "parent-begin",
            "context-begin",
            "s1",
            "aclose-trigger",
            "cancelled-s2",
            "aclose-done",  # aclose() should have waited until cancellation
        ]


@pytest.mark.asyncio
async def test_taskscope_shielded_nested_1() -> None:
    """
    Test if nested task scopes with shield defers cancellation properly.
    """
    result_holder: list[str] = []

    async def nested_task() -> None:
        result_holder.append("task-start")
        await asyncio.sleep(0.1)
        async with TaskScope(shield=True):
            result_holder.append("outer-scope-begin")
            await asyncio.sleep(0.1)
            async with TaskScope(shield=True):
                result_holder.append("inner-scope-step0")
                await asyncio.sleep(0.1)  # cancelled here
                result_holder.append("inner-scope-step1")  # not skipped
            await asyncio.sleep(0.1)
            result_holder.append("outer-scope-end")  # not skipped
        # raising up deferred cancellation here
        await asyncio.sleep(0.1)
        result_holder.append("task-done")

    with VirtualClock().patch_loop():
        result_holder.clear()
        task = asyncio.create_task(nested_task())
        await asyncio.sleep(0.25)
        await cancel_and_wait(task)
        assert result_holder == [
            "task-start",
            "outer-scope-begin",
            "inner-scope-step0",
            "inner-scope-step1",
            "outer-scope-end",
        ]
        assert task.cancelled()


@pytest.mark.asyncio
async def test_taskscope_shielded_nested_2() -> None:
    """
    Test if inner scopes are also shielded by an outer scope with shield
    while the cancellation is deferred until the outmost scope's exit.
    """
    result_holder: list[str] = []

    async def nested_task() -> None:
        result_holder.append("task-start")
        await asyncio.sleep(0.1)
        async with TaskScope(shield=True):
            result_holder.append("outer-scope-begin")
            await asyncio.sleep(0.1)
            async with TaskScope(shield=False):
                result_holder.append("inner-scope-step0")
                await asyncio.sleep(0.1)  # cancelled here
                result_holder.append("inner-scope-step1")  # not skipped
            await asyncio.sleep(0.1)
            result_holder.append("outer-scope-end")  # not skipped
        # raising up deferred cancellation here
        await asyncio.sleep(0.1)
        result_holder.append("task-done")

    with VirtualClock().patch_loop():
        task = asyncio.create_task(nested_task())
        await asyncio.sleep(0.25)
        await cancel_and_wait(task)
        assert result_holder == [
            "task-start",
            "outer-scope-begin",
            "inner-scope-step0",
            "inner-scope-step1",  # shielded
            "outer-scope-end",  # shielded
        ]
        assert task.cancelled()


@pytest.mark.asyncio
async def test_taskscope_shielded_nested_3() -> None:
    """
    Test if outer non-shielded scope is cancelled by deferred
    cancellation from an inner shielded scope.
    """
    result_holder: list[str] = []

    async def nested_task() -> None:
        result_holder.append("task-start")
        await asyncio.sleep(0.1)
        async with TaskScope(shield=False):
            result_holder.append("outer-scope-begin")
            await asyncio.sleep(0.1)
            async with TaskScope(shield=True):
                result_holder.append("inner-scope-step0")
                await asyncio.sleep(0.1)  # cancelled here
                result_holder.append("inner-scope-step1")
            # raising up deferred cancellation here
            await asyncio.sleep(0.1)
            result_holder.append("outer-scope-end")
        await asyncio.sleep(0.1)
        result_holder.append("task-done")

    with VirtualClock().patch_loop():
        result_holder.clear()
        task = asyncio.create_task(nested_task())
        await asyncio.sleep(0.25)
        await cancel_and_wait(task)
        assert result_holder == [
            "task-start",
            "outer-scope-begin",
            "inner-scope-step0",
            "inner-scope-step1",  # shielded
        ]
        assert task.cancelled()


@pytest.mark.asyncio
async def test_taskscope_shielded_nested_4() -> None:
    """
    Deep nesting via scopes within a single task.

    The cancellation propagates upwards with different effects
    to each taskscope level depending on the shield option.
    """
    context_body_trace: list[str] = []
    task_result_trace: set[str] = set()

    async def work(delay: float, result: str) -> None:
        try:
            print(f"work begin ({result})")
            await asyncio.sleep(delay)
            task_result_trace.add(result)
            print(f"work end ({result})")
        except asyncio.CancelledError:
            print(f"cancelled ({result})")
            raise

    async def nested_task() -> None:
        context_body_trace.append("level0-begin")
        await asyncio.sleep(0.1)
        # Upper level subtasks run longer than lower level subtasks
        # to check their cancellation triggers.
        async with TaskScope(shield=False) as ts1:
            context_body_trace.append("level1-begin")
            ts1.create_task(work(15.0, "L1"), name="L1")
            await asyncio.sleep(0.1)
            async with TaskScope(shield=True) as ts2:
                context_body_trace.append("level2-begin")
                ts2.create_task(work(14.0, "L2"), name="L2")
                async with TaskScope(shield=False) as ts3:
                    context_body_trace.append("level3-begin")
                    ts3.create_task(work(13.0, "L3"), name="L3")
                    async with TaskScope(shield=False) as ts4:
                        context_body_trace.append("level4-begin")
                        ts4.create_task(work(12.0, "L4"), name="L4")
                        async with TaskScope(shield=True) as ts5:
                            context_body_trace.append("level5-begin")
                            ts5.create_task(work(11.0, "L5"), name="L5")
                            await asyncio.sleep(0.1)  # cancelled here
                            context_body_trace.append("level5-end")
                        await asyncio.sleep(0.1)
                        context_body_trace.append("level4-end")
                    await asyncio.sleep(0.1)
                    context_body_trace.append("level3-end")
                await asyncio.sleep(0.1)
                context_body_trace.append("level2-end")
            # raising up deferred cancellation here
            await asyncio.sleep(0.1)
            context_body_trace.append("level1-end")
        await asyncio.sleep(0.1)
        context_body_trace.append("level0-end")

    with VirtualClock().patch_loop():
        context_body_trace.clear()
        task = asyncio.create_task(nested_task())
        await asyncio.sleep(0.25)
        await cancel_and_wait(task)
        assert context_body_trace == [
            "level0-begin",
            "level1-begin",
            "level2-begin",
            "level3-begin",
            "level4-begin",
            "level5-begin",
            "level5-end",
            "level4-end",
            "level3-end",
            "level2-end",
        ]
        assert task.cancelled()
        assert task_result_trace == {"L5", "L4", "L3", "L2"}


@pytest.mark.asyncio
async def test_taskscope_shielded_nested_4_mix() -> None:
    """
    Deep nesting via scopes within a single task, but with
    a mix of asyncio.TaskGroup and aiotools.TaskScope(shield=True).

    This test shows the consistent behavior when mixing aiotools.TaskScope
    and asyncio.TaskGroup.

    The cancellation is shielded by the topmost TaskScope(shield=True).
    """
    context_body_trace: list[str] = []
    task_result_trace: set[str] = set()

    async def work(delay: float, result: str) -> None:
        try:
            print(f"work begin ({result})")
            await asyncio.sleep(delay)
            task_result_trace.add(result)
            print(f"work end ({result})")
        except asyncio.CancelledError:
            print(f"cancelled ({result})")
            raise

    async def nested_task() -> None:
        context_body_trace.append("level0-begin")
        await asyncio.sleep(0.1)
        # Upper level subtasks run longer than lower level subtasks
        # to check their cancellation triggers.
        async with asyncio.TaskGroup() as ts1:
            context_body_trace.append("level1-begin")
            ts1.create_task(work(15.0, "L1"), name="L1")
            await asyncio.sleep(0.1)
            async with TaskScope(shield=True) as ts2:
                context_body_trace.append("level2-begin")
                ts2.create_task(work(14.0, "L2"), name="L2")
                async with asyncio.TaskGroup() as ts3:
                    context_body_trace.append("level3-begin")
                    ts3.create_task(work(13.0, "L3"), name="L3")
                    async with asyncio.TaskGroup() as ts4:
                        context_body_trace.append("level4-begin")
                        ts4.create_task(work(12.0, "L4"), name="L4")
                        async with TaskScope(shield=True) as ts5:
                            context_body_trace.append("level5-begin")
                            ts5.create_task(work(11.0, "L5"), name="L5")
                            await asyncio.sleep(0.1)  # cancelled here
                            context_body_trace.append("level5-end")
                        await asyncio.sleep(0.1)
                        context_body_trace.append("level4-end")
                    await asyncio.sleep(0.1)
                    context_body_trace.append("level3-end")
                await asyncio.sleep(0.1)
                context_body_trace.append("level2-end")
            # raising up deferred cancellation here
            await asyncio.sleep(0.1)
            context_body_trace.append("level1-end")
        await asyncio.sleep(0.1)
        context_body_trace.append("level0-end")

    with VirtualClock().patch_loop():
        context_body_trace.clear()
        task = asyncio.create_task(nested_task())
        await asyncio.sleep(0.25)
        await cancel_and_wait(task)
        assert context_body_trace == [
            "level0-begin",
            "level1-begin",
            "level2-begin",
            "level3-begin",
            "level4-begin",
            "level5-begin",
            "level5-end",
            "level4-end",
            "level3-end",
            "level2-end",
            # level1, level0 are cancelled
        ]
        assert task.cancelled()
        assert task_result_trace == {"L5", "L4", "L3", "L2"}


@pytest.mark.asyncio
async def test_taskscope_shielded_nested_5() -> None:
    """
    Deep nesting via tasks.

    Since cancellation is delivered till the topmost shielded taskscope
    and it keeps all its child tasks running, inner tasks at any inner
    level are not affected by the cancellation at all.
    """
    context_body_trace: list[str] = []
    task_result_trace: set[str] = set()
    shield_per_level = [False, False, True, False, False, True]

    async def store_result(result: str, level: int) -> None:
        try:
            print(f"work begin ({result})")
            await asyncio.sleep(30.0 - level * 2)
            print(f"work end ({result})")
            task_result_trace.add(result)
        except asyncio.CancelledError:
            print(f"work cancelled ({result})")
            raise

    async def work(level: int) -> None:
        context_body_trace.append(f"level{level}-begin")
        async with TaskScope(shield=shield_per_level[level]) as ts:
            ts.create_task(store_result(f"L{level}", level), name=f"L{level}")
            if level < 5:
                ts.create_task(work(level + 1))
        # if not shielded, ts raises deferred cancellation here.
        await asyncio.sleep(0.1)
        context_body_trace.append(f"level{level}-end")

    with VirtualClock().patch_loop():
        context_body_trace.clear()
        task = asyncio.create_task(work(0))
        await asyncio.sleep(0.55)
        await cancel_and_wait(task)
        assert context_body_trace == [
            "level0-begin",
            "level1-begin",
            "level2-begin",
            "level3-begin",
            "level4-begin",
            "level5-begin",
            "level5-end",
            "level4-end",
            "level3-end",
            # level2-end is skipped as the exit handler of TaskScope
            # raises the deferred cancellation.
            # level1, level0 are also cancelled.
        ]
        assert task.cancelled()
        assert task_result_trace == {
            "L5",
            "L4",
            "L3",
            "L2",  # the task inside the shielded scope is done.
        }


@pytest.mark.parametrize("shield_scope_factory", shield_scope_factories)
@pytest.mark.asyncio
async def test_shieldscope_code_block_inside_task(
    shield_scope_factory: Callable[[], AbstractContextManager[Any, None]],
) -> None:
    results: list[str] = []

    async def work() -> None:
        try:
            results.append("work-begin")
            await asyncio.sleep(0.10)
            results.append("work-end")
        finally:
            with shield_scope_factory():
                results.append("cleanup-begin")
                await asyncio.sleep(0.10)
                results.append("cleanup-done")

    with VirtualClock().patch_loop():
        task = asyncio.create_task(work())
        await asyncio.sleep(0.05)  # cancel before shieldscope
        await cancel_and_wait(task)
        assert results == [
            "work-begin",
            "cleanup-begin",
            "cleanup-done",
        ]

        results.clear()
        task = asyncio.create_task(work())
        await asyncio.sleep(0.15)  # cancel during shieldscope
        await cancel_and_wait(task)
        assert results == [
            "work-begin",
            "work-end",
            "cleanup-begin",
            "cleanup-done",
        ]


@pytest.mark.parametrize("shield_scope_factory", shield_scope_factories)
@pytest.mark.asyncio
async def test_shieldscope_code_block_bare(
    shield_scope_factory: Callable[[], CancellableContextManager],
) -> None:
    results: list[str] = []

    def canceller(scope: CancellableContextManager) -> None:
        scope.cancel()

    async def work() -> None:
        scope = shield_scope_factory()
        loop = asyncio.get_running_loop()
        loop.call_later(0.15, lambda: canceller(scope))
        try:
            results.append("work-begin")
            await asyncio.sleep(0.10)
            results.append("work-end")
        finally:
            with scope:
                results.append("cleanup-begin")
                await asyncio.sleep(0.10)
                results.append("cleanup-done")

    with VirtualClock().patch_loop():
        task = asyncio.create_task(work())
        await task
        # await asyncio.sleep(0.15)  # cancel during shieldscope
        # await cancel_and_wait(task)
        assert results == [
            "work-begin",
            "work-end",
            "cleanup-begin",
            "cleanup-done",  # still missing with anyio.CancelScope?
        ]
