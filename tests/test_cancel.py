"""
Tests for utils.cancel_and_wait() with taskgroup cancellation scenarios.

These tests focus on the correct propagation and suppression of cancellations
based on Python 3.11's asyncio.Task.cancelling() method.

Key semantics to test:
1. cancel_and_wait() increments the task's cancelling() count and expects it to be
   exactly +1 after cancellation
2. If the cancelling() count is higher than expected, it means external cancellation
   occurred and CancelledError should be re-raised
3. TaskGroup behavior: child CancelledError doesn't propagate, but exceptions do

Test scenarios covered:
- Child exception triggering sibling cancellations
- External cancellation concurrent with cancel_and_wait()
- Self-cancellation within taskgroup body
- Normal cancellation (only from cancel_and_wait)
- Child exception handling (ExceptionGroup propagation)
- Already-done task handling
- Multiple concurrent external cancellations
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

import pytest

from aiotools import (
    ErrorArg,
    ShieldScope,
    TaskScope,
    VirtualClock,
    cancel_and_wait,
)


@pytest.mark.asyncio
async def test_cancel_and_wait_simple_task_already_done() -> None:
    """
    Test cancel_and_wait() on a task that's already completed.
    Should return immediately without any action.
    """
    result_holder: list[str] = []

    async def simple_task() -> None:
        await asyncio.sleep(0.1)
        result_holder.append("done")

    with VirtualClock().patch_loop():
        # cancel_and_wait() on already-done task should return immediately
        task = asyncio.create_task(simple_task())
        await task
        await cancel_and_wait(task)
        assert result_holder == ["done"]
        assert task.done()
        assert not task.cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_cancelled_by_another() -> None:
    """
    Test cancel_and_wait() on a task that's already completed.
    Should return immediately without any action.
    """

    async def simple_task() -> None:
        try:
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            await asyncio.sleep(0.1)

    async def cancel_task() -> None:
        inner_task = asyncio.create_task(simple_task())
        await asyncio.sleep(0.01)  # let inner task proceed
        try:
            await cancel_and_wait(inner_task)  # waiting for inner cleanup
        finally:
            assert inner_task.cancelled()

    with VirtualClock().patch_loop():
        task = asyncio.create_task(cancel_task())
        await asyncio.sleep(0.05)
        await cancel_and_wait(task)  # cancel outer waiting for inner cleanup
        assert task.done()
        assert task.cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_simple_task_long_cancellation() -> None:
    """
    Test cancellation on a task that has long cleanup upon cancellation.
    """
    result_holder: list[str] = []

    async def simple_task() -> None:
        try:
            await asyncio.sleep(0.1)
            result_holder.append("done")
        except asyncio.CancelledError:
            await asyncio.sleep(0.1)
            result_holder.append("cancelled")
            raise

    with VirtualClock().patch_loop():
        task = asyncio.create_task(simple_task())
        await asyncio.sleep(0.05)
        await cancel_and_wait(task)
        assert result_holder == ["cancelled"]
        assert task.cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_simple_task_shielded() -> None:
    """
    Test cancellation on a task that its body is shielded.
    """
    result_holder: list[str] = []

    async def simple_task() -> None:
        result_holder.append("task-start")
        await asyncio.sleep(0.1)
        with ShieldScope():
            result_holder.append("shield-start")
            await asyncio.sleep(0.1)
            result_holder.append("shield-end")
        await asyncio.sleep(0.1)
        result_holder.append("task-done")

    with VirtualClock().patch_loop():
        task = asyncio.create_task(simple_task())
        await asyncio.sleep(0.05)
        # Cancelling before entering a shieldscope should be cancelled normally.
        await cancel_and_wait(task)
        assert result_holder == ["task-start"]
        assert task.cancelled()

        result_holder.clear()
        task = asyncio.create_task(simple_task())
        await asyncio.sleep(0.15)
        # The shieldscope raises the cancellation received from cancel_and_wait(),
        # so cancel_and_wait() SHOULD NOT raise InvalidStateError.
        await cancel_and_wait(task)
        # The task is marked as cancelled, while its body is complete.
        assert result_holder == ["task-start", "shield-start", "shield-end"]
        assert task.cancelled()

        result_holder.clear()
        task = asyncio.create_task(simple_task())
        await asyncio.sleep(0.25)
        # The shieldscope should have restored the original task cancellation machinery.
        await cancel_and_wait(task)
        # The task is marked as cancelled, while its body is complete.
        assert result_holder == ["task-start", "shield-start", "shield-end"]
        assert task.cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_simple_task_self_cancelled_within_shield_scope() -> None:
    """
    Test cancellation on a task that its body is shielded but cancels by itself.
    """
    result_holder: list[str] = []

    async def simple_task() -> None:
        result_holder.append("task-start")
        await asyncio.sleep(0.1)
        with ShieldScope():
            result_holder.append("shield-start")
            await asyncio.sleep(0.1)
            result_holder.append("shield-middle")
            raise asyncio.CancelledError
            # should not reach here
            result_holder.append("shield-end")

    with VirtualClock().patch_loop():
        result_holder.clear()
        task = asyncio.create_task(simple_task())
        # The shield scope should transparently raise-up the cancellation.
        with pytest.raises(asyncio.CancelledError):
            await task
        # The task is marked as cancelled, while its body is complete until cancellation.
        assert result_holder == ["task-start", "shield-start", "shield-middle"]
        assert task.cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_simple_task_invalid_state() -> None:
    """
    Test cancellation on a task that does not propagate cancellation.

    In legacy asyncio codes, top-level tasks often swallowed the cancellation to
    prevent _bogus_ cancellation errors.  However, this is now considered a bad
    practice and any underlying frameworks should consume them appropriately.

    Now ``cancel_and_wait()`` correctly handles the decision whether to re-raise
    cancellation for externally triggered cancellations, while the call itself
    consumes the cancellation.  To achieve this, ``cancel_and_wait()`` internally
    validates if the target task re-raises cancellation when required.
    """
    result_holder: list[str] = []

    async def simple_task() -> None:
        try:
            await asyncio.sleep(0.1)
            result_holder.append("done")
        except asyncio.CancelledError:
            await asyncio.sleep(0.1)
            result_holder.append("cancelled")
            # Missing re-raise here!

    with VirtualClock().patch_loop():
        task = asyncio.create_task(simple_task())
        await asyncio.sleep(0.05)
        with pytest.raises(asyncio.InvalidStateError):
            await cancel_and_wait(task)
        assert result_holder == ["cancelled"]
        assert task.done()


@pytest.mark.asyncio
async def test_cancel_and_wait_taskgroup_external_cancellation() -> None:
    """
    Test cancellation from outside while awaiting cancel_and_wait() against a
    task which internally creates a taskgroup.

    This tests the scenario where an external source cancels the task DURING
    the cancel_and_wait() await. The cancelling() count will be 2 (ours + external),
    not the expected 1, so CancelledError is re-raised.
    """
    tg_entered = asyncio.Event()
    cancel_and_wait_started = asyncio.Event()
    child_cancelled = False

    async def child_task() -> None:
        nonlocal child_cancelled
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            child_cancelled = True
            # Delay to keep task alive during concurrent cancellation
            await asyncio.sleep(0.1)
            raise

    async def parent_task_with_tg() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child_task())
            tg_entered.set()
            await asyncio.sleep(2.0)

    async def concurrent_canceller(task: asyncio.Task[None]) -> None:
        """Applies external cancellation DURING cancel_and_wait's await"""
        await cancel_and_wait_started.wait()
        await asyncio.sleep(0.05)
        task.cancel("concurrent external cancellation")

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())
        canceller_task = asyncio.create_task(concurrent_canceller(parent_task))

        # Wait for taskgroup to be entered
        await tg_entered.wait()

        # Now call cancel_and_wait with concurrent external cancellation
        cancel_and_wait_started.set()

        # The concurrent_canceller will apply another cancel DURING the await
        # This will make cancelling() = 2 instead of expected 1
        with pytest.raises(asyncio.CancelledError):
            await cancel_and_wait(parent_task)

        assert child_cancelled
        assert parent_task.cancelled()

        # Clean up
        await canceller_task


@pytest.mark.asyncio
async def test_cancel_and_wait_taskgroup_self_cancellation() -> None:
    """
    Test self-cancellation within the taskgroup context body while awaiting
    cancel_and_wait() against a task which internally creates a taskgroup.

    When a task raises CancelledError inside the taskgroup body, TaskGroup
    cancels all children and propagates the CancelledError. If this happens
    WHILE we're calling cancel_and_wait(), the cancelling count will show the
    self-cancellation, and we should re-raise.
    """
    ready_to_cancel = asyncio.Event()
    child_cancelled = False

    async def child_task() -> None:
        nonlocal child_cancelled
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            child_cancelled = True
            raise

    async def parent_task_with_tg() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child_task())
            ready_to_cancel.set()
            await asyncio.sleep(0.2)
            # This self-cancel will trigger before cancel_and_wait's cancel
            raise asyncio.CancelledError("self-cancellation")

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait until task is ready
        await ready_to_cancel.wait()

        # Let the self-cancellation happen
        await asyncio.sleep(0.25)

        # Task should already be cancelled by self-cancellation
        # cancel_and_wait on already-done task returns immediately
        await cancel_and_wait(parent_task)

        assert child_cancelled
        assert parent_task.cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_taskgroup_normal_cancellation() -> None:
    """
    Test normal cancellation via cancel_and_wait() when the task has a taskgroup.

    When only cancel_and_wait() cancels the task (no external or self-cancellation),
    the function should return normally without raising CancelledError.
    """
    tg_active = asyncio.Event()
    child_cancelled = False

    async def child_task() -> None:
        nonlocal child_cancelled
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            child_cancelled = True
            raise

    async def parent_task_with_tg() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child_task())
            tg_active.set()
            await asyncio.sleep(2.0)

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait for taskgroup to be active
        await tg_active.wait()

        # Cancel via cancel_and_wait - should return normally (no exception)
        await cancel_and_wait(parent_task)

        assert child_cancelled
        assert parent_task.cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_taskgroup_child_exception() -> None:
    """
    Test cancel_and_wait() when a child task raises a non-CancelledError exception.

    When a child task fails with an exception, the taskgroup will cancel siblings
    and propagate an ExceptionGroup. If the task completes with the exception before
    cancel_and_wait() is called, cancel_and_wait() returns immediately (task is done).
    """
    child_started = asyncio.Event()
    sibling_cancelled = False

    async def failing_child() -> None:
        child_started.set()
        await asyncio.sleep(0.1)
        raise ValueError("child error")

    async def sibling_child() -> None:
        nonlocal sibling_cancelled
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            sibling_cancelled = True
            raise  # this raised-up cancellation is absorbed by taskgroup

    async def parent_task_with_tg() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_child())
            tg.create_task(sibling_child())

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait for child to start and fail
        await child_started.wait()
        await asyncio.sleep(0.15)

        # Task should be done with ExceptionGroup by now
        # cancel_and_wait returns immediately for done tasks
        await cancel_and_wait(parent_task)

        assert sibling_cancelled
        assert parent_task.done()
        # The task already raised an exception, not cancelled
        assert not parent_task.cancelled()
        with pytest.raises(ExceptionGroup) as exc_info:
            parent_task.result()
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], ValueError)


@pytest.mark.asyncio
async def test_cancel_and_wait_taskgroup_long_cancellation() -> None:
    """
    Test cancel_and_wait() when a child task raises a non-CancelledError exception.

    When a child task fails with an exception, the taskgroup will cancel siblings
    and propagate an ExceptionGroup. If the task completes with the exception before
    cancel_and_wait() is called, cancel_and_wait() returns immediately (task is done).
    """
    child_started = asyncio.Event()
    sibling_cancelled_after_cleanup = False

    async def failing_child() -> None:
        child_started.set()
        await asyncio.sleep(0.1)
        raise ValueError("child error")

    async def sibling_child() -> None:
        nonlocal sibling_cancelled_after_cleanup
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            await asyncio.sleep(0.5)
            sibling_cancelled_after_cleanup = True
            raise  # this raised-up cancellation is absorbed by taskgroup

    async def parent_task_with_tg() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_child())
            tg.create_task(sibling_child())

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait for child to start and fail
        await child_started.wait()
        await asyncio.sleep(0.15)

        # Task is not finished yet; the taskgroup is waiting for
        # its own triggered cancellation of the sibling child.
        # cancel_and_wait propagates the parent's exception AFTER that waiting.
        with pytest.raises(ExceptionGroup) as exc_info:
            await cancel_and_wait(parent_task)

        assert sibling_cancelled_after_cleanup
        assert parent_task.done()
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], ValueError)


@pytest.mark.asyncio
async def test_cancel_and_wait_taskgroup_error_during_cancellation() -> None:
    """
    Test cancel_and_wait() when there is a child failing during cancellation
    with a taskgroup.
    """
    child_started = asyncio.Event()

    async def failing_child() -> None:
        child_started.set()
        await asyncio.sleep(0.1)
        raise ValueError("child error")

    async def sibling_child() -> None:
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            await asyncio.sleep(0.5)
            raise ZeroDivisionError()

    async def parent_task_with_tg() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_child())
            tg.create_task(sibling_child())

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait for child to start and fail
        await child_started.wait()
        await asyncio.sleep(0.15)

        # Unhandled exceptions during cancellation is captured by taskgroup.
        with pytest.raises(ExceptionGroup) as exc_info:
            await cancel_and_wait(parent_task)

        assert parent_task.done()
        # Check errors captured by taskgroup
        assert len(exc_info.value.exceptions) == 2
        assert isinstance(exc_info.value.exceptions[0], ValueError)
        assert isinstance(exc_info.value.exceptions[1], ZeroDivisionError)


@pytest.mark.asyncio
async def test_cancel_and_wait_taskgroup_outer_cancelled() -> None:
    results: list[str] = []

    async def failing_child(delay: float) -> None:
        await asyncio.sleep(delay)
        raise ZeroDivisionError

    async def parent() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_child(0.2))
            await asyncio.sleep(0.5)
            results.append("context-final")  # cancelled as taskgroup is cancelled
        await asyncio.sleep(0.5)
        results.append("parent-final")  # cancelled as parent is cancelled

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent())
        await asyncio.sleep(0.1)  # trigger cancellation before child fails
        await cancel_and_wait(parent_task)  # no error as it's successful cancellation
        assert results == []


@pytest.mark.asyncio
async def test_cancel_and_wait_taskscope_child_exception() -> None:
    """
    Test cancel_and_wait() when a child task raises a non-CancelledError exception.

    Although a child task fails with an exception, the taskscope will NOT cancel siblings
    immediately but wait for all subtasks conclude. cancel_and_wait() cancels this waiting.
    """
    child_started = asyncio.Event()
    sibling_cancelled = False

    async def failing_child() -> None:
        child_started.set()
        await asyncio.sleep(0.1)
        raise ValueError("child error")

    async def sibling_child() -> None:
        nonlocal sibling_cancelled
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            sibling_cancelled = True
            raise  # this raised-up cancellation is absorbed by taskscope

    async def parent_task_with_tg() -> None:
        async with TaskScope() as ts:
            ts.create_task(failing_child())
            ts.create_task(sibling_child())

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait for child to start and fail
        await child_started.wait()
        await asyncio.sleep(0.15)

        # Task is still running as TaskScope will wait for the sibiling task.
        # cancel_and_wait cancels it.
        await cancel_and_wait(parent_task)

        assert sibling_cancelled
        assert parent_task.cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_taskscope_long_cancellation() -> None:
    """
    Test cancel_and_wait() when there is a subtask doing some async cleanup work upon
    cancellation.

    Although a child task fails with an exception, the taskscope will NOT cancel siblings
    immediately but wait for all subtasks conclude. cancel_and_wait() cancels this waiting
    but still waits the completion of async cancellation.
    """
    child_started = asyncio.Event()
    sibling_cancelled_after_cleanup = False
    errors: list[BaseException] = []

    def error_callback(info: ErrorArg) -> None:
        errors.append(info["exception"])

    async def failing_child() -> None:
        child_started.set()
        await asyncio.sleep(0.1)
        raise ValueError("child error")

    async def sibling_child() -> None:
        nonlocal sibling_cancelled_after_cleanup
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            # simulate async resource cleanup
            await asyncio.sleep(0.5)
            sibling_cancelled_after_cleanup = True
            raise  # this raised-up cancellation is absorbed by taskscope

    async def parent_task_with_tg() -> None:
        async with TaskScope(exception_handler=error_callback) as ts:
            ts.create_task(failing_child())
            ts.create_task(sibling_child())

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait for child to start and fail
        await child_started.wait()
        await asyncio.sleep(0.15)

        # Task is still running as the taskscope will wait for the sibiling task.
        # cancel_and_wait cancels it but waits until the sibling task finishes cleanup.
        await cancel_and_wait(parent_task)

        assert sibling_cancelled_after_cleanup
        assert parent_task.cancelled()
        # Check captured taskscope errors
        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)


@pytest.mark.asyncio
async def test_cancel_and_wait_taskscope_error_during_cancellation() -> None:
    """
    Test cancel_and_wait() when there is a child failing during cancellation
    with a taskscope.
    """
    child_started = asyncio.Event()
    errors: list[BaseException] = []

    def error_callback(info: ErrorArg) -> None:
        errors.append(info["exception"])

    async def failing_child() -> None:
        child_started.set()
        await asyncio.sleep(0.1)
        raise ValueError("child error")

    async def sibling_child() -> None:
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            # simulate a failure during async resource cleanup
            await asyncio.sleep(0.5)
            raise ZeroDivisionError()

    async def parent_task_with_tg() -> None:
        async with TaskScope(exception_handler=error_callback) as ts:
            ts.create_task(failing_child())
            ts.create_task(sibling_child())

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait for child to start and fail
        await child_started.wait()
        await asyncio.sleep(0.15)

        # Unhandled exceptions during cancellation is captured by taskscope.
        await cancel_and_wait(parent_task)

        assert parent_task.cancelled()
        # Check captured taskscope errors
        assert len(errors) == 2
        assert isinstance(errors[0], ValueError)
        assert isinstance(errors[1], ZeroDivisionError)


@pytest.mark.asyncio
async def test_cancel_and_wait_taskscope_outer_cancelled() -> None:
    results: list[str] = []

    async def failing_child(delay: float) -> None:
        await asyncio.sleep(delay)
        raise ZeroDivisionError

    async def parent() -> None:
        async with TaskScope() as ts:
            ts.create_task(failing_child(0.2))
            await asyncio.sleep(0.5)
            results.append("context-final")  # cancelled as taskscope is cancelled
        await asyncio.sleep(0.5)
        results.append("parent-final")  # cancelled as parent is cancelled

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent())
        await asyncio.sleep(0.1)  # trigger cancellation before child fails
        await cancel_and_wait(parent_task)  # no error as it's successful cancellation
        assert results == []


@pytest.mark.asyncio
async def test_cancel_and_wait_taskgroup_multiple_external_cancels() -> None:
    """
    Test cancel_and_wait() when multiple external cancellations are applied.

    The cancelling count will be much higher than expected, and this should
    be detected properly.
    """
    tg_active = asyncio.Event()
    cancel_and_wait_started = asyncio.Event()
    child_cancelled = False

    async def child_task() -> None:
        nonlocal child_cancelled
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            child_cancelled = True
            await asyncio.sleep(0.1)
            raise

    async def parent_task_with_tg() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child_task())
            tg_active.set()
            await asyncio.sleep(2.0)

    async def multiple_canceller(task: asyncio.Task[None]) -> None:
        """Applies multiple external cancellations DURING cancel_and_wait's await"""
        await cancel_and_wait_started.wait()
        await asyncio.sleep(0.05)
        task.cancel("external 1")
        task.cancel("external 2")

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())
        canceller_task = asyncio.create_task(multiple_canceller(parent_task))

        # Wait for taskgroup to be active
        await tg_active.wait()

        # Now call cancel_and_wait
        cancel_and_wait_started.set()

        # Multiple external cancellations will be applied DURING the await
        # cancelling() will be 3 (ours + 2 external), not expected 1
        with pytest.raises(asyncio.CancelledError):
            await cancel_and_wait(parent_task)

        assert child_cancelled
        assert parent_task.cancelled()

        # Clean up
        await canceller_task


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="eager task factory is not available before 3.12",
)
@pytest.mark.parametrize("use_eager_task_factory", [False, True])
def test_cancel_and_wait_eager_tasks(use_eager_task_factory: bool) -> None:
    """
    Test cancel_and_wait() on a task that completes immediately without any await.

    Eager tasks (or synchronous tasks) finish before create_task() returns, so they
    are already done when cancel_and_wait() is called. Should return immediately.
    """
    result_holder: list[str] = []

    async def eager_task() -> None:
        # No await - completes synchronously
        result_holder.append("done")

    async def _test() -> None:
        if use_eager_task_factory:
            loop = asyncio.get_running_loop()
            loop.set_task_factory(asyncio.eager_task_factory)  # type: ignore[attr-defined]

        task = asyncio.create_task(eager_task())
        await cancel_and_wait(task)
        if use_eager_task_factory:
            # The task is already done when the eager task factory is used.
            assert result_holder == ["done"]
            assert task.done()
            assert not task.cancelled()
        else:
            # The task is not started yet until the event loop tick progresses.
            # It is cancelled without being executed in this case.
            assert result_holder == []
            assert task.done()
            assert task.cancelled()

    asyncio.run(_test())


@pytest.mark.asyncio
async def test_cancel_and_wait_grouped_cancellation() -> None:
    """
    Test cancellation of a group of tasks (without TaskScope)
    """

    async def simple_task(work_delay: float = 0.1, cancel_delay: float = 0.1) -> str:
        try:
            await asyncio.sleep(work_delay)
            return "done"
        except asyncio.CancelledError:
            await asyncio.sleep(cancel_delay)
            raise

    async def failing_task(work_delay: float = 0.1) -> str:
        await asyncio.sleep(work_delay)
        raise ZeroDivisionError

    with VirtualClock().patch_loop():
        tasks = [
            asyncio.create_task(simple_task()),
            asyncio.create_task(simple_task()),
            asyncio.create_task(simple_task()),
            asyncio.create_task(simple_task()),
        ]
        await asyncio.sleep(0.05)
        await cancel_and_wait(tasks)
        assert tasks[0].cancelled()
        assert tasks[1].cancelled()
        assert tasks[2].cancelled()
        assert tasks[3].cancelled()

        tasks = [
            asyncio.create_task(simple_task()),
            asyncio.create_task(simple_task()),
            asyncio.create_task(simple_task()),
            asyncio.create_task(simple_task()),
        ]
        await asyncio.sleep(0.15)
        await cancel_and_wait(tasks)
        assert tasks[0].result() == "done"
        assert tasks[1].result() == "done"
        assert tasks[2].result() == "done"
        assert tasks[3].result() == "done"

        tasks = [
            asyncio.create_task(simple_task(0.2)),
            asyncio.create_task(simple_task(0.2)),
            asyncio.create_task(simple_task(0.1)),
            asyncio.create_task(simple_task(0.1)),
        ]
        await asyncio.sleep(0.15)
        await cancel_and_wait(tasks)
        # Completed tasks finish earlier.
        assert tasks[0].cancelled()
        assert tasks[1].cancelled()
        assert tasks[2].result() == "done"
        assert tasks[3].result() == "done"

        tasks = [
            asyncio.create_task(simple_task(0.2)),  # cancelled
            asyncio.create_task(simple_task(0.2)),  # cancelled
            asyncio.create_task(failing_task(0.13)),  # failed
            asyncio.create_task(simple_task(0.1)),  # done
            asyncio.create_task(failing_task(0.23)),  # cancelled
            asyncio.create_task(simple_task(0.3)),  # cancelled
        ]
        await asyncio.sleep(0.15)
        await cancel_and_wait(tasks)
        # Completed tasks finish earlier.
        assert tasks[0].cancelled()
        assert tasks[1].cancelled()
        assert isinstance(tasks[2].exception(), ZeroDivisionError)
        assert tasks[3].result() == "done"
        assert tasks[4].cancelled()
        assert tasks[5].cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_cancelling_cancellation() -> None:
    """
    Test cancelling cancellation.
    """
    results: list[str] = []

    async def simple_task() -> None:
        try:
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            results.append("cancelling")
            await asyncio.sleep(0.1)
            results.append("cancelled")
            raise

    async def canceller(t: asyncio.Task[Any]) -> None:
        await cancel_and_wait(t)

    with VirtualClock().patch_loop():
        t1 = asyncio.create_task(simple_task())
        await asyncio.sleep(0.05)
        # At this point, t1 is running.

        t2 = asyncio.create_task(canceller(t1))
        await asyncio.sleep(0)
        # t2 triggers cancellation of t1.
        # At this point, t2 is waiting for cancellation handler in t1.

        t3 = asyncio.create_task(canceller(t2))
        await t3
        # t3 triggers cancellation of t2.
        # At this point, t2 is cancelled raising up the injected CancelledError.
        # At this point, t1's cancellation is also cancelled.

        assert t1.cancelled()
        assert t2.cancelled()
        assert results == ["cancelling"]  # not concluded


@pytest.mark.asyncio
async def test_cancel_and_wait_cancelling_shielded_cancellation() -> None:
    """
    Test cancelling shielded cancellation.

    As the cancellation handler is shielded, it should run to completion,
    even when the canceller is cancelled.
    """
    results: list[str] = []

    async def simple_task() -> None:
        try:
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            with ShieldScope():
                results.append("cancelling")
                await asyncio.sleep(0.5)
                results.append("cancelled")
                raise

    async def canceller(t: asyncio.Task[Any]) -> None:
        await cancel_and_wait(t)

    with VirtualClock().patch_loop():
        t1 = asyncio.create_task(simple_task())
        await asyncio.sleep(0.05)
        # At this point, t1 is running.

        t2 = asyncio.create_task(canceller(t1))
        await asyncio.sleep(0)
        # t2 triggers cancellation of t1.
        # At this point, t2 is waiting for cancellation handler in t1.

        t3 = asyncio.create_task(canceller(t2))
        await t3
        # t3 triggers cancellation of t2.
        # At this point, t2 is cancelled raising up the injected CancelledError.
        # At this point, t1's cancellation continues thanks to shield_scope.
        # awaiting t3 makes the control flow to suspend until t1, t2's completion

        assert t1.cancelled()
        assert t2.cancelled()
        assert results == ["cancelling", "cancelled"]  # concluded


@pytest.mark.asyncio
async def test_cancel_and_wait_shielded_taskscope() -> None:
    results: list[int] = []
    errors: list[BaseException] = []
    cancelled = 0
    tasks: list[asyncio.Task[Any]] = []

    async def fail_after(delay: float) -> None:
        await asyncio.sleep(delay)
        raise ZeroDivisionError()

    async def parent():
        async with TaskScope(shield=True) as ts:
            tasks.append(ts.create_task(asyncio.sleep(0.1, 1)))
            tasks.append(ts.create_task(fail_after(0.2)))
            # cancelled here
            tasks.append(ts.create_task(asyncio.sleep(0.3, 3)))

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent())
        await asyncio.sleep(0.25)
        await cancel_and_wait(parent_task)
        for t in tasks:
            try:
                results.append(t.result())
            except asyncio.CancelledError:
                cancelled += 1
            except Exception as e:
                errors.append(e)

    assert results == [1, 3]
    assert len(errors) == 1
    assert isinstance(errors[0], ZeroDivisionError)
    assert cancelled == 0  # nothing is cancelled
