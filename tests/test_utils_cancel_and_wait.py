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

import pytest

from aiotools import TaskScope, VirtualClock, cancel_and_wait


@pytest.mark.asyncio
async def test_cancel_and_wait_parent_task_already_done() -> None:
    """
    Test cancel_and_wait() on a task with taskgroup that's already completed.

    Should return immediately without any cancellation.
    """
    result_holder: list[str] = []

    async def child_task() -> None:
        await asyncio.sleep(0.1)
        result_holder.append("done")

    async def parent_task_with_tg() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child_task())

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait for task to complete
        await parent_task

        # cancel_and_wait on already-done task should return immediately
        await cancel_and_wait(parent_task)

        assert result_holder == ["done"]
        assert parent_task.done()
        assert not parent_task.cancelled()


@pytest.mark.asyncio
async def test_cancel_and_wait_parent_task_already_failed() -> None:
    """
    Test cancellation from child subtask exception while awaiting cancel_and_wait()
    against a task which internally creates a taskgroup.

    When a child raises an exception (not CancelledError), TaskGroup cancels all
    siblings. If we then call cancel_and_wait(), it adds another cancellation.
    The task should have already finished with ExceptionGroup by the time we call
    cancel_and_wait(), so it returns immediately.
    """
    child_started = asyncio.Event()
    sibling_cancelled = False

    async def failing_child() -> None:
        child_started.set()
        await asyncio.sleep(0.1)
        raise ValueError("child failure")

    async def sibling_child() -> None:
        nonlocal sibling_cancelled
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            sibling_cancelled = True
            raise

    async def parent_task_with_tg() -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(failing_child())
            tg.create_task(sibling_child())

    with VirtualClock().patch_loop():
        parent_task = asyncio.create_task(parent_task_with_tg())

        # Wait for child to start and fail
        await child_started.wait()
        await asyncio.sleep(0.15)

        # By now, parent should have failed with ExceptionGroup
        # cancel_and_wait on already-done task returns immediately
        await cancel_and_wait(parent_task)

        assert sibling_cancelled
        assert parent_task.done()
        assert not parent_task.cancelled()  # It raised an exception, not cancelled


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
        async with TaskScope() as ts:
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
