from __future__ import annotations

import asyncio
from typing import Any

__all__ = ("cancel_and_wait",)


async def cancel_and_wait(
    task: asyncio.Task[Any],
    msg: str | None = None,
) -> None:
    """
    Safely cancels and waits until the given task is concluded as cancelled.
    If the task is already terminated, it does nothing.

    When cancellation is triggered from the task itself or its inner subtasks,
    or the caller of this function is cancelled during waiting,
    those cancellations are transparently raised up.

    See the discussion in https://github.com/python/cpython/issues/103486 for details.
    """
    if task.done():
        return
    # After our cancellation, cancelling() should be incremented by 1.
    # If its incremented by more than 1, it means cancel was requested externally.
    # In that case CancelledError should be raised to also end waiting task.
    cancelling_expected = task.cancelling() + 1
    task.cancel(msg)
    try:
        await task
    except asyncio.CancelledError:
        if task.cancelling() != cancelling_expected:
            raise
        else:
            return  # this is the only non-exceptional return
    else:
        raise asyncio.InvalidStateError("Cancelled task did not end with an exception")
