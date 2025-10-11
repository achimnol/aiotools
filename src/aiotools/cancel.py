from __future__ import annotations

import asyncio
from collections.abc import Collection
from typing import Any

__all__ = ("cancel_and_wait",)


async def cancel_and_wait(
    tasks: asyncio.Task[Any] | Collection[asyncio.Task[Any]],
    /,
    msg: str | None = None,
) -> None:
    """
    Safely cancels and waits until the given task is concluded as cancelled.
    If the task is already terminated, it does nothing.

    When the caller of this function is cancelled during waiting,
    cancellation is transparently raised up.  Otherwise, it consumes and absorbs
    :exc:`asyncio.CancelledError` raised while awaiting the cancelled task.
    This is the key point of this function: it guarantees the structural safety by
    ensuring the caller's control over cancellation.

    If the task does not re-raise :exc:`asyncio.CancelledError` when awaited,
    it explicitly raises :exc:`asyncio.InvalidStateError` to warn that the task
    should transparently raise up injected cancellations.

    See the discussion in https://github.com/python/cpython/issues/103486 for details.

    .. note::

       If you use :ref:`eager task factories <python:eager-task-factory>` in Python
       3.12 or later, tasks may have completed already if they do not contain any
       context switches (awaits) even when they are cancelled immediately after
       :func:`asyncio.create_task()` returns.
       In such cases, ``cancel_and_wait()`` becomes a no-op.

    .. versionadded:: 2.0
    """
    if isinstance(tasks, Collection):
        results = await asyncio.gather(
            *(cancel_and_wait(t) for t in tasks),
            return_exceptions=True,
        )
        has_cancelled = any(
            map(
                lambda result: isinstance(result, asyncio.CancelledError),
                (result for result in results),
            )
        )
        if has_cancelled:
            raise asyncio.CancelledError()
        exceptions = [result for result in results if isinstance(result, BaseException)]
        if exceptions:
            raise BaseExceptionGroup(
                "Unhandled exceptions during grouped cancellation", exceptions
            )
    else:
        task = tasks
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
            if task.cancelling() > cancelling_expected:
                raise
            else:
                return  # this is the only non-exceptional return
        else:
            # the cancellation request count may be reduced to zero by explicit uncancellation.
            if task.cancelling() > 0:
                raise asyncio.InvalidStateError(
                    f"The cancelled task {task!r} did not raise up cancellation."
                )
