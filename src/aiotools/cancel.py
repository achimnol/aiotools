from __future__ import annotations

import asyncio
from collections.abc import Collection
from typing import Any, Self

from aiotools.types import OptExcInfo

__all__ = (
    "cancel_and_wait",
    "ShieldScope",
)


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
            if task.cancelling() != cancelling_expected:
                raise
            else:
                return  # this is the only non-exceptional return
        else:
            raise asyncio.InvalidStateError(
                f"The cancelled task {task!r} did not raise up cancellation."
            )


class ShieldScope:
    """
    A context-manager to make the codes within the scope to be shielded from cancellation,
    delaying any cancellation attempts in the middle to be re-raised afterwards.
    You may use it as an async context manager as well.

    See https://github.com/python/cpython/issues/99714#issuecomment-1817941789
    for the original ideation.

    To self-cancel from the inside of ShieldScope, you may simply raise
    :exc:`asyncio.CancelledError`.

    .. versionadded:: 2.1
    """

    def __init__(self) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.parent_task = task
        self._cancel_messages: list[str | None] = []
        self._exited = False

    def _cancel(self, msg: str | None = None) -> bool:
        self._cancel_messages.append(msg)
        if msg == "deadline exceeded":
            # for timeouts, exit immediately.
            self.__exit__()
        return True

    def _cancelling(self) -> int:
        return len(self._cancel_messages)

    def _uncancel(self) -> int:
        self._cancel_messages.pop()
        return len(self._cancel_messages)

    def __enter__(self) -> Self:
        # It's a little hacky implementation, but it serves the purpose.
        # Our cancellation machinery just tracks the cancellation requests
        # but does not make actual cancellation.
        self._orig_cancel, self.parent_task.cancel = (  # type: ignore[method-assign]
            self.parent_task.cancel,
            self._cancel,
        )
        self._orig_cancelling, self.parent_task.cancelling = (  # type: ignore[method-assign]
            self.parent_task.cancelling,
            self._cancelling,
        )
        self._orig_uncancel, self.parent_task.uncancel = (  # type: ignore[method-assign]
            self.parent_task.uncancel,
            self._uncancel,
        )
        return self

    def __exit__(self, *exc_info: OptExcInfo) -> None:
        if self._exited:
            return
        self.parent_task.cancel = self._orig_cancel  # type: ignore[method-assign]
        self.parent_task.cancelling = self._orig_cancelling  # type: ignore[method-assign]
        self.parent_task.uncancel = self._orig_uncancel  # type: ignore[method-assign]
        # Synchronize back the cancellation requests.
        for msg in self._cancel_messages:
            self.parent_task.cancel(msg)
        self._exited = True

    def __aenter__(self) -> Self:
        self.__enter__()
        return self

    def __aexit__(self, *exc_info: OptExcInfo) -> None:
        self.__aexit__(*exc_info)
