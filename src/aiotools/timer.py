"""
Provides a simple implementation of timers run inside asyncio event loops.
"""

import asyncio
import contextlib
import enum
import functools
from collections.abc import Callable, Iterator
from typing import TypeVar
from unittest import mock

from .cancel import cancel_and_wait
from .compat import get_running_loop
from .taskscope import TaskScope
from .types import CoroutineLike

__all__ = (
    "create_timer",
    "TimerDelayPolicy",
    "VirtualClock",
)

T_SelectorRet = TypeVar("T_SelectorRet")


class TimerDelayPolicy(enum.Enum):
    """
    An enumeration of supported policies for when the timer function takes
    longer on each tick than the given timer interval.
    """

    DEFAULT = 0
    CANCEL = 1


def create_timer(
    cb: Callable[[float], CoroutineLike[None]],
    interval: float,
    delay_policy: TimerDelayPolicy = TimerDelayPolicy.DEFAULT,
    loop: asyncio.AbstractEventLoop | None = None,
) -> asyncio.Task[None]:
    """
    Schedule a timer with the given callable and the interval in seconds.
    The interval value is also passed to the callable.
    If the callable takes longer than the timer interval, all accumulated
    callable's tasks will be cancelled when the timer is cancelled.

    Args:
        cb: An async timer callback function accepting the configured interval value.

    Returns:
        You can stop the timer by cancelling the returned task.

    .. versionchanged:: 2.0

       Unhandled exceptions in the tick tasks no longer terminates the timer task,
       as it is changed to use :class:`~aiotools.taskscope.TaskScope` instead of
       :class:`~aiotools.taskgroup.TaskGroup`.

    .. versionchanged:: 2.0

       Cancelling and awaiting the task returned by ``create_timer()`` now raises
       :exc:`asyncio.CancelledError`.  Use :func:`~aiotools.cancel.cancel_and_wait()`
       to safely consume or re-raise it.
    """

    async def _timer() -> None:
        fired_tasks: list[asyncio.Task[None]] = []
        async with TaskScope() as ts:
            while True:
                if delay_policy == TimerDelayPolicy.CANCEL:
                    await cancel_and_wait(fired_tasks)
                    fired_tasks.clear()
                else:
                    fired_tasks[:] = [t for t in fired_tasks if not t.done()]
                t = ts.create_task(cb(interval))
                fired_tasks.append(t)
                await asyncio.sleep(interval)

    return asyncio.create_task(_timer())


class VirtualClock:
    """
    Provide a virtual clock for an asyncio event loop
    which makes timing-based tests deterministic and instantly completed.
    """

    def __init__(self) -> None:
        self.vtime = 0.0

    def virtual_time(self) -> float:
        """
        Return the current virtual time.
        """
        return self.vtime

    def _virtual_select(
        self,
        orig_select: Callable[[float | None], T_SelectorRet],
        timeout: float | None,
    ) -> T_SelectorRet:
        if timeout is not None:
            self.vtime += timeout
        return orig_select(0)  # override the timeout to zero

    @contextlib.contextmanager
    def patch_loop(self) -> Iterator[None]:
        """
        Override some methods of the current event loop
        so that sleep instantly returns while proceeding the virtual clock.
        """
        loop = get_running_loop()
        # Both SelectorEventLoop and ProactorEventLoop has "_selector" with "select()"
        # method which accepts a timeout value until the next wake-up point.
        assert hasattr(loop, "_selector"), (
            "Only stdlib's SelectorEventLoop or ProactorEventLoop is supported."
        )
        with (
            mock.patch.object(
                loop._selector,
                "select",
                new=functools.partial(self._virtual_select, loop._selector.select),
            ),
            mock.patch.object(
                loop,
                "_clock_resolution",
                new=0.001,
            ),
            mock.patch.object(
                loop,
                "time",
                new=self.virtual_time,
            ),
        ):
            yield
