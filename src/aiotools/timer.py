"""
Provides a simple implementation of timers run inside asyncio event loops.
"""

import asyncio
import contextlib
import enum
import functools
from typing import Callable, Optional
from unittest import mock

from .compat import get_running_loop
from .taskgroup import TaskGroup

__all__ = (
    'create_timer',
    'TimerDelayPolicy',
    'VirtualClock',
)


class TimerDelayPolicy(enum.Enum):
    """
    An enumeration of supported policies for when the timer function takes
    longer on each tick than the given timer interval.
    """
    DEFAULT = 0
    CANCEL = 1


def create_timer(cb: Callable[[float], None], interval: float,
                 delay_policy: TimerDelayPolicy = TimerDelayPolicy.DEFAULT,
                 loop: Optional[asyncio.AbstractEventLoop] = None) -> asyncio.Task:
    """
    Schedule a timer with the given callable and the interval in seconds.
    The interval value is also passed to the callable.
    If the callable takes longer than the timer interval, all accumulated
    callable's tasks will be cancelled when the timer is cancelled.

    Args:
        cb: TODO - fill argument descriptions

    Returns:
        You can stop the timer by cancelling the returned task.
    """
    if loop is None:
        loop = get_running_loop()

    async def _timer():
        fired_tasks = []
        try:
            async with TaskGroup() as task_group:
                while True:
                    if delay_policy == TimerDelayPolicy.CANCEL:
                        for t in fired_tasks:
                            if not t.done():
                                t.cancel()
                        await asyncio.gather(*fired_tasks, return_exceptions=True)
                        fired_tasks.clear()
                    else:
                        fired_tasks[:] = [t for t in fired_tasks if not t.done()]
                    t = task_group.create_task(cb(interval=interval))
                    fired_tasks.append(t)
                    await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
        finally:
            await asyncio.sleep(0)

    return loop.create_task(_timer())


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

    def _virtual_select(self, orig_select, timeout):
        if timeout is not None:
            self.vtime += timeout
        return orig_select(0)  # override the timeout to zero

    @contextlib.contextmanager
    def patch_loop(self):
        """
        Override some methods of the current event loop
        so that sleep instantly returns while proceeding the virtual clock.
        """
        loop = get_running_loop()
        with mock.patch.object(
            loop._selector,
            'select',
            new=functools.partial(self._virtual_select, loop._selector.select),
        ), \
            mock.patch.object(
            loop,
            'time',
            new=self.virtual_time,
        ):
            yield
