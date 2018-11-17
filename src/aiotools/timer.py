'''
Provides a simple implementation of timers run inside asyncio event loops.
'''

import asyncio
import enum
from typing import Callable, Optional

__all__ = ('create_timer', 'TimerDelayPolicy')


class TimerDelayPolicy(enum.Enum):
    '''
    An enumeration of supported policies for when the timer function takes
    longer on each tick than the given timer interval.
    '''
    DEFAULT = 0
    CANCEL = 1


def create_timer(cb: Callable[[float], None], interval: float,
                 delay_policy: TimerDelayPolicy = TimerDelayPolicy.DEFAULT,
                 loop: Optional[asyncio.BaseEventLoop] = None) -> asyncio.Task:
    '''
    Schedule a timer with the given callable and the interval in seconds.
    The interval value is also passed to the callable.
    If the callable takes longer than the timer interval, all accumulated
    callable's tasks will be cancelled when the timer is cancelled.

    Args:
        cb: TODO - fill argument descriptions

    Returns:
        You can stop the timer by cancelling the returned task.
    '''
    if not loop:
        loop = asyncio.get_event_loop()

    async def _timer():
        fired_tasks = []
        try:
            while True:
                if delay_policy == TimerDelayPolicy.CANCEL:
                    for t in fired_tasks:
                        if not t.done():
                            t.cancel()
                            await t
                    fired_tasks.clear()
                else:
                    fired_tasks[:] = [t for t in fired_tasks if not t.done()]
                t = loop.create_task(cb(interval=interval))
                fired_tasks.append(t)
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            for t in fired_tasks:
                t.cancel()
            await asyncio.gather(*fired_tasks)

    return loop.create_task(_timer())
