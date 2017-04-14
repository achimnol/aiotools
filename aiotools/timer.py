import asyncio
import enum
from typing import Callable, Optional

__all__ = ('create_timer', 'TimerDelayPolicy')


class TimerDelayPolicy(enum.Enum):
    default = 0
    cancel = 1


def create_timer(cb: Callable[[float], None], interval: float,
                 delay_policy: TimerDelayPolicy=TimerDelayPolicy.default,
                 loop: Optional[asyncio.BaseEventLoop]=None) -> asyncio.Task:
    '''
    Schedule a timer with the given callable and the timer interval in seconds.
    The interval value is also passed to the callable.
    If the callable takes longer than the timer interval, all accumulated
    callable's tasks will be cancelled when the timer is cancelled.
    '''
    if not loop:
        loop = asyncio.get_event_loop()

    async def _timer():
        fired_tasks = []
        try:
            while True:
                fired_tasks[:] = [t for t in fired_tasks if not t.done()]
                t = loop.create_task(cb(interval=interval))
                fired_tasks.append(t)
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            for t in fired_tasks:
                t.cancel()
            await asyncio.gather(*fired_tasks)

    return loop.create_task(_timer())
