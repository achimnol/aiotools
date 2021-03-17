from __future__ import annotations

from abc import ABCMeta, abstractmethod
import os
from typing import Callable


class AbstractChildProcess(metaclass=ABCMeta):
    """
    The abstract interface to control and monitor a forked child process.
    """

    @abstractmethod
    def send_signal(self, signum: int) -> None:
        """
        Send a UNIX signal to the child process.
        If the child process is already terminated, it will log
        a warning message and return.
        """
        raise NotImplementedError

    @abstractmethod
    async def wait(self) -> int:
        """
        Wait until the child process terminates or reclaim the child process' exit
        code if already terminated.
        If there are other coroutines that has waited the same process, it may
        return 255 and log a warning message.
        """
        raise NotImplementedError


def _child_main(init_func, init_pipe, child_func: Callable[[], int]) -> int:
    if init_func is not None:
        init_func()
    # notify the parent that the child is ready to execute the requested function.
    os.write(init_pipe, b"\0")
    os.close(init_pipe)
    return child_func()