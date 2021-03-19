from __future__ import annotations

from abc import ABCMeta, abstractmethod


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
