from __future__ import annotations

import asyncio
import logging
from sys import platform

from .compat import get_running_loop
from .fork_base import AbstractChildProcess, _child_main

logger = logging.getLogger(__name__)


class WindowsChildProcess(AbstractChildProcess):
    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self._proc = proc
        self._terminated = False

    def send_signal(self, signum: int) -> None:
        if self._terminated:
            logger.warning(
                "WindowsChildProcess(%d).send_signal(%d): "
                "The process has already terminated.",
                self._pid,
                signum,
            )
            return
        self._proc.send_signal(signum)

    async def wait(self) -> int:
        loop = get_running_loop()
        try:
            ## _, status = await loop.run_in_executor(None, os.waitpid, self._pid, 0)
            raise NotImplementedError
        except ChildProcessError:
            # The child process is already reaped
            # (may happen if waitpid() is called elsewhere).
            self._returncode = 255
            logger.warning(
                "child process pid %d exit status already read: "
                " will report returncode 255",
                self._pid)
        else:
            ## if os.WIFSIGNALED(status):
            ##     self._returncode = -os.WTERMSIG(status)
            ## elif os.WIFEXITED(status):
            ##     self._returncode = os.WEXITSTATUS(status)
            ## else:
            ##     self._returncode = status
            raise NotImplementedError
        finally:
            self._terminated = True
        return self._returncode


async def _spawn_windows():
    raise NotImplementedError
