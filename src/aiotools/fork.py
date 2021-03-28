"""
This module implements a simple :func:`os.fork()`-like interface,
but in an asynchronous way with full support for PID file descriptors
on Python 3.9 or higher and the Linux kernel 5.4 or higher.

It internally synchronizes the beginning and readiness status of child processes
so that the users may assume that the child process is completely interruptible after
:func:`afork()` returns.
"""

from __future__ import annotations

import logging
import sys
from typing import Callable

from .fork_base import AbstractChildProcess

__all__ = [
    'AbstractChildProcess',
    'afork',
]

logger = logging.getLogger(__name__)

if sys.platform.startswith("win"):
    from .fork_windows import (
        WindowsChildProcess,
        _spawn_windows,
    )
    __all__.extend([
        'WindowsChildProcess',
    ])

    async def afork(child_func: Callable[[], int]) -> AbstractChildProcess:
        """
        Fork the current process and execute the given function in the child.
        The return value of the function will become the exit code of the child
        process.

        Args:
            child_func: A function that represents the main function of the child and
                        returns an integer as its exit code.
                        Note that the function must set up a new event loop if it
                        wants to run asyncio codes.
        """
        proc = await _spawn_windows(child_func)
        return WindowsChildProcess(proc)

else:
    from .fork_unix import (
        PosixChildProcess,
        PidfdChildProcess,
        _clone_pidfd,
        _fork_posix,
        _has_pidfd,
    )
    __all__.extend([
        'PosixChildProcess',
        'PidfdChildProcess',
    ])

    async def afork(child_func: Callable[[], int]) -> AbstractChildProcess:
        """
        Fork the current process and execute the given function in the child.
        The return value of the function will become the exit code of the child
        process.

        Args:
            child_func: A function that represents the main function of the child and
                        returns an integer as its exit code.
                        Note that the function must set up a new event loop if it
                        wants to run asyncio codes.
        """
        if _has_pidfd:
            pid, pidfd = await _clone_pidfd(child_func)
            return PidfdChildProcess(pid, pidfd)
        else:
            pid = await _fork_posix(child_func)
            return PosixChildProcess(pid)
