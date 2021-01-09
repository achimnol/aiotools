import asyncio
import ctypes
import functools
import logging
import os
import resource
import signal
import sys
from abc import ABCMeta, abstractmethod
from ctypes import (
    CFUNCTYPE,
    byref,
    c_int,
    c_char_p,
    c_void_p,
    cast,
)
from typing import Callable, Optional, Tuple

logger = logging.getLogger(__package__)

_libc = ctypes.CDLL(None)
_syscall = _libc.syscall
_default_stack_size = (8 * (2**20))  # 8 MiB
_has_pidfd = hasattr(signal, 'pidfd_send_signal')


class AbstractChildProcess(metaclass=ABCMeta):

    @abstractmethod
    def send_signal(self, signum: int) -> None:
        raise NotImplementedError

    @abstractmethod
    async def wait(self) -> int:
        raise NotImplementedError


class PosixChildProcess(AbstractChildProcess):

    def __init__(self, pid: int) -> None:
        self._pid = pid

    def send_signal(self, signum: int) -> None:
        os.kill(self._pid, signum)

    async def wait(self) -> int:
        # TODO: implement async version
        _, status = os.waitpid(self._pid, 0)
        return status


class PidfdChildProcess(AbstractChildProcess):

    def __init__(self, pid: int, pidfd: int) -> None:
        self._pid = pid
        self._pidfd = pidfd
        self._returncode = None
        self._wait_event = asyncio.Event()

    def send_signal(self, signum: int) -> None:
        signal.pidfd_send_signal(self._pidfd, signum)  # type: ignore

    def _do_wait(self):
        loop = asyncio.get_running_loop()
        try:
            # The flag is WEXITED | __WALL from linux/wait.h
            # (__WCLONE value is out of range of int...)
            status_info = os.waitid(
                os.P_PIDFD,
                self._pidfd,
                os.WEXITED | 0x40000000,
            )
        except ChildProcessError:
            # The child process is already reaped
            # (may happen if waitpid() is called elsewhere).
            self._returncode = 255
            logger.warning(
                "child process pid %d exit status already read: "
                " will report returncode 255",
                self._pid)
        else:
            self._returncode = status_info.si_status
        finally:
            loop.remove_reader(self._pidfd)
            os.close(self._pidfd)
            self._wait_event.set()

    async def wait(self) -> int:
        loop = asyncio.get_running_loop()
        loop.add_reader(self._pidfd, self._do_wait)
        await self._wait_event.wait()
        assert self._returncode is not None
        return self._returncode


def _child_main(init_func, init_pipe, child_func: Callable[[], int]) -> int:
    init_func()
    signal.pthread_sigmask(
        signal.SIG_UNBLOCK,
        (signal.SIGINT, signal.SIGTERM),
    )
    # notify the parent that the child is ready to execute the requested function.
    os.write(init_pipe, b"\0")
    os.close(init_pipe)
    return child_func()


async def _clone_pidfd(child_func: Callable[[], int]) -> Tuple[int, int]:
    # reference: os_fork_impl() in the CPython source code
    fd = c_int()
    loop = asyncio.get_running_loop()

    # prepare the stack memory
    stack_size = resource.getrlimit(resource.RLIMIT_STACK)[0]
    if stack_size <= 0:
        stack_size = _default_stack_size
    stack = c_char_p(b"\0" * stack_size)

    init_pipe = os.pipe()
    init_event = asyncio.Event()
    loop.add_reader(init_pipe[0], init_event.set)

    func = CFUNCTYPE(c_int)(
        functools.partial(
            _child_main,
            ctypes.pythonapi.PyOS_AfterFork_Child,
            init_pipe[1],
            child_func,
        )
    )
    stack_top = c_void_p(cast(stack, c_void_p).value + stack_size)  # type: ignore
    signal.pthread_sigmask(
        signal.SIG_BLOCK,
        (signal.SIGINT, signal.SIGTERM, signal.SIGCHLD),
    )
    ctypes.pythonapi.PyOS_BeforeFork()
    # The flag value is CLONE_PIDFD from linux/sched.h
    pid = _libc.clone(func, stack_top, 0x1000, 0, byref(fd))
    ctypes.pythonapi.PyOS_AfterFork_Parent()
    signal.pthread_sigmask(
        signal.SIG_UNBLOCK,
        (signal.SIGINT, signal.SIGTERM),
    )

    # Wait for the child's readiness notification
    await init_event.wait()
    loop.remove_reader(init_pipe[0])
    os.read(init_pipe[0], 1)
    os.close(init_pipe[0])

    if pid == -1:
        raise OSError("failed to fork")
    return pid, fd.value


async def fork(child_func: Callable[[], int]) -> Optional[AbstractChildProcess]:
    if _has_pidfd:
        pid, pidfd = await _clone_pidfd(child_func)
        return PidfdChildProcess(pid, pidfd)
    else:
        pid = os.fork()
        if pid == 0:
            # I am the child.
            ret = -1
            try:
                ret = child_func()
            finally:
                sys.exit(ret)
        else:
            return PosixChildProcess(pid)