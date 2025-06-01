"""
This module implements a simple :func:`os.fork()`-like interface,
but in an asynchronous way with full support for PID file descriptors
on Python 3.9 or higher and the Linux kernel 5.4 or higher.

It internally synchronizes the beginning and readiness status of child processes
so that the users may assume that the child process is completely interruptible after
:func:`afork()` returns.
"""

import asyncio
import errno
import logging
import multiprocessing as mp
import multiprocessing.connection as mpc
import multiprocessing.context as mpctx
import os
import signal
import traceback
from abc import ABCMeta, abstractmethod, abstractproperty
from typing import Callable, ClassVar, Optional, Tuple, TypeAlias

from .compat import get_running_loop

__all__ = (
    "AbstractChildProcess",
    "PosixChildProcess",
    "PidfdChildProcess",
    "afork",
)

logger = logging.getLogger(__name__)

MPProcess: TypeAlias = (
    mpctx.Process | mpctx.SpawnProcess | mpctx.ForkProcess | mpctx.ForkServerProcess
)
MPContext: TypeAlias = (
    mpctx.DefaultContext
    | mpctx.ForkContext
    | mpctx.ForkServerContext
    | mpctx.SpawnContext
)

_has_pidfd = False
if hasattr(os, "pidfd_open"):
    # signal.pidfd_send_signal() is available in Linux kernel 5.1+
    # and os.pidfd_open() is available in Linux kernel 5.3+.
    # So let's check with os.pidfd_open() which requires higher version.
    try:
        os.pidfd_open(0, 0)  # type: ignore
    except OSError as e:
        if e.errno in (errno.EBADF, errno.EINVAL):
            _has_pidfd = True
        # if the kernel does not support this,
        # it will say errno.ENOSYS or errno.EPERM


class AbstractChildProcess(metaclass=ABCMeta):
    """
    The abstract interface to control and monitor a forked child process.
    """

    @abstractproperty
    def pid(self) -> int:
        """
        The process ID of the child process.
        """
        raise NotImplementedError

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


class PosixChildProcess(AbstractChildProcess):
    """
    A POSIX-compatible version of :class:`AbstractChildProcess`.
    """

    poll_interval: ClassVar[float] = 0.05

    def __init__(self, proc: MPProcess, pid: int) -> None:
        self._proc = proc
        self._pid = pid
        self._terminated = False
        self._returncode: int | None = None

    @property
    def pid(self) -> int:
        return self._pid

    def send_signal(self, signum: int) -> None:
        if self._terminated:
            if signum != signal.SIGKILL:
                logger.warning(
                    "PosixChildProcess(%d).send_signal(%d): "
                    "The process has already terminated.",
                    self._pid,
                    signum,
                )
            return
        if signum == signal.SIGKILL:
            logger.warning("Force-killed hanging child: %d", self._pid)
        try:
            os.kill(self._pid, signum)
        except ProcessLookupError:
            logger.warning(
                "PosixChildProcess(%d).send_signal(%d): "
                "The process has already terminated.",
                self._pid,
                signum,
            )

    async def wait(self) -> int:
        status = 0
        while self._returncode is None:
            try:
                while True:
                    pid, status = os.waitpid(self._pid, os.WNOHANG)
                    if pid == self._pid:
                        self._returncode = os.waitstatus_to_exitcode(status)
                        break
                    await asyncio.sleep(self.poll_interval)
            except ChildProcessError:
                # The child process is not yet created.
                # See the multiprocessing.popen_fork module.
                await asyncio.sleep(self.poll_interval)
                continue
        self._terminated = True
        return self._returncode


class PidfdChildProcess(AbstractChildProcess):
    """
    A PID file descriptor-based version of :class:`AbstractChildProcess`.
    """

    def __init__(self, proc: MPProcess, pid: int, pidfd: int) -> None:
        self._proc = proc
        self._pid = pid
        self._pidfd = pidfd
        self._returncode = None
        self._wait_event = asyncio.Event()
        self._terminated = False
        loop = get_running_loop()
        loop.add_reader(self._pidfd, self._do_wait)

    @property
    def pid(self) -> int:
        return self._pid

    def send_signal(self, signum: int) -> None:
        if self._terminated:
            if signum != signal.SIGKILL:
                logger.warning(
                    "PidfdChildProcess(%d, %d).send_signal(%d): "
                    "The process has already terminated.",
                    self._pid,
                    self._pidfd,
                    signum,
                )
            return
        if signum == signal.SIGKILL:
            logger.warning("Force-killed hanging child: %d", self._pid)
        signal.pidfd_send_signal(self._pidfd, signum)  # type: ignore

    def _do_wait(self):
        loop = get_running_loop()
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
                "child process %d exit status already read: "
                "it will report returncode 255",
                self._pid,
            )
        else:
            if status_info.si_code == os.CLD_KILLED:
                self._returncode = -status_info.si_status  # signal number
            elif status_info.si_code == os.CLD_EXITED:
                self._returncode = status_info.si_status
            elif status_info.si_code == os.CLD_DUMPED:
                self._returncode = -status_info.si_status  # signal number
            else:
                logger.warning(
                    "unexpected si_code %d and si_status %d for child process %d",
                    status_info.si_code,
                    status_info.si_status,
                    self._pid,
                )
                self._returncode = 255
        finally:
            loop.remove_reader(self._pidfd)
            os.close(self._pidfd)
            self._terminated = True
            self._wait_event.set()

    async def wait(self) -> int:
        await self._wait_event.wait()
        assert self._returncode is not None
        return self._returncode


def _child_main(
    write_pipe: mpc.Connection,
    child_func: Callable[[], int],
) -> int:
    ret = -255
    try:
        # notify the parent that the child is ready to execute the requested function.
        write_pipe.send_bytes(b"\0")
        write_pipe.close()
        ret = child_func()
    except KeyboardInterrupt:
        ret = -signal.SIGINT
    except SystemExit:
        ret = -signal.SIGTERM
    except Exception:
        traceback.print_exc()
    finally:
        os._exit(ret)
    return ret


async def _fork_posix(
    child_func: Callable[[], int],
    mp_context: MPContext,
) -> tuple[MPProcess, int]:
    loop = get_running_loop()
    read_pipe, write_pipe = mp_context.Pipe()
    proc = mp_context.Process(
        target=_child_main,
        args=(write_pipe, child_func),
        daemon=True,
    )
    proc.start()
    assert proc.pid is not None
    pid = proc.pid

    # Wait for the child's readiness notification
    init_event = asyncio.Event()
    loop.add_reader(read_pipe.fileno(), init_event.set)
    await init_event.wait()
    loop.remove_reader(read_pipe.fileno())
    read_pipe.recv_bytes(1)
    read_pipe.close()
    return proc, pid


async def _clone_pidfd(
    child_func: Callable[[], int],
    mp_context: MPContext,
) -> Tuple[MPProcess, int, int]:
    loop = get_running_loop()
    read_pipe, write_pipe = mp_context.Pipe()
    proc = mp_context.Process(
        target=_child_main,
        args=(write_pipe, child_func),
        daemon=True,
    )
    proc.start()
    assert proc.pid is not None
    pid = proc.pid
    fd = os.pidfd_open(pid, 0)  # type: ignore

    # Wait for the child's readiness notification
    init_event = asyncio.Event()
    loop.add_reader(read_pipe.fileno(), init_event.set)
    await init_event.wait()
    loop.remove_reader(read_pipe.fileno())
    read_pipe.recv_bytes(1)
    read_pipe.close()
    return proc, pid, fd


async def afork(
    child_func: Callable[[], int],
    *,
    mp_context: Optional[MPContext] = None,
) -> AbstractChildProcess:
    """
    Fork the current process and execute the given function in the child.
    The return value of the function will become the exit code of the child
    process.

    Args:
        child_func: A function that represents the main function of the child and
                    returns an integer as its exit code.
                    Note that the function must set up a new event loop if it
                    wants to run asyncio codes.
        mp_context: The multiprocessing context to use. If not provided, the default
                    context will be used.

    .. versionadded:: 1.9.0

        The argument ``mp_context``.
    """
    if mp_context is None:
        mp_context = mp.get_context()
    if _has_pidfd:
        proc, pid, pidfd = await _clone_pidfd(child_func, mp_context)
        return PidfdChildProcess(proc, pid, pidfd)
    else:
        proc, pid = await _fork_posix(child_func, mp_context)
        return PosixChildProcess(proc, pid)
