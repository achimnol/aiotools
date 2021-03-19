from __future__ import annotations

import asyncio
import importlib
import logging
import os
import secrets
import signal
import sys

from .compat import get_running_loop
from .fork_base import AbstractChildProcess

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
                self._proc.pid,
                signum,
            )
            return
        self._proc.send_signal(signum)

    async def wait(self) -> int:
        try:
            self._returncode = await self._proc.wait()
        except ChildProcessError:
            # The child process is already reaped
            # (may happen if waitpid() is called elsewhere).
            self._returncode = 255
            logger.warning(
                "child process pid %d exit status already read: "
                " will report returncode 255",
                self._proc.pid)
        finally:
            self._terminated = True
        return self._returncode


class ParentProtocol(asyncio.Protocol):
    def __init__(self, init_event: asyncio.Event) -> None:
        self.buf = []
        self.init_event = init_event

    def connection_made(self, trans: asyncio.BaseTransport) -> None:
        self.trans = trans

    def data_received(self, data: bytes) -> None:
        self.trans.close()
        self.init_event.set()


def _child_main_async(init_func, pipe_name, child_func_ref: str) -> int:
    if init_func is not None:
        init_func()

    async def notify_parent():
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader(loop=loop)
        r_proto = asyncio.StreamReaderProtocol(reader)
        w_transp, w_proto = await loop.create_pipe_connection(
            lambda: r_proto, pipe_name,
        )
        writer = asyncio.StreamWriter(w_transp, w_proto, reader, loop=loop)
        writer.write(b'\0')
        await writer.drain()
        writer.close()
        await asyncio.sleep(0.1)

    asyncio.run(notify_parent())
    # o = importlib.import_module(child_func_ref)
    # return o()
    print("hello windows")
    return 999


def _spawn_main():
    pipe_name = sys.argv[1]
    child_func_ref = sys.argv[2]
    ret = -1
    try:
        ret = _child_main_async(None, pipe_name, child_func_ref)
    except KeyboardInterrupt:
        ret = -signal.SIGINT
    finally:
        os._exit(ret)


async def _spawn_windows(child_func) -> WindowsChildProcess:
    loop = get_running_loop()
    assert isinstance(loop, asyncio.ProactorEventLoop)

    _random_key = secrets.token_hex(16)
    pipe_name = f"\\\\.\\pipe\\aiotools-fork-{os.getpid()}-{_random_key}"
    init_event = asyncio.Event()
    [server] = await loop.start_serving_pipe(
        lambda: ParentProtocol(init_event), pipe_name,  # type: ignore
    )
    await asyncio.sleep(0)

    func_fqdn = f"{child_func.__module__}.{child_func.__name__}"
    proc = await asyncio.create_subprocess_exec(
        sys.executable, '-m', 'aiotools.fork_windows', pipe_name, func_fqdn,
    )
    await init_event.wait()
    server.close()
    return WindowsChildProcess(proc)


if __name__ == "__main__":
    _spawn_main()
