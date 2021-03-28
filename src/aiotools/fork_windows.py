from __future__ import annotations

import asyncio
import logging
import os
import pickle
import runpy
from typing import (
    Any,
    Callable,
    Mapping,
    Sequence,
)
import secrets
import signal
import struct
import sys
import traceback
import types

from .compat import get_running_loop
from .fork_base import AbstractChildProcess

logger = logging.getLogger(__name__)

try:
    ORIGINAL_DIR = os.path.abspath(os.getcwd())
except OSError:
    ORIGINAL_DIR = None

old_main_modules = []


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
    def __init__(
        self,
        init_event: asyncio.Event,
        init_data: Mapping[str, Any],
        child_func: Callable[[], int],
    ) -> None:
        self.buf = []
        self.init_event = init_event
        self.init_data = init_data
        self.child_func = child_func

    def connection_made(self, trans: asyncio.BaseTransport) -> None:
        self.trans = trans

    def data_received(self, data: bytes) -> None:
        if data == b'\0':
            init_data = pickle.dumps(self.init_data)
            self.trans.write(struct.pack('I', len(init_data)))
            self.trans.write(init_data)
            child_func_data = pickle.dumps(self.child_func)
            self.trans.write(struct.pack('I', len(child_func_data)))
            self.trans.write(child_func_data)
        if data == b'\1':
            self.trans.close()
            self.init_event.set()


WINEXE = getattr(sys, 'frozen', False)
WINSERVICE = sys.executable.lower().endswith("pythonservice.exe")

if WINSERVICE:
    _python_exe = os.path.join(sys.exec_prefix, 'python.exe')
else:
    _python_exe = sys.executable


def pack_init_data() -> bytes:
    """
    Pickles initialization parameters for the child process.

    Reference: CPython's multiprocessing.spwan.get_preparation_data()
    """

    init_data = {
        "orig_dir": ORIGINAL_DIR,
        "dir": os.getcwd(),
        "sys_argv": sys.argv,
    }

    sys_path = sys.path.copy()
    try:
        i = sys_path.index('')
    except ValueError:
        pass
    else:
        sys_path[i] = ORIGINAL_DIR
    init_data["sys_path"] = sys_path

    main_module = sys.modules['__main__']
    main_mod_name = getattr(main_module.__spec__, "name", None)
    if main_mod_name is not None:
        init_data['init_main_from_name'] = main_mod_name
    elif sys.platform != 'win32' or (not WINEXE and not WINSERVICE):
        main_path = getattr(main_module, '__file__', None)
        if main_path is not None:
            if (
                not os.path.isabs(main_path) and
                ORIGINAL_DIR is not None
            ):
                main_path = os.path.join(ORIGINAL_DIR, main_path)
            init_data['init_main_from_path'] = os.path.normpath(main_path)
    return init_data


def populate_init_data(data: bytes) -> None:
    """
    Unpickles the initialization parameters and configure
    the current process using them.

    Reference: CPython's multiprocessing.spwan.prepare()
    """
    if 'sys_path' in data:
        sys.path = data['sys_path']

    if 'sys_argv' in data:
        sys.argv = data['sys_argv']

    if 'dir' in data:
        os.chdir(data['dir'])

    if 'orig_dir' in data:
        global ORIGINAL_DIR
        ORIGINAL_DIR = data['orig_dir']

    if 'init_main_from_name' in data:
        _fixup_main_from_name(data['init_main_from_name'])
    elif 'init_main_from_path' in data:
        _fixup_main_from_path(data['init_main_from_path'])


# Helpers taken from CPython's multiprocessing.spawn module

def _fixup_main_from_name(mod_name: str) -> None:
    # __main__.py files for packages, directories, zip archives, etc, run
    # their "main only" code unconditionally, so we don't even try to
    # populate anything in __main__, nor do we make any changes to
    # __main__ attributes
    current_main = sys.modules['__main__']
    if mod_name == "__main__" or mod_name.endswith(".__main__"):
        return

    # If this process was forked, __main__ may already be populated
    if getattr(current_main.__spec__, "name", None) == mod_name:
        return

    # Otherwise, __main__ may contain some non-main code where we need to
    # support unpickling it properly. We rerun it as __mp_main__ and make
    # the normal __main__ an alias to that
    old_main_modules.append(current_main)
    main_module = types.ModuleType("__mp_main__")
    main_content = runpy.run_module(
        mod_name,
        run_name="__mp_main__",
        alter_sys=True,
    )
    main_module.__dict__.update(main_content)
    sys.modules['__main__'] = sys.modules['__mp_main__'] = main_module


def _fixup_main_from_path(main_path: str) -> None:
    # If this process was forked, __main__ may already be populated
    current_main = sys.modules['__main__']

    # Unfortunately, the main ipython launch script historically had no
    # "if __name__ == '__main__'" guard, so we work around that
    # by treating it like a __main__.py file
    # See https://github.com/ipython/ipython/issues/4698
    main_name = os.path.splitext(os.path.basename(main_path))[0]
    if main_name == 'ipython':
        return

    # Otherwise, if __file__ already has the setting we expect,
    # there's nothing more to do
    if getattr(current_main, '__file__', None) == main_path:
        return

    # If the parent process has sent a path through rather than a module
    # name we assume it is an executable script that may contain
    # non-main code that needs to be executed
    old_main_modules.append(current_main)
    main_module = types.ModuleType("__mp_main__")
    main_content = runpy.run_path(
        main_path,
        run_name="__mp_main__",
    )
    main_module.__dict__.update(main_content)
    sys.modules['__main__'] = sys.modules['__mp_main__'] = main_module


def _child_main_async(init_func: Callable[[], None] | None, pipe_name: str) -> int:
    if init_func is not None:
        init_func()
    init_data: bytes
    child_func = None

    async def notify_parent():
        nonlocal init_data, child_func
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader(loop=loop)
        r_proto = asyncio.StreamReaderProtocol(reader)
        w_transp, w_proto = await loop.create_pipe_connection(
            lambda: r_proto, pipe_name,
        )
        writer = asyncio.StreamWriter(w_transp, w_proto, reader, loop=loop)
        writer.write(b'\0')
        await writer.drain()
        try:
            init_data_size = struct.unpack('I', await reader.readexactly(4))[0]
            init_data = pickle.loads(await reader.readexactly(init_data_size))
            populate_init_data(init_data)
            child_func_size = struct.unpack('I', await reader.readexactly(4))[0]
            child_func = pickle.loads(await reader.readexactly(child_func_size))
        except Exception:
            traceback.print_exc()
        finally:
            writer.write(b'\1')
            await writer.drain()
            writer.close()

    asyncio.run(notify_parent())
    return child_func()


def is_forking(argv: Sequence[str]) -> bool:
    return len(argv) >= 3 and argv[2] == '--aiotools-fork'


def _spawn_main() -> None:
    """
    The child process' main function
    """
    assert is_forking(sys.argv), \
        "_spawn_main() must be called in an aiotools-fokred child process."
    pipe_name = sys.argv[1]
    ret = -1
    try:
        ret = _child_main_async(None, pipe_name)
    except KeyboardInterrupt:
        ret = -signal.SIGINT
    finally:
        os._exit(ret)


async def _spawn_windows(child_func) -> WindowsChildProcess:
    loop = get_running_loop()
    assert isinstance(loop, asyncio.ProactorEventLoop)
    init_data = pack_init_data()

    _random_key = secrets.token_hex(16)
    pipe_name = f"\\\\.\\pipe\\aiotools-fork-{os.getpid()}-{_random_key}"
    init_event = asyncio.Event()
    [server] = await loop.start_serving_pipe(
        lambda: ParentProtocol(init_event, init_data, child_func),  # type: ignore
        pipe_name,
    )
    await asyncio.sleep(0)

    proc = await asyncio.create_subprocess_exec(
        _python_exe,
        '-c', 'from aiotools.fork_windows import _spawn_main; _spawn_main()',
        pipe_name, '--aiotools-fork',
    )
    await init_event.wait()
    server.close()
    return WindowsChildProcess(proc)
