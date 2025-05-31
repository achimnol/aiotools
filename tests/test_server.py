import asyncio
import functools
import glob
import logging.config
import multiprocessing as mp
import os
import signal
import sys
import tempfile
import threading
import time
from collections.abc import AsyncGenerator, Generator
from typing import Any, Optional, Sequence

import pytest

import aiotools
from aiotools.fork import MPContext

if os.environ.get("CI", "") and sys.version_info < (3, 9, 0):
    pytest.skip(
        "skipped to prevent kill CI agents due to signals on CI environments",
        allow_module_level=True,
    )

target_mp_contexts = [
    pytest.param(mp.get_context(method), id=method)
    for method in mp.get_all_start_methods()
    if method != "forkserver"
]


@pytest.fixture
def restore_signal():
    os.setpgrp()
    old_alrm = signal.getsignal(signal.SIGALRM)
    old_intr = signal.getsignal(signal.SIGINT)
    old_term = signal.getsignal(signal.SIGTERM)
    old_intr = signal.getsignal(signal.SIGUSR1)
    yield
    signal.signal(signal.SIGALRM, old_alrm)
    signal.signal(signal.SIGINT, old_intr)
    signal.signal(signal.SIGTERM, old_term)
    signal.signal(signal.SIGUSR1, old_term)


@pytest.fixture
def set_timeout():
    def make_timeout(sec, callback):
        def _callback(signum, frame):
            signal.alarm(0)
            callback()

        signal.signal(signal.SIGALRM, _callback)
        signal.setitimer(signal.ITIMER_REAL, sec)

    yield make_timeout


def write_record(record_name: str, msg: str) -> None:
    path = f"{record_name}.{os.getpid()}"
    with open(path, "a", encoding="utf8") as writer:
        writer.write(msg + "\n")


def read_records(record_name: str) -> Sequence[str]:
    lines: list[str] = []
    for path in glob.glob(f"{record_name}.*"):
        with open(path, "r", encoding="utf8") as reader:
            lines.extend(line.strip() for line in reader.readlines())
    return lines


@pytest.fixture
def exec_recorder():
    f = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf8",
        prefix="aiotools.tests.server.",
    )
    f.close()

    yield f.name

    for path in glob.glob(f"{f.name}.*"):
        os.unlink(path)


def interrupt():
    os.kill(0, signal.SIGINT)


def interrupt_usr1():
    os.kill(os.getpid(), signal.SIGUSR1)


@aiotools.server_context
async def myserver_simple(
    loop: asyncio.AbstractEventLoop,
    proc_idx: int,
    args: Sequence[Any],
) -> AsyncGenerator[None, signal.Signals]:
    record_name = args[0]
    await asyncio.sleep(0)
    write_record(record_name, f"started:{proc_idx}")

    yield

    await asyncio.sleep(0)
    write_record(record_name, f"terminated:{proc_idx}")


@pytest.mark.parametrize("mp_context", target_mp_contexts)
def test_server_singleproc(
    set_timeout,
    restore_signal,
    exec_recorder,
    mp_context: MPContext,
) -> None:
    record_name = exec_recorder
    set_timeout(0.2, interrupt)
    aiotools.start_server(
        myserver_simple,
        args=(record_name,),
        mp_context=mp_context,
    )
    lines = set(read_records(record_name))
    assert "started:0" in lines
    assert "terminated:0" in lines


@pytest.mark.parametrize("mp_context", target_mp_contexts)
def test_server_multiproc(
    set_timeout,
    restore_signal,
    exec_recorder,
    mp_context: MPContext,
) -> None:
    record_name = exec_recorder
    set_timeout(0.2, interrupt)
    aiotools.start_server(
        myserver_simple,
        num_workers=3,
        args=(record_name,),
        mp_context=mp_context,
    )
    lines = set(read_records(record_name))
    assert lines == {
        "started:0",
        "started:1",
        "started:2",
        "terminated:0",
        "terminated:1",
        "terminated:2",
    }


@aiotools.server_context
async def myserver_signal(
    loop: asyncio.AbstractEventLoop,
    proc_idx: int,
    args: Sequence[Any],
) -> AsyncGenerator[None, signal.Signals]:
    record_name = args[0]
    await asyncio.sleep(0)
    write_record(record_name, f"started:{proc_idx}")

    received_signum = yield

    await asyncio.sleep(0)
    write_record(record_name, f"terminated:{proc_idx}:{received_signum}")


@pytest.mark.parametrize("mp_context", target_mp_contexts)
def test_server_multiproc_custom_stop_signals(
    set_timeout,
    restore_signal,
    exec_recorder,
    mp_context: MPContext,
) -> None:
    record_name = exec_recorder
    set_timeout(0.2, interrupt_usr1)
    aiotools.start_server(
        myserver_signal,
        num_workers=2,
        stop_signals={signal.SIGUSR1},
        args=(record_name,),
        mp_context=mp_context,
    )
    lines = set(read_records(record_name))
    assert {"started:0", "started:1"} < lines
    assert {
        f"terminated:0:{int(signal.SIGUSR1)}",
        f"terminated:1:{int(signal.SIGUSR1)}",
    } < lines


@aiotools.server_context
async def myserver_worker_init_error(loop, proc_idx, args):
    record_name = args[0]

    class _LogAdaptor:
        def __init__(self, writer):
            self.writer = writer

        def write(self, msg):
            msg = msg.strip().replace("\n", " ")
            self.writer(f"log:{proc_idx}:{msg}")

    log_stream = _LogAdaptor(functools.partial(write_record, record_name))
    logging.config.dictConfig({
        "version": 1,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": log_stream,
                "level": "DEBUG",
            },
        },
        "loggers": {
            "aiotools": {
                "handlers": ["console"],
                "level": "DEBUG",
            },
        },
    })
    log = logging.getLogger("aiotools")
    write_record(record_name, f"started:{proc_idx}")
    log.debug("hello")
    if proc_idx in (0, 2):
        # delay until other workers start normally.
        await asyncio.sleep(0.1 * proc_idx)
        raise ZeroDivisionError("oops")

    yield

    # should not be reached if errored.
    await asyncio.sleep(0)
    write_record(record_name, f"terminated:{proc_idx}")


@pytest.mark.parametrize("mp_context", target_mp_contexts)
def test_server_worker_init_error(
    restore_signal,
    exec_recorder,
    mp_context: MPContext,
) -> None:
    record_name = exec_recorder
    aiotools.start_server(
        myserver_worker_init_error,
        num_workers=4,
        args=(record_name,),
        mp_context=mp_context,
    )
    lines = set(read_records(record_name))
    assert sum(1 if line.startswith("started:") else 0 for line in lines) == 4
    # workers who did not raise errors have already started,
    # and they should have terminated normally
    # when the errorneous worker interrupted the main loop.
    assert sum(1 if line.startswith("terminated:") else 0 for line in lines) == 2
    assert sum(1 if "hello" in line else 0 for line in lines) == 4
    assert sum(1 if "ZeroDivisionError: oops" in line else 0 for line in lines) == 2


main_enter = False
main_exit = False
main_signal = 0


@aiotools.main_context
def mymain_user_main() -> Generator[int, signal.Signals]:
    global main_enter, main_exit
    main_enter = True
    yield 987
    main_exit = True


@aiotools.server_context
async def myworker_user_main(
    loop: asyncio.AbstractEventLoop,
    proc_idx: int,
    args: Sequence[Any],
) -> AsyncGenerator[None, signal.Signals]:
    assert args[0] == 987  # first arg from user main
    assert args[1] == 123  # second arg from start_server args
    yield


@pytest.mark.parametrize("mp_context", target_mp_contexts)
def test_server_user_main(
    set_timeout,
    restore_signal,
    mp_context: MPContext,
) -> None:
    global main_enter, main_exit
    main_enter = False
    main_exit = False
    set_timeout(0.2, interrupt)
    aiotools.start_server(
        myworker_user_main,
        mymain_user_main,
        num_workers=3,
        args=(123,),
        mp_context=mp_context,
    )
    assert main_enter
    assert main_exit


@aiotools.main_context
def mymain_for_custom_stop_signals() -> Generator[None, signal.Signals]:
    global main_enter, main_exit, main_signal
    main_enter = True
    main_signal = yield
    main_exit = True


@aiotools.server_context
async def myworker_for_custom_stop_signals(
    loop: asyncio.AbstractEventLoop,
    proc_idx: int,
    args: Sequence[Any],
) -> AsyncGenerator[None, signal.Signals]:
    worker_signals = args[0]
    worker_signals[proc_idx] = yield


@pytest.mark.parametrize("mp_context", target_mp_contexts)
def test_server_user_main_custom_stop_signals(
    set_timeout,
    restore_signal,
    mp_context: MPContext,
) -> None:
    global main_enter, main_exit, main_signal
    main_enter = False
    main_exit = False
    main_signal = 0
    worker_signals = mp_context.Array("i", 3)

    def noop(signum, frame):
        pass

    set_timeout(0.2, interrupt_usr1)
    aiotools.start_server(
        myworker_for_custom_stop_signals,
        mymain_for_custom_stop_signals,
        num_workers=3,
        stop_signals={signal.SIGUSR1},
        args=(worker_signals,),
        mp_context=mp_context,
    )

    assert main_enter
    assert main_exit
    assert main_signal == signal.SIGUSR1
    assert worker_signals[0] == signal.SIGUSR1
    assert worker_signals[1] == signal.SIGUSR1
    assert worker_signals[2] == signal.SIGUSR1


@aiotools.main_context
def mymain_for_main_tuple() -> Generator[tuple[int, int], signal.Signals]:
    global main_enter, main_exit
    main_enter = True
    yield 987, 654
    main_exit = True


@aiotools.server_context
async def myworker_for_main_tuple(
    loop: asyncio.AbstractEventLoop,
    proc_idx: int,
    args: Sequence[Any],
) -> AsyncGenerator[None, signal.Signals]:
    assert args[0] == 987  # first arg from user main
    assert args[1] == 654  # second arg from user main
    assert args[2] == 123  # third arg from start_server args
    yield


@pytest.mark.parametrize("mp_context", target_mp_contexts)
def test_server_user_main_tuple(
    set_timeout,
    restore_signal,
    mp_context: MPContext,
) -> None:
    global main_enter, main_exit
    main_enter = False
    main_exit = False

    set_timeout(0.2, interrupt)
    aiotools.start_server(
        myworker_for_main_tuple,
        mymain_for_main_tuple,
        num_workers=3,
        args=(123,),
        mp_context=mp_context,
    )

    assert main_enter
    assert main_exit


@aiotools.server_context
async def myworker_for_extra_proc(
    loop: asyncio.AbstractEventLoop,
    proc_idx: int,
    args: Sequence[Any],
) -> AsyncGenerator[None, signal.Signals]:
    yield


def extra_proc_plain(
    key: int,
    /,
    intr_event: Optional[threading.Event],
    pidx: int,
    args: Sequence[Any],
) -> None:
    assert intr_event is None  # unused with multiprocessing mode
    extras = args[0]
    extras[key] = 980 + key
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print(f"extra[{key}] interrupted", file=sys.stderr)
    except Exception as e:
        print(f"extra[{key}] exception", e, file=sys.stderr)
    finally:
        print(f"extra[{key}] finish", file=sys.stderr)
        extras[key] = 990 + key


@pytest.mark.parametrize("mp_context", target_mp_contexts)
def test_server_extra_proc(set_timeout, restore_signal, mp_context: MPContext) -> None:
    extras = mp_context.Array("i", [0, 0])
    set_timeout(0.2, interrupt)
    aiotools.start_server(
        myworker_for_extra_proc,
        extra_procs=[
            functools.partial(extra_proc_plain, 0),
            functools.partial(extra_proc_plain, 1),
        ],
        num_workers=3,
        args=(extras,),
        mp_context=mp_context,
    )

    assert extras[0] == 990
    assert extras[1] == 991


@aiotools.server_context
async def myworker_with_extra_proc_for_custom_stop_signal(
    loop: asyncio.AbstractEventLoop,
    proc_idx: int,
    args: Sequence[Any],
) -> AsyncGenerator[None, signal.Signals]:
    yield


def extra_proc_for_custom_stop_signal(
    key: int,
    /,
    intr_event: Optional[threading.Event],
    pidx: int,
    args: Sequence[Any],
) -> None:
    assert intr_event is None  # unused with multiprocessing mode
    received_signals = args[0]
    try:
        while True:
            time.sleep(0.1)
    except aiotools.InterruptedBySignal as e:
        received_signals[key] = e.args[0]


@pytest.mark.parametrize("mp_context", target_mp_contexts)
def test_server_extra_proc_custom_stop_signal(
    set_timeout,
    restore_signal,
    mp_context: MPContext,
) -> None:
    received_signals = mp_context.Array("i", [0, 0])
    set_timeout(0.3, interrupt_usr1)
    aiotools.start_server(
        myworker_with_extra_proc_for_custom_stop_signal,
        extra_procs=[
            functools.partial(extra_proc_for_custom_stop_signal, 0),
            functools.partial(extra_proc_for_custom_stop_signal, 1),
        ],
        stop_signals={signal.SIGUSR1},
        args=(received_signals,),
        num_workers=3,
        mp_context=mp_context,
    )

    assert received_signals[0] == signal.SIGUSR1
    assert received_signals[1] == signal.SIGUSR1
