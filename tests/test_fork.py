from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import signal
import sys
import time
from unittest import mock

import pytest

from aiotools import fork as fork_mod
from aiotools.fork import (
    AbstractChildProcess,
    MPContext,
    PidfdChildProcess,
    _has_pidfd,
    afork,
)

if sys.platform == "win32":
    pytest.skip(
        "forks tests not supported on Windows",
        allow_module_level=True,
    )


pidfd_params = [
    pytest.param(False, id="posix"),
    pytest.param(
        True,
        marks=pytest.mark.skipif(
            not _has_pidfd,
            reason="Your Python build does not support pidfd (supported in Python 3.9+ and Linux kernel 5.4+)",
        ),
        id="pidfd",
    ),
]

target_mp_contexts = [
    pytest.param(mp.get_context(method), id=method)
    for method in mp.get_all_start_methods()
]


def child_for_fork() -> int:
    print("hello world")
    time.sleep(0.1)
    return 99


@pytest.mark.parametrize("has_pidfd", pidfd_params)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork(has_pidfd: bool, mp_context: MPContext) -> None:
    with mock.patch.object(fork_mod, "_has_pidfd", has_pidfd):
        proc = await afork(child_for_fork, mp_context=mp_context)
        assert proc.pid > 0
        if isinstance(proc, PidfdChildProcess):
            assert proc._pidfd > 0
        ret = await proc.wait()
        assert ret == 99


def child_for_fork_already_terminated() -> int:
    time.sleep(0.1)
    return 99


@pytest.mark.parametrize("has_pidfd", pidfd_params)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_already_terminated(has_pidfd: bool, mp_context: MPContext) -> None:
    with mock.patch.object(fork_mod, "_has_pidfd", has_pidfd):
        proc = await afork(child_for_fork_already_terminated, mp_context=mp_context)
        assert proc.pid > 0
        if isinstance(proc, PidfdChildProcess):
            assert proc._pidfd > 0
        await asyncio.sleep(0.5)
        ret = await proc.wait()
        assert ret == 99


def child_for_fork_signal() -> int:
    try:
        time.sleep(10)
    except KeyboardInterrupt:
        return 101
    return 100


@pytest.mark.parametrize("has_pidfd", pidfd_params)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_signal(has_pidfd: bool, mp_context: MPContext) -> None:
    with mock.patch.object(fork_mod, "_has_pidfd", has_pidfd):
        os.setpgrp()
        proc = await afork(child_for_fork_signal, mp_context=mp_context)
        assert proc.pid > 0
        if isinstance(proc, PidfdChildProcess):
            assert proc._pidfd > 0
        await asyncio.sleep(0.1)
        proc.send_signal(signal.SIGINT)
        ret = await proc.wait()
        # FIXME: Sometimes it returns 254
        assert ret == 101


def child_for_fork_segfault() -> int:
    time.sleep(0.1)
    import ctypes

    ctypes.string_at(0)  # segfault!
    return 0


@pytest.mark.parametrize("has_pidfd", pidfd_params)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_segfault(has_pidfd: bool, mp_context: MPContext) -> None:
    with mock.patch.object(fork_mod, "_has_pidfd", has_pidfd):
        os.setpgrp()
        proc = await afork(child_for_fork_segfault, mp_context=mp_context)
        assert proc.pid > 0
        if isinstance(proc, PidfdChildProcess):
            assert proc._pidfd > 0
        ret = await proc.wait()
        assert ret == -11  # SIGSEGV


def child_for_fork_many() -> int:
    try:
        time.sleep(10)
    except KeyboardInterrupt:
        return 101
    return 100


@pytest.mark.parametrize("has_pidfd", pidfd_params)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_many(has_pidfd: bool, mp_context: MPContext) -> None:
    with mock.patch.object(fork_mod, "_has_pidfd", has_pidfd):
        os.setpgrp()
        proc_list: list[AbstractChildProcess] = []
        for _ in range(32):
            proc = await afork(child_for_fork_many, mp_context=mp_context)
            proc_list.append(proc)
            assert proc.pid > 0
            if isinstance(proc, PidfdChildProcess):
                assert proc._pidfd > 0
        for i in range(16):
            proc_list[i].send_signal(signal.SIGINT)
        for i in range(16, 32):
            proc_list[i].send_signal(signal.SIGTERM)
        ret_list = await asyncio.gather(*[proc.wait() for proc in proc_list])
        for i in range(16):
            assert ret_list[i] == 101
        for i in range(16, 32):
            assert ret_list[i] == -15  # killed by SIGTERM
