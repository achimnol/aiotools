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
from aiotools.fork import MPContext, PidfdChildProcess, _has_pidfd, afork

if sys.platform == "win32":
    pytest.skip(
        "forks tests not supported on Windows",
        allow_module_level=True,
    )


target_mp_contexts = [
    pytest.param(mp.get_context(method), id=method)
    for method in mp.get_all_start_methods()
    if method != "forkserver"
]


def child_for_fork() -> int:
    print("hello world")
    time.sleep(0.1)
    return 99


async def _do_test_fork(mp_context: MPContext):
    proc = await afork(child_for_fork, mp_context=mp_context)
    assert proc.pid > 0
    if isinstance(proc, PidfdChildProcess):
        assert proc._pidfd > 0
    ret = await proc.wait()
    assert ret == 99


def child_for_fork_already_terminated() -> int:
    time.sleep(0.1)
    return 99


async def _do_test_fork_already_terminated(mp_context: MPContext):
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


async def _do_test_fork_signal(mp_context: MPContext):
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


async def _do_test_fork_segfault(mp_context: MPContext):
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


async def _do_test_fork_many(mp_context: MPContext):
    os.setpgrp()
    proc_list = []
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


@pytest.mark.skipif(
    not _has_pidfd, reason="pidfd is supported in Python 3.9+ and Linux kernel 5.4+"
)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork(mp_context: MPContext):
    await _do_test_fork(mp_context)


@pytest.mark.skipif(
    not _has_pidfd, reason="pidfd is supported in Python 3.9+ and Linux kernel 5.4+"
)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_already_terminated(mp_context: MPContext):
    await _do_test_fork_already_terminated(mp_context)


@pytest.mark.skipif(
    not _has_pidfd, reason="pidfd is supported in Python 3.9+ and Linux kernel 5.4+"
)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_signal(mp_context: MPContext):
    await _do_test_fork_signal(mp_context)


@pytest.mark.skipif(
    not _has_pidfd, reason="pidfd is supported in Python 3.9+ and Linux kernel 5.4+"
)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_segfault(mp_context: MPContext):
    await _do_test_fork_segfault(mp_context)


@pytest.mark.skipif(
    not _has_pidfd, reason="pidfd is supported in Python 3.9+ and Linux kernel 5.4+"
)
@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_many(mp_context: MPContext):
    await _do_test_fork_many(mp_context)


@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_fallback(mp_context: MPContext):
    with mock.patch.object(
        fork_mod,
        "_has_pidfd",
        False,
    ):
        await _do_test_fork(mp_context)


@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_already_termination_fallback(mp_context: MPContext):
    with mock.patch.object(
        fork_mod,
        "_has_pidfd",
        False,
    ):
        await _do_test_fork_already_terminated(mp_context)


@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_signal_fallback(mp_context: MPContext):
    with mock.patch.object(
        fork_mod,
        "_has_pidfd",
        False,
    ):
        await _do_test_fork_signal(mp_context)


@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_segfault_fallback(mp_context: MPContext):
    with mock.patch.object(
        fork_mod,
        "_has_pidfd",
        False,
    ):
        await _do_test_fork_segfault(mp_context)


@pytest.mark.parametrize("mp_context", target_mp_contexts)
@pytest.mark.asyncio
async def test_fork_many_fallback(mp_context: MPContext):
    with mock.patch.object(
        fork_mod,
        "_has_pidfd",
        False,
    ):
        await _do_test_fork_many(mp_context)
