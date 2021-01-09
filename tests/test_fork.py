import asyncio
import signal
import time
from unittest import mock

import pytest

from aiotools import fork as fork_mod
from aiotools.fork import fork, PidfdChildProcess


async def _do_test_fork():

    def child():
        print("hello world")
        time.sleep(0.1)
        return 99

    proc = await fork(child)
    assert proc._pid > 0
    if isinstance(proc, PidfdChildProcess):
        assert proc._pidfd > 0
    ret = await proc.wait()
    assert ret == 99


async def _do_test_fork_already_terminated():

    def child():
        time.sleep(0.1)
        return 99

    proc = await fork(child)
    assert proc._pid > 0
    if isinstance(proc, PidfdChildProcess):
        assert proc._pidfd > 0
    await asyncio.sleep(0.5)
    ret = await proc.wait()
    assert ret == 99


async def _do_test_fork_signal():

    def child():
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            return 101
        return 100

    proc = await fork(child)
    assert proc._pid > 0
    if isinstance(proc, PidfdChildProcess):
        assert proc._pidfd > 0
    proc.send_signal(signal.SIGINT)
    ret = await proc.wait()
    assert ret == 101


async def _do_test_fork_many():

    def child():
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            return 101
        return 100

    proc_list = []
    for _ in range(32):
        proc = await fork(child)
        proc_list.append(proc)
        assert proc._pid > 0
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
    not hasattr(signal, 'pidfd_send_signal'),
    reason="pidfd is supported in Python 3.9+ and Linux kernel 5.4+"
)
@pytest.mark.asyncio
async def test_fork():
    await _do_test_fork()


@pytest.mark.skipif(
    not hasattr(signal, 'pidfd_send_signal'),
    reason="pidfd is supported in Python 3.9+ and Linux kernel 5.4+"
)
@pytest.mark.asyncio
async def test_fork_already_terminated():
    await _do_test_fork_already_terminated()


@pytest.mark.skipif(
    not hasattr(signal, 'pidfd_send_signal'),
    reason="pidfd is supported in Python 3.9+ and Linux kernel 5.4+"
)
@pytest.mark.asyncio
async def test_fork_signal():
    await _do_test_fork_signal()


@pytest.mark.skipif(
    not hasattr(signal, 'pidfd_send_signal'),
    reason="pidfd is supported in Python 3.9+ and Linux kernel 5.4+"
)
@pytest.mark.asyncio
async def test_fork_many():
    await _do_test_fork_many()


@pytest.mark.asyncio
async def test_fork_fallback():
    with mock.patch.object(
        fork_mod, '_has_pidfd', False,
    ):
        await _do_test_fork()


@pytest.mark.asyncio
async def test_fork_already_termination_fallback():
    with mock.patch.object(
        fork_mod, '_has_pidfd', False,
    ):
        await _do_test_fork_already_terminated()


@pytest.mark.asyncio
async def test_fork_signal_fallback():
    with mock.patch.object(
        fork_mod, '_has_pidfd', False,
    ):
        await _do_test_fork_signal()


@pytest.mark.asyncio
async def test_fork_many_fallback():
    with mock.patch.object(
        fork_mod, '_has_pidfd', False,
    ):
        await _do_test_fork_many()
