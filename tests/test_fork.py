import asyncio
import signal
import time
from unittest import mock

import pytest

from aiotools import fork as fork_mod
from aiotools.fork import fork, PidfdChildProcess


@pytest.mark.asyncio
async def test_fork():

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


@pytest.mark.asyncio
async def test_fork_already_termination():

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


@pytest.mark.asyncio
async def test_fork_signal():

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


@pytest.mark.asyncio
async def test_fork_fallback():
    with mock.patch.object(
        fork_mod, '_has_pidfd', False,
    ):
        def child():
            print("hello world")
            time.sleep(0.1)
            return 99

        proc = await fork(child)
        assert proc._pid > 0
        ret = await proc.wait()
        assert ret == 99


@pytest.mark.asyncio
async def test_fork_already_termination_fallback():
    with mock.patch.object(
        fork_mod, '_has_pidfd', False,
    ):
        def child():
            time.sleep(0.1)
            return 99

        proc = await fork(child)
        assert proc._pid > 0
        await asyncio.sleep(0.5)
        ret = await proc.wait()
        assert ret == 99


@pytest.mark.asyncio
async def test_fork_signal_fallback():
    with mock.patch.object(
        fork_mod, '_has_pidfd', False,
    ):
        def child():
            try:
                time.sleep(10)
            except KeyboardInterrupt:
                return 101
            return 100

        proc = await fork(child)
        assert proc._pid > 0
        proc.send_signal(signal.SIGINT)
        ret = await proc.wait()
        assert ret == 101
