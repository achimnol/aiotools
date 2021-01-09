import signal
import time

import pytest

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
