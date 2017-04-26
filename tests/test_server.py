import pytest

import asyncio
import multiprocessing as mp
import os
import signal

import aiotools


@pytest.fixture
def restore_signal():
    yield
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def test_server(restore_signal):

    started = False
    terminated = False

    def send_term_signal():
        os.kill(0, signal.SIGINT)

    @aiotools.actxmgr
    async def myserver(loop, proc_idx, args):
        nonlocal started, terminated
        assert proc_idx == 0
        assert len(args) == 0
        await asyncio.sleep(0)
        started = True
        loop.call_later(0.2, send_term_signal)

        yield

        await asyncio.sleep(0)
        terminated = True

    aiotools.start_server(myserver)

    assert started
    assert terminated


def test_server_multiproc(restore_signal):

    started = mp.Value('i', 0)
    terminated = mp.Value('i', 0)
    proc_idxs = mp.Array('i', 3)

    @aiotools.actxmgr
    async def myserver(loop, proc_idx, args):
        started, terminated, proc_idxs = args
        await asyncio.sleep(0)
        with started.get_lock():
            started.value += 1
        proc_idxs[proc_idx] = proc_idx

        yield

        await asyncio.sleep(0)
        with terminated.get_lock():
            terminated.value += 1

    def handler(signum, frame):
        os.kill(0, signal.SIGINT)

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(1)
    aiotools.start_server(myserver, num_proc=3,
                          args=(started, terminated, proc_idxs))

    assert started.value == 3
    assert terminated.value == 3
    assert list(proc_idxs) == [0, 1, 2]
    assert len(mp.active_children()) == 0
