import pytest

import asyncio
import os
import signal

import aiotools


def send_term_signal():
    # FIXME: TravisCI makes this to fail.
    #os.kill(0, signal.SIGINT)
    raise SystemExit


def test_server():

    started = False
    terminated = False

    @aiotools.actxmgr
    async def myserver(loop):
        nonlocal started, terminated
        started = True
        loop.call_later(0.2, send_term_signal)
        yield
        terminated = True

    loop = asyncio.new_event_loop()
    aiotools.start_server(myserver, loop=loop)

    assert started
    assert terminated
    assert loop.is_closed()
