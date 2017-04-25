import pytest

import asyncio
import os
import signal

import aiotools


def send_term_signal():
    os.kill(0, signal.SIGTERM)


def test_server():

    started = False
    terminated = False

    @aiotools.actxmgr
    async def myserver(loop):
        nonlocal started, terminated
        started = True
        loop.call_later(0.1, send_term_signal)
        yield
        terminated = True

    aiotools.start_server(myserver)

    assert started
    assert terminated
