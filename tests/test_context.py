import pytest

import asyncio
import aiotools


@pytest.mark.asyncio
async def test_actxmgr(event_loop):

    step = 0

    @aiotools.actxmgr
    async def simple_ctx(msg):
        nonlocal step
        step = 1
        await asyncio.sleep(0.2)
        step = 2
        try:
            yield msg
            step = 3
        finally:
            await asyncio.sleep(0.2)
            step = 4

    begin = event_loop.time()
    step = 0
    async with simple_ctx('hello') as msg:
        assert step == 2
        assert msg == 'hello'
        await asyncio.sleep(0.2)
    assert step == 4
    assert 0.55 <= (event_loop.time() - begin) <= 0.65

    begin = event_loop.time()
    step = 0
    try:
        async with simple_ctx('world') as msg:
            assert step == 2
            assert msg == 'world'
            await asyncio.sleep(0.2)
            raise ValueError('something wrong')
    except Exception as e:
        await asyncio.sleep(0.2)
        assert e.args[0] == 'something wrong'
        assert step == 4
    assert 0.75 <= (event_loop.time() - begin) <= 0.85


@pytest.mark.asyncio
async def test_actxmgr_no_yield(event_loop):

    @aiotools.actxmgr
    async def no_yield_ctx1(msg):
        pass

    @aiotools.actxmgr
    @asyncio.coroutine
    def no_yield_ctx2(msg):
        pass

    with pytest.raises(RuntimeError):
        async with no_yield_ctx1('hello') as msg:
            pass

    with pytest.raises(RuntimeError):
        async with no_yield_ctx2('hello') as msg:
            pass
