import pytest

import asyncio
import aiotools
from aiotools.context import AbstractAsyncContextManager


def test_actxmgr_types():

    assert issubclass(aiotools.AsyncContextManager, AbstractAsyncContextManager)

    class boilerplate_ctx():
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

    @aiotools.actxmgr
    async def simple_ctx():
        yield

    async def simple_agen():
        yield

    assert issubclass(boilerplate_ctx, AbstractAsyncContextManager)
    assert isinstance(simple_ctx(), AbstractAsyncContextManager)
    assert not isinstance(simple_agen(), AbstractAsyncContextManager)


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
async def test_actxmgr_reuse(event_loop):

    @aiotools.actxmgr
    async def simple_ctx(msg):
        yield msg

    cm = simple_ctx('hello')

    async with cm as msg:
        assert msg == 'hello'

    with pytest.raises(RuntimeError) as excinfo:
        async with cm as msg:
            assert msg == 'hello'
        assert "didn't yield" in excinfo.value.args[0]

    cm = cm._recreate_cm()

    async with cm as msg:
        assert msg == 'hello'


@pytest.mark.asyncio
async def test_actxmgr_error(event_loop):

    @aiotools.actxmgr
    async def simple_ctx(msg):
        yield msg

    with pytest.raises(RuntimeError) as excinfo:
        async with simple_ctx('hello') as msg:
            assert msg == 'hello'
            raise RuntimeError('bomb1')
        assert 'bomb1' == excinfo.value.args[0]

    @aiotools.actxmgr
    async def simple_ctx(msg):
        try:
            yield msg
        finally:
            raise ValueError('bomb2')

    with pytest.raises(ValueError) as excinfo:
        async with simple_ctx('hello') as msg:
            assert msg == 'hello'
            raise RuntimeError('bomb1')
        assert 'bomb2' == excinfo.value.args[0]
        assert isinstance(excinfo.value.__cause__, ValueError)
        assert 'bomb1' == excinfo.value.__cause__.args[0]



@pytest.mark.asyncio
async def test_actxmgr_no_stop(event_loop):

    @aiotools.actxmgr
    async def simple_ctx(msg):
        try:
            yield msg
        except:
            pass
        finally:
            yield msg
        yield msg

    with pytest.raises(RuntimeError) as excinfo:
        async with simple_ctx('hello') as msg:
            assert msg == 'hello'
        assert "didn't stop" in excinfo.value.args[0]

    with pytest.raises(RuntimeError) as excinfo:
        async with simple_ctx('hello') as msg:
            raise ValueError('oops')
        assert "didn't stop after athrow" in excinfo.value.args[0]


@pytest.mark.asyncio
async def test_actxmgr_no_yield(event_loop):

    @aiotools.actxmgr
    async def no_yield_ctx1(msg):
        pass

    @aiotools.actxmgr
    @asyncio.coroutine
    def no_yield_ctx2(msg):
        pass

    with pytest.raises(RuntimeError) as excinfo:
        async with no_yield_ctx1('hello') as msg:
            pass
        assert "must be async-gen" in excinfo.value.args[0]

    with pytest.raises(RuntimeError) as excinfo:
        async with no_yield_ctx2('hello') as msg:
            pass
        assert "must be async-gen" in excinfo.value.args[0]


@pytest.mark.asyncio
async def test_actxdecorator(event_loop):

    step = 0

    class myacontext(aiotools.AsyncContextDecorator):

        async def __aenter__(self):
            nonlocal step
            step = 1
            return self

        async def __aexit__(self, exc_type, exc, tb):
            nonlocal step
            step = 2
            return False

    # NOTE: don't forget the ending parenthesis which instantiates myacontext
    @myacontext()
    async def myfunc():
        assert step == 1

    assert step == 0
    await myfunc()
    assert step == 2

    @myacontext()
    async def myfunc():
        assert step == 1
        raise RuntimeError('oops')

    step = 0
    assert step == 0
    try:
        await myfunc()
    except BaseException as e:
        assert e.args[0] == 'oops'
        assert step == 2
    finally:
        assert step == 2

    @myacontext()
    async def myfunc():
        assert step == 1

    # Use as a plain async context manager.
    step = 0
    assert step == 0
    async with myacontext():
        assert step == 1
    assert step == 2


@pytest.mark.asyncio
async def test_actxgroup(event_loop):

    # Test basic function.
    exit_count = 0

    @aiotools.actxmgr
    async def ctx(a):
        nonlocal exit_count
        yield a + 10
        exit_count += 1

    ctxgrp = aiotools.actxgroup()

    for i in range(3):
        ctxgrp.add(ctx(i))

    async with ctxgrp as values:
        assert values[0] == 10
        assert values[1] == 11
        assert values[2] == 12
        assert values == ctxgrp.contexts()

    assert exit_count == 3

    # Test generator/iterator initialization
    exit_count = 0
    ctxgrp = aiotools.actxgroup(ctx(i) for i in range(3))

    async with ctxgrp as values:
        assert values[0] == 10
        assert values[1] == 11
        assert values[2] == 12
        assert values == ctxgrp.contexts()

    assert exit_count == 3
