import pytest

import asyncio
from aiotools.defer import defer, adefer
from aiotools.func import apartial


def test_defer():

    x = []

    @defer
    def myfunc(defer):
        x.append(1)
        defer(lambda: x.append(1))
        x.append(2)
        defer(lambda: x.append(2))
        x.append(3)
        defer(lambda: x.append(3))

    myfunc()
    assert x == [1, 2, 3, 3, 2, 1]


def test_defer_inner_exception():

    x = []

    @defer
    def myfunc(defer):
        x.append(1)
        defer(lambda: x.append(1))
        x.append(2)
        defer(lambda: x.append(2))
        raise ZeroDivisionError
        x.append(3)
        defer(lambda: x.append(3))

    with pytest.raises(ZeroDivisionError):
        myfunc()
    assert x == [1, 2, 2, 1]


def test_defer_wrong_func():
    with pytest.raises(AssertionError):
        @defer
        async def myfunc(defer):
            pass


@pytest.mark.asyncio
async def test_adefer():

    x = []

    @adefer
    async def myfunc(defer):
        x.append(1)
        defer(lambda: x.append(1))
        x.append(2)
        defer(lambda: x.append(2))
        x.append(3)
        defer(lambda: x.append(3))

    await myfunc()
    assert x == [1, 2, 3, 3, 2, 1]


def test_adefer_wrong_func():
    with pytest.raises(AssertionError):
        @adefer
        def myfunc(defer):
            pass


@pytest.mark.asyncio
async def test_adefer_coro():

    x = []

    async def async_append(target, item):
        target.append(item)
        await asyncio.sleep(0)

    @adefer
    async def myfunc(defer):
        x.append(1)
        defer(async_append(x, 1))
        x.append(2)
        defer(async_append(x, 2))
        x.append(3)
        defer(async_append(x, 3))

    await myfunc()
    assert x == [1, 2, 3, 3, 2, 1]


@pytest.mark.asyncio
async def test_adefer_corofunc():

    x = []

    async def async_append(target, item):
        target.append(item)
        await asyncio.sleep(0)

    @adefer
    async def myfunc(defer):
        x.append(1)
        defer(apartial(async_append, x, 1))
        x.append(2)
        defer(apartial(async_append, x, 2))
        x.append(3)
        defer(apartial(async_append, x, 3))

    await myfunc()
    assert x == [1, 2, 3, 3, 2, 1]


@pytest.mark.asyncio
async def test_adefer_inner_exception():

    x = []

    async def async_append(target, item):
        target.append(item)
        await asyncio.sleep(0)

    @adefer
    async def myfunc(defer):
        x.append(1)
        defer(apartial(async_append, x, 1))
        x.append(2)
        defer(apartial(async_append, x, 2))
        raise ZeroDivisionError
        x.append(3)
        defer(apartial(async_append, x, 3))

    with pytest.raises(ZeroDivisionError):
        await myfunc()
    assert x == [1, 2, 2, 1]
