import asyncio

import pytest

from aiotools.defer import AsyncDeferFunc, DeferFunc, adefer, defer
from aiotools.func import apartial


def test_defer() -> None:
    x: list[int] = []

    @defer
    def myfunc(defer: DeferFunc) -> None:
        x.append(1)
        defer(lambda: x.append(1))
        x.append(2)
        defer(lambda: x.append(2))
        x.append(3)
        defer(lambda: x.append(3))

    myfunc()
    assert x == [1, 2, 3, 3, 2, 1]


def test_defer_inner_exception() -> None:
    x: list[int] = []

    @defer
    def myfunc(defer: DeferFunc) -> None:
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


def test_defer_wrong_func() -> None:
    with pytest.raises(AssertionError):

        @defer
        async def myfunc(defer: DeferFunc) -> None:
            pass


@pytest.mark.asyncio
async def test_adefer() -> None:
    x: list[int] = []

    @adefer
    async def myfunc(defer: AsyncDeferFunc) -> None:
        x.append(1)
        defer(lambda: x.append(1))
        x.append(2)
        defer(lambda: x.append(2))
        x.append(3)
        defer(lambda: x.append(3))

    await myfunc()
    assert x == [1, 2, 3, 3, 2, 1]


def test_adefer_wrong_func() -> None:
    with pytest.raises(AssertionError):

        @adefer  # type: ignore[arg-type]
        def myfunc(defer: AsyncDeferFunc) -> None:
            pass


@pytest.mark.asyncio
async def test_adefer_coro() -> None:
    x: list[int] = []

    async def async_append(target: list[int], item: int) -> None:
        target.append(item)
        await asyncio.sleep(0)

    @adefer
    async def myfunc(defer: AsyncDeferFunc) -> None:
        x.append(1)
        defer(async_append(x, 1))
        x.append(2)
        defer(async_append(x, 2))
        x.append(3)
        defer(async_append(x, 3))

    await myfunc()
    assert x == [1, 2, 3, 3, 2, 1]


@pytest.mark.asyncio
async def test_adefer_corofunc() -> None:
    x: list[int] = []

    async def async_append(target: list[int], item: int) -> None:
        target.append(item)
        await asyncio.sleep(0)

    @adefer
    async def myfunc(defer: AsyncDeferFunc) -> None:
        x.append(1)
        defer(apartial(async_append, x, 1))
        x.append(2)
        defer(apartial(async_append, x, 2))
        x.append(3)
        defer(apartial(async_append, x, 3))

    await myfunc()
    assert x == [1, 2, 3, 3, 2, 1]


@pytest.mark.asyncio
async def test_adefer_inner_exception() -> None:
    x: list[int] = []

    async def async_append(target: list[int], item: int) -> None:
        target.append(item)
        await asyncio.sleep(0)

    @adefer
    async def myfunc(defer: AsyncDeferFunc) -> None:
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
