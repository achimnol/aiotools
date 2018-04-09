import asyncio

import pytest

from aiotools.iter import aiter


def test_iter():
    # extra test for cross-check with builtin iter()

    def stream():
        for i in range(10):
            yield i

    last_x = -1
    for x in iter(stream()):
        last_x = x

    assert last_x == 9


def test_iter_sentinel():
    # extra test for cross-check with builtin iter()

    _call_count = 0
    _sentinel = 5

    def get_next():
        nonlocal _call_count
        _call_count += 1
        return _call_count

    last_x = -1
    for x in iter(get_next, _sentinel):
        last_x = x

    assert last_x == _sentinel - 1


@pytest.mark.asyncio
async def test_aiter():

    async def stream():
        for i in range(10):
            yield i

    last_x = -1
    async for x in aiter(stream()):
        last_x = x

    assert last_x == 9


@pytest.mark.asyncio
async def test_aiter_with_sentinel():

    _call_count = 0
    _sentinel = 5

    async def get_next():
        nonlocal _call_count
        _call_count += 1
        await asyncio.sleep(0.001)
        return _call_count

    last_x = -1
    async for x in aiter(get_next, _sentinel):
        last_x = x

    assert last_x == _sentinel - 1


@pytest.mark.asyncio
async def test_aiter_with_null_sentinel():

    _call_count = 0
    _sentinel = 3

    async def get_next():
        nonlocal _call_count
        _call_count += 1
        await asyncio.sleep(0.001)
        if _call_count >= _sentinel:
            return None
        return _call_count

    last_x = -1
    async for x in aiter(get_next, None):
        last_x = x

    assert last_x == _sentinel - 1
