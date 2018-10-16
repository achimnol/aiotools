import asyncio

import pytest

from aiotools.func import apartial, lru_cache


async def do(a, b, *, c=1, d=2):
    '''hello world'''
    return (a, b, c, d)


@pytest.mark.asyncio
async def test_apartial_orig():
    do2 = apartial(do)
    ret = await do2(1, 2, c=3, d=4)
    assert ret == (1, 2, 3, 4)


@pytest.mark.asyncio
async def test_apartial_args():
    do2 = apartial(do, 9)
    ret = await do2(2, c=5, d=6)
    assert ret == (9, 2, 5, 6)


@pytest.mark.asyncio
async def test_apartial_kwargs():
    do2 = apartial(do, c=8)
    ret = await do2(1, 2, d=4)
    assert ret == (1, 2, 8, 4)


@pytest.mark.asyncio
async def test_apartial_args_kwargs():
    do2 = apartial(do, 9, c=8)
    ret = await do2(7, d=6)
    assert ret == (9, 7, 8, 6)


@pytest.mark.asyncio
async def test_apartial_wraps():
    do2 = apartial(do)
    assert do2.__doc__.strip() == 'hello world'
    assert do2.__doc__ == do.__doc__
    assert do.__name__ == 'do'
    assert do2.__name__ == 'do'


@pytest.mark.asyncio
async def test_lru_cache():

    calc_count = 0

    @lru_cache(maxsize=2)
    async def calc(n):
        '''testing'''
        nonlocal calc_count
        await asyncio.sleep(0)
        calc_count += 1
        return n * n

    assert calc.__name__ == 'calc'
    assert calc.__doc__ == 'testing'

    assert (await calc(1)) == 1
    assert calc_count == 1
    assert (await calc(2)) == 4
    assert calc_count == 2
    assert (await calc(1)) == 1
    assert calc_count == 2
    assert (await calc(3)) == 9
    assert calc_count == 3
    assert (await calc(1)) == 1  # evicted and re-executed
    assert calc_count == 4
    assert (await calc(1)) == 1  # cached again
    assert calc_count == 4

    with pytest.raises(NotImplementedError):
        calc.cache_info()

    calc.cache_clear()

    assert (await calc(1)) == 1
    assert calc_count == 5
    assert (await calc(3)) == 9
    assert calc_count == 6


@pytest.mark.asyncio
async def test_lru_cache_with_expiration():

    calc_count = 0

    @lru_cache(maxsize=2)
    async def calc_no_exp(n):
        nonlocal calc_count
        await asyncio.sleep(0)
        calc_count += 1
        return n * n

    assert (await calc_no_exp(3)) == 9
    assert calc_count == 1
    assert (await calc_no_exp(3)) == 9
    assert calc_count == 1
    await asyncio.sleep(0.1)
    assert (await calc_no_exp(3)) == 9
    assert calc_count == 1

    calc_count = 0

    @lru_cache(maxsize=2, expire_after=0.05)
    async def calc_exp(n):
        nonlocal calc_count
        await asyncio.sleep(0)
        calc_count += 1
        return n * n

    assert (await calc_exp(3)) == 9
    assert calc_count == 1
    assert (await calc_exp(3)) == 9
    assert calc_count == 1
    await asyncio.sleep(0.1)
    assert (await calc_exp(3)) == 9
    assert calc_count == 2
