import pytest

from aiotools.func import apartial


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
