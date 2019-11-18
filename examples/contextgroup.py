from pprint import pprint
import asyncio
import aiotools


@aiotools.actxmgr
async def mygen(id):
    yield f'mygen id is {id}'


async def run():
    ctxgrp = aiotools.actxgroup(mygen(i) for i in range(10))
    async with ctxgrp as values:
        pprint(values)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run())
    finally:
        loop.stop()
