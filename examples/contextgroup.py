import asyncio
from collections.abc import AsyncIterator
from pprint import pprint

import aiotools


@aiotools.actxmgr
async def mygen(id: int) -> AsyncIterator[str]:
    yield f"mygen id is {id}"


async def main() -> None:
    ctxgrp = aiotools.actxgroup(mygen(i) for i in range(10))
    async with ctxgrp as values:
        pprint(values)


if __name__ == "__main__":
    asyncio.run(main())
