import asyncio
from collections.abc import AsyncIterator

import aiotools

lock = asyncio.Lock()


@aiotools.actxmgr
async def mygen(input_value: str) -> AsyncIterator[str]:
    print(input_value)
    await lock.acquire()
    print("The lock is acquired.")
    try:
        yield "return_value"
    finally:
        lock.release()
        print("The lock is released.")


async def main() -> None:
    try:
        async with mygen("input_value") as return_value:
            print(return_value)
            raise RuntimeError
    except RuntimeError:
        print("RuntimeError is caught!")  # you can catch exceptions here.


if __name__ == "__main__":
    asyncio.run(main())
