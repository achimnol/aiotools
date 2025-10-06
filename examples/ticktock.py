import asyncio

import aiotools


async def mytick(interval: float) -> None:
    print("tick")


async def main() -> None:
    timer_task = aiotools.create_timer(mytick, 1.0)
    await asyncio.sleep(4)
    await aiotools.cancel_and_wait(timer_task)


if __name__ == "__main__":
    asyncio.run(main())
