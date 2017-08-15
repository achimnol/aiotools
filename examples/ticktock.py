import aiotools
import asyncio

async def mytick(interval):
    print('tick')

async def run():
    t = aiotools.create_timer(mytick, 1.0)
    await asyncio.sleep(4)
    t.cancel()
    await t

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run())
    except:
        loop.close()
