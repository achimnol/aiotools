import asyncio
from typing import (
    Any,
    AsyncIterator,
    Sequence,
)

import aiotools


async def echo(reader, writer):
    data = await reader.read(100)
    writer.write(data)
    await writer.drain()
    writer.close()


@aiotools.server_context
async def worker_main(
    loop: asyncio.AbstractEventLoop,
    pidx: int,
    args: Sequence[Any],
) -> AsyncIterator[None]:
    # Create a listening socket with SO_REUSEPORT option so that each worker
    # process can share the same listening port and the kernel balances
    # incoming connections across multiple worker processes.
    server = await asyncio.start_server(
        echo, "0.0.0.0", 8888, reuse_port=True, loop=loop
    )
    print(f"[{pidx}] started")

    yield  # wait until terminated

    server.close()
    await server.wait_closed()
    print(f"[{pidx}] terminated")


if __name__ == "__main__":
    # Run the above server using 4 worker processes.
    aiotools.start_server(worker_main, num_workers=4)
