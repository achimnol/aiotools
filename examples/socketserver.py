import asyncio
import signal
from collections.abc import AsyncGenerator, Sequence
from typing import Any

import aiotools


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    data = await reader.read(100)
    writer.write(data)
    await writer.drain()
    writer.close()


@aiotools.server_context
async def worker_main(
    loop: asyncio.AbstractEventLoop,
    pidx: int,
    args: Sequence[Any],
) -> AsyncGenerator[None, signal.Signals]:
    # Create a listening socket with SO_REUSEPORT option so that each worker
    # process can share the same listening port and the kernel balances
    # incoming connections across multiple worker processes.
    server = await asyncio.start_server(
        echo, "0.0.0.0", 8888, reuse_port=True, loop=loop
    )
    print(f"[{pidx}] started")

    sig = None
    try:
        sig = yield  # wait until terminated
    finally:
        server.close()
        await server.wait_closed()
        print(f"[{pidx}] terminated with {sig=}")


if __name__ == "__main__":
    # Run the above server using 4 worker processes.
    aiotools.start_server(worker_main, num_workers=4)
