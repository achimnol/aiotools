import asyncio
import logging
import os
from typing import (
    Any,
    AsyncIterator,
    Sequence,
    Set,
)

import zmq
import zmq.asyncio

import aiotools

num_workers = 4

log_init_states: Set[str] = set()


def get_logger(name: str, pid: int) -> logging.Logger:
    log = logging.getLogger(name)
    # initialize only once for each logger identified by the name
    if name not in log_init_states:
        sh = logging.StreamHandler()
        fmt = logging.Formatter(
            f"%(relativeCreated).3f %(name)s[{pid}] %(levelname)s: %(message)s"
        )
        sh.setFormatter(fmt)
        log.addHandler(sh)
        log.propagate = False
        log.setLevel(logging.INFO)
        log_init_states.add(name)
    return log


def router_main(
    loop: asyncio.AbstractEventLoop,
    pidx: int,
    args: Sequence[Any],
) -> None:
    log = get_logger("examples.zmqserver.extra", pidx)
    zctx = zmq.Context()
    zctx.linger = 0
    in_sock = zctx.socket(zmq.PULL)
    in_sock.bind("tcp://*:5033")
    out_sock = zctx.socket(zmq.PUSH)
    out_sock.bind("ipc://example-events")
    try:
        log.info("router proxy started")
        zmq.proxy(in_sock, out_sock)
    except KeyboardInterrupt:
        pass
    except Exception:
        log.exception("unexpected error")
    finally:
        for _ in range(num_workers):
            out_sock.send(b"")  # sentinel
        log.info("router proxy terminated")
        in_sock.close()
        out_sock.close()
        zctx.term()
        os.unlink("example-events")


@aiotools.server_context
async def worker_main(
    loop: asyncio.AbstractEventLoop,
    pidx: int,
    args: Sequence[Any],
) -> AsyncIterator[None]:
    log = get_logger("examples.zmqserver.worker", pidx)
    zctx = zmq.asyncio.Context()
    router = zctx.socket(zmq.PULL)
    router.connect("ipc://example-events")

    async def process_incoming(router):
        while True:
            data = await router.recv()
            if not data:
                return
            log.info(data)

    task = loop.create_task(process_incoming(router))
    log.info("started")

    try:
        yield
    finally:
        await task
        router.close()
        zctx.term()
        log.info("terminated")


if __name__ == "__main__":
    # This example must be run with multiprocessing.
    aiotools.start_server(
        worker_main,
        num_workers=num_workers,
        extra_procs=[router_main],
    )
