import asyncio
import contextlib
import logging

import aiotools
import aiozmq
import zmq


def get_logger(name, pid):
    log = logging.getLogger(name)
    sh = logging.StreamHandler()
    fmt = logging.Formatter(
        f'%(relativeCreated).3f %(name)s[{pid}] %(levelname)s: %(message)s')
    sh.setFormatter(fmt)
    log.addHandler(sh)
    log.propagate = False
    log.setLevel(logging.INFO)
    return log


def router_main(_, pidx, args):
    log = get_logger('examples.zmqserver.extra', pidx)
    ctx = zmq.Context()
    ctx.linger = 0
    in_sock = ctx.socket(zmq.PULL)
    in_sock.bind('tcp://*:5000')
    out_sock = ctx.socket(zmq.PUSH)
    out_sock.bind('ipc://example-events')
    try:
        log.info('router proxy started')
        zmq.proxy(in_sock, out_sock)
    except KeyboardInterrupt:
        pass
    except:
        log.exception('unexpected error')
    finally:
        log.info('router proxy terminated')
        in_sock.close()
        out_sock.close()
        ctx.term()


@aiotools.actxmgr
async def worker_main(loop, pidx, args):
    log = get_logger('examples.zmqserver.worker', pidx)
    router = await aiozmq.create_zmq_stream(
        zmq.PULL,
        connect='ipc://example-events')

    async def process_incoming(router):
        while True:
            try:
                data = await router.read()
            except aiozmq.ZmqStreamClosed:
                break
            log.info(data)

    task = loop.create_task(process_incoming(router))
    log.info('started')

    yield

    router.close()
    await task
    log.info('terminated')


if __name__ == '__main__':
    server = aiotools.start_server(
        worker_main,
        num_workers=4,
        extra_procs=[router_main],
    )
