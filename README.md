aiotools
========

[![PyPI release version](https://badge.fury.io/py/aiotools.svg)](https://pypi.org/project/aiotools/)
![Supported Python versions](https://img.shields.io/pypi/pyversions/aiotools.svg)
[![Build Status](https://travis-ci.org/achimnol/aiotools.svg?branch=master)](https://travis-ci.org/achimnol/aiotools)
[![Code Coverage](https://codecov.io/gh/achimnol/aiotools/branch/master/graph/badge.svg)](https://codecov.io/gh/achimnol/aiotools)

Idiomatic asyncio utilties

*NOTE:* This project is under early stage of developement. The public APIs may break version by version.


Modules
-------

* [Async Context Manager](http://aiotools.readthedocs.io/en/latest/aiotools.context.html)
* [Async Functools](http://aiotools.readthedocs.io/en/latest/aiotools.func.html)
* [Async Itertools](http://aiotools.readthedocs.io/en/latest/aiotools.iter.html)
* [Async Server](http://aiotools.readthedocs.io/en/latest/aiotools.server.html)
* [Async Timer](http://aiotools.readthedocs.io/en/latest/aiotools.timer.html)


Examples
--------

### Async Context Manager

This is an asynchronous version of `contextlib.contextmanager` to make it
easier to write asynchronous context managers without creating boilerplate
classes.

```python
import asyncio
import aiotools

@aiotools.actxmgr
async def mygen(a):
   await asyncio.sleep(1)
   yield a + 1
   await asyncio.sleep(1)

async def somewhere():
   async with mygen(1) as b:
       assert b == 2
```

Note that you need to wrap `yield` with a try-finally block to
ensure resource releases (e.g., locks), even in the case when
an exception is ocurred inside the async-with block.

```python
import asyncio
import aiotools

lock = asyncio.Lock()

@aiotools.actxmgr
async def mygen(a):
   await lock.acquire()
   try:
       yield a + 1
   finally:
       lock.release()

async def somewhere():
   try:
       async with mygen(1) as b:
           raise RuntimeError('oops')
   except RuntimeError:
       print('caught!')  # you can catch exceptions here.
```

You can also create a group of async context managers, which
are entered/exited all at once using `asyncio.gather()`.

```python
import asyncio
import aiotools

@aiotools.actxmgr
async def mygen(a):
   yield a + 10

async def somewhere():
   ctxgrp = aiotools.actxgroup(mygen(i) for i in range(10))
   async with ctxgrp as values:
       assert len(values) == 10
       for i in range(10):
           assert values[i] == i + 10
```

### Async Server

This implements a common pattern to launch asyncio-based server daemons.

```python
import asyncio
import aiotools

async def echo(reader, writer):
   data = await reader.read(100)
   writer.write(data)
   await writer.drain()
   writer.close()

@aiotools.actxmgr
async def myworker(loop, pidx, args):
   server = await asyncio.start_server(echo, '0.0.0.0', 8888,
       reuse_port=True, loop=loop)
   print(f'[{pidx}] started')
   yield  # wait until terminated
   server.close()
   await server.wait_closed()
   print(f'[{pidx}] terminated')

if __name__ == '__main__':
   # Run the above server using 4 worker processes.
   aiotools.start_server(myworker, num_workers=4)
```

It handles SIGINT/SIGTERM signals automatically to stop the server,
as well as lifecycle management of event loops running on multiple processes.


### Async Timer

```python
import aiotools

i = 0

async def mytick(interval):
   print(i)
   i += 1

async def somewhere():
   t = aiotools.create_timer(mytick, 1.0)
   ...
   t.cancel()
   await t
```

`t` is an `asyncio.Task` object.
To stop the timer, call `t.cancel(); await t`.
Please don't forget `await`-ing `t` because it requires extra steps to
cancel and await all pending tasks.
To make your timer function to be cancellable, add a try-except clause
catching `asyncio.CancelledError` since we use it as a termination
signal.

You may add `TimerDelayPolicy` argument to control the behavior when the
timer-fired task takes longer than the timer interval.
`DEFAULT` is to accumulate them and cancel all the remainings at once when
the timer is cancelled.
`CANCEL` is to cancel any pending previously fired tasks on every interval.

```python
import asyncio
import aiotools

async def mytick(interval):
   await asyncio.sleep(100)  # cancelled on every next interval.

async def somewhere():
   t = aiotools.create_timer(mytick, 1.0, aiotools.TimerDelayPolicy.CANCEL)
   ...
   t.cancel()
   await t
```
