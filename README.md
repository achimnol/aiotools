# aiotools

[![Build Status](https://travis-ci.org/achimnol/aiotools.svg?branch=master)](https://travis-ci.org/achimnol/aiotools)
[![PyPI version](https://badge.fury.io/py/aiotools.svg)](https://badge.fury.io/py/aiotools)

Idiomatic asyncio utilties


## Asynchronous context manager

This is an asynchronous version of
[`contextlib.contextmanager`](https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager)
to make it easier to write asynchronous context managers without creating boilerplate classes.

```python
import asyncio
import aiotools

@aiotools.actxmgr
async def mygen(a):
    await asyncio.sleep(1)
    yield a + 1
    await asyncio.sleep(1)

async with mygen(1) as b:
    assert b == 2
```

Note that you need to wrap `yield` with a try-finally block to ensure resource releases (e.g., locks).

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

try:
    async with mygen(1) as b:
	raise RuntimeError('oops')
except RuntimeError:
    print('caught!')  # you can catch exceptions here.
```


## Timer

```python
import aiotools

async def mytick(interval):
    print(i)

t = aiotools.create_timer(mytick, 1.0)
```

`t` is an [`asyncio.Task`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task) object.
To stop the timer, call `t.cancel()`.

You may add `TimerDelayPolicy` argument to control the behavior when the
timer-fired task takes longer than the timer interval.
`DEFAULT` is to accumulate them and cancel all the remainings at once when the timer is cancelled.
`CANCEL` is to cancel any pending previously fired tasks on every interval.

```python
import asyncio
import aiotools

async def mytick(interval):
    await asyncio.sleep(100)  # cancelled on every next interval.

t = aiotools.create_timer(mytick, 1.0, aiotools.TimerDelayPolicy.CANCEL)
```
