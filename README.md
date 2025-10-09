aiotools
========

[![PyPI release version](https://badge.fury.io/py/aiotools.svg)](https://pypi.org/project/aiotools/)
![Supported Python versions](https://img.shields.io/pypi/pyversions/aiotools.svg)
[![CI Status](https://github.com/achimnol/aiotools/actions/workflows/ci.yml/badge.svg)](https://github.com/achimnol/aiotools/actions/workflows/ci.yml)
[![Code Coverage](https://codecov.io/gh/achimnol/aiotools/branch/master/graph/badge.svg)](https://codecov.io/gh/achimnol/aiotools)

Idiomatic asyncio utilities


Modules
-------

* [Safe Cancellation](http://aiotools.readthedocs.io/en/latest/aiotools.cancel.html)
* [Async Context Manager](http://aiotools.readthedocs.io/en/latest/aiotools.context.html)
* [Async Defer](http://aiotools.readthedocs.io/en/latest/aiotools.defer.html)
* [Async Fork](http://aiotools.readthedocs.io/en/latest/aiotools.fork.html)
* [Async Functools](http://aiotools.readthedocs.io/en/latest/aiotools.func.html)
* [Async Itertools](http://aiotools.readthedocs.io/en/latest/aiotools.iter.html)
* [Async Server](http://aiotools.readthedocs.io/en/latest/aiotools.server.html)
* [Async Timer](http://aiotools.readthedocs.io/en/latest/aiotools.timer.html)
* [TaskContext](http://aiotools.readthedocs.io/en/latest/aiotools.taskcontext.html)
* [TaskScope](http://aiotools.readthedocs.io/en/latest/aiotools.taskscope.html)
* (alias of TaskScope) [Supervisor](http://aiotools.readthedocs.io/en/latest/aiotools.supervisor.html)
* (deprecated since 2.0) [(Persistent)TaskGroup](http://aiotools.readthedocs.io/en/latest/aiotools.taskgroup.html)
* [High-level Coroutine Utilities](http://aiotools.readthedocs.io/en/latest/aiotools.utils.html)

Full API documentation: https://aiotools.readthedocs.io


See also
--------

* [anyio](https://github.com/agronholm/anyio): High level asynchronous concurrency and networking framework that works on top of either Trio or asyncio
* [trio](https://github.com/python-trio/trio): An alternative implementation of asyncio focusing on structured concurrency
* [aiometer](https://github.com/florimondmanca/aiometer): A Python concurrency scheduling library, compatible with asyncio and trio.
* [aiojobs](https://github.com/aio-libs/aiojobs): A concurrency-limiting, task-shielding scheduler for asyncio tasks for graceful shutdown

Currently aiotools targets the vanilly asyncio ecosystem only
(with some tight coupling with asyncio internals),
but you may find similar problem definitions and alternative solutions in the above libraries.


Examples
--------

### Safe Cancellation

Consider the following commonly used pattern:
```python
task = asyncio.create_task(...)
task.cancel()
await task  # would it raise CancelledError or not?
```

It has been the reponsibility of the author of tasks and the caller of them to
coordinate whether to re-raise injected cancellation error.

Now we can use the structured cancellation introduced in Python 3.11:
```python
task = asyncio.create_task(...)
await aiotools.cancel_and_wait(task)
```
which will re-raise the cancellation when there is an external cancellation
request and absorb it otherwise.

Relying on this API whenever you need to cancel asyncio tasks will make your
codebase more consistent because you no longer need to decide whether to
(re-)raise or suppress `CancelledError` in your task codes.


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

@aiotools.server
async def myworker(loop, pidx, args):
    server = await asyncio.start_server(echo, '0.0.0.0', 8888, reuse_port=True)
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
Internally it uses `aiotools.fork` module to get kernel support to resolve
potential signal/PID related races via PID file descriptors on supported versions
(Python 3.9+ and Linux kernel 5.4+).


### Async TaskScope

TaskScope is a variant of TaskGroup which ensures all child tasks run to completion
(either having results or exceptions) unless the context manager is cancelled.

This could be considered as a safer version (in terms of lifecycle tracking) of
`asyncio.gather(*, return_exceptions=True)` and a more convenient version of it
because you can decouple the timing of task creation and their scheduling
within the TaskScope context.

```python
import aiotools

async def do():
    async with aiotools.TaskScope() as ts:
        ts.create_task(...)
        ts.create_task(...)
        # each task will run to completion regardless sibling failures.
        ...
    # at this point, all subtasks are either cancelled or done.
```

You may customize exception handler for each scope to receive and process
unhandled exceptions in child tasks.  For use in long-running server contexts,
TaskScope does not store any exceptions or results by itself.

See also high-level coroutine utilities such as `as_completed_safe()`,
`gather_safe()`, and `race()` functions in the utils module.

TaskScope itself and these utilities integrate with
[the call-graph inspection](https://docs.python.org/3/library/asyncio-graph.html)
introduced in Python 3.14.


### Async Timer

```python
import aiotools

i = 0

async def mytick(interval):
    print(i)
    i += 1

async def somewhere():
    task = aiotools.create_timer(mytick, 1.0)
    ...
    await aiotools.cancel_and_wait(task)
```

The returned `task` is an `asyncio.Task` object.
To stop the timer, it should be cancelled explicitly.
Use `cancel_and_wait()` to ensure complete shutdown of any ongoing tick tasks.
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
    await aiotools.cancel_and_wait(t)
```

#### Virtual Clock

It provides a virtual clock that advances the event loop time instantly upon
any combination of `asyncio.sleep()` calls in multiple coroutine tasks,
by temporarily patching the event loop selector.

This is also used in [our timer test suite](https://github.com/achimnol/aiotools/blob/master/tests/test_timer.py).

```python
import aiotools
import pytest

@pytest.mark.asyncio
async def test_sleeps():
    loop = aiotools.compat.get_running_loop()
    vclock = aiotools.VirtualClock()
    with vclock.patch_loop():
        print(loop.time())  # -> prints 0
        await asyncio.sleep(3600)
        print(loop.time())  # -> prints 3600
```
