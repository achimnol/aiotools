"""
Provides a Golang-like ``defer()`` API using decorators, which allows grouping
resource initialization and cleanup in one place without extra indentations.

Example:

.. code-block:: python3

   async def init(x):
       ...

   async def cleanup(x):
       ...

   @aiotools.adefer
   async def do(defer):  # <-- be aware of defer argument!
       x = SomeResource()
       await init(x)
       defer(cleanup(x))
       ...
       ...

This is equivalent to:

.. code-block:: python3

   async def do():
       x = SomeResource()
       await init(x)
       try:
           ...
           ...
       finally:
           await cleanup(x)

Note that :class:`aiotools.context.AsyncContextGroup` or
:class:`contextlib.AsyncExitStack` serves well for the same purpose, but for simple
cleanups, this defer API makes your codes simple because it steps aside the main
execution context without extra indentations.

.. warning::

   Any exception in the deferred functions is raised transparently, and may block
   execution of the remaining deferred functions.
   This behavior may be changed in the future versions, though.
"""

from collections import deque
import functools
import inspect
from typing import (
    Union,
    Awaitable,
    Callable,
)

__all__ = (
    'defer', 'adefer',
)


def defer(func):
    """
    A synchronous version of the defer API.
    It can only defer normal functions.
    """
    assert not inspect.iscoroutinefunction(func), \
           'the decorated function must not be async'

    @functools.wraps(func)
    def _wrapped(*args, **kwargs):
        deferreds = deque()

        def defer(f: Callable) -> None:
            assert not inspect.iscoroutinefunction(f), \
                   'the deferred function must not be async'
            assert not inspect.iscoroutine(f), \
                   'the deferred object must not be a coroutine'
            deferreds.append(f)

        try:
            return func(defer, *args, **kwargs)
        finally:
            while deferreds:
                f = deferreds.pop()
                f()

    return _wrapped


def adefer(func):
    """
    An asynchronous version of the defer API.
    It can defer coroutine functions, coroutines, and normal functions.
    """
    assert inspect.iscoroutinefunction(func), \
           'the decorated function must be async'

    @functools.wraps(func)
    async def _wrapped(*args, **kwargs):
        deferreds = deque()

        def defer(f: Union[Callable, Awaitable]) -> None:
            deferreds.append(f)

        try:
            return await func(defer, *args, **kwargs)
        finally:
            while deferreds:
                f = deferreds.pop()
                if inspect.iscoroutinefunction(f):
                    await f()
                elif inspect.iscoroutine(f):
                    await f
                else:
                    f()

    return _wrapped
