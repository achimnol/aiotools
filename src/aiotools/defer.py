import functools
import inspect
from typing import (
    Union,
    Awaitable,
    Callable,
)


def defer(func):
    assert not inspect.iscoroutinefunction(func), \
           'the decorated function must not be async'

    @functools.wraps(func)
    def _wrapped(*args, **kwargs):
        deferreds = []

        def defer(f: Callable) -> None:
            assert not inspect.iscoroutinefunction(f), \
                   'the deferred function must not be async'
            assert not inspect.iscoroutine(f), \
                   'the deferred object must not be a coroutine'
            deferreds.append(f)

        try:
            return func(defer, *args, **kwargs)
        finally:
            for f in reversed(deferreds):
                f()

    return _wrapped


def adefer(func):
    assert inspect.iscoroutinefunction(func), \
           'the decorated function must be async'

    @functools.wraps(func)
    async def _wrapped(*args, **kwargs):
        deferreds = []

        def defer(f: Union[Callable, Awaitable]) -> None:
            deferreds.append(f)

        try:
            return await func(defer, *args, **kwargs)
        finally:
            for f in reversed(deferreds):
                if inspect.iscoroutinefunction(f):
                    await f()
                elif inspect.iscoroutine(f):
                    await f
                else:
                    f()

    return _wrapped
