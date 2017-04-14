import asyncio
import functools
import inspect
import sys

__all__ = ('AsyncGenContextManager', 'async_ctx_manager', 'actxmgr')


class AsyncGenContextManager:

    def __init__(self, func, args, kwargs):
        if not inspect.isasyncgenfunction(func):
            raise RuntimeError('The context function must be an async-generator.')
        self.func = func
        self.args = args
        self.kwargs = kwargs

    async def __aenter__(self):
        self.agen = self.func(*self.args, **self.kwargs)
        return (await self.agen.__anext__())

    async def __aexit__(self, exc_type, exc_value, tb):
        if exc_type is None:
            try:
                await self.agen.__anext__()
            except StopAsyncIteration:
                return
            else:
                raise RuntimeError("async-generator didn't yield") from None
        else:
            if exc_value is None:
                exc_value = exc_type()
            try:
                await self.agen.athrow(exc_type, exc_value, tb)
                raise RuntimeError("async-generator didn't stop after athrow()")
            except StopAsyncIteration as exc:
                return exc is not exc_value
            except RuntimeError as exc:
                if exc is exc_value:
                    return False
                if exc.__cause__ is exc_value:
                    return False
                raise
            except:
                if sys.exc_info()[1] is not exc_value:
                    raise


def async_ctx_manager(func):
    @functools.wraps(func)
    def helper(*args, **kwargs):
        return AsyncGenContextManager(func, args, kwargs)
    return helper


# Shorter alias
actxmgr = async_ctx_manager
