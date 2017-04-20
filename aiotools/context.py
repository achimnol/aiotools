import abc
import asyncio
import functools
import inspect
from typing import Any, Callable, Iterable, Optional
import sys

__all__ = (
    'AsyncContextManager', 'async_ctx_manager', 'actxmgr',
    'AsyncContextDecorator', 'actxdecorator',
    'AsyncContextGroup', 'actxgroup',
)


class AbstractAsyncContextManager(abc.ABC):

    async def __aenter__(self):
        return self  # pragma: no cover

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_value, tb):
        return None  # pragma: no cover

    @classmethod
    def __subclasshook__(cls, C):
        if cls is AbstractAsyncContextManager:
            if (any('__aenter__' in B.__dict__ for B in C.__mro__) and
                any('__aexit__' in B.__dict__ for B in C.__mro__)):
                return True
        return NotImplemented


class AsyncContextDecorator:

    def _recreate_cm(self):
        return self

    def __call__(self, func):
        @functools.wraps(func)
        async def inner(*args, **kwargs):
            async with self._recreate_cm():
                return (await func(*args, **kwargs))
        return inner


class AsyncContextManager(AsyncContextDecorator, AbstractAsyncContextManager):

    def __init__(self, func: Callable[..., Any], args, kwargs):
        if not inspect.isasyncgenfunction(func):
            raise RuntimeError('Context manager function must be async-generator')
        self._agen = func(*args, **kwargs)
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def _recreate_cm(self):
        return self.__class__(self.func, self.args, self.kwargs)

    async def __aenter__(self):
        try:
            return (await self._agen.__anext__())
        except StopAsyncIteration:
            raise RuntimeError("async-generator didn't yield") from None

    async def __aexit__(self, exc_type, exc_value, tb):
        if exc_type is None:
            try:
                await self._agen.__anext__()
            except StopAsyncIteration:
                return
            else:
                raise RuntimeError("async-generator didn't stop") from None
        else:
            if exc_value is None:
                exc_value = exc_type()
            try:
                await self._agen.athrow(exc_type, exc_value, tb)
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
        return AsyncContextManager(func, args, kwargs)
    return helper


class AsyncContextGroup:

    def __init__(self,
                 context_managers: Optional[Iterable[AsyncContextManager]]=None):
        self._cm = list(context_managers) if context_managers else []
        self._cm_yields = []

    def add(self, cm):
        self._cm.append(cm)

    async def __aenter__(self):
        self._cm_yields[:] = await asyncio.gather(*(e.__aenter__() for e in self._cm))
        return self._cm_yields

    def contexts(self):
        return self._cm_yields

    async def __aexit__(self, *exc_info):
        # TODO: improve exception handling
        #  - should all ctxmgrs receive the same exception info?
        await asyncio.gather(*(e.__aexit__(*exc_info) for e in self._cm))


# Shorter aliases
actxmgr = async_ctx_manager
actxdecorator = AsyncContextDecorator
actxgroup = AsyncContextGroup
