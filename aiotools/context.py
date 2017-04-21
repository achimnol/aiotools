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
            # The generator should yield at least once.
            raise RuntimeError("async-generator didn't yield") from None

    async def __aexit__(self, exc_type, exc_value, tb):
        if exc_type is None:
            # This is the normal path when the context body
            # did not raise any exception.
            try:
                await self._agen.__anext__()
            except StopAsyncIteration:
                return
            else:
                # The generator has already yielded,
                # no more yields are allowed.
                raise RuntimeError("async-generator didn't stop") from None
        else:
            # The context body has raised an exception.
            if exc_value is None:
                # Ensure exc_value is a valid Exception.
                exc_value = exc_type()
            try:
                # Throw the catched exception into the generator,
                # so that it can handle as it wants.
                await self._agen.athrow(exc_type, exc_value, tb)
                # Here the generator should have finished!
                # (i.e., it should not yield again in except/finally blocks!)
                raise RuntimeError("async-generator didn't stop after athrow()")
                # NOTE for PEP-479
                #   StopAsyncIteration raised inside the context body
                #   is converted to RuntimeError.
                #   In the standard library's contextlib.py, there is
                #   an extra except clause to catch StopIteration here,
                #   but this is unnecessary now.
            except RuntimeError as exc_new_value:
                # When the context body did not catch the exception, re-raise.
                if exc_new_value is exc_value:
                    return False
                # When the context body's exception handler raises
                # another chained exception, re-raise.
                if exc_new_value.__cause__ is exc_value:
                    return False
                # If this is a purely new exception, raise the new one.
                raise
            except:
                # If the finalization part of the generator throws a new
                # exception, re-raise it.
                if sys.exc_info()[1] is exc_value:
                    # The context body did not catch the exception.
                    return False  # re-raise original
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
