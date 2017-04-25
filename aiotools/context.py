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
    '''
    Make an asynchronous context manager be used as a decorator function.
    '''

    def _recreate_cm(self):
        return self

    def __call__(self, func):
        @functools.wraps(func)
        async def inner(*args, **kwargs):
            async with self._recreate_cm():
                return (await func(*args, **kwargs))
        return inner


class AsyncContextManager(AsyncContextDecorator, AbstractAsyncContextManager):
    '''
    Converts an async-generator function into asynchronous context manager.
    '''

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
    '''
    Merges a group of context managers into a single context manager.
    It uses `asyncio.gather()` to execute them with overlapping, to reduce the execution time
    potentially.

    It always sets `return_exceptions=True` for `asyncio.gather()`, so that
    exceptions from small subset of context managers would not prevent execution of others.
    This means that, it is user's responsibility to check if the returned context values
    are exceptions or the intended ones inside the context body after entering.
    After exits, it stores the same `asyncio.gather` results from `__aexit__()`
    methods of the context managers and you may access them via `exit_states()`
    method.  Any exception inside the context body is thrown into `__aexit__()`
    handlers of the context managers, but their failures are independent to
    each other, and you can catch it outside the with block.

    To prevent memory leak, the context variables captured during `__aenter__()` are cleared
    when starting `__aexit__()`.
    '''

    def __init__(self,
                 context_managers: Optional[Iterable[AbstractAsyncContextManager]]=None):
        self._cm = list(context_managers) if context_managers else []
        self._cm_yields = []
        self._cm_exits = []

    def add(self, cm):
        self._cm.append(cm)

    async def __aenter__(self):
        # Exceptions in context managers are stored into _cm_yields list.
        # NOTE: There is no way to "skip" the context body even if the entering
        #       process fails.
        self._cm_yields[:] = await asyncio.gather(
            *(e.__aenter__() for e in self._cm),
            return_exceptions=True)
        return self._cm_yields

    async def __aexit__(self, *exc_info):
        # Clear references to context variables.
        self._cm_yields.clear()
        # Exceptions are stored into _cm_exits list.
        self._cm_exits[:] = await asyncio.gather(
            *(e.__aexit__(*exc_info) for e in self._cm),
            return_exceptions=True)

    def exit_states(self):
        return self._cm_exits


# Shorter aliases
actxmgr = async_ctx_manager
actxdecorator = AsyncContextDecorator
actxgroup = AsyncContextGroup
