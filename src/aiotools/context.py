'''
Provides an implementation of asynchronous context manager and its applications.

.. note::

   The async context managers in this module are transparent aliases to
   ``contextlib.asynccontextmanager`` of the standard library in Python 3.7
   and later.
'''

import abc
import contextlib
import asyncio
import functools
import inspect
from typing import Any, Callable, Iterable, Optional

__all__ = [
    'AsyncContextManager', 'async_ctx_manager', 'actxmgr', 'aclosing',
    'AsyncContextGroup', 'actxgroup',
]


if hasattr(contextlib, 'asynccontextmanager'):
    __all__ += ['AsyncExitStack']

    AbstractAsyncContextManager = contextlib.AbstractAsyncContextManager
    AsyncContextManager = contextlib._AsyncGeneratorContextManager
    AsyncExitStack = contextlib.AsyncExitStack
    async_ctx_manager = contextlib.asynccontextmanager
else:
    __all__ += ['AsyncContextDecorator', 'actxdecorator']

    class AbstractAsyncContextManager(abc.ABC):
        '''
        The base abstract interface for asynchronous context manager.
        '''

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

    actxdecorator = AsyncContextDecorator

    class AsyncContextManager(AsyncContextDecorator, AbstractAsyncContextManager):
        '''
        Converts an async-generator function into asynchronous context manager.
        '''

        def __init__(self, func: Callable[..., Any], args, kwargs):
            if not inspect.isasyncgenfunction(func):
                raise RuntimeError('Context manager function must be '
                                   'an async-generator')
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
                except StopAsyncIteration as exc_new_value:
                    return exc_new_value is not exc_value
                except RuntimeError as exc_new_value:
                    # When the context body did not catch the exception, re-raise.
                    if exc_new_value is exc_value:
                        return False
                    # When the context body's exception handler raises
                    # another chained exception, re-raise.
                    if isinstance(exc_value, (StopIteration, StopAsyncIteration)):
                        if exc_new_value.__cause__ is exc_value:
                            return False
                    # If this is a purely new exception, raise the new one.
                    raise
                except BaseException as exc:
                    if exc is not exc_value:
                        raise

    def async_ctx_manager(func):
        @functools.wraps(func)
        def helper(*args, **kwargs):
            return AsyncContextManager(func, args, kwargs)
        return helper


class aclosing:
    '''
    An analogy to :func:`contextlib.closing` for async generators.

    The motivation has been proposed by:

    * https://github.com/njsmith/async_generator
    * https://vorpus.org/blog/some-thoughts-on-asynchronous-api-design-\
in-a-post-asyncawait-world/#cleanup-in-generators-and-async-generators
    * https://www.python.org/dev/peps/pep-0533/
    '''

    def __init__(self, thing):
        self.thing = thing

    async def __aenter__(self):
        return self.thing

    async def __aexit__(self):
        await self.thing.aclose()


class AsyncContextGroup:
    '''
    Merges a group of context managers into a single context manager.
    Internally it uses :func:`asyncio.gather()` to execute them with overlapping,
    to reduce the execution time via asynchrony.

    Upon entering, you can get values produced by the entering steps from
    the passed context managers (those ``yield``-ed) using an ``as`` clause of
    the ``async with``
    statement.

    After exits, you can check if the context managers have finished
    successfully by ensuring that the return values of ``exit_states()`` method
    are ``None``.

    .. note::

       You cannot return values in context managers because they are
       generators.

    If an exception is raised before the ``yield`` statement of an async
    context manager, it is stored at the corresponding manager index in the
    as-clause variable.  Similarly, if an exception is raised after the
    ``yield`` statement of an async context manager, it is stored at the
    corresponding manager index in the ``exit_states()`` return value.

    Any exceptions in a specific context manager does not interrupt others;
    this semantic is same to ``asyncio.gather()``'s when
    ``return_exceptions=True``.  This means that, it is user's responsibility
    to check if the returned context values are exceptions or the intended ones
    inside the context body after entering.

    :param context_managers: An iterable of async context managers.
                             If this is ``None``, you may add async context
                             managers one by one using the :meth:`~.add`
                             method.

    '''

    def __init__(self,
                 context_managers: Optional[Iterable[AbstractAsyncContextManager]] = None):  # noqa
        self._cm = list(context_managers) if context_managers else []
        self._cm_yields = []
        self._cm_exits = []

    def add(self, cm):
        '''
        TODO: fill description
        '''
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
        '''
        TODO: fill description
        '''
        return self._cm_exits


# Shorter aliases
actxmgr = async_ctx_manager
actxgroup = AsyncContextGroup
