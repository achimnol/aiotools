"""
Provides an implementation of asynchronous context manager and its applications.

.. note::

   The async context managers in this module are transparent aliases to
   ``contextlib.asynccontextmanager`` of the standard library in Python 3.7
   and later.
"""

import asyncio
import contextlib
from collections.abc import Iterable
from contextvars import ContextVar
from typing import Any, Generic, TypeVar

from .types import AsyncClosable, OptExcInfo

__all__ = [
    "resetting",
    "AsyncContextManager",
    "async_ctx_manager",
    "actxmgr",
    "aclosing",
    "closing_async",
    "AsyncContextGroup",
    "actxgroup",
    "AsyncExitStack",
]


T = TypeVar("T")
T_AsyncClosable = TypeVar("T_AsyncClosable", bound=AsyncClosable)

AbstractAsyncContextManager = contextlib.AbstractAsyncContextManager
AsyncContextManager = contextlib._AsyncGeneratorContextManager
AsyncExitStack = contextlib.AsyncExitStack
async_ctx_manager = contextlib.asynccontextmanager
aclosing = contextlib.aclosing


class resetting(Generic[T]):
    """
    An extra context manager to auto-reset the given context variable.
    It supports both the standard contextmanager protocol and the
    async-contextmanager protocol.

    .. versionadded:: 1.8.0
    """

    def __init__(self, ctxvar: ContextVar[T], value: T) -> None:
        self._ctxvar = ctxvar
        self._value = value

    def __enter__(self) -> None:
        self._token = self._ctxvar.set(self._value)

    async def __aenter__(self) -> None:
        self._token = self._ctxvar.set(self._value)

    def __exit__(self, *exc_info: OptExcInfo) -> bool | None:
        self._ctxvar.reset(self._token)
        return None

    async def __aexit__(self, *exc_info: OptExcInfo) -> bool | None:
        self._ctxvar.reset(self._token)
        return None


class closing_async(Generic[T_AsyncClosable]):
    """
    An analogy to :func:`contextlib.closing` for objects defining the ``close()``
    method as an async function.

    .. versionadded:: 1.5.6
    """

    def __init__(self, thing: T_AsyncClosable) -> None:
        self.thing = thing

    async def __aenter__(self) -> T_AsyncClosable:
        return self.thing

    async def __aexit__(self, *exc_info: OptExcInfo) -> bool | None:
        await self.thing.close()
        return None


class AsyncContextGroup:
    """
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

    """

    def __init__(
        self, context_managers: Iterable[AbstractAsyncContextManager[Any]] | None = None
    ) -> None:
        self._cm: list[AbstractAsyncContextManager[Any]] = (
            list(context_managers) if context_managers else []
        )
        self._cm_yields: list[Any | BaseException] = []
        self._cm_exits: list[bool | None | BaseException] = []

    def add(self, cm: AbstractAsyncContextManager[Any]) -> None:
        self._cm.append(cm)

    async def __aenter__(self) -> list[Any | BaseException]:
        # Exceptions in context managers are stored into _cm_yields list.
        # NOTE: There is no way to "skip" the context body even if the entering
        #       process fails.
        self._cm_yields[:] = await asyncio.gather(
            *(e.__aenter__() for e in self._cm), return_exceptions=True
        )
        return self._cm_yields

    async def __aexit__(self, *exc_info: OptExcInfo) -> None:
        # Clear references to context variables.
        self._cm_yields.clear()
        # Exceptions are stored into _cm_exits list.
        # The type-ignore comment is required to embrace the difference between T_T:
        # types.OptExcInfo                                      = tuple[type[BaseException], BaseException, TracebackType] | tuple[None, None, None]
        # args@contextlib.AbstractAsyncContextManager.__aexit__ = tuple[type[BaseException] | None, BaseException | None, TracebackType | None]
        self._cm_exits[:] = await asyncio.gather(
            *(e.__aexit__(*exc_info) for e in self._cm),  # type: ignore[arg-type]
            return_exceptions=True,
        )

    def exit_states(self) -> list[bool | None | BaseException]:
        return self._cm_exits


# Shorter aliases
actxmgr = async_ctx_manager
actxgroup = AsyncContextGroup
