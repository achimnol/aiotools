from __future__ import annotations

from collections.abc import Callable, Coroutine
from functools import partial, partialmethod
from typing import Any, TypeAlias, TypeVar, overload

from async_lru import (
    _LRUCacheWrapper,  # type: ignore  # (intentionally importing private type)
    alru_cache,
)

__all__ = (
    "apartial",
    "lru_cache",
)

_T = TypeVar("_T")
_R = TypeVar("_R")
_Coro: TypeAlias = Coroutine[Any, Any, _R]
_CB: TypeAlias = Callable[..., _Coro[_R]]
_CBP: TypeAlias = _CB[_R] | partial[_Coro[_R]] | partialmethod[_Coro[_R]]


apartial = partial  # alias


# Decoration with arguments
@overload
def lru_cache(
    maxsize: int | None = 128,
    typed: bool = False,
    *,
    expire_after: float | None = None,
) -> Callable[[_CBP[_R]], _LRUCacheWrapper[_R]]: ...


# Decoration without arguments
@overload
def lru_cache(
    maxsize: _CBP[_R],
    /,
) -> _LRUCacheWrapper[_R]: ...


def lru_cache(
    maxsize: int | None | _CBP[_R] = 128,
    typed: bool = False,
    *,
    expire_after: float | None = None,
) -> Callable[[_CBP[_R]], _LRUCacheWrapper[_R]] | _LRUCacheWrapper[_R]:
    """
    A simple LRU cache just like :func:`functools.lru_cache`, but it works for
    coroutines.  This is not as heavily optimized as :func:`functools.lru_cache`
    which uses an internal C implementation, as it targets async operations
    that take a long time.

    It follows the same API that the standard functools provides.

    Note that calling the coroutine multiple times with the same arguments
    before the first call returns may incur duplicate executions.

    This function is not thread-safe.

    Args:
       maxsize: The maximum number of cached entries.

       typed: Cache keys in different types separately (e.g., ``3`` and ``3.0`` will
              be different keys).

       expire_after: Re-calculate the value if the configured time has passed even
                     when the cache is hit.  When re-calculation happens the
                     expiration timer is also reset.

    .. versionchanged:: 2.2.2

       This now reuses a more thorough implementation from `the aio-libs/async_lru
       package <https://github.com/aio-libs/async-lru>`_ with a minor difference
       in the argument naming for backward compatibility with existing aiotools
       users.

    .. versionchanged:: 2.2.2

       Thanks to the above change, it can now decorate instance methods
       while preserving the full typing information.
    """
    return alru_cache(
        maxsize=maxsize,
        typed=typed,
        ttl=expire_after,
    )
