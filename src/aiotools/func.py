import collections
import functools
from collections.abc import Awaitable, Callable
from typing import Any, Generic, NamedTuple, ParamSpec, TypeVar, cast

from .compat import get_running_loop

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

__all__ = (
    "apartial",
    "lru_cache",
)


class _CacheEntry(NamedTuple, Generic[T]):
    value: T
    expire_at: float | None


apartial = functools.partial


def lru_cache(
    maxsize: int | None = 128,
    typed: bool = False,
    expire_after: float | None = None,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """
    A simple LRU cache just like :func:`functools.lru_cache`, but it works for
    coroutines.  This is not as heavily optimized as :func:`functools.lru_cache`
    which uses an internal C implementation, as it targets async operations
    that take a long time.

    It follows the same API that the standard functools provides.  The wrapped
    function has ``cache_clear()`` method to flush the cache manually, but
    leaves ``cache_info()`` for statistics unimplemented.

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
    """

    def wrapper(coro: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        sentinel = object()  # unique object to distinguish None as result
        cache: collections.OrderedDict[Any, _CacheEntry[R]] = collections.OrderedDict()
        cache_get = cache.get
        cache_del = cache.__delitem__
        cache_set = cache.__setitem__
        cache_len = cache.__len__
        cache_move = cache.move_to_end
        make_key = functools._make_key

        # We don't use explicit locks like the standard functools,
        # because this lru_cache is intended for use in asyncio coroutines.
        # The only context interleaving happens when calling the user-defined
        # coroutine, so there is no need to add extra synchronization guards.

        @functools.wraps(coro)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            now = get_running_loop().time()
            k = make_key(args, kwargs, typed)
            entry = cache_get(k, sentinel)
            if entry is not sentinel:
                # Type narrowing: entry is now _CacheEntry[R]
                entry = cast(_CacheEntry[R], entry)
                if entry.expire_at is None:
                    return entry.value
                if entry.expire_at >= now:
                    return entry.value
                cache_del(k)
            result = await coro(*args, **kwargs)
            if maxsize is not None and cache_len() >= maxsize:
                cache.popitem(last=False)
            if expire_after is not None:
                expire_at = now + expire_after
            else:
                expire_at = None
            cache_set(k, _CacheEntry(result, expire_at))
            cache_move(k, last=True)
            return result

        def cache_clear() -> None:
            cache.clear()

        def cache_info() -> None:
            raise NotImplementedError

        wrapped.cache_clear = cache_clear  # type: ignore[attr-defined]
        wrapped.cache_info = cache_info  # type: ignore[attr-defined]
        return wrapped

    return wrapper
