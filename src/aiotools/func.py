import collections
import functools

from .compat import get_running_loop

__all__ = [
    'apartial',
    'lru_cache',
]

_CacheEntry = collections.namedtuple('_CacheEntry', 'value expire_at')


def apartial(coro, *args, **kwargs):
    '''
    Wraps a coroutine function with pre-defined arguments (including keyword
    arguments).  It is an asynchronous version of :func:`functools.partial`.
    '''

    @functools.wraps(coro)
    async def wrapped(*cargs, **ckwargs):
        return await coro(*args, *cargs, **kwargs, **ckwargs)

    return wrapped


def lru_cache(maxsize: int = 128,
              typed: bool = False,
              expire_after: float = None):
    '''
    A simple LRU cache just like :func:`functools.lru_cache`, but it works for
    coroutines.  This is not as heavily optimized as :func:`functools.lru_cache`
    which uses an internal C implementation, as it targets async operations
    that take a long time.

    It follows the same API that the standard functools provides.  The wrapped
    function has ``cache_clear()`` method to flush the cache manually, but
    leaves ``cache_info()`` for statistics unimplemented.

    Note that calling the coroutine multiple times with the same arguments
    before the first call returns may incur duplicate exectuions.

    This function is not thread-safe.

    Args:
       maxsize: The maximum number of cached entries.

       typed: Cache keys in different types separately (e.g., ``3`` and ``3.0`` will
              be different keys).

       expire_after: Re-calculate the value if the configured time has passed even
                     when the cache is hit.  When re-calculation happens the
                     expiration timer is also reset.
    '''

    if maxsize is not None and not isinstance(maxsize, int):
        raise TypeError('Expected maxsize to be an integer or None')

    def wrapper(coro):

        sentinel = object()  # unique object to distinguish None as result
        cache = collections.OrderedDict()
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
        async def wrapped(*args, **kwargs):
            now = get_running_loop().time()
            k = make_key(args, kwargs, typed)
            entry = cache_get(k, sentinel)
            if entry is not sentinel:
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

        def cache_clear():
            cache.clear()

        def cache_info():
            raise NotImplementedError

        wrapped.cache_clear = cache_clear
        wrapped.cache_info = cache_info
        return wrapped

    return wrapper
