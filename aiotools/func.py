import collections
import functools

__all__ = [
    'apartial',
    'lru_cache',
]


def apartial(coro, *args, **kwargs):
    '''
    Wraps a coroutine function with pre-defined arguments (including keyword
    arguments).  It is an asynchronous version of :func:`functools.partial`.
    '''
    @functools.wraps(coro)
    async def wrapped(*cargs, **ckwargs):
        return await coro(*args, *cargs, **kwargs, **ckwargs)
    return wrapped


def lru_cache(maxsize=128, typed=False):
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
    '''

    if maxsize is not None and not isinstance(maxsize, int):
        raise TypeError('Expected maxsize to be an integer or None')

    def wrapper(coro):

        sentinel = object()  # unique object to distinguish None as result
        cache = collections.OrderedDict()
        cache_get = cache.get
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
            k = make_key(args, kwargs, typed)
            result = cache_get(k, sentinel)
            if result is not sentinel:
                return result
            result = await coro(*args, **kwargs)
            if maxsize is not None and cache_len() >= maxsize:
                cache.popitem(last=False)
            cache_set(k, result)
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
