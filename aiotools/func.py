import functools

__all__ = [
    'apartial',
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
