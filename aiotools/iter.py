__all__ = (
    'aiter',
)


_sentinel = object()


async def aiter(obj, sentinel=_sentinel):
    '''
    Analogous to the builtin :func:`iter()`.
    '''

    if sentinel is _sentinel:
        # Since we cannot directly return the return value of obj.__aiter__()
        # as being an async-generator, we do the async-iteration here.
        async for item in obj:
            yield item
    else:
        while True:
            item = await obj()
            if item == sentinel:
                break
            yield item
