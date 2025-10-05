from __future__ import annotations

import enum
from builtins import aiter as _builtin_aiter
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import Final, TypeVar, cast, overload

__all__ = ("aiter",)

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)


class _Sentinel(enum.Enum):
    MISSING = 0


MISSING: Final = _Sentinel.MISSING


@overload
def aiter(source: AsyncIterable[_T], /) -> AsyncIterator[_T]: ...


@overload
def aiter(
    func: Callable[[], Awaitable[_T]], sentinel: _T | _Sentinel, /
) -> AsyncIterator[_T]: ...


def aiter(
    arg: AsyncIterable[_T] | Callable[[], Awaitable[_T]],
    sentinel: _T | _Sentinel = MISSING,
) -> AsyncIterator[_T]:
    """
    Analogous to the builtin :func:`iter()`.

    As of Python 3.10, this is a small extension accepting an explicit sentinel
    while the base implementation without sentinel uses the builtin :func:`aiter()`.
    """

    # The forced typecasting here looks ugly, but it is an inevitable choice:
    # - runtime checks using functools.singledispatch cannot evaluate type annotations with generics.
    # - runtime checks using the pattern matching syntax has the same limitation.
    # -

    if sentinel is MISSING:
        # Fast-path using the builtin directly.
        return _builtin_aiter(cast(AsyncIterable[_T], arg))

    func = cast(Callable[[], Awaitable[_T]], arg)

    # The "yield" statement changes the type of function to generator,
    # so we need to separate the type using an inner function.
    async def _gen() -> AsyncIterator[_T]:
        while True:
            item = await func()
            if item == sentinel:
                break
            yield item

    return _gen()
