from __future__ import annotations

import sys
from typing import (
    Any,
    Awaitable,
    Coroutine,
    Generator,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
)

from typing_extensions import TypeAlias

_T = TypeVar("_T")


@runtime_checkable
class AClosable(Protocol):
    async def aclose(self) -> None:
        ...


@runtime_checkable
class AsyncClosable(Protocol):
    async def close(self) -> None:
        ...


# taken from the typeshed
if sys.version_info >= (3, 12):
    AwaitableLike: TypeAlias = Awaitable[_T]  # noqa: Y047
    CoroutineLike: TypeAlias = Coroutine[Any, Any, _T]  # noqa: Y047
else:
    AwaitableLike: TypeAlias = Union[Generator[Any, None, _T], Awaitable[_T]]
    CoroutineLike: TypeAlias = Union[Generator[Any, None, _T], Coroutine[Any, Any, _T]]
