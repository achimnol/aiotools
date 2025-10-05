from __future__ import annotations

import sys
from collections.abc import Awaitable, Coroutine, Generator
from types import TracebackType
from typing import (
    Any,
    Protocol,
    TypeAlias,
    TypeVar,
    runtime_checkable,
)

__all__ = (
    "AsyncClosable",
    "AwaitableLike",
    "CoroutineLike",
    "ExcInfo",
    "OptExcInfo",
)

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)


@runtime_checkable
class AsyncClosable(Protocol):
    async def close(self) -> None: ...


# Exception info type aliases for __aexit__ and similar methods
ExcInfo: TypeAlias = tuple[type[BaseException], BaseException, TracebackType]
OptExcInfo: TypeAlias = ExcInfo | tuple[None, None, None]

# taken from the typeshed
if sys.version_info >= (3, 12):
    AwaitableLike: TypeAlias = Awaitable[_T_co]  # noqa: Y047
    CoroutineLike: TypeAlias = Coroutine[Any, Any, _T_co]  # noqa: Y047
else:
    AwaitableLike: TypeAlias = Generator[Any, None, _T_co] | Awaitable[_T_co]
    CoroutineLike: TypeAlias = Generator[Any, None, _T_co] | Coroutine[Any, Any, _T_co]
