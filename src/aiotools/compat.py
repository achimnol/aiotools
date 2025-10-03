from __future__ import annotations

import asyncio
import sys
import warnings
from collections.abc import Awaitable, Coroutine, Generator
from typing import Any, TypeAlias, TypeVar

__all__ = (
    "get_running_loop",
    "all_tasks",
    "current_task",
)

get_running_loop = asyncio.get_running_loop
all_tasks = asyncio.all_tasks


def current_task() -> asyncio.Task[Any]:
    """
    A simple null-guarded version of :func:`asyncio.current_task()` wrapper
    to simplify type handling in the caller side.
    """
    task = asyncio.current_task()
    if task is None:
        raise RuntimeError(
            "The current stack does not belong to any running asyncio task."
        )
    return task


def set_task_name(task: asyncio.Task[Any], name: str | None) -> None:
    # This compatibility function had been in asyncio.tasks until Python 3.12,
    # but removed since Python 3.13.
    if name is not None:
        try:
            set_name = task.set_name
        except AttributeError:
            warnings.warn(
                "Task name customization may not be available in 3rd-party event loops before Python 3.13",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            set_name(name)


_T_co = TypeVar("_T_co", covariant=True)

if sys.version_info >= (3, 12):
    AwaitableLike: TypeAlias = Awaitable[_T_co]  # noqa: Y047
    CoroutineLike: TypeAlias = Coroutine[Any, Any, _T_co]  # noqa: Y047
else:
    AwaitableLike: TypeAlias = Generator[Any, None, _T_co] | Awaitable[_T_co]
    CoroutineLike: TypeAlias = Generator[Any, None, _T_co] | Coroutine[Any, Any, _T_co]
