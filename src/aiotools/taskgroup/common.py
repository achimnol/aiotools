"""
A set of common utilities for legacy Pythons
"""

from __future__ import annotations

import asyncio
from typing import TypeVar

from ..types import CoroutineLike

_has_task_name: bool = hasattr(asyncio.Task, "get_name")
T = TypeVar("T")


def create_task_with_name(
    coro: CoroutineLike[T], *, name: str | None = None
) -> asyncio.Task[T]:
    loop = asyncio.get_running_loop()
    if _has_task_name and name:
        child_task = loop.create_task(coro, name=name)
    else:
        child_task = loop.create_task(coro)
    return child_task
