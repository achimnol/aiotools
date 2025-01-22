import asyncio

from .types import MultiError, TaskGroupError

__all__ = [
    "MultiError",
    "TaskGroup",
    "TaskGroupError",
    "PersistentTaskGroup",
    "as_completed_safe",
    "persistent",
    "persistent_compat",
    "current_ptaskgroup",
    "has_contextvars",
]

if hasattr(asyncio, "TaskGroup"):
    from . import persistent
    from .base import *  # noqa
    from .persistent import *  # noqa

    has_contextvars = True
else:
    from . import persistent_compat
    from .base_compat import *  # type: ignore  # noqa
    from .base_compat import has_contextvars
    from .persistent_compat import *  # type: ignore  # noqa

from .utils import as_completed_safe  # noqa
