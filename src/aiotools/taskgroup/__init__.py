import asyncio

from .types import MultiError, TaskGroupError

if hasattr(asyncio, 'TaskGroup'):
    from . import persistent
    from .base import *        # noqa
    from .persistent import *  # noqa
    __all__ = [
        "MultiError",
        "TaskGroup",
        "TaskGroupError",
        "current_taskgroup",
        *persistent.__all__,
    ]
else:
    from . import persistent_compat
    from .base_compat import *        # type: ignore  # noqa
    from .persistent_compat import *  # type: ignore  # noqa
    __all__ = [  # type: ignore
        "MultiError",
        "TaskGroup",
        "TaskGroupError",
        *persistent_compat.__all__,
    ]

from .utils import as_completed_safe  # noqa

__all__.append("as_completed_safe")
