import asyncio

from .types import MultiError, TaskGroupError

if hasattr(asyncio, 'TaskGroup'):
    from . import base
    from . import persistent
    from .base import *        # noqa
    from .persistent import *  # noqa
    __all__ = [
        'MultiError',
        'TaskGroupError',
        *base.__all__,
        *persistent.__all__,
    ]
else:
    from . import base_compat
    from . import persistent_compat
    from .base_compat import *        # type: ignore  # noqa
    from .persistent_compat import *  # type: ignore  # noqa
    __all__ = [  # type: ignore
        'MultiError',
        'TaskGroupError',
        *base_compat.__all__,
        *persistent_compat.__all__,
    ]

from .utils import GroupResult, as_completed_safe, gather_safe  # noqa

__all__.append("GroupResult")
__all__.append("as_completed_safe")
__all__.append("gather_safe")
