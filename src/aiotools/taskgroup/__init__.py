import asyncio

from .types import TaskGroupError

if not hasattr(asyncio, 'TaskGroup'):
    from . import base_compat
    from . import persistent_compat
    from .base_compat import *        # noqa
    from .persistent_compat import *  # noqa
    __all__ = (
        'TaskGroupError',
        *base_compat.__all__,
        *persistent_compat.__all__,
    )
else:
    from . import base
    from . import persistent
    from .base import *        # noqa
    from .persistent import *  # noqa
    __all__ = (
        'TaskGroupError',
        *base.__all__,
        *persistent.__all__,
    )
