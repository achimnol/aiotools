# Import submodules only when installed properly
from . import (
    context,
    defer as _defer,
    fork as _fork,
    func,
    iter as _iter,
    server,
    taskgroup,
    timer,
)

import pkgutil
_version_data = pkgutil.get_data("aiotools", "VERSION")
if _version_data is None:
    __version__ = '0.0.dev'
else:
    __version__ = _version_data.decode('utf8').strip()

__all__ = (
    *context.__all__,
    *_defer.__all__,
    *_fork.__all__,
    *func.__all__,
    *_iter.__all__,
    *server.__all__,
    *taskgroup.__all__,
    *timer.__all__,
    '__version__',
)

from .context import *     # noqa
from .defer import *       # noqa
from .fork import *        # noqa
from .func import *        # noqa
from .iter import *        # noqa
from .taskgroup import *   # noqa
from .timer import *       # noqa
from .server import *      # noqa
