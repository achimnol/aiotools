# Import submodules only when installed properly
from . import (
    context,
    func,
    iter,
    timer,
    server,
)

import pkgutil
_version_data = pkgutil.get_data("aiotools", "VERSION")
if _version_data is None:
    __version__ = '0.0.dev'
else:
    __version__ = _version_data.decode('utf8').strip()

__all__ = (
    *context.__all__,
    *func.__all__,
    *iter.__all__,
    *timer.__all__,
    *server.__all__,
    '__version__',
)

from .context import *  # noqa
from .func import *     # noqa
from .iter import *     # noqa
from .timer import *    # noqa
from .server import *   # noqa
