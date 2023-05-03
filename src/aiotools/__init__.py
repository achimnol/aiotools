from importlib.metadata import version

# Import submodules only when installed properly
from . import (
    context,
    defer as _defer,
    fork as _fork,
    func,
    iter as _iter,
    server,
    taskgroup,
    timeouts,
    timer,
    utils,
)

__all__ = (
    *context.__all__,
    *_defer.__all__,
    *_fork.__all__,
    *func.__all__,
    *_iter.__all__,
    *server.__all__,
    *taskgroup.__all__,
    *timeouts.__all__,
    *timer.__all__,
    *utils.__all__,
    "__version__",
)

from .context import *     # noqa
from .defer import *       # noqa
from .fork import *        # noqa
from .func import *        # noqa
from .iter import *        # noqa
from .server import *      # noqa
from .taskgroup import *   # noqa
from .timeouts import *    # noqa
from .timer import *       # noqa
from .utils import *       # noqa

__version__ = version("aiotools")
