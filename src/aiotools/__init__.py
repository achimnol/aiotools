from importlib.metadata import version

# Import submodules only when installed properly
from . import context
from . import defer as _defer
from . import fork as _fork
from . import func
from . import iter as _iter
from . import server, taskgroup, timeouts, timer, utils

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
    # "@aiotools.server" still works,
    # but server_context is provided to silence typecheckers.
    "server_context",
)

from .context import *  # noqa
from .defer import *  # noqa
from .fork import *  # noqa
from .func import *  # noqa
from .iter import *  # noqa
from .server import *  # noqa
from .taskgroup import *  # noqa
from .timeouts import *  # noqa
from .timer import *  # noqa
from .utils import *  # noqa

server_context = server._server_ctxmgr

__version__ = version("aiotools")
