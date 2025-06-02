from importlib.metadata import version

# Import submodules only when installed properly
from . import (
    context,
    func,
    server,
    taskgroup,
    timer,
)
from . import defer as _defer
from . import fork as _fork
from . import iter as _iter

__all__ = (
    *context.__all__,
    *_defer.__all__,
    *_fork.__all__,
    *func.__all__,
    *_iter.__all__,
    *server.__all__,
    *taskgroup.__all__,
    *timer.__all__,
    "__version__",
    # "@aiotools.server" still works,
    # but server_context is provided to silence typecheckers.
    "main_context",
    "server_context",
)

from .context import *  # noqa
from .defer import *  # noqa
from .fork import *  # noqa
from .func import *  # noqa
from .iter import *  # noqa
from .server import *  # noqa
from .taskgroup import *  # noqa
from .timer import *  # noqa

main_context = server._main_ctxmgr
server_context = server._server_ctxmgr

__version__ = version("aiotools")
