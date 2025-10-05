# mypy: disable-error-code="deprecated"
# pyright: reportDeprecated=false

from importlib.metadata import version

__version__ = version("aiotools")

from .cancel import (
    cancel_and_wait,
)
from .compat import (
    all_tasks,
    current_task,
    get_running_loop,
    set_task_name,
)
from .context import (
    AsyncContextGroup,
    AsyncContextManager,
    AsyncExitStack,
    aclosing,
    actxgroup,
    actxmgr,
    async_ctx_manager,
    closing_async,
    resetting,
)
from .defer import (
    AsyncDeferFunc,
    AsyncDeferrable,
    DeferFunc,
    adefer,
    defer,
)
from .fork import (
    AbstractChildProcess,
    PidfdChildProcess,
    PosixChildProcess,
    afork,
)
from .func import (
    apartial,
    lru_cache,
)
from .iter import aiter
from .server import (
    AsyncServerContextManager,
    InterruptedBySignal,
    ServerMainContextManager,
    main_context,
    process_index,
    server_context,
    start_server,
)
from .supervisor import Supervisor
from .taskcontext import (
    ErrorArg,
    ErrorCallback,
    TaskContext,
)
from .taskgroup import (
    MultiError,
    PersistentTaskGroup,
    TaskGroup,
    TaskGroupError,
    current_ptaskgroup,
    current_taskgroup,
)
from .taskscope import TaskScope
from .timeouts import (
    Timeout,
    timeout,
    timeout_at,
)
from .timer import (
    TimerDelayPolicy,
    VirtualClock,
    create_timer,
)
from .types import (
    AsyncClosable,
    AwaitableLike,
    CoroutineLike,
)
from .utils import (
    as_completed_safe,
    gather_safe,
    race,
)

main = main_context

__all__ = (
    # .cancel
    "cancel_and_wait",
    # .compat
    "all_tasks",
    "get_running_loop",
    "current_task",
    "set_task_name",
    # .context
    "resetting",
    "AsyncContextManager",
    "async_ctx_manager",
    "actxmgr",
    "aclosing",
    "closing_async",
    "AsyncContextGroup",
    "actxgroup",
    "AsyncExitStack",
    # .defer
    "DeferFunc",
    "AsyncDeferrable",
    "AsyncDeferFunc",
    "adefer",
    "defer",
    # .fork
    "AbstractChildProcess",
    "PosixChildProcess",
    "PidfdChildProcess",
    "afork",
    # .func
    "apartial",
    "lru_cache",
    # .iter
    "aiter",
    # .server
    "main",
    "main_context",
    # NOTE: "@aiotools.server" still works,
    #       but server_context is provided to silence typecheckers.
    "server_context",
    "start_server",
    "process_index",
    "AsyncServerContextManager",
    "ServerMainContextManager",
    "InterruptedBySignal",
    # .supervisor
    "Supervisor",
    # .taskcontext
    "ErrorArg",
    "ErrorCallback",
    "TaskContext",
    # .taskgroup
    "MultiError",
    "TaskGroup",
    "TaskGroupError",
    "current_taskgroup",
    "PersistentTaskGroup",
    "current_ptaskgroup",
    # .taskscope
    "TaskScope",
    # .timeouts
    "Timeout",
    "timeout",
    "timeout_at",
    # .timer
    "TimerDelayPolicy",
    "VirtualClock",
    "create_timer",
    # .types
    "AsyncClosable",
    "AwaitableLike",
    "CoroutineLike",
    # .utils
    "as_completed_safe",
    "gather_safe",
    "race",
    "__version__",
)
