# mypy: disable-error-code="deprecated"
# pyright: reportDeprecated=false

from importlib.metadata import version

__version__ = version("aiotools")

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
    cancel_and_wait,
    gather_safe,
    race,
)

main = main_context

__all__ = (
    "resetting",
    "AsyncContextManager",
    "async_ctx_manager",
    "actxmgr",
    "aclosing",
    "closing_async",
    "AsyncContextGroup",
    "actxgroup",
    "AsyncExitStack",
    "all_tasks",
    "get_running_loop",
    "current_task",
    "set_task_name",
    "defer",
    "adefer",
    "AbstractChildProcess",
    "PosixChildProcess",
    "PidfdChildProcess",
    "afork",
    "apartial",
    "lru_cache",
    "aiter",
    "main",
    "start_server",
    "process_index",
    "AsyncServerContextManager",
    "ServerMainContextManager",
    "InterruptedBySignal",
    "Supervisor",
    "ErrorArg",
    "ErrorCallback",
    "TaskContext",
    "MultiError",
    "TaskGroup",
    "TaskGroupError",
    "current_taskgroup",
    "PersistentTaskGroup",
    "current_ptaskgroup",
    "TaskScope",
    "Timeout",
    "timeout",
    "timeout_at",
    "create_timer",
    "TimerDelayPolicy",
    "VirtualClock",
    "AsyncClosable",
    "AwaitableLike",
    "CoroutineLike",
    "cancel_and_wait",
    "as_completed_safe",
    "gather_safe",
    "race",
    "__version__",
    # "@aiotools.server" still works,
    # but server_context is provided to silence typecheckers.
    "main_context",
    "server_context",
)
