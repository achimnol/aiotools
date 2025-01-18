import asyncio
import warnings

get_running_loop = asyncio.get_running_loop
all_tasks = asyncio.all_tasks
current_task = asyncio.current_task


def set_task_name(task, name):
    # This compatibility function had been in asyncio.tasks until Python 3.12,
    # but removed since Python 3.13.
    if name is not None:
        try:
            set_name = task.set_name
        except AttributeError:
            warnings.warn(
                "Task.set_name() was added in Python 3.8, "
                "the method support will be mandatory for third-party "
                "task implementations since 3.13.",
                DeprecationWarning,
                stacklevel=3,
            )
        else:
            set_name(name)
