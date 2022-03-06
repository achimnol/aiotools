"""
A set of common utilities for legacy Pythons
"""

import asyncio
import functools

from .. import compat

_has_task_name = hasattr(asyncio.Task, 'get_name')


def create_task_with_name(coro, *, name=None):
    loop = compat.get_running_loop()
    if _has_task_name and name:
        child_task = loop.create_task(coro, name=name)
    else:
        child_task = loop.create_task(coro)
    return child_task


def patch_task(task) -> None:
    # In Python 3.8 we'll need proper API on asyncio.Task to
    # make TaskGroups possible. We need to be able to access
    # information about task cancellation, more specifically,
    # we need a flag to say if a task was cancelled or not.
    # We also need to be able to flip that flag.

    def _task_cancel(task, orig_cancel, msg=None):
        task.__cancel_requested__ = True
        if msg is not None:
            return orig_cancel(msg)
        else:
            return orig_cancel()

    if hasattr(task, '__cancel_requested__'):
        return

    task.__cancel_requested__ = False
    # confirm that we were successful at adding the new attribute:
    assert not task.__cancel_requested__

    orig_cancel = task.cancel
    task.cancel = functools.partial(_task_cancel, task, orig_cancel)
