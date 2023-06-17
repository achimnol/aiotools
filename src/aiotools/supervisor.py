__all__ = ["Supervisor"]

from .taskscope import TaskScope


class Supervisor(TaskScope):
    """
    Supervisor is a primitive structure to provide a long-lived context manager scope
    for an indefinite set of subtasks.  During its lifetime, it is free to spawn new
    subtasks at any time.  If the supervisor itself is cancelled from outside or
    :meth:`shutdown()` is called, it will cancel all running tasks immediately, wait
    for their completion, and then exit the context manager block.

    The main difference to :class:`asyncio.TaskGroup` is that it keeps running
    sibling subtasks even when there is an unhandled exception from one of the
    subtasks.

    To prevent memory leaks, a supervisor does not store any result or exception
    from its subtasks.  Instead, the callers must use additional task-done
    callbacks to process subtask results and exceptions.

    Supervisor provides the same analogy to Kotlin's ``SupervisorScope`` and
    Javascript's ``Promise.allSettled()``, while :class:`asyncio.TaskGroup` provides
    the same analogy to Kotlin's ``CoroutineScope`` and Javascript's
    ``Promise.all()``.

    The original implementation is based on DontPanicO's pull request
    (https://github.com/achimnol/cpython/pull/31) and :class:`PersistentTaskGroup`,
    but it is modified *not* to store unhandled subtask exceptions.

    .. versionadded:: 2.0
    """

    def __init__(self) -> None:
        super().__init__(delegate_errors=None)
