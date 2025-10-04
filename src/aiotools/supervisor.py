import contextvars
from typing import Optional

from .taskscope import TaskScope

__all__ = ("Supervisor",)


class Supervisor(TaskScope):
    """
    Supervisor is a primitive structure to provide a long-lived context manager scope
    for an indefinite set of subtasks.  During its lifetime, it is free to spawn new
    subtasks at any time.  If the supervisor itself is cancelled from outside or
    :meth:`aclose()` is called, it will cancel all running tasks immediately, wait
    for their completion, and then exit the context manager block.

    The main difference to :class:`asyncio.TaskGroup` is that it keeps running
    sibling subtasks even when there is an unhandled exception from one of the
    subtasks.

    To prevent memory leaks, a supervisor does not store any result or exception
    from its subtasks.  Instead, the callers must use additional task-done
    callbacks to process subtask results and exceptions.

    Supervisor provides the same analogy to Kotlin's |SupervisorScope|_ and
    Javascript's |Promise.allSettled()|_, while :class:`asyncio.TaskGroup` provides
    the same analogy to Kotlin's |CoroutineScope|_ and Javascript's |Promise.all()|_.

    .. |SupervisorScope| replace:: ``SupervisorScope``
    .. |CoroutineScope| replace:: ``CoroutineScope``
    .. _SupervisorScope: https://kotlinlang.org/api/kotlinx.coroutines/kotlinx-coroutines-core/kotlinx.coroutines/supervisor-scope.html
    .. _CoroutineScope: https://kotlinlang.org/api/kotlinx.coroutines/kotlinx-coroutines-core/kotlinx.coroutines/-coroutine-scope/
    .. |Promise.allSettled()| replace:: ``Promise.allSettled()``
    .. |Promise.all()| replace:: ``Promise.all()``
    .. _Promise.allSettled(): https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/allSettled
    .. _Promise.all(): https://developer.mozilla.org/ko/docs/Web/JavaScript/Reference/Global_Objects/Promise/all

    The original implementation is based on DontPanicO's pull request
    (https://github.com/achimnol/cpython/pull/31) and :class:`PersistentTaskGroup`,
    but it is modified *not* to store unhandled subtask exceptions.

    .. versionadded:: 2.0
    """  # noqa: E501

    def __init__(self, context: Optional[contextvars.Context] = None) -> None:
        super().__init__(exception_handler=None, context=context)
