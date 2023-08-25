from asyncio import events, exceptions, tasks
from typing import Optional

__all__ = ["Supervisor"]


class Supervisor:
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

    def __init__(self):
        self._entered = False
        self._exiting = False
        self._aborting = False
        self._loop = None
        self._parent_task = None
        self._parent_cancel_requested = False
        self._tasks = set()
        self._base_error = None
        self._on_completed_fut = None

    def __repr__(self):
        info = [""]
        if self._tasks:
            info.append(f"tasks={len(self._tasks)}")
        if self._aborting:
            info.append("cancelling")
        elif self._entered:
            info.append("entered")

        info_str = " ".join(info)
        return f"<Supervisor{info_str}>"

    async def __aenter__(self):
        if self._entered:
            raise RuntimeError(f"Supervisor {self!r} has been already entered")
        self._entered = True

        if self._loop is None:
            self._loop = events.get_running_loop()

        self._parent_task = tasks.current_task(self._loop)
        if self._parent_task is None:
            raise RuntimeError(f"Supervisor {self!r} cannot determine the parent task")

        return self

    async def __aexit__(self, et, exc, tb):
        self._exiting = True

        if exc is not None and self._is_base_error(exc) and self._base_error is None:
            # SystemExit or KeyboardInterrupt in "async with"
            # so we cancel other tasks.
            self._base_error = exc
            self._abort()

        propagate_cancellation_error = exc if et is exceptions.CancelledError else None
        if self._parent_cancel_requested:
            assert self._parent_task is not None
            # If this flag is set we *must* call uncancel().
            if self._parent_task.uncancel() == 0:
                # If there are no pending cancellations left,
                # don't propagate CancelledError.
                propagate_cancellation_error = None

        prop_ex = await self._wait_completion()
        assert not self._tasks
        if prop_ex is not None:
            propagate_cancellation_error = prop_ex

        if self._base_error is not None:
            raise self._base_error

        # Propagate CancelledError if there is one, except if there
        # are other errors -- those have priority.
        if propagate_cancellation_error:
            raise propagate_cancellation_error

        # In the original version, it raises BaseExceptionGroup
        # if there are collected errors in self._errors.
        # This part is deliberately removed to prevent memory leak
        # due to accumulating error objects for an indefinite length of time.

    def create_task(self, coro, *, name=None, context=None):
        if not self._entered:
            raise RuntimeError(f"Supervisor {self!r} has not been entered")
        if self._exiting and not self._tasks:
            raise RuntimeError(f"Supervisor {self!r} is finished")
        if self._aborting:
            raise RuntimeError(f"Supervisor {self!r} is shutting down")
        if context is None:
            task = self._loop.create_task(coro)
        else:
            task = self._loop.create_task(coro, context=context)
        tasks._set_task_name(task, name)
        task.add_done_callback(self._on_task_done)
        self._tasks.add(task)
        return task

    # Since Python 3.8 Tasks propagate all exceptions correctly,
    # except for KeyboardInterrupt and SystemExit which are
    # still considered special.

    def _is_base_error(self, exc: BaseException) -> bool:
        assert isinstance(exc, BaseException)
        return isinstance(exc, (SystemExit, KeyboardInterrupt))

    def _abort(self, msg: Optional[str] = None):
        self._aborting = True

        for t in self._tasks:
            if not t.done():
                t.cancel(msg=msg)

    async def _wait_completion(self):
        # We use while-loop here because "self._on_completed_fut"
        # can be cancelled multiple times if our parent task
        # is being cancelled repeatedly (or even once, when
        # our own cancellation is already in progress)
        propagate_cancellation_error = None
        while self._tasks:
            if self._on_completed_fut is None:
                self._on_completed_fut = self._loop.create_future()

            try:
                await self._on_completed_fut
            except exceptions.CancelledError as ex:
                if not self._aborting:
                    # Our parent task is being cancelled:
                    #
                    #    async def wrapper():
                    #        async with TaskGroup() as g:
                    #            g.create_task(foo)
                    #
                    # "wrapper" is being cancelled while "foo" is
                    # still running.
                    propagate_cancellation_error = ex
                    self._abort(msg=ex.args[0] if ex.args else None)
            self._on_completed_fut = None

        return propagate_cancellation_error

    async def shutdown(self) -> None:
        self._abort(msg="supervisor.shutdown")
        await self._wait_completion()

    def _on_task_done(self, task):
        self._tasks.discard(task)

        if self._on_completed_fut is not None and not self._tasks:
            if not self._on_completed_fut.done():
                self._on_completed_fut.set_result(True)

        if task.cancelled():
            return

        exc = task.exception()
        if exc is None:
            return

        _is_base_error = self._is_base_error(exc)
        if _is_base_error and self._base_error is None:
            self._base_error = exc

        assert self._parent_task is not None
        if self._parent_task.done():
            # Not sure if this case is possible, but we want to handle
            # it anyways.
            self._loop.call_exception_handler(
                {
                    "message": (
                        f"Task {task!r} has errored out but its parent "
                        f"task {self._parent_task} is already completed"
                    ),
                    "exception": exc,
                    "task": task,
                }
            )
            return

        if _is_base_error and not self._aborting and not self._parent_cancel_requested:
            # For base SystemExit and KeyboardInterrupt ONLY, if parent task
            # *is not* being cancelled, it means that we want to manually cancel
            # it to abort whatever is being run right now in the Supervisor.
            # But we want to mark parent task as "not cancelled" later in __aexit__.
            # Example situation that we need to handle:
            #
            #    async def foo():
            #        try:
            #            async with Supervisor() as s:
            #                s.create_task(crash_soon())
            #                await something  # <- this needs to be waited
            #                                 #    by the Supervisor, unless
            #                                 #    crash_soon() raises either
            #                                 #    SystemExit or KeyboardInterrupt
            #        except Exception:
            #            # Ignore any exceptions raised in the Supervisor
            #            pass
            #        await something_else     # this line has to be called
            #                                 # after TaskGroup is finished.
            self._abort()
            self._parent_cancel_requested = True
            self._parent_task.cancel()
