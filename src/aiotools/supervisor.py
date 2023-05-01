__all__ = ["Supervisor"]

from asyncio import events
from asyncio import exceptions
from asyncio import tasks


class Supervisor:

    def __init__(self):
        self._entered = False
        self._exiting = False
        self._aborting = False
        self._loop = None
        self._parent_task = None
        self._parent_cancel_requested = False
        self._tasks = set()
        self._errors = []
        self._base_error = None
        self._on_completed_fut = None

    def __repr__(self):
        info = ['']
        if self._tasks:
            info.append(f'tasks={len(self._tasks)}')
        if self._errors:
            info.append(f'errors={len(self._errors)}')
        if self._aborting:
            info.append('cancelling')
        elif self._entered:
            info.append('entered')

        info_str = ' '.join(info)
        return f'<Supervisor{info_str}>'

    async def __aenter__(self):
        if self._entered:
            raise RuntimeError(
                f"Supervisor {self!r} has been already entered")
        self._entered = True

        if self._loop is None:
            self._loop = events.get_running_loop()

        self._parent_task = tasks.current_task(self._loop)
        if self._parent_task is None:
            raise RuntimeError(
                f'Supervisor {self!r} cannot determine the parent task')

        return self

    async def __aexit__(self, et, exc, tb):
        self._exiting = True
        ## print(f"supervisor.__aexit__(): {exc=}")

        if (exc is not None and
                self._is_base_error(exc) and
                self._base_error is None):
            # SystemExit or KeyboardInterrupt in "async with"
            # so we cancel other tasks.
            ## print(f"supervisor.__aexit__(): aborting")
            self._base_error = exc
            self._abort()

        propagate_cancellation_error = \
            exc if et is exceptions.CancelledError else None
        if self._parent_cancel_requested:
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
        if propagate_cancellation_error and not self._errors:
            raise propagate_cancellation_error

        # if et is not None and et is not exceptions.CancelledError:
        #     self._errors.append(exc)

        # if self._errors:
        #     # Exceptions are heavy objects that can have object
        #     # cycles (bad for GC); let's not keep a reference to
        #     # a bunch of them.
        #     try:
        #         me = BaseExceptionGroup('unhandled errors in a Supervisor', self._errors)
        #         raise me from None
        #     finally:
        #         self._errors = None

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

    def _abort(self):
        self._aborting = True

        for t in self._tasks:
            if not t.done():
                t.cancel()

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
                ## print("supervisor._wait_completion(): begin")
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
                    self._abort()
            self._on_completed_fut = None

        ## print("supervisor._wait_completion(): returning", propagate_cancellation_error)
        return propagate_cancellation_error

    async def shutdown(self) -> None:
        self._abort()
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

        # self._errors.append(exc)
        _is_base_error = self._is_base_error(exc)
        if _is_base_error and self._base_error is None:
            self._base_error = exc

        if self._parent_task.done():
            # Not sure if this case is possible, but we want to handle
            # it anyways.
            self._loop.call_exception_handler({
                'message': f'Task {task!r} has errored out but its parent '
                           f'task {self._parent_task} is already completed',
                'exception': exc,
                'task': task,
            })
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
