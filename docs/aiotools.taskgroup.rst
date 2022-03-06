Task Group
==========

..
   Since taskgroup dynamically imports the implementation classes
   depending on the feature availability, documentations are gathered
   to here to prevent duplication.

.. attribute:: current_taskgroup

   A :class:`contextvars.ContextVar` that has the reference to the current innermost
   :class:`TaskGroup` instance.  Available only in Python 3.7 or later.

.. class:: TaskGroup(*, name=None)

   Provides a guard against a group of tasks spawend via its :meth:`create_task()`
   method instead of the vanilla fire-and-forgetting :func:`asyncio.create_task()`.

   See the motivation and rationale in `the trio's documentation
   <https://trio.readthedocs.io/en/stable/reference-core.html#nurseries-and-spawning>`_.

   In Python 3.11 or later, this wraps :class:`asyncio.TaskGroup` with a small
   extension to set the current taskgroup in a context variable.

   .. method:: create_task(coro, *, name=None)

      Spawns a new task inside the taskgroup and returns the reference to
      the task.  Setting the name of tasks is supported in Python 3.8 or
      later only and ignored in older versions.

   .. method:: get_name()

      Returns the name set when creating the instance.

   .. versionadded:: 1.0.0

   .. versionchanged:: 1.5.0

      Fixed edge-case bugs by referring the Python 3.11 stdlib's
      :class:`asyncio.TaskGroup` implementation, including abrupt
      cancellation before all nested spawned tasks start without context
      switches and propagation of the source exception when the context
      manager (parent task) is getting cancelled but continued.

.. class:: PersistentTaskGroup(*, name=None, exception_handler=None)

   Provides an abstraction of long-running task groups for server applications.
   The main use case is to implement a dispatcher of async event handlers, to group
   RPC/API request handlers, etc. with safe and graceful shutdown.
   Here "long-running" means that all tasks should keep going even when sibling
   tasks fail with unhandled errors and such errors must be reported immediately.
   Here "safety" means that all spawned tasks should be reclaimed before exit or
   shutdown.

   When used as an async context manager, it works similarly to
   :func:`asyncio.gather()` with ``return_exceptions=True`` option.  It exits the
   context scope when all tasks finish, just like :class:`asyncio.TaskGroup`, but
   it does NOT abort when there are unhandled exceptions from child tasks; just
   keeps sibling tasks running and reporting errors as they occur (see below).

   When *not* used as an async context maanger (e.g., used as attributes of
   long-lived objects), it persists running until :meth:`shutdown()` is called
   explicitly.  Note that it is the user's responsibility to call
   :meth:`shutdown()` because :class:`PersistentTaskGroup` does not provide the
   ``__del__()`` method.

   Regardless how it is executed, it lets all spawned tasks run to their completion
   and calls the exception handler to report any unhandled exceptions immediately.
   If there are exceptions occurred again in the exception handlers, then it uses
   :meth:`AbstractEventLoop.call_exception_handler() <asyncio.loop.call_exception_handler()>`
   as the last resort.

   Since the exception handling and reporting takes places immediately, it
   eliminates potential arbitrary report delay due to other tasks or the execution
   method.  This resolves a critical debugging pain when only termination of the
   application displays accumulated errors, as sometimes we don't want to terminate
   but just inspect what is happening.

   .. method:: create_task(coro, *, name=None)

      Spawns a new task inside the taskgroup and returns the reference to
      the task.  Setting the name of tasks is supported in Python 3.8 or
      later only and ignored in older versions.

   .. method:: get_name()

      Returns the name set when creating the instance.

   .. method:: shutdown()
      :async:

      Triggers immediate shutdown of this taskgroup, cancelling all
      unfinished tasks and waiting for their completion.

   .. versionadded:: 1.4.0

   .. versionchanged:: 1.5.0

      Rewrote the overall implementation referring the Python 3.11 stdlib's
      :class:`asyncio.TaskGroup` implementation and adapting it to the
      semantics for "persistency".


.. exception:: TaskGroupError

   Represents a collection of errors raised inside a task group.
   Callers may iterate over the errors using the ``__errors__`` attribute.

   In Python 3.11 or later, this is a mere wrapper of underlying
   :exc:`BaseExceptionGroup`.  This allows existing user codes to run
   without modification while users can take advantage of the new
   ``except*`` syntax and :exc:`ExceptionGroup` methods if they use Python
   3.11 or later.  Note that if none of the passed exceptions passed is a
   :exc:`BaseException`, it automatically becomes :exc:`ExceptionGroup`.
