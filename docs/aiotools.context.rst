Async Context Manager
=====================

.. automodule:: aiotools.context

.. currentmodule:: aiotools.context

.. autoclass:: aiotools.context.AbstractAsyncContextManager
   :members:

.. autoclass:: aiotools.context.AsyncContextManager
   :members:

.. function:: async_ctx_manager(func)

   A helper function to ease use of :class:`~.AsyncContextManager`.

.. function:: actxmgr(func)

   An alias of :func:`~.async_ctx_manager`.

.. autoclass:: aiotools.context.AsyncContextGroup
   :members:

   Example:

   .. code-block:: python3

      @aiotools.actxmgr
      async def ctx(v):
        yield v + 10

      g = aiotools.actxgroup([ctx(1), ctx(2)])

      async with g as values:
          assert values[0] == 11
          assert values[1] == 12

      rets = g.exit_states()
      assert rets[0] is None  # successful shutdown
      assert rets[1] is None

.. class:: actxgroup

   An alias of :class:`~.AsyncContextGroup`.
