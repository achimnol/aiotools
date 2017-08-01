Async Context Manager
=====================

.. module:: aiotools.context

.. autoclass:: aiotools.context.AbstractAsyncContextManager
   :members:

.. autoclass:: aiotools.context.AsyncContextManager
   :members:

.. autoclass:: aiotools.context.AsyncContextGroup
   :members:

   Alias: ``actxmgr``

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

.. autofunction:: aiotools.context.async_ctx_manager

   Alias: ``actxmgr``
