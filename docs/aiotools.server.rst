Multi-process Server
====================

.. function:: server(func)

    A decorator wrapper for :class:`AsyncServerContextManager`.

    Usage example:

    .. code:: python

       @aiotools.server
       async def myserver(loop, pidx, args):
           await do_init(args)
           stop_sig = yield
           if stop_sig == signal.SIGINT:
               await do_graceful_shutdown()
           else:
               await do_forced_shutdown()

       aiotools.start_server(myserver, ...)

.. automodule:: aiotools.server
    :members:
    :undoc-members:
