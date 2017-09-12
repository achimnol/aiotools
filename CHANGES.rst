Changelog
=========

0.4.4 (2017-09-12)
------------------

- Add a new module ``aiotools.func`` with ``apartial()`` function which is an
  async version of ``functools.partial()`` in the standard library

0.4.3 (2017-08-06)
------------------

- Add ``aclosing()`` context manager like ``closing()`` in the standard library

- Speed up Travis CI builds for packaging

- Now provide README in rst as well as CHANGES (this file)

0.4.2 (2017-08-01)
------------------

- ``server``: Fix spawning subprocesses in child workers

- Add support for ``uvloop``

0.4.0 (2017-08-01)
------------------

- Add ``use_threading`` argument to 

- Add initial documentation (which currently not served
  on readthedocs.io due to Python version problem)

0.3.2 (2017-07-31)
------------------

- Add ``extra_procs`` argument to ``start_server()`` function

- Add socket and ZeroMQ server examples

- Improve CI configs

0.3.1 (2017-07-26)
------------------

- Improve CI scripts

- Adopt editorconfig

0.3.0 (2017-04-26)
------------------

- Add ``start_server()`` function using multiprocessing
  with automatic children lifecycle management

- Clarify the semantics of ``AsyncContextGroup`` using
  ``asyncio.gather()`` with ``return_exceptions=True``

0.2.0 (2017-04-20)
------------------

- Add abstract types for ``AsyncContextManager``

- Rename ``AsyncGenContextManager`` to ``AsyncContextManager``

- Add ``AsyncContextGroup``

0.1.1 (2017-04-14)
------------------

- Initial release
