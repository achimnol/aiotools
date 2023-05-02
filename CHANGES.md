Changelog
=========

<!--
    You should *NOT* be adding new change log entries to this file, this
    file is managed by towncrier. You *may* edit previous change logs to
    fix problems like typo corrections or such.

    To add a new change log entry, please refer
    https://pip.pypa.io/en/latest/development/contributing/#news-entries

    We named the news folder "changes".

    WARNING: Don't drop the last line!
-->

<!-- towncrier release notes start -->

1.6.1 (2023-05-02)
------------------

### Fixes
* PersistentTaskGroup no longer stores the history of unhandled exceptions and raises them as an exception group to prevent memory leaks ([#54](https://github.com/achimnol/aiotools/issues/54))


1.6.0 (2023-03-14)
------------------

### Features
* Add `as_completed_safe()` which enhances `asyncio.as_completed()` using `PersistentTaskGroup` ([#52](https://github.com/achimnol/aiotools/issues/52))


1.5.9 (2022-04-26)
------------------

### Fixes
* Improve checks for pidfd availability to avoid corner cases that may fail on Linux kernel 5.1 and 5.2 where `signal.pidfd_send_signal()` is available but `os.pidfd_open()` is not ([#51](https://github.com/achimnol/aiotools/issues/51))


1.5.8 (2022-04-25)
------------------

### Fixes
* Explicitly attach the event loop to the `PidfdChildWatcher` when first initialized ([#50](https://github.com/achimnol/aiotools/issues/50))


1.5.7 (2022-04-12)
------------------

### Fixes
* Fix regression of the default imports in macOS by removing the unused code that caused the misleading fix in #47 ([#49](https://github.com/achimnol/aiotools/issues/49))


1.5.6 (2022-04-11)
------------------

### Features
* Add the `closing_async()` async context manager, in addition to `aclosing()` ([#48](https://github.com/achimnol/aiotools/issues/48))
### Fixes
* Allow importing aiotools on Windows platforms, removing incompatible modules from the default `__all__` import list ([#47](https://github.com/achimnol/aiotools/issues/47))


1.5.5 (2022-03-22)
------------------

### Features
* Add `wait_timeout` option to `start_server()` ([#46](https://github.com/achimnol/aiotools/issues/46))

### Fixes
* Resolve singal races by minimizing expose of event loop in `afork()`-ed child processes ([#46](https://github.com/achimnol/aiotools/issues/46))

### Miscellaneous
* Now the CI runs with Python 3.11a6 or later, with stdlib support of `asyncio.TaskGroup` ([#45](https://github.com/achimnol/aiotools/issues/45))


1.5.4 (2022-03-10)
------------------

### Features
* Propagate task results and exceptions via separate future instances if they are `await`-ed by the caller of `create_task()` in `PersistentTaskGroup`, in addition to invocation of task group exception handler.  Note that `await`-ing those futures hangs indefinitely in Python 3.6 but we don't fix it since Python 3.6 is EoL as of December 2021. ([#44](https://github.com/achimnol/aiotools/issues/44))


1.5.3 (2022-03-07)
------------------

### Fixes
* Fix feature detection for `ExceptionGroup` and let `MultiError` inherit `ExceptionGroup` instead of `BaseExceptionGroup` ([#42](https://github.com/achimnol/aiotools/issues/42))


1.5.2 (2022-03-06)
------------------

### Fixes
* Restore the default export of `MultiError` for backward compatibility ([#40](https://github.com/achimnol/aiotools/issues/40))
* Set `current_ptaskgroup` only when `PersistentTaskGroup` is used via the `async with` statement. ([#41](https://github.com/achimnol/aiotools/issues/41))


1.5.1 (2022-03-06)
------------------

### Fixes
* Fix missing naming support of `TaskGroup` in Python 3.11 ([#39](https://github.com/achimnol/aiotools/issues/39))


1.5.0 (2022-03-06)
------------------

### Features
* Add support for Python 3.9's `msg` argument to `Task.cancel()`. ([#32](https://github.com/achimnol/aiotools/issues/32))
* Fix "unexpected cancel" bug in `TaskGroup`. ([#35](https://github.com/achimnol/aiotools/issues/35))
* Rewrite PersistentTaskGroup to use Python 3.11's latest additions such as `Task.uncancel()` and `Task.cancelling()` while still supporting older Python versions ([#36](https://github.com/achimnol/aiotools/issues/36))
* Add `PersistentTaskGroup.all()` to enumerate all non-terminated persistent task groups ([#38](https://github.com/achimnol/aiotools/issues/38))


1.4.0 (2022-01-10)
------------------

### Features
* **ptaskgroup**: Implement `PersistentTaskGroup` ([#30](https://github.com/achimnol/aiotools/issues/30))
* **server**: Expose `process_index` context variable for worker processes ([#31](https://github.com/achimnol/aiotools/issues/31))


1.3.0 (2021-12-19)
------------------

### Fixes
* Add support for Python 3.10. ([#28](https://github.com/achimnol/aiotools/issues/28))

### Documentation Changes
* Fix documentation builds on Python 3.10 and Sphinx 4.x, by removing the 3rd-party autodoc-typehints extension and custom stylesheet overrides. ([#28](https://github.com/achimnol/aiotools/issues/28))


1.2.2 (2021-06-07)
------------------

### Fixes
* **fork:** Handle children's segfault (core-dump with signals) explicitly in `PidfdChildProcess` ([#27](https://github.com/achimnol/aiotools/issues/27))


1.2.1 (2021-01-12)
------------------

### Fixes
* Avoid side effects of custom `clone()` function and resorts back to the combinatino of `os.fork()` and `os.pidfd_open()` for now ([#25](https://github.com/achimnol/aiotools/issues/25))


1.2.0 (2021-01-12)
------------------

### Breaking Changes
* **server:** The `use_threading` argument for `start_server()` is completely deprecated. ([#23](https://github.com/achimnol/aiotools/issues/23))

### Features
* Now the primary target is Python 3.9, though we still support from Python 3.6 ([#22](https://github.com/achimnol/aiotools/issues/22))
* **fork:** Add a new module `fork` to support PID file descriptors in Linux 5.4+ and a POSIX-compatible fallback to asynchornously fork the Python process without signal/PID races. ([#22](https://github.com/achimnol/aiotools/issues/22))
* **server:** Completely rewrote the module using the new `fork` module with handling of various edge cases such as async failures of sibiling child processes ([#23](https://github.com/achimnol/aiotools/issues/23))


1.1.1 (2020-12-16)
------------------

### Fixes
* Fix a potential memory leak with `TaskGroup` when it's used for long-lived asyncio tasks. ([#21](https://github.com/achimnol/aiotools/issues/21))


1.1.0 (2020-10-18)
------------------

### Features
* Add a `current_taskgroup` context-variable to the taskgroup module (only available for Python 3.7 or later)

### Fixes
* Fix missing auto-import of `taskgroup` module exports in the `aiotools` root package.

1.0.0 (2020-10-18)
------------------

### Features
* Adopt an implementation of the taskgroup API as `aiotools.taskgroup` from [EdgeDB](https://github.com/edgedb/edgedb-python/) ([#18](https://github.com/achimnol/aiotools/issues/18))
* Add `timer.VirtualClock` which provides a virtual clock that makes a block of asyncio codes using `asyncio.sleep()` to complete instantly and deterministically ([#19](https://github.com/achimnol/aiotools/issues/19))

### Miscellaneous
* Adopt towncrier for changelog management ([#15](https://github.com/achimnol/aiotools/issues/15))
* Migrate to GitHub Actions for CI ([#19](https://github.com/achimnol/aiotools/issues/19))

0.9.1 (2020-02-25)
------------------

* A maintenance release to fix up the ``defer`` module exports in the ``aiotools`` namespace.

0.9.0 (2020-02-25)
------------------

* **defer:** A new module that emulates Golang's ``defer()`` API with asyncio awareness.

0.8.5 (2019-11-19)
------------------

* **server:** Rewrite internals of the worker main functions to use native `async with`
  instead of manually unrolling `__aenter__()` and `__aexit__()` dunder methods, to keep
  the code simple and avoid potential bugs.

0.8.4 (2019-11-18)
------------------

* Python 3.8 is now officially supported.
* **server:** Fix errors when `multiprocessing.set_start_method("spawn")` is used.
  - NOTE: This is now the default for macOS since Python 3.8.
  - KNOWN ISSUE: [#12](https://github.com/achimnol/aiotools/issues/12)
* Remove some packaging hacks in `__init__.py` and let setuptools read the version
  from a separate `aiotools/VERSION` text file.

0.8.3 (2019-10-07)
------------------

* **context:** Fix `aclosing()`'s `__aexit__()` exception arguments.

0.8.2 (2019-08-28)
------------------

* **context**, **server:** Catch asyncio.CancelledError along with BaseException to
  make the cancellation behavior consistent in Python 3.6, 3.7, and 3.8.

0.8.1 (2019-02-24)
------------------

* **server:** Fix yields of the received stop signal in main/worker context managers
  when using threaded workers.

0.8.0 (2018-11-18)
------------------

* **server:** Updated stop signal handling and now user-defined worker/main context
  managers have a way to distinguish the stop signal received.  See the updated
  docs for more details.

0.7.3 (2018-10-16)
------------------

* This ia a technical release to fix a test case preventing the automated CI
  release procedure.

0.7.2 (2018-10-16)
------------------

* Improve support for Python 3.6/3.7 using a small compatibility module against asyncio.
* func: Add `expire_after` option to `lru_cache()` function.

0.7.1 (2018-08-24)
------------------

* Minor updates to the documentation

0.7.0 (2018-08-24)
------------------

* Add support for Python 3.7
* **context:** Updated to work like Python 3.7
* **context:** Deprecated `AsyncContextDecorator` stuffs in Python 3.7+
* **context:** Added an alias to `contextlib.AsyncExitStack` in the standard library.

0.6.0 (2018-04-10)
------------------

* Introduce a new module `aiotools.iter` with `aiter()` function which
  corresponds to an async version of the builtin `iter()`.

0.5.4 (2018-02-01)
------------------

* **server:** Remove use of unncessary setpgrp syscall, which is also blocked by
  Docker's default seccomp profile!

0.5.3 (2018-01-12)
------------------

* **server:** Ooops! (a finally block should have been an else block)

0.5.2 (2018-01-12)
------------------

* **server:** Improve inner beauty (code readability)
* **server:** Improve reliability and portability of worker-to-main interrupts

0.5.1 (2018-01-11)
------------------

* **server:** Fix a race condition related to handling of worker
  initialization errors with multiple workers

0.5.0 (2017-11-08)
------------------

* **func:** Add `lru_cache()` which is a coroutine version of
  `functools.lru_cache()`

0.4.5 (2017-10-14)
------------------

* **server:** Fix a race condition related to signal handling in the
  multiprocessing module during termination
* **server:** Improve error handling during initialization of workers
  (automatic shutdown of other workers and the main loop after
  logging the exception)

0.4.4 (2017-09-12)
------------------

* Add a new module `aiotools.func` with `apartial()` function which is an
  async version of `functools.partial()` in the standard library

0.4.3 (2017-08-06)
------------------

* Add `aclosing()` context manager like `closing()` in the standard library
* Speed up Travis CI builds for packaging
* Now provide README in rst as well as CHANGES (this file)

0.4.2 (2017-08-01)
------------------

* `server`: Fix spawning subprocesses in child workers
* Add support for `uvloop`

0.4.0 (2017-08-01)
------------------

* Add `use_threading` argument to 
* Add initial documentation (which currently not served
  on readthedocs.io due to Python version problem)

0.3.2 (2017-07-31)
------------------

* Add `extra_procs` argument to `start_server()` function
* Add socket and ZeroMQ server examples
* Improve CI configs

0.3.1 (2017-07-26)
------------------

* Improve CI scripts
* Adopt editorconfig

0.3.0 (2017-04-26)
------------------

* Add `start_server()` function using multiprocessing
  with automatic children lifecycle management
* Clarify the semantics of `AsyncContextGroup` using
  `asyncio.gather()` with `return_exceptions=True`

0.2.0 (2017-04-20)
------------------

* Add abstract types for `AsyncContextManager`
* Rename `AsyncGenContextManager` to `AsyncContextManager`
* Add `AsyncContextGroup`

0.1.1 (2017-04-14)
------------------

* Initial release
