from __future__ import annotations

import asyncio
import sys
from typing import Any, Callable, Coroutine

__all__ = (
    "LoopFactory",
    "Runner",
    "get_fast_loop_factory",
    "get_fast_runner",
)

# Type alias for event loop factories (for use with asyncio.Runner)
LoopFactory = Callable[[], asyncio.AbstractEventLoop]

# Type alias for event loop runners (asyncio.run, uvloop.run, winloop.run)
Runner = Callable[[Coroutine[Any, Any, None]], None]


def get_fast_loop_factory() -> LoopFactory | None:
    """
    Return the fastest available event loop factory for the current platform.

    This function returns a loop factory suitable for use with Python 3.11+'s
    :class:`asyncio.Runner` API:

    - On Windows: Returns ``winloop.new_event_loop`` if winloop is installed
    - On Unix/Linux/macOS: Returns ``uvloop.new_event_loop`` if uvloop is installed
    - Returns ``None`` if no fast event loop is available (use default)

    Returns:
        A loop factory callable, or ``None`` to use the default event loop.

    Example:
        Using with ``asyncio.Runner``::

            import asyncio
            from aiotools.loop import get_fast_loop_factory

            async def main():
                print("Hello from fast event loop!")

            with asyncio.Runner(loop_factory=get_fast_loop_factory()) as runner:
                runner.run(main())

    .. versionadded:: 2.3.0
    """
    if sys.platform == "win32":
        try:
            import winloop

            return winloop.new_event_loop
        except ImportError:
            pass
    else:
        try:
            import uvloop

            return uvloop.new_event_loop
        except ImportError:
            pass
    return None


def get_fast_runner() -> Runner:
    """
    Return the fastest available event loop runner for the current platform.

    This function attempts to import and return the appropriate fast event loop
    runner based on the platform:

    - On Windows: Returns ``winloop.run`` if winloop is installed
    - On Unix/Linux/macOS: Returns ``uvloop.run`` if uvloop is installed
    - Falls back to ``asyncio.run`` if no fast event loop is available

    Returns:
        A callable that can be used as the ``runner`` parameter in
        :func:`aiotools.start_server` or called directly to run a coroutine.

    Example:
        Using with ``start_server``::

            from aiotools import start_server
            from aiotools.loop import get_fast_runner

            start_server(
                main_func,
                num_workers=4,
                runner=get_fast_runner(),
            )

        Using directly::

            from aiotools.loop import get_fast_runner

            async def main():
                print("Hello, fast event loop!")

            runner = get_fast_runner()
            runner(main())
    """
    if sys.platform == "win32":
        try:
            import winloop

            return winloop.run
        except ImportError:
            pass
    else:
        try:
            import uvloop

            return uvloop.run
        except ImportError:
            pass
    return asyncio.run
