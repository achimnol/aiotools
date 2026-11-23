import asyncio
from collections.abc import Awaitable, Generator
from typing import Any

import pytest

from aiotools.defer import AsyncDeferFunc, adefer


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["future", "task", "custom"])
async def test_adefer_awaitable_object(kind: str) -> None:
    events: list[str] = []
    loop = asyncio.get_running_loop()

    async def cleanup() -> None:
        await asyncio.sleep(0)
        events.append("cleanup")

    class CleanupAwaitable:
        def __await__(self) -> Generator[Any, None, None]:
            return cleanup().__await__()

    if kind == "future":
        future: asyncio.Future[None] = loop.create_future()

        def finish() -> None:
            events.append("cleanup")
            future.set_result(None)

        loop.call_soon(finish)
        deferred: Awaitable[None] = future
    elif kind == "task":
        deferred = asyncio.create_task(cleanup())
    else:
        deferred = CleanupAwaitable()

    @adefer
    async def work(defer: AsyncDeferFunc) -> str:
        defer(lambda: events.append("last"))
        defer(deferred)
        events.append("body")
        return "result"

    try:
        assert await work() == "result"
        assert events == ["body", "cleanup", "last"]
    finally:
        if isinstance(deferred, asyncio.Task):
            deferred.cancel()
            await asyncio.gather(deferred, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["async_instance", "factory"])
async def test_adefer_callable_returning_awaitable(kind: str) -> None:
    events: list[str] = []

    async def cleanup() -> None:
        await asyncio.sleep(0)
        events.append("cleanup")

    class AsyncCleanup:
        async def __call__(self) -> None:
            await cleanup()

    callback = AsyncCleanup() if kind == "async_instance" else lambda: cleanup()

    @adefer
    async def work(defer: AsyncDeferFunc) -> None:
        defer(lambda: events.append("last"))
        defer(callback)
        events.append("body")

    await work()
    assert events == ["body", "cleanup", "last"]


@pytest.mark.asyncio
async def test_adefer_returned_awaitable_error_stops_cleanup() -> None:
    events: list[str] = []
    error = RuntimeError("cleanup failed")

    async def cleanup() -> None:
        await asyncio.sleep(0)
        raise error

    @adefer
    async def work(defer: AsyncDeferFunc) -> None:
        defer(lambda: events.append("not reached"))
        defer(lambda: cleanup())

    with pytest.raises(RuntimeError) as raised:
        await work()
    assert raised.value is error
    assert events == []


@pytest.mark.asyncio
async def test_adefer_awaitable_cleanup_preserves_body_error() -> None:
    events: list[str] = []
    error = ValueError("body failed")

    async def cleanup() -> None:
        await asyncio.sleep(0)
        events.append("cleanup")

    @adefer
    async def work(defer: AsyncDeferFunc) -> None:
        defer(lambda: cleanup())
        raise error

    with pytest.raises(ValueError) as raised:
        await work()
    assert raised.value is error
    assert events == ["cleanup"]


@pytest.mark.asyncio
async def test_adefer_returned_future_cancellation_stops_cleanup() -> None:
    events: list[str] = []
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.cancel()

    @adefer
    async def work(defer: AsyncDeferFunc) -> None:
        defer(lambda: events.append("not reached"))
        defer(lambda: future)

    with pytest.raises(asyncio.CancelledError):
        await work()
    assert events == []
