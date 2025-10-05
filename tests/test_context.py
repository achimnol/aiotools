from __future__ import annotations

import asyncio
import sys
import warnings
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import suppress
from contextvars import ContextVar

import pytest

import aiotools
from aiotools.context import (
    AbstractAsyncContextManager,
    resetting,
)

my_variable: ContextVar[int] = ContextVar("my_variable")


def test_resetting_ctxvar() -> None:
    with pytest.raises(LookupError):
        my_variable.get()
    with resetting(my_variable, 1):
        assert my_variable.get() == 1
        with resetting(my_variable, 2):
            assert my_variable.get() == 2
        assert my_variable.get() == 1
    with pytest.raises(LookupError):
        my_variable.get()

    # should behave the same way even when an exception occurs
    with suppress(RuntimeError):
        with resetting(my_variable, 10):
            assert my_variable.get() == 10
            raise RuntimeError("oops")
    with pytest.raises(LookupError):
        my_variable.get()


@pytest.mark.asyncio
async def test_resetting_ctxvar_async() -> None:
    with pytest.raises(LookupError):
        my_variable.get()
    async with resetting(my_variable, 1):
        assert my_variable.get() == 1
        async with resetting(my_variable, 2):
            assert my_variable.get() == 2
        assert my_variable.get() == 1
    with pytest.raises(LookupError):
        my_variable.get()

    # should behave the same way even when an exception occurs
    with suppress(RuntimeError):
        async with resetting(my_variable, 10):
            assert my_variable.get() == 10
            raise RuntimeError("oops")
    with pytest.raises(LookupError):
        my_variable.get()


def test_actxmgr_types() -> None:
    from typing import Any

    assert issubclass(aiotools.AsyncContextManager, AbstractAsyncContextManager)

    class boilerplate_ctx:
        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, *exc_info: Any) -> None:
            return None

    @aiotools.actxmgr
    async def simple_ctx() -> AsyncIterator[None]:
        yield

    async def simple_agen() -> AsyncGenerator[None]:
        yield

    assert issubclass(boilerplate_ctx, AbstractAsyncContextManager)
    assert isinstance(simple_ctx(), AbstractAsyncContextManager)
    assert not isinstance(simple_agen(), AbstractAsyncContextManager)


@pytest.mark.asyncio
async def test_actxmgr() -> None:
    step = 0

    @aiotools.actxmgr
    async def simple_ctx(msg: str) -> AsyncIterator[str]:
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        step = 2
        try:
            yield msg
            step = 3
        finally:
            await asyncio.sleep(0)
            step = 4

    step = 0
    async with simple_ctx("hello") as msg:
        assert step == 2
        assert msg == "hello"
    assert step == 4

    step = 0
    try:
        async with simple_ctx("world") as msg:
            assert step == 2
            assert msg == "world"
            await asyncio.sleep(0)
            raise ValueError("something wrong")
    except Exception as e:
        await asyncio.sleep(0)
        assert e.args[0] == "something wrong"
        assert step == 4


@pytest.mark.skipif(
    sys.version_info >= (3, 7, 0), reason="Deprecated in Python 3.7 or higher"
)
@pytest.mark.asyncio
async def test_actxmgr_reuse() -> None:
    @aiotools.actxmgr
    async def simple_ctx(msg: str) -> AsyncIterator[str]:
        yield msg

    cm = simple_ctx("hello")

    async with cm as msg:
        assert msg == "hello"

    try:
        async with cm as msg:
            assert msg == "hello"
    except BaseException as exc:
        assert isinstance(exc, RuntimeError)
        assert "didn't yield" in exc.args[0]

    cm = cm._recreate_cm()

    async with cm as msg:
        assert msg == "hello"


@pytest.mark.asyncio
async def test_actxmgr_exception_in_context_body() -> None:
    # Exceptions raised in the context body
    # should be transparently raised.

    @aiotools.actxmgr
    async def simple_ctx(msg: str) -> AsyncIterator[str]:
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    with pytest.raises(ZeroDivisionError):
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
            raise ZeroDivisionError

    exc = RuntimeError("oops")
    try:
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
            raise exc
    except BaseException as e:
        assert e is exc
        assert e.args[0] == "oops"
    else:
        pytest.fail()

    cm = simple_ctx("hello")
    ret = await cm.__aenter__()
    assert ret == "hello"
    ret = await cm.__aexit__(ValueError, None, None)
    assert not ret


@pytest.mark.asyncio
async def test_actxmgr_exception_in_initialization() -> None:
    # Any exception before first yield is just transparently
    # raised out to the context block.

    @aiotools.actxmgr
    async def simple_ctx1(msg: str) -> AsyncIterator[str]:
        await asyncio.sleep(0)
        raise ZeroDivisionError
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    try:
        async with simple_ctx1("hello") as msg:
            assert msg == "hello"
    except ZeroDivisionError:
        pass
    else:
        pytest.fail()

    exc = RuntimeError("oops")

    @aiotools.actxmgr
    async def simple_ctx2(msg: str) -> AsyncIterator[str]:
        await asyncio.sleep(0)
        raise exc
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    try:
        async with simple_ctx2("hello") as msg:
            assert msg == "hello"
    except BaseException as e:
        assert e is exc
        assert e.args[0] == "oops"
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_exception_in_finalization() -> None:
    @aiotools.actxmgr
    async def simple_ctx1(msg: str) -> AsyncIterator[str]:
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)
        raise ZeroDivisionError
        await asyncio.sleep(0)

    try:
        async with simple_ctx1("hello") as msg:
            assert msg == "hello"
    except ZeroDivisionError:
        pass
    else:
        pytest.fail()

    exc = RuntimeError("oops")

    @aiotools.actxmgr
    async def simple_ctx2(msg: str) -> AsyncIterator[str]:
        yield msg
        raise exc

    try:
        async with simple_ctx2("hello") as msg:
            assert msg == "hello"
    except BaseException as e:
        assert e is exc
        assert e.args[0] == "oops"
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_exception_uncaught() -> None:
    @aiotools.actxmgr
    async def simple_ctx(msg: str) -> AsyncIterator[str]:
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    try:
        async with simple_ctx("hello"):
            raise IndexError("bomb")
    except BaseException as e:
        assert isinstance(e, IndexError)
        assert e.args[0] == "bomb"


@pytest.mark.asyncio
async def test_actxmgr_exception_nested() -> None:
    @aiotools.actxmgr
    async def simple_ctx(msg: str) -> AsyncIterator[str]:
        yield msg

    try:
        async with simple_ctx("hello") as msg1:
            async with simple_ctx("world") as msg2:
                assert msg1 == "hello"
                assert msg2 == "world"
                raise IndexError("bomb1")
    except BaseException as exc:
        assert isinstance(exc, IndexError)
        assert "bomb1" == exc.args[0]
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_exception_chained() -> None:
    @aiotools.actxmgr
    async def simple_ctx(msg: str) -> AsyncIterator[str]:
        try:
            await asyncio.sleep(0)
            yield msg
        except Exception as e:
            await asyncio.sleep(0)
            # exception is chained
            raise ValueError("bomb2") from e

    try:
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
            raise IndexError("bomb1")
    except BaseException as exc:
        assert isinstance(exc, ValueError)
        assert "bomb2" == exc.args[0]
        assert isinstance(exc.__cause__, IndexError)
        assert "bomb1" == exc.__cause__.args[0]
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_exception_replaced() -> None:
    @aiotools.actxmgr
    async def simple_ctx(msg: str) -> AsyncIterator[str]:
        try:
            await asyncio.sleep(0)
            yield msg
        except Exception:
            await asyncio.sleep(0)
            # exception is replaced
            raise ValueError("bomb2")

    try:
        async with simple_ctx("hello") as msg:
            assert msg == "hello"
            raise IndexError("bomb1")
    except BaseException as exc:
        assert isinstance(exc, ValueError)
        assert "bomb2" == exc.args[0]
        assert exc.__cause__ is None
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_stopaiter() -> None:
    @aiotools.actxmgr
    async def simple_ctx1() -> AsyncIterator[int]:
        await asyncio.sleep(0)
        yield 1
        await asyncio.sleep(0)
        yield 2
        await asyncio.sleep(0)

    cm = simple_ctx1()
    ret_enter = await cm.__aenter__()
    assert ret_enter == 1
    ret_exit = await cm.__aexit__(StopAsyncIteration, None, None)
    assert ret_exit is False

    step = 0

    @aiotools.actxmgr
    async def simple_ctx2() -> AsyncIterator[None]:
        nonlocal step
        step = 1
        try:
            await asyncio.sleep(0)
            yield
        finally:
            step = 2
            await asyncio.sleep(0)
            raise StopAsyncIteration("x")

    try:
        step = 0
        async with simple_ctx2():
            assert step == 1
    except RuntimeError:
        # converted to RuntimeError
        assert step == 2
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_transparency() -> None:
    step = 0

    @aiotools.actxmgr
    async def simple_ctx1() -> AsyncIterator[None]:
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        yield
        step = 2
        await asyncio.sleep(0)

    exc = StopAsyncIteration("x")
    try:
        step = 0
        async with simple_ctx1():
            assert step == 1
            raise exc
    except StopAsyncIteration as e:
        # exception is not handled
        assert e is exc
        assert step == 1
    else:
        pytest.fail()

    @aiotools.actxmgr
    async def simple_ctx2() -> AsyncIterator[None]:
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        yield
        step = 2
        await asyncio.sleep(0)

    try:
        step = 0
        async with simple_ctx2():
            assert step == 1
            raise ValueError
    except ValueError:
        # exception is not handled
        assert step == 1
    else:
        pytest.fail()

    @aiotools.actxmgr
    async def simple_ctx3() -> AsyncIterator[None]:
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        try:
            yield
        finally:
            step = 2
            await asyncio.sleep(0)

    exc = StopAsyncIteration("x")
    try:
        step = 0
        async with simple_ctx3():
            assert step == 1
            raise exc
    except StopAsyncIteration as e:
        # exception is not handled,
        # but context is finalized
        assert e is exc
        assert step == 2
    else:
        pytest.fail()

    @aiotools.actxmgr
    async def simple_ctx4() -> AsyncIterator[None]:
        nonlocal step
        step = 1
        await asyncio.sleep(0)
        try:
            yield
        finally:
            step = 2
            await asyncio.sleep(0)

    try:
        step = 0
        async with simple_ctx4():
            assert step == 1
            raise ValueError
    except ValueError:
        # exception is not handled,
        # but context is finalized
        assert step == 2
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_no_stop() -> None:
    @aiotools.actxmgr
    async def simple_ctx1(msg: str) -> AsyncIterator[str]:
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)
        yield msg
        await asyncio.sleep(0)

    try:
        async with simple_ctx1("hello") as msg:
            assert msg == "hello"
    except RuntimeError as exc:
        assert "didn't stop" in exc.args[0]
    else:
        pytest.fail()

    try:
        async with simple_ctx1("hello") as msg:
            assert msg == "hello"
            raise ValueError("oops")
    except ValueError as exc:
        assert exc.args[0] == "oops"
    else:
        pytest.fail()

    @aiotools.actxmgr
    async def simple_ctx2(msg: str) -> AsyncIterator[str]:
        try:
            await asyncio.sleep(0)
            yield msg
        finally:
            await asyncio.sleep(0)
            yield msg

    try:
        async with simple_ctx2("hello") as msg:
            assert msg == "hello"
            raise ValueError("oops")
    except RuntimeError as exc:
        assert "didn't stop after" in exc.args[0]
    else:
        pytest.fail()


@pytest.mark.asyncio
async def test_actxmgr_no_yield() -> None:
    @aiotools.actxmgr  # type: ignore[arg-type]
    async def no_yield_ctx1(msg: str) -> AsyncIterator[None]:  # type: ignore[empty-body]
        pass

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        try:
            async with no_yield_ctx1("hello"):
                pass
        except RuntimeError as exc:
            assert "must be an async-gen" in exc.args[0]
        except AttributeError:  # in Python 3.7
            pass
        except TypeError as exc:  # in Python 3.10
            assert "not an async iterator" in exc.args[0]
        else:
            pytest.fail()


@pytest.mark.asyncio
async def test_actxgroup_exception_from_cm() -> None:
    @aiotools.actxmgr
    async def ctx1(a: int) -> AsyncIterator[int]:
        await asyncio.sleep(0)
        raise asyncio.CancelledError
        await asyncio.sleep(0)
        yield a

    @aiotools.actxmgr
    async def ctx2(a: int) -> AsyncIterator[int]:
        await asyncio.sleep(0)
        raise ZeroDivisionError
        await asyncio.sleep(0)
        yield a

    ctxgrp = aiotools.actxgroup([ctx1(1), ctx2(2)])

    async with ctxgrp as values:
        assert isinstance(values[0], asyncio.CancelledError)
        assert isinstance(values[1], ZeroDivisionError)

    @aiotools.actxmgr
    async def ctx3(a: int) -> AsyncIterator[int]:
        yield a
        raise asyncio.CancelledError

    @aiotools.actxmgr
    async def ctx4(a: int) -> AsyncIterator[int]:
        yield a
        raise ZeroDivisionError

    ctxgrp = aiotools.actxgroup([ctx3(1), ctx4(2)])

    async with ctxgrp as values:
        assert values[0] == 1
        assert values[1] == 2

    exits = ctxgrp.exit_states()
    assert isinstance(exits[0], asyncio.CancelledError)
    assert isinstance(exits[1], ZeroDivisionError)


@pytest.mark.asyncio
async def test_actxgroup_exception_from_body() -> None:
    exit_count = 0

    @aiotools.actxmgr
    async def ctx1(a: int) -> AsyncIterator[int]:
        nonlocal exit_count
        await asyncio.sleep(0)
        yield a
        await asyncio.sleep(0)
        # yield raises the exception from the context body.
        # If not handled, finalization will not be executed.
        exit_count += 1

    ctxgrp = aiotools.actxgroup([ctx1(1), ctx1(2)])

    try:
        async with ctxgrp as values:
            assert values[0] == 1
            assert values[1] == 2
            raise ZeroDivisionError
    except Exception as e:
        assert isinstance(e, ZeroDivisionError)

    exits = ctxgrp.exit_states()
    assert not exits[0]  # __aexit__ are called successfully
    assert not exits[1]
    assert exit_count == 0  # but they errored internally

    exit_count = 0

    @aiotools.actxmgr
    async def ctx2(a: int) -> AsyncIterator[int]:
        nonlocal exit_count
        try:
            await asyncio.sleep(0)
            yield a
        finally:
            await asyncio.sleep(0)
            # Ensure finalization is executed.
            exit_count += 1

    ctxgrp = aiotools.actxgroup([ctx2(1), ctx2(2)])

    try:
        async with ctxgrp as values:
            assert values[0] == 1
            assert values[1] == 2
            raise ZeroDivisionError
    except Exception as e:
        assert isinstance(e, ZeroDivisionError)

    exits = ctxgrp.exit_states()
    assert not exits[0]  # __aexit__ are called successfully
    assert not exits[1]
    assert exit_count == 2  # they also suceeeded


@pytest.mark.asyncio
async def test_aclosing() -> None:
    finalized = False

    async def myiter() -> AsyncGenerator[int]:
        nonlocal finalized
        try:
            yield 1
            await asyncio.sleep(0.2)
            yield 10
            await asyncio.sleep(0.2)
            yield 100
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)
            finalized = True
            raise

    mysum = 0
    detect_cancellation = False

    async def mytask() -> None:
        nonlocal mysum, detect_cancellation
        try:
            async with aiotools.aclosing(myiter()) as agen:
                async for x in agen:
                    mysum += x
        except asyncio.CancelledError:
            detect_cancellation = True

    t = asyncio.create_task(mytask())
    await asyncio.sleep(0.3)
    t.cancel()
    await t

    assert mysum == 11
    assert finalized
    assert detect_cancellation
