import pytest

import asyncio
import functools
import logging.config
import multiprocessing as mp
import os
import signal
import struct
import sys
import threading
import time

import aiotools


if os.environ.get('CI', '') and sys.version_info < (3, 9, 0):
    pytest.skip(
        'skipped to prevent kill CI agents due to signals on CI environments',
        allow_module_level=True,
    )


@pytest.fixture
def restore_signal():
    os.setpgrp()
    old_alrm = signal.getsignal(signal.SIGALRM)
    old_intr = signal.getsignal(signal.SIGINT)
    old_term = signal.getsignal(signal.SIGTERM)
    old_intr = signal.getsignal(signal.SIGUSR1)
    yield
    signal.signal(signal.SIGALRM, old_alrm)
    signal.signal(signal.SIGINT, old_intr)
    signal.signal(signal.SIGTERM, old_term)
    signal.signal(signal.SIGUSR1, old_term)


@pytest.fixture
def set_timeout():
    def make_timeout(sec, callback):

        def _callback(signum, frame):
            signal.alarm(0)
            callback()

        signal.signal(signal.SIGALRM, _callback)
        signal.setitimer(signal.ITIMER_REAL, sec)

    yield make_timeout


def interrupt():
    os.kill(0, signal.SIGINT)


def interrupt_usr1():
    os.kill(os.getpid(), signal.SIGUSR1)


@aiotools.server   # type: ignore
async def myserver_simple(loop, proc_idx, args):
    started_wfd, terminated_wfd = args
    assert proc_idx == 0
    await asyncio.sleep(0)
    print('myserver.started')
    os.write(started_wfd, struct.pack('B', proc_idx))
    print('myserver.yield')

    yield

    await asyncio.sleep(0)
    print('myserver.terminated')
    os.write(terminated_wfd, struct.pack('B', proc_idx))


def read_all(fd: int, n: int) -> bytes:
    chars = []
    for _ in range(n):
        print('try read')
        char = os.read(fd, 1)
        print('read', fd, n, char)
        chars.append(char)
    return b''.join(chars)


# @pytest.mark.parametrize('start_method', ['fork', 'spawn'])
# def test_server_singleproc(mocker, set_timeout, restore_signal, start_method):
def test_server_singleproc(mocker, set_timeout, restore_signal):
    started_rfd, started_wfd = os.pipe()
    terminated_rfd, terminated_wfd = os.pipe()

    set_timeout(0.2, interrupt)
    aiotools.start_server(
        myserver_simple,
        args=(started_wfd, terminated_wfd),
    )

    started_pidx = struct.unpack('B', os.read(started_rfd, 1))[0]
    terminated_pidx = struct.unpack('B', os.read(terminated_rfd, 1))[0]

    assert started_pidx == 0
    assert terminated_pidx == 0

    os.close(started_rfd, started_wfd)
    os.close(terminated_rfd, terminated_wfd)


def test_server_multiproc(mocker, set_timeout, restore_signal):
    started_rfd, started_wfd = os.pipe()
    terminated_rfd, terminated_wfd = os.pipe()

    set_timeout(0.2, interrupt)
    aiotools.start_server(
        myserver_simple,
        num_workers=3,
        args=(started_wfd, terminated_wfd),
    )

    # TODO: reading too late after child termination blocks...
    #       let's just use plain files to record things.
    started_pidx_list = struct.unpack('BBB', read_all(started_rfd, 3))
    terminated_pidx_list = struct.unpack('BBB', read_all(terminated_rfd, 3))

    assert set(started_pidx_list) == {0, 1, 2}
    assert set(terminated_pidx_list) == {0, 1, 2}

    os.close(started_rfd, started_wfd)
    os.close(terminated_rfd, terminated_wfd)


@aiotools.server  # type: ignore
async def myserver_multiproc_custom_stop_signals(loop, proc_idx, args):
    started, terminated, received_signals, proc_idxs = args
    await asyncio.sleep(0)
    with started.get_lock():
        started.value += 1
    proc_idxs[proc_idx] = proc_idx

    received_signals[proc_idx] = yield

    await asyncio.sleep(0)
    with terminated.get_lock():
        terminated.value += 1


@pytest.mark.parametrize('start_method', ['fork', 'spawn'])
def test_server_multiproc_custom_stop_signals(
        mocker, set_timeout, restore_signal, start_method):

    mpctx = mp.get_context(start_method)
    mocker.patch('aiotools.server.mp', mpctx)

    started = mpctx.Value('i', 0)
    terminated = mpctx.Value('i', 0)
    received_signals = mpctx.Array('i', 2)
    proc_idxs = mpctx.Array('i', 2)

    set_timeout(0.2, interrupt_usr1)
    aiotools.start_server(myserver_multiproc_custom_stop_signals,
                          num_workers=2,
                          stop_signals={signal.SIGUSR1},
                          args=(started, terminated, received_signals, proc_idxs))

    assert started.value == 2
    assert terminated.value == 2
    assert list(received_signals) == [signal.SIGUSR1, signal.SIGUSR1]
    assert list(proc_idxs) == [0, 1]
    assert len(mpctx.active_children()) == 0


@aiotools.server  # type: ignore
async def myserver_worker_init_error(loop, proc_idx, args):
    started, terminated, log_queue = args
    logging.config.dictConfig({
        'version': 1,
        'handlers': {
            'q': {
                'class': 'logging.handlers.QueueHandler',
                'queue': log_queue,
                'level': 'DEBUG',
            },
            'console': {
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stderr',
                'level': 'DEBUG',
            },
        },
        'loggers': {
            'aiotools': {
                'handlers': ['q', 'console'],
                'level': 'DEBUG',
            },
        },
    })

    with started.get_lock():
        started.value += 1
    if proc_idx == 0:
        # delay until other workers start normally.
        await asyncio.sleep(0.2)
        raise ZeroDivisionError('oops')

    yield

    # should not be reached if errored.
    await asyncio.sleep(0)
    with terminated.get_lock():
        terminated.value += 1


@pytest.mark.parametrize('use_threading,start_method', [
    (True, 'fork'),
    (False, 'fork'),
    (False, 'spawn'),
])
def test_server_worker_init_error(
        mocker, restore_signal, use_threading, start_method):

    mpctx = mp.get_context(start_method)
    mocker.patch('aiotools.server.mp', mpctx)

    started = mpctx.Value('i', 0)
    terminated = mpctx.Value('i', 0)
    log_queue = mpctx.Queue()

    aiotools.start_server(myserver_worker_init_error,
                          num_workers=3,
                          use_threading=use_threading,
                          args=(started, terminated, log_queue))
    # it should automatically shut down!

    # reset logging
    logging.shutdown()

    assert started.value == 3
    # workers who did not raise errors have already started,
    # and they should have terminated normally
    # when the errorneous worker interrupted the main loop.
    assert terminated.value == 2
    assert len(mp.active_children()) == 0
    assert not log_queue.empty()
    has_error_log = False
    while not log_queue.empty():
        rec = log_queue.get()
        if rec.levelname == 'ERROR':
            has_error_log = True
            assert 'initialization' in rec.message
            # exception info is logged to the console,
            # but we cannot access it here because exceptions
            # are not picklable.
            assert rec.exc_info is None
    assert has_error_log


@aiotools.server  # type: ignore
async def myserver_worker_init_error_multi(loop, proc_idx, args):
    started, terminated, log_queue = args
    logging.config.dictConfig({
        'version': 1,
        'handlers': {
            'q': {
                'class': 'logging.handlers.QueueHandler',
                'queue': log_queue,
                'level': 'DEBUG',
            },
            'console': {
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stderr',
                'level': 'DEBUG',
            },
        },
        'loggers': {
            'aiotools': {
                'handlers': ['q', 'console'],
                'level': 'DEBUG',
            },
        },
    })
    # make the error timing to spread over some time
    await asyncio.sleep(0.2 * proc_idx)
    if proc_idx == 1:
        raise ZeroDivisionError('oops')
    with started.get_lock():
        started.value += 1

    yield

    # should not be reached if errored.
    await asyncio.sleep(0)
    with terminated.get_lock():
        terminated.value += 1


@pytest.mark.parametrize('use_threading,start_method', [
    (True, 'fork'),
    (False, 'fork'),
    (False, 'spawn'),
])
def test_server_worker_init_error_multi(
        mocker, restore_signal, use_threading, start_method):

    mpctx = mp.get_context(start_method)
    mocker.patch('aiotools.server.mp', mpctx)

    started = mpctx.Value('i', 0)
    terminated = mpctx.Value('i', 0)
    log_queue = mpctx.Queue()

    aiotools.start_server(myserver_worker_init_error_multi,
                          num_workers=3,
                          use_threading=use_threading,
                          args=(started, terminated, log_queue))
    # it should automatically shut down!

    # reset logging
    logging.shutdown()

    assert started.value >= 1
    # non-errored workers should have been terminated normally.
    assert terminated.value >= 1
    # there is one worker remaining -- which is "cancelled"!
    # just ensure that all workers have terminated now.
    assert len(mpctx.active_children()) == 0
    assert not log_queue.empty()
    has_error_log = False
    while not log_queue.empty():
        rec = log_queue.get()
        if rec.levelname == 'ERROR':
            has_error_log = True
            assert 'initialization' in rec.message
            # exception info is logged to the console,
            # but we cannot access it here because exceptions
            # are not picklable.
            assert rec.exc_info is None
    assert has_error_log


def test_server_multiproc_threading(set_timeout, restore_signal):

    started = 0
    terminated = 0
    proc_idxs = [0, 0, 0]
    value_lock = threading.Lock()

    @aiotools.server
    async def myserver(loop, proc_idx, args):
        nonlocal started, terminated, proc_idxs
        await asyncio.sleep(0)
        with value_lock:
            started += 1
            proc_idxs[proc_idx] = proc_idx

        yield

        await asyncio.sleep(0)
        with value_lock:
            terminated += 1

    def interrupt():
        os.kill(os.getpid(), signal.SIGINT)

    set_timeout(0.2, interrupt)
    aiotools.start_server(myserver, num_workers=3, use_threading=True)

    assert started == 3
    assert terminated == 3
    assert list(proc_idxs) == [0, 1, 2]


@pytest.mark.parametrize('start_method', ['fork'])
def test_server_user_main(mocker, set_timeout, restore_signal, start_method):

    mpctx = mp.get_context(start_method)
    mocker.patch('aiotools.server.mp', mpctx)

    main_enter = False
    main_exit = False

    # FIXME: This should work with start_method = "spawn", but to test with it
    #        we need to allow passing arguments to user-provided main functions.

    @aiotools.main
    def mymain_user_main():
        nonlocal main_enter, main_exit
        main_enter = True
        yield 987
        main_exit = True

    @aiotools.server  # type: ignore
    async def myworker_user_main(loop, proc_idx, args):
        assert args[0] == 987  # first arg from user main
        assert args[1] == 123  # second arg from start_server args
        yield

    set_timeout(0.2, interrupt)
    aiotools.start_server(myworker_user_main,
                          mymain_user_main,
                          num_workers=3,
                          args=(123, ))

    assert main_enter
    assert main_exit


def test_server_user_main_custom_stop_signals(set_timeout, restore_signal):
    main_enter = False
    main_exit = False
    main_signal = None
    worker_signals = mp.Array('i', 3)

    @aiotools.main
    def mymain():
        nonlocal main_enter, main_exit, main_signal
        main_enter = True
        main_signal = yield
        main_exit = True

    @aiotools.server
    async def myworker(loop, proc_idx, args):
        worker_signals = args[0]
        worker_signals[proc_idx] = yield

    def interrupt():
        os.kill(os.getpid(), signal.SIGUSR1)

    def noop(signum, frame):
        pass

    set_timeout(0.2, interrupt)
    aiotools.start_server(myworker, mymain, num_workers=3,
                          stop_signals={signal.SIGUSR1},
                          args=(worker_signals, ))

    assert main_enter
    assert main_exit
    assert main_signal == signal.SIGUSR1
    assert list(worker_signals) == [signal.SIGUSR1] * 3


def test_server_user_main_tuple(set_timeout, restore_signal):
    main_enter = False
    main_exit = False

    @aiotools.main
    def mymain():
        nonlocal main_enter, main_exit
        main_enter = True
        yield 987, 654
        main_exit = True

    @aiotools.server
    async def myworker(loop, proc_idx, args):
        assert args[0] == 987  # first arg from user main
        assert args[1] == 654  # second arg from user main
        assert args[2] == 123  # third arg from start_server args
        yield

    def interrupt():
        os.kill(os.getpid(), signal.SIGINT)

    set_timeout(0.2, interrupt)
    aiotools.start_server(myworker, mymain, num_workers=3,
                          args=(123, ))

    assert main_enter
    assert main_exit


def test_server_user_main_threading(set_timeout, restore_signal):
    main_enter = False
    main_exit = False

    @aiotools.main
    def mymain():
        nonlocal main_enter, main_exit
        main_enter = True
        yield 987
        main_exit = True

    @aiotools.server
    async def myworker(loop, proc_idx, args):
        assert args[0] == 987  # first arg from user main
        assert args[1] == 123  # second arg from start_server args
        yield

    def interrupt():
        os.kill(os.getpid(), signal.SIGINT)

    set_timeout(0.2, interrupt)
    aiotools.start_server(myworker, mymain, num_workers=3,
                          use_threading=True,
                          args=(123, ))

    assert main_enter
    assert main_exit


def test_server_extra_proc(set_timeout, restore_signal):

    extras = mp.Array('i', [0, 0])

    def extra_proc(key, _, pidx, args):
        assert _ is None
        extras[key] = 980 + key
        try:
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            print(f'extra[{key}] interrupted', file=sys.stderr)
        except Exception as e:
            print(f'extra[{key}] exception', e, file=sys.stderr)
        finally:
            print(f'extra[{key}] finish', file=sys.stderr)
            extras[key] = 990 + key

    @aiotools.server
    async def myworker(loop, pidx, args):
        yield

    def interrupt():
        os.kill(os.getpid(), signal.SIGINT)

    set_timeout(0.2, interrupt)
    aiotools.start_server(myworker, extra_procs=[
                              functools.partial(extra_proc, 0),
                              functools.partial(extra_proc, 1)],
                          num_workers=3, args=(123, ))

    assert extras[0] == 990
    assert extras[1] == 991


def test_server_extra_proc_custom_stop_signal(set_timeout, restore_signal):

    received_signals = mp.Array('i', [0, 0])

    def extra_proc(key, _, pidx, args):
        received_signals = args[0]
        try:
            while True:
                time.sleep(0.1)
        except aiotools.InterruptedBySignal as e:
            received_signals[key] = e.args[0]

    @aiotools.server
    async def myworker(loop, pidx, args):
        yield

    def interrupt():
        os.kill(os.getpid(), signal.SIGUSR1)

    set_timeout(0.3, interrupt)
    aiotools.start_server(myworker, extra_procs=[
                              functools.partial(extra_proc, 0),
                              functools.partial(extra_proc, 1)],
                          stop_signals={signal.SIGUSR1},
                          args=(received_signals, ),
                          num_workers=3)

    assert received_signals[0] == signal.SIGUSR1
    assert received_signals[1] == signal.SIGUSR1


def test_server_extra_proc_threading(set_timeout, restore_signal):

    # When using extra_procs with threading, you need to provide a way to
    # explicitly interrupt your synchronous loop.
    # Here, we use a threading.Event object to signal interruption.

    extras = [0, 0]
    value_lock = threading.Lock()

    def extra_proc(key, intr_event, pidx, args):
        assert isinstance(intr_event, threading.Event)
        with value_lock:
            extras[key] = 980 + key
        try:
            while not intr_event.is_set():
                time.sleep(0.1)
        except Exception as e:
            print(f'extra[{key}] exception', e)
        finally:
            with value_lock:
                extras[key] = 990 + key

    @aiotools.server
    async def myworker(loop, pidx, args):
        yield

    def interrupt():
        os.kill(os.getpid(), signal.SIGINT)

    set_timeout(0.2, interrupt)
    aiotools.start_server(myworker, extra_procs=[
                              functools.partial(extra_proc, 0),
                              functools.partial(extra_proc, 1)],
                          use_threading=True,
                          num_workers=3, args=(123, ))

    assert extras[0] == 990
    assert extras[1] == 991
