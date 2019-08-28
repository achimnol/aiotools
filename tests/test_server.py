import pytest

import asyncio
import functools
import logging.config
import multiprocessing as mp
import os
import signal
import sys
import threading
import time

import aiotools


@pytest.fixture
def restore_signal():
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


def test_server_singleproc(restore_signal):

    started = mp.Value('i', 0)
    terminated = mp.Value('i', 0)

    def interrupt():
        os.kill(0, signal.SIGINT)

    @aiotools.server
    async def myserver(loop, proc_idx, args):
        nonlocal started, terminated
        assert proc_idx == 0
        assert len(args) == 0
        await asyncio.sleep(0)
        with started.get_lock():
            started.value += 1
        loop.call_later(0.2, interrupt)

        yield

        await asyncio.sleep(0)
        with terminated.get_lock():
            terminated.value += 1

    aiotools.start_server(myserver)

    assert started.value == 1
    assert terminated.value == 1


def test_server_singleproc_threading(restore_signal):

    started = 0
    terminated = 0
    value_lock = threading.Lock()

    def interrupt():
        os.kill(os.getpid(), signal.SIGINT)

    @aiotools.server
    async def myserver(loop, proc_idx, args):
        nonlocal started, terminated
        assert proc_idx == 0
        assert len(args) == 0
        await asyncio.sleep(0)
        with value_lock:
            started += 1
        loop.call_later(0.2, interrupt)

        yield

        await asyncio.sleep(0)
        with value_lock:
            terminated += 1

    aiotools.start_server(myserver, use_threading=True)

    assert started == 1
    assert terminated == 1


def test_server_multiproc(set_timeout, restore_signal):

    started = mp.Value('i', 0)
    terminated = mp.Value('i', 0)
    proc_idxs = mp.Array('i', 3)

    @aiotools.server
    async def myserver(loop, proc_idx, args):
        started, terminated, proc_idxs = args
        await asyncio.sleep(0)
        with started.get_lock():
            started.value += 1
        proc_idxs[proc_idx] = proc_idx

        yield

        await asyncio.sleep(0)
        with terminated.get_lock():
            terminated.value += 1

    def interrupt():
        os.kill(os.getpid(), signal.SIGINT)

    set_timeout(0.2, interrupt)
    aiotools.start_server(myserver, num_workers=3,
                          args=(started, terminated, proc_idxs))

    assert started.value == 3
    assert terminated.value == 3
    assert list(proc_idxs) == [0, 1, 2]
    assert len(mp.active_children()) == 0


@pytest.mark.skipif(os.environ.get('TRAVIS', '') == 'true', reason='on Travis CI')
def test_server_multiproc_custom_stop_signals(set_timeout, restore_signal):

    started = mp.Value('i', 0)
    terminated = mp.Value('i', 0)
    received_signals = mp.Array('i', 2)
    proc_idxs = mp.Array('i', 2)

    @aiotools.server
    async def myserver(loop, proc_idx, args):
        started, terminated, received_signals, proc_idxs = args
        await asyncio.sleep(0)
        with started.get_lock():
            started.value += 1
        proc_idxs[proc_idx] = proc_idx

        received_signals[proc_idx] = yield

        await asyncio.sleep(0)
        with terminated.get_lock():
            terminated.value += 1

    def interrupt():
        os.kill(os.getpid(), signal.SIGUSR1)

    set_timeout(0.2, interrupt)
    aiotools.start_server(myserver, num_workers=2,
                          stop_signals={signal.SIGUSR1},
                          args=(started, terminated, received_signals, proc_idxs))

    assert started.value == 2
    assert terminated.value == 2
    assert list(received_signals) == [signal.SIGUSR1, signal.SIGUSR1]
    assert list(proc_idxs) == [0, 1]
    assert len(mp.active_children()) == 0


@pytest.mark.parametrize('use_threading', [False, True])
def test_server_worker_init_error(restore_signal, use_threading):

    started = mp.Value('i', 0)
    terminated = mp.Value('i', 0)
    log_queue = mp.Queue()

    @aiotools.server
    async def myserver(loop, proc_idx, args):
        started, terminated = args
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

    aiotools.start_server(myserver, num_workers=3,
                          use_threading=use_threading,
                          args=(started, terminated))
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


@pytest.mark.parametrize('use_threading', [False, True])
def test_server_worker_init_error_multi(restore_signal, use_threading):

    started = mp.Value('i', 0)
    terminated = mp.Value('i', 0)
    log_queue = mp.Queue()

    @aiotools.server
    async def myserver(loop, proc_idx, args):
        started, terminated = args
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

    aiotools.start_server(myserver, num_workers=3,
                          use_threading=use_threading,
                          args=(started, terminated))
    # it should automatically shut down!

    # reset logging
    logging.shutdown()

    assert started.value == 1
    # non-errored workers should have been terminated normally.
    assert terminated.value == 1
    # there is one worker remaining -- which is "cancelled"!
    # just ensure that all workers have terminated now.
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


def test_server_user_main(set_timeout, restore_signal):
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
                          args=(123, ))

    assert main_enter
    assert main_exit


@pytest.mark.skipif(os.environ.get('TRAVIS', '') == 'true', reason='on Travis CI')
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


@pytest.mark.skipif(os.environ.get('TRAVIS', '') == 'true', reason='on Travis CI')
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
