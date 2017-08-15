import asyncio


def pytest_addoption(parser):
    parser.addoption('--loop-policy', action='store',
                     default=None, help='set event loop policy')


def pytest_collection_modifyitems(config, items):
    # TODO: We could skip some tests regarding the specific type of
    # event loop policy.
    loop_policy = config.getoption('--loop-policy')
    if loop_policy == 'uvloop':
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        return
    elif loop_policy == 'tokio':
        # import tokio
        # asyncio.set_event_loop_policy(tokio.EventLoopPolicy())
        raise NotImplementedError('tokio module is not setup properly.')
    return
