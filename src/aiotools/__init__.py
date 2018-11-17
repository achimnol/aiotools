import pkg_resources
try:
    pkg_resources.get_distribution('aiotools')
except pkg_resources.DistributionNotFound:
    pass
else:
    # Import submodules only when installed properly
    from .context import *  # noqa
    from .func import *  # noqa
    from .iter import *  # noqa
    from .timer import *  # noqa
    from .server import *  # noqa

    __all__ = (
        context.__all__,  # noqa
        func.__all__,  # noqa
        iter.__all__,  # noqa
        timer.__all__,  # noqa
        server.__all__,  # noqa
    )

__version__ = '0.8.0'
