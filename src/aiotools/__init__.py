import pkg_resources
try:
    pkg_resources.get_distribution('aiotools')
except pkg_resources.DistributionNotFound:
    pass
else:
    # Import submodules only when installed properly
    from .context import *  # noqa
    from .func import *     # noqa
    from .iter import *     # noqa
    from .timer import *    # noqa
    from .server import *   # noqa

    __all__ = (
        context.__all__,  # type: ignore  # noqa
        func.__all__,     # type: ignore  # noqa
        iter.__all__,     # type: ignore  # noqa
        timer.__all__,    # type: ignore  # noqa
        server.__all__,   # type: ignore  # noqa
    )

__version__ = '0.8.3'
