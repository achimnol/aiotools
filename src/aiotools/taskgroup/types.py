from types import TracebackType
from typing import Protocol


class AsyncExceptionHandler(Protocol):
    """
    A shorthand for an async exception handler type.
    This is always called under exception context where
    :func:`sys.exc_info()` is available.
    """

    async def __call__(
        self,
        exc_type: type[Exception],
        exc_obj: Exception,
        exc_tb: TracebackType,
    ) -> None: ...


class MultiError(ExceptionGroup):
    def __init__(self, msg: str, errors=()) -> None:
        super().__init__(msg, errors)
        self.__errors__ = errors

    def get_error_types(self) -> set[type[Exception]]:
        return {type(e) for e in self.exceptions}


class TaskGroupError(MultiError):
    pass
