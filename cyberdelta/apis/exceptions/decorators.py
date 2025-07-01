"""Decorator-related exceptions for CyberDelta.

These exceptions handle errors that occur when applying decorators
to functions or methods.
"""


class DecoratorError(Exception):
    """Base class for decorator-related errors."""

    def __init__(
        self,
        message: str,
        *,
        decorator_name: str | None = None,
        target_function: str | None = None,
        **metadata: object,
    ) -> None:
        """Initialize decorator error.

        Args:
            message: Human-readable error description
            decorator_name: Name of the decorator
            target_function: Name of the function being decorated
            **metadata: Additional error context
        """
        super().__init__(message)
        self.decorator_name = decorator_name
        self.target_function = target_function
        self.metadata = metadata


class AsyncDecoratorError(TypeError, DecoratorError):
    """Raised when a decorator that requires async functions is applied to a sync function."""

    def __init__(self, decorator_name: str, target_function: str | None = None) -> None:
        """Initialize async decorator error.

        Args:
            decorator_name: Name of the decorator that requires async
            target_function: Name of the function that is not async
        """
        message = f"{decorator_name} decorator can only be applied to async functions"
        if target_function:
            message = f"{message} (attempted on '{target_function}')"

        super().__init__(message)
        DecoratorError.__init__(
            self,
            message,
            decorator_name=decorator_name,
            target_function=target_function,
        )


class NoExceptionCapturedError(RuntimeError):
    """Raised when retry logic completes without capturing an exception."""

    def __init__(self) -> None:
        """Initialize no exception captured error."""
        super().__init__("No exception captured")
