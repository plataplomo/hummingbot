"""Base exception classes for portfolio management."""

from __future__ import annotations

from typing import Any


class CoreError(Exception):
    """Base exception for all portfolio-related errors."""

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        context: dict[str, Any] | None = None,
        recoverable: bool = True,
    ) -> None:
        """Initialize portfolio exception.

        Args:
            message: Error message
            error_code: Optional error code for categorization
            context: Additional context information
            recoverable: Whether the error is recoverable
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self._get_default_error_code()
        self.context = context or {}
        self.recoverable = recoverable

    def _get_default_error_code(self) -> str:
        """Get default error code based on exception class.

        Returns:
            Default error code string.
        """
        return f"PORTFOLIO_{self.__class__.__name__.upper()}"

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for logging/serialization.

        Returns:
            Dictionary representation of the exception.
        """
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "context": self.context,
            "recoverable": self.recoverable,
        }

    def __str__(self) -> str:
        """String representation of the exception.

        Returns:
            String representation of the exception.
        """
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class PortfolioCriticalError(CoreError):
    """Critical exception that requires immediate attention."""

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Initialize critical exception (always non-recoverable)."""
        super().__init__(
            message=message,
            error_code=error_code,
            context=context,
            recoverable=False,
        )
