"""WebSocket Error Foundation Module.

Type-safe error foundation without protocol coupling.
Provides base types and utilities for the decoupled WebSocket error system.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from pydantic import BaseModel, Field


if TYPE_CHECKING:
    from logging import Logger


# Error Severity Levels
class ErrorSeverity(IntEnum):
    """WebSocket error severity levels."""

    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50
    FATAL = 60


# WebSocket Recovery Strategies (replacing boolean is_retryable)
class WebSocketRecoveryStrategy(IntEnum):
    """Type-safe recovery strategies for WebSocket errors."""

    # No recovery possible
    NONE = 0

    # Simple retry strategies
    IMMEDIATE_RETRY = 100
    EXPONENTIAL_BACKOFF = 101
    LINEAR_BACKOFF = 102

    # Connection strategies
    RECONNECT_SAME = 200
    RECONNECT_DIFFERENT = 201
    FULL_RECONNECT = 202

    # Subscription strategies
    RESUBSCRIBE_SINGLE = 300
    RESUBSCRIBE_ALL = 301
    RESUBSCRIBE_SELECTIVE = 302

    # Advanced strategies
    CIRCUIT_BREAKER = 400
    FALLBACK_EXCHANGE = 401
    DEGRADE_SERVICE = 402


# Typed Logger Protocol
TLogData_contra = TypeVar("TLogData_contra", bound=BaseModel, contravariant=True)


class TypedLogger(Protocol[TLogData_contra]):
    """Protocol for type-safe logging."""

    def log_error(
        self,
        severity: ErrorSeverity,
        log_data: TLogData_contra,
        exc_info: Exception | None = None,
    ) -> None:
        """Log error with typed data."""
        ...

    def get_logger(self) -> Logger:
        """Get underlying logger instance."""
        ...


# Error Timestamp Mixin
class ErrorTimestampMixin:
    """Mixin for consistent error timestamps."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize with UTC timestamp."""
        super().__init__(*args, **kwargs)
        self._timestamp = datetime.now(UTC)
        self._timestamp_ms = int(self._timestamp.timestamp() * 1000)

    @property
    def timestamp(self) -> datetime:
        """Get error timestamp.

        Returns:
            Error timestamp in UTC.
        """
        return self._timestamp

    @property
    def timestamp_ms(self) -> int:
        """Get error timestamp in milliseconds.

        Returns:
            Error timestamp in milliseconds since epoch.
        """
        return self._timestamp_ms

    def age_seconds(self) -> float:
        """Get age of error in seconds.

        Returns:
            Age of error in seconds.
        """
        now = datetime.now(UTC)
        return (now - self._timestamp).total_seconds()


# Error Context Validator Base
class ErrorContextValidator:
    """Base validator for error contexts."""

    @staticmethod
    def validate_non_empty_string(value: str | None, field_name: str) -> str:
        """Validate non-empty string field.

        Returns:
            Stripped non-empty string.

        Raises:
            ValueError: If value is empty or None.
        """
        if not value or not value.strip():
            msg = f"{field_name} must be a non-empty string"
            raise ValueError(msg)
        return value.strip()

    @staticmethod
    def validate_positive_integer(value: int | None, field_name: str) -> int | None:
        """Validate positive integer field.

        Returns:
            Value if positive or None.

        Raises:
            ValueError: If value is negative.
        """
        if value is not None and value < 0:
            msg = f"{field_name} must be a positive integer"
            raise ValueError(msg)
        return value

    @staticmethod
    def validate_enum_value(value: object, enum_class: type[IntEnum], field_name: str) -> object:
        """Validate enum value.

        Returns:
            Valid enum value or None.

        Raises:
            ValueError: If value is not a valid enum member.
        """
        if value is not None and not isinstance(value, enum_class):
            try:
                return enum_class(value)  # type: ignore[arg-type]
            except (ValueError, TypeError) as e:
                msg = f"{field_name} must be a valid {enum_class.__name__}"
                raise ValueError(msg) from e
        return value


# Base Error Metadata Model
class ErrorMetadata(BaseModel):
    """Base metadata for all errors."""

    correlation_id: str | None = Field(default=None, description="Correlation ID for tracing")
    retry_count: int = Field(default=0, description="Number of retry attempts")
    max_retries: int = Field(default=3, description="Maximum retry attempts allowed")
    backoff_ms: int | None = Field(default=None, description="Backoff time in milliseconds")

    class Config:
        """Pydantic configuration."""

        frozen = False
        validate_assignment = True
        use_enum_values = False


# Error Chain Support
class ErrorChain(BaseModel):
    """Support for error chaining and root cause analysis."""

    error_class: str = Field(description="Error class name")
    error_message: str = Field(description="Error message")
    error_code: str | int | None = Field(default=None, description="Error code if available")
    timestamp_ms: int = Field(description="Timestamp in milliseconds")

    @classmethod
    def from_exception(cls, exc: Exception, timestamp_ms: int | None = None) -> ErrorChain:
        """Create from exception.

        Returns:
            ErrorChain instance created from exception.
        """
        if timestamp_ms is None:
            timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)

        return cls(
            error_class=exc.__class__.__name__,
            error_message=str(exc),
            error_code=getattr(exc, "code", None),
            timestamp_ms=timestamp_ms,
        )


# Error Context Protocol
class ErrorContextProtocol(Protocol):
    """Protocol for error context objects."""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging."""
        ...

    def validate(self) -> None:
        """Validate context data."""
        ...
