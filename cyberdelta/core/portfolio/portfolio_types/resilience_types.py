"""Type-safe resilience types and result classes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TYPE_CHECKING, TypeVar

from pydantic import BaseModel, ConfigDict


if TYPE_CHECKING:
    pass

from cyberdelta.core.portfolio.exceptions.service import (
    ServiceTimeoutError,
    ServiceUnavailableError,
)


T = TypeVar("T", bound=object)


class ResilienceResultError(RuntimeError):
    """Raised when result has no value or error."""

    def __init__(self) -> None:
        """Initialize with a standard message."""
        super().__init__("No value or error")


class ResilienceErrorType(Enum):
    """Types of resilience-related errors."""

    TIMEOUT = "timeout"
    CIRCUIT_OPEN = "circuit_open"
    RETRY_EXHAUSTED = "retry_exhausted"
    SERVICE_UNAVAILABLE = "service_unavailable"
    DEGRADED_MODE = "degraded_mode"
    HEALTH_CHECK_FAILED = "health_check_failed"


class ResilienceError(BaseModel):
    """Structured resilience error information."""

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    error_type: ResilienceErrorType
    message: str
    service_name: str | None = None
    retry_after: int | None = None
    attempts: int | None = None
    cause: Exception | None = None

    def to_exception(self) -> Exception:
        """Convert to appropriate exception type.

        Returns:
            Exception: ServiceTimeoutError for timeout errors, ServiceUnavailableError
                for circuit breaker and service unavailable errors, or RuntimeError
                for other error types.
        """
        if self.error_type == ResilienceErrorType.TIMEOUT:
            return ServiceTimeoutError(self.message)
        if self.error_type in {
            ResilienceErrorType.CIRCUIT_OPEN,
            ResilienceErrorType.SERVICE_UNAVAILABLE,
        }:
            return ServiceUnavailableError(
                self.message,
                service_name=self.service_name,
                retry_after=self.retry_after,
            )
        return RuntimeError(f"Resilience error: {self.message}")


class ResilienceMetrics(BaseModel):
    """Metrics about resilience execution."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    total_attempts: int
    successful_attempts: int
    failed_attempts: int
    circuit_breaker_trips: int
    fallback_executions: int
    total_duration_ms: float
    last_error: ResilienceError | None = None


class ResilienceResult[T](BaseModel):
    """Type-safe result container for resilience operations."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    value: T | None
    success: bool
    metrics: ResilienceMetrics
    error: ResilienceError | None = None
    degraded: bool = False

    @classmethod
    def successful(
        cls,
        value: T,
        metrics: ResilienceMetrics,
        degraded: bool = False,
    ) -> ResilienceResult[T]:
        """Create a successful result.

        Returns:
            ResilienceResult[T]: A successful result containing the provided value.
        """
        return cls(
            value=value,
            success=True,
            metrics=metrics,
            error=None,
            degraded=degraded,
        )

    @classmethod
    def failed(
        cls,
        error: ResilienceError,
        metrics: ResilienceMetrics,
    ) -> ResilienceResult[T]:
        """Create a failed result.

        Returns:
            ResilienceResult[T]: A failed result containing the error information.
        """
        return cls(
            value=None,
            success=False,
            metrics=metrics,
            error=error,
            degraded=False,
        )

    def unwrap(self) -> T:
        """Get the value or raise the error.

        Returns:
            T: The successful value if the result is successful.

        Raises:
            ResilienceResultError: If the result has no value or error.
        """
        if self.success and self.value is not None:
            return self.value
        if self.error:
            raise self.error.to_exception()
        raise ResilienceResultError

    def unwrap_or(self, default: T) -> T:
        """Get the value or return default.

        Returns:
            T: The successful value if the result is successful, otherwise the default.
        """
        if self.success and self.value is not None:
            return self.value
        return default

    def map(self, func: Callable[[T], U]) -> ResilienceResult[U]:
        """Transform the value if successful.

        Returns:
            ResilienceResult[U]: A new result with the transformed value if successful,
                or a failed result if the transformation fails or the original result failed.
        """
        if self.success and self.value is not None:
            try:
                new_value = func(self.value)
                return ResilienceResult[U].successful(new_value, self.metrics, self.degraded)
            except (ValueError, TypeError, AttributeError, RuntimeError) as e:
                error = ResilienceError(
                    error_type=ResilienceErrorType.SERVICE_UNAVAILABLE,
                    message=f"Mapping function failed: {e!s}",
                    cause=e,
                )
                return ResilienceResult[U].failed(error, self.metrics)
        else:
            # Preserve the error for the new type
            return ResilienceResult[U](
                value=None,
                success=self.success,
                metrics=self.metrics,
                error=self.error,
                degraded=self.degraded,
            )


# Type alias for async resilience operations
ResilienceOperation = Callable[..., Awaitable[T]]
ResilienceWrapper = Callable[[ResilienceOperation[T]], ResilienceOperation[ResilienceResult[T]]]


U = TypeVar("U", bound=object)
