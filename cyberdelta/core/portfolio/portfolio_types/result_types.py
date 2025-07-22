"""Result types for async operations providing type-safe error handling."""

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.exceptions import (
    ResultAndThenNoneErrorError,
    ResultAndThenNoneValueError,
    ResultErrorWithoutErrorError,
    ResultErrorWithValueError,
    ResultMapErrorNoneErrorError,
    ResultMapErrorNoneValueError,
    ResultMapNoneErrorError,
    ResultMapNoneValueError,
    ResultOrElseNoneErrorError,
    ResultOrElseNoneValueError,
    ResultSuccessWithErrorError,
    ResultSuccessWithoutValueError,
    ResultUnwrapErrorOnSuccessError,
    ResultUnwrapNoneErrorError,
    ResultUnwrapNoneValueError,
    ResultUnwrapOnErrorError,
)
from cyberdelta.core.portfolio.portfolio_types.discriminated_unions import (
    BusinessLogicErrorData,
    ErrorUnion,
    NetworkErrorData,
    ValidationErrorData,
)
from cyberdelta.core.portfolio.portfolio_types.exception_models import ExceptionContext


T = TypeVar("T", bound=object)  # Success value type


E = TypeVar("E", bound=object)  # Error type


class Result[T, E](BaseModel):
    """Generic result type for operations that can succeed or fail.

    Inspired by Rust's Result type, provides type-safe error handling
    without exceptions for expected failure cases.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    _value: T | None = None
    _error: E | None = None
    _is_success: bool = False

    def model_post_init(self, __context: object, /) -> None:
        """Validate that exactly one of value or error is set."""
        if self._is_success and self._value is None:
            raise ResultSuccessWithoutValueError
        if not self._is_success and self._error is None:
            raise ResultErrorWithoutErrorError
        if self._is_success and self._error is not None:
            raise ResultSuccessWithErrorError
        if not self._is_success and self._value is not None:
            raise ResultErrorWithValueError

    @classmethod
    def ok(cls, value: T) -> Result[T, E]:
        """Create a successful result."""
        return cls(_value=value, _is_success=True)

    @classmethod
    def error(cls, error: E) -> Result[T, E]:
        """Create an error result."""
        return cls(_error=error, _is_success=False)

    @property
    def is_ok(self) -> bool:
        """Check if the result is successful."""
        return self._is_success

    @property
    def is_error(self) -> bool:
        """Check if the result is an error."""
        return not self._is_success

    def unwrap(self) -> T:
        """Get the value, raising an exception if this is an error result."""
        if not self._is_success:
            raise ResultUnwrapOnErrorError(self._error)
        if self._value is None:
            raise ResultUnwrapNoneValueError
        return self._value

    def unwrap_or(self, default: T) -> T:
        """Get the value or return default if this is an error result."""
        return self._value if self._is_success and self._value is not None else default

    def unwrap_error(self) -> E:
        """Get the error, raising an exception if this is a success result."""
        if self._is_success:
            raise ResultUnwrapErrorOnSuccessError(self._value)
        if self._error is None:
            raise ResultUnwrapNoneErrorError
        return self._error

    def map(self, func: Callable[[T], U]) -> Result[U, E]:
        """Transform the value if this is a success result."""
        if self._is_success:
            if self._value is None:
                raise ResultMapNoneValueError
            return Result[U, E].ok(func(self._value))
        if self._error is None:
            raise ResultMapNoneErrorError
        return Result[U, E].error(self._error)

    def map_error(self, func: Callable[[E], F]) -> Result[T, F]:
        """Transform the error if this is an error result."""
        if not self._is_success:
            if self._error is None:
                raise ResultMapErrorNoneErrorError
            return Result[T, F].error(func(self._error))
        if self._value is None:
            raise ResultMapErrorNoneValueError
        return Result[T, F].ok(self._value)

    def and_then(self, func: Callable[[T], Result[U, E]]) -> Result[U, E]:
        """Chain operations that return Results (flatMap/bind)."""
        if self._is_success:
            if self._value is None:
                raise ResultAndThenNoneValueError
            return func(self._value)
        if self._error is None:
            raise ResultAndThenNoneErrorError
        return Result[U, E].error(self._error)

    def or_else(self, func: Callable[[E], Result[T, F]]) -> Result[T, F]:
        """Provide alternative on error."""
        if not self._is_success:
            if self._error is None:
                raise ResultOrElseNoneErrorError
            return func(self._error)
        if self._value is None:
            raise ResultOrElseNoneValueError
        return Result[T, F].ok(self._value)


# Type aliases for common Result types
U = TypeVar("U", bound=object)
F = TypeVar("F", bound=object)


# Portfolio-specific Result types
class PortfolioResultError(BaseModel):
    """Portfolio error model for Result types."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str = Field(..., description="Error code")
    message: str = Field(..., description="Human-readable error message")
    details: ErrorUnion | None = Field(default=None, description="Error-specific details")
    timestamp: float = Field(..., description="Error timestamp")
    context: ExceptionContext = Field(
        default_factory=ExceptionContext, description="Additional context"
    )
    is_retryable: bool = Field(default=False, description="Whether the operation can be retried")


# Type aliases for common portfolio operations
PortfolioResult = Result[T, PortfolioResultError]
AsyncPortfolioResult = Result[T, PortfolioResultError]


class OperationMetrics(BaseModel):
    """Metrics for async operations."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation_name: str
    start_time: float
    end_time: float
    duration_ms: float = Field(default=0.0, init=False)
    retry_count: int = 0
    was_cached: bool = False

    def model_post_init(self, __context: object, /) -> None:
        """Calculate duration."""
        self.duration_ms = (self.end_time - self.start_time) * 1000


class AsyncOperationResult[T](BaseModel):
    """Enhanced result for async operations with metrics and context."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    result: PortfolioResult[T]
    metrics: OperationMetrics
    warnings: list[str] = Field(default_factory=list)

    @property
    def is_ok(self) -> bool:
        """Check if the operation was successful."""
        return self.result.is_ok

    @property
    def is_error(self) -> bool:
        """Check if the operation failed."""
        return self.result.is_error

    def unwrap(self) -> T:
        """Get the value, raising an exception if this is an error result."""
        return self.result.unwrap()

    def unwrap_or(self, default: T) -> T:
        """Get the value or return default if this is an error result."""
        return self.result.unwrap_or(default)


# Helper functions for creating common portfolio results
def ok_result[T](value: T) -> PortfolioResult[T]:
    """Create a successful portfolio result."""
    return Result[T, PortfolioResultError].ok(value)


def error_result(
    code: str,
    message: str,
    details: ErrorUnion | None = None,
    timestamp: float | None = None,
    context: ExceptionContext | None = None,
    is_retryable: bool = False,
) -> PortfolioResult[object]:
    """Create an error portfolio result."""
    error = PortfolioResultError(
        code=code,
        message=message,
        details=details,
        timestamp=timestamp or time.time(),
        context=context or ExceptionContext(),
        is_retryable=is_retryable,
    )
    return Result[object, PortfolioResultError].error(error)


def validation_error_result(
    field_name: str,
    constraint: str,
    actual_value: str,
    message: str | None = None,
) -> PortfolioResult[object]:
    """Create a validation error result."""
    details = ValidationErrorData(
        field_name=field_name,
        constraint=constraint,
        actual_value=actual_value,
    )

    return error_result(
        code="VALIDATION_ERROR",
        message=message or f"Validation failed for field {field_name}",
        details=details,
    )


def network_error_result(
    endpoint: str,
    status_code: int | None = None,
    retry_count: int = 0,
    message: str | None = None,
) -> PortfolioResult[object]:
    """Create a network error result."""
    details = NetworkErrorData(
        status_code=status_code,
        endpoint=endpoint,
        retry_count=retry_count,
    )

    return error_result(
        code="NETWORK_ERROR",
        message=message or f"Network error for endpoint {endpoint}",
        details=details,
        is_retryable=True,
    )


def business_logic_error_result(
    rule_name: str,
    context: dict[str, str] | None = None,
    message: str | None = None,
) -> PortfolioResult[object]:
    """Create a business logic error result."""
    details = BusinessLogicErrorData(
        rule_name=rule_name,
        context=context or {},
    )

    return error_result(
        code="BUSINESS_LOGIC_ERROR",
        message=message or f"Business rule violation: {rule_name}",
        details=details,
    )


# Result combinators for working with multiple results
def combine_results(*results: PortfolioResult[object]) -> PortfolioResult[list[object]]:
    """Combine multiple results into one. Fails if any result fails."""
    values: list[object] = []
    for result in results:
        if result.is_error:
            return Result[list[object], PortfolioResultError].error(result.unwrap_error())
        values.append(result.unwrap())
    return ok_result(values)


def collect_results[T](results: list[PortfolioResult[T]]) -> PortfolioResult[list[T]]:
    """Collect a list of results into a single result."""
    values: list[T] = []
    for result in results:
        if result.is_error:
            # Create a new error result with the correct type
            return Result[list[T], PortfolioResultError].error(result.unwrap_error())
        values.append(result.unwrap())
    return ok_result(values)


def first_ok_result[T](*results: PortfolioResult[T]) -> PortfolioResult[T]:
    """Return the first successful result, or the last error if all fail."""
    last_error: PortfolioResult[T] | None = None
    for result in results:
        if result.is_ok:
            return result
        last_error = result
    return (
        last_error if last_error is not None 
        else Result[T, PortfolioResultError].error(PortfolioResultError(
            code="NO_RESULTS", 
            message="No results provided",
            timestamp=time.time()
        ))
    )


# Async result utilities
async def wrap_async_operation[T](
    operation_name: str,
    func: Callable[[], T] | Callable[[], Awaitable[T]],
    *,
    retry_count: int = 0,
    was_cached: bool = False,
) -> AsyncOperationResult[T]:
    """Wrap an async operation with result tracking and metrics."""
    start_time = time.time()

    try:
        # Handle both sync and async functions properly
        value: T
        if inspect.iscoroutinefunction(func):
            # It's an async function, await it directly
            value = await func()
        else:
            # It's a sync function, call it
            result = func()
            # Check if the result is awaitable
            if inspect.isawaitable(result):
                value = await result
            else:
                value = result
        end_time = time.time()

        metrics = OperationMetrics(
            operation_name=operation_name,
            start_time=start_time,
            end_time=end_time,
            retry_count=retry_count,
            was_cached=was_cached,
        )

        return AsyncOperationResult(
            result=ok_result(value),
            metrics=metrics,
        )

    except (ValueError, TypeError, RuntimeError, OSError) as e:
        end_time = time.time()

        metrics = OperationMetrics(
            operation_name=operation_name,
            start_time=start_time,
            end_time=end_time,
            retry_count=retry_count,
            was_cached=was_cached,
        )

        error: PortfolioResult[T] = Result[T, PortfolioResultError].error(
            PortfolioResultError(
                code="OPERATION_FAILED",
                message=f"Operation {operation_name} failed: {e}",
                timestamp=time.time(),
                context=ExceptionContext(tags={"exception_type": type(e).__name__}),
            )
        )

        return AsyncOperationResult(
            result=error,
            metrics=metrics,
        )
