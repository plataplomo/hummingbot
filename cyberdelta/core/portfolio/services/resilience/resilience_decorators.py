"""Type-safe resilience decorators with proper generic constraints."""

from __future__ import annotations

import functools
import time
from typing import TYPE_CHECKING, ParamSpec, Protocol, Self, TypeVar, runtime_checkable

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.infrastructure import (
    ResilienceError,
    ResilienceErrorType,
    ResilienceMetrics,
    ResilienceResult,
)


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from cyberdelta.core.portfolio.services.resilience.graceful_degradation_service import (
        GracefulDegradationService,
    )


logger = get_logger(__name__)

P = ParamSpec("P")
T = TypeVar("T", bound=object)


@runtime_checkable
class HasResilienceService(Protocol):
    """Protocol for objects that have resilience services."""

    degradation_service: GracefulDegradationService


def with_resilience[**P, T](
    func_or_service: Callable[P, Awaitable[T]] | str | None = None,
    *,
    use_circuit_breaker: bool = True,
    use_retry: bool = True,
    use_fallback: bool = True,
) -> (
    Callable[P, Awaitable[ResilienceResult[T]]]
    | Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[ResilienceResult[T]]]]
):
    """Type-safe resilience decorator.

    Can be used as:
    - @with_resilience  # Uses function name as service name
    - @with_resilience("service_name")  # Custom service name
    - @with_resilience("service_name", use_retry=False)  # With options

    Args:
        func_or_service: Either the function to decorate or service name string
        use_circuit_breaker: Whether to enable circuit breaker protection
        use_retry: Whether to enable retry logic
        use_fallback: Whether to enable fallback execution

    Returns:
        Either a decorated function returning ResilienceResult, or a decorator
        function depending on the calling pattern.
    """

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[ResilienceResult[T]]]:
        service_name = func_or_service if isinstance(func_or_service, str) else func.__name__

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> ResilienceResult[T]:
            start_time = time.time()
            metrics = ResilienceMetrics(
                total_attempts=0,
                successful_attempts=0,
                failed_attempts=0,
                circuit_breaker_trips=0,
                fallback_executions=0,
                total_duration_ms=0,
            )

            try:
                # Get degradation service from first argument if it has one
                degradation_service = None
                if args and isinstance(args[0], HasResilienceService):
                    degradation_service = args[0].degradation_service

                if degradation_service:
                    # Use degradation service if available
                    result = await degradation_service.execute_with_fallback(
                        service_name, func, *args, **kwargs
                    )

                    # Update metrics
                    duration_ms = (time.time() - start_time) * 1000
                    metrics = ResilienceMetrics(
                        total_attempts=1,
                        successful_attempts=1,
                        failed_attempts=0,
                        circuit_breaker_trips=0,
                        fallback_executions=0,
                        total_duration_ms=duration_ms,
                    )

                    return ResilienceResult[T].successful(result, metrics)
                # No resilience service, execute directly
                result = await func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                metrics = ResilienceMetrics(
                    total_attempts=1,
                    successful_attempts=1,
                    failed_attempts=0,
                    circuit_breaker_trips=0,
                    fallback_executions=0,
                    total_duration_ms=duration_ms,
                )
                return ResilienceResult[T].successful(result, metrics)

            except TimeoutError as e:
                duration_ms = (time.time() - start_time) * 1000
                error = ResilienceError(
                    error_type=ResilienceErrorType.TIMEOUT,
                    message=f"Operation timed out: {e!s}",
                    service_name=service_name,
                    cause=e,
                )
                metrics = ResilienceMetrics(
                    total_attempts=1,
                    successful_attempts=0,
                    failed_attempts=1,
                    circuit_breaker_trips=0,
                    fallback_executions=0,
                    total_duration_ms=duration_ms,
                    last_error=error,
                )
                logger.warning(
                    "resilience_timeout",
                    service_name=service_name,
                    duration_ms=duration_ms,
                )
                return ResilienceResult[T].failed(error, metrics)

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                error = ResilienceError(
                    error_type=ResilienceErrorType.SERVICE_UNAVAILABLE,
                    message=f"Service error: {e!s}",
                    service_name=service_name,
                    cause=e,
                )
                metrics = ResilienceMetrics(
                    total_attempts=1,
                    successful_attempts=0,
                    failed_attempts=1,
                    circuit_breaker_trips=0,
                    fallback_executions=0,
                    total_duration_ms=duration_ms,
                    last_error=error,
                )
                logger.exception(
                    "resilience_error",
                    service_name=service_name,
                    duration_ms=duration_ms,
                )
                return ResilienceResult[T].failed(error, metrics)

        return wrapper

    # Handle both @with_resilience and @with_resilience(...) syntax
    if callable(func_or_service) and not isinstance(func_or_service, str):
        return decorator(func_or_service)
    return decorator


def resilient_method(
    service_name: str | None = None,
    *,
    use_circuit_breaker: bool = True,
    use_retry: bool = True,
    use_fallback: bool = True,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Decorator for methods that unwraps ResilienceResult automatically.

    This is useful for methods that should be resilient but need to return
    the actual value rather than a ResilienceResult.

    Args:
        service_name: Optional service name (defaults to function name)
        use_circuit_breaker: Whether to enable circuit breaker protection
        use_retry: Whether to enable retry logic
        use_fallback: Whether to enable fallback execution

    Returns:
        A decorator function that wraps methods with resilience capabilities.
    """

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        actual_service_name = service_name or func.__name__

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            # Apply resilience manually instead of using the decorator
            try:
                # Get degradation service from first argument if it has one
                degradation_service = None
                if args and isinstance(args[0], HasResilienceService):
                    degradation_service = args[0].degradation_service

                if degradation_service:
                    # Use degradation service if available
                    return await degradation_service.execute_with_fallback(
                        actual_service_name, func, *args, **kwargs
                    )
                # No resilience service, execute directly
                return await func(*args, **kwargs)
            except Exception:
                # If it's already a ResilienceResult with error, handle appropriately
                logger.exception("resilient_method_error", service_name=actual_service_name)
                raise

        return wrapper

    return decorator


class ResilienceContext:
    """Context manager for resilience operations with metrics tracking."""

    def __init__(
        self,
        service_name: str,
        degradation_service: GracefulDegradationService | None = None,
    ) -> None:
        """Initialize resilience context."""
        self.service_name = service_name
        self.degradation_service = degradation_service
        self.start_time: float = 0
        self.metrics: ResilienceMetrics | None = None

    async def __aenter__(self) -> Self:
        """Enter resilience context.

        Returns:
            Self for use in async context manager.
        """
        self.start_time = time.time()
        logger.debug("resilience_context_entered", service_name=self.service_name)
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object
    ) -> bool:
        """Exit resilience context and record metrics.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred

        Returns:
            False to indicate exceptions should not be suppressed.
        """
        duration_ms = (time.time() - self.start_time) * 1000

        if exc_type is None:
            self.metrics = ResilienceMetrics(
                total_attempts=1,
                successful_attempts=1,
                failed_attempts=0,
                circuit_breaker_trips=0,
                fallback_executions=0,
                total_duration_ms=duration_ms,
            )
            logger.debug(
                "resilience_context_success",
                service_name=self.service_name,
                duration_ms=duration_ms,
            )
        else:
            error = ResilienceError(
                error_type=ResilienceErrorType.SERVICE_UNAVAILABLE,
                message=f"Context error: {exc_val!s}",
                service_name=self.service_name,
                cause=exc_val if isinstance(exc_val, Exception) else None,
            )
            self.metrics = ResilienceMetrics(
                total_attempts=1,
                successful_attempts=0,
                failed_attempts=1,
                circuit_breaker_trips=0,
                fallback_executions=0,
                total_duration_ms=duration_ms,
                last_error=error,
            )
            logger.warning(
                "resilience_context_error",
                service_name=self.service_name,
                duration_ms=duration_ms,
                error=str(exc_val),
            )

        # Don't suppress exceptions
        return False
