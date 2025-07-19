"""Service-related exceptions for portfolio management."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Unpack

from cyberdelta.core.portfolio.exceptions.base import PortfolioError
from cyberdelta.core.portfolio.portfolio_types.exception_models import ServiceExceptionContext


if TYPE_CHECKING:
    from typing_extensions import TypedDict

    class ExceptionKwargs(TypedDict, total=False):
        """Typed dictionary for exception kwargs."""

        error_code: str | None
        context: dict[str, Any] | None
        recoverable: bool


# HTTP status code constants
HTTP_SERVER_ERROR_START = 500
HTTP_SERVER_ERROR_END = 600


class ServiceError(PortfolioError):
    """Base exception for service errors."""

    def _get_default_error_code(self) -> str:
        """Get default error code for service exceptions."""
        return f"SERVICE_{self.__class__.__name__.upper()}"


class ConfigurationError(ServiceError):
    """Raised when configuration is invalid."""

    def __init__(self, source_type: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize configuration error."""
        super().__init__(f"Invalid configuration from source type: {source_type}", **kwargs)


class ServiceUnavailableError(ServiceError):
    """Raised when a service is unavailable."""

    def __init__(
        self,
        message: str,
        service_name: str | None = None,
        retry_after: int | None = None,
        fallback_available: bool = False,
        context: ServiceExceptionContext | None = None,
    ) -> None:
        """Initialize service unavailable exception.

        Args:
            message: Error message
            service_name: Name of unavailable service
            retry_after: Seconds to wait before retry
            fallback_available: Whether fallback is available
            context: Additional context
        """
        if context is None:
            context = ServiceExceptionContext(
                error_code="SERVICE_UNAVAILABLE",
                service_name=service_name,
                retry_after=retry_after,
                fallback_available=fallback_available,
                recoverable=True,
            )

        super().__init__(
            message,
            error_code=context.error_code,
            context=context.model_dump(),
            recoverable=context.recoverable,
        )


class ExternalAPIError(ServiceError):
    """Raised when external API calls fail."""

    def __init__(
        self,
        message: str,
        api_name: str | None = None,
        endpoint: str | None = None,
        status_code: int | None = None,
        response_body: str | None = None,
        context: ServiceExceptionContext | None = None,
    ) -> None:
        """Initialize external API exception.

        Args:
            message: Error message
            api_name: Name of the API
            endpoint: API endpoint
            status_code: HTTP status code
            response_body: Response body from API
            context: Additional context
        """
        if context is None:
            # Determine if the error is recoverable based on status code
            is_recoverable = (
                status_code is None
                or HTTP_SERVER_ERROR_START <= status_code < HTTP_SERVER_ERROR_END
            )

            context = ServiceExceptionContext(
                error_code="SERVICE_API_ERROR",
                api_name=api_name,
                endpoint=endpoint,
                status_code=status_code,
                response_body=response_body,
                recoverable=is_recoverable,
            )

        super().__init__(
            message,
            error_code=context.error_code,
            context=context.model_dump(),
            recoverable=context.recoverable,
        )


class CacheError(ServiceError):
    """Raised when cache operations fail."""

    def __init__(
        self,
        message: str,
        operation: str | None = None,
        cache_key: str | None = None,
        cache_backend: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize cache exception.

        Args:
            message: Error message
            operation: Cache operation (get/set/delete)
            cache_key: Cache key involved
            cache_backend: Cache backend type
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "operation": operation,
            "cache_key": cache_key,
            "cache_backend": cache_backend,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "SERVICE_CACHE_ERROR"
        super().__init__(message, **kwargs)


class PriceServiceError(ServiceError):
    """Raised when price service operations fail."""

    def __init__(
        self,
        message: str,
        symbol: str | None = None,
        exchange: str | None = None,
        price_type: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize price service exception.

        Args:
            message: Error message
            symbol: Trading symbol
            exchange: Exchange name
            price_type: Type of price (spot/mark/index)
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "symbol": symbol,
            "exchange": exchange,
            "price_type": price_type,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "SERVICE_PRICE_ERROR"
        super().__init__(message, **kwargs)


class SymbolServiceError(ServiceError):
    """Raised when symbol service operations fail."""

    def __init__(
        self,
        message: str,
        symbol: str | None = None,
        exchange: str | None = None,
        operation: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize symbol service exception.

        Args:
            message: Error message
            symbol: Trading symbol
            exchange: Exchange name
            operation: Operation attempted
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "symbol": symbol,
            "exchange": exchange,
            "operation": operation,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "SERVICE_SYMBOL_ERROR"
        super().__init__(message, **kwargs)


class ServiceTimeoutError(ServiceError):
    """Raised when service operations timeout."""

    def __init__(
        self,
        message: str,
        service_name: str | None = None,
        operation: str | None = None,
        timeout_seconds: float | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service timeout exception.

        Args:
            message: Error message
            service_name: Service that timed out
            operation: Operation that timed out
            timeout_seconds: Timeout duration
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "service_name": service_name,
            "operation": operation,
            "timeout_seconds": timeout_seconds,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "SERVICE_TIMEOUT"
        super().__init__(message, **kwargs)


class RateLimitError(ServiceError):
    """Raised when rate limits are exceeded."""

    def __init__(
        self,
        message: str,
        service_name: str | None = None,
        limit_type: str | None = None,
        retry_after: int | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize rate limit exception.

        Args:
            message: Error message
            service_name: Service with rate limit
            limit_type: Type of rate limit
            retry_after: Seconds to wait before retry
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "service_name": service_name,
            "limit_type": limit_type,
            "retry_after": retry_after,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "SERVICE_RATE_LIMIT"
        super().__init__(message, **kwargs)


class ServiceInitializationError(ServiceError):
    """Raised when service initialization fails."""

    def __init__(
        self,
        message: str,
        service_name: str | None = None,
        initialization_phase: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service initialization exception.

        Args:
            message: Error message
            service_name: Name of service that failed to initialize
            initialization_phase: Phase where initialization failed
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "service_name": service_name,
            "initialization_phase": initialization_phase,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "SERVICE_INITIALIZATION_ERROR"
        super().__init__(message, **kwargs)


class ServiceShutdownError(ServiceError):
    """Raised when service shutdown fails."""

    def __init__(
        self,
        message: str,
        service_name: str | None = None,
        cleanup_phase: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service shutdown exception.

        Args:
            message: Error message
            service_name: Name of service that failed to shutdown
            cleanup_phase: Phase where shutdown failed
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "service_name": service_name,
            "cleanup_phase": cleanup_phase,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "SERVICE_SHUTDOWN_ERROR"
        super().__init__(message, **kwargs)


class ServiceStateError(ServiceError):
    """Raised when service is in invalid state."""

    def __init__(
        self,
        message: str,
        service_name: str | None = None,
        current_state: str | None = None,
        expected_state: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service state exception.

        Args:
            message: Error message
            service_name: Name of service
            current_state: Current state of service
            expected_state: Expected state of service
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "service_name": service_name,
            "current_state": current_state,
            "expected_state": expected_state,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "SERVICE_STATE_ERROR"
        super().__init__(message, **kwargs)


class ServiceInitializationStateError(ServiceStateError):
    """Raised when service cannot be initialized due to invalid state."""

    def __init__(
        self,
        service_name: str | None = None,
        current_state: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service initialization state exception.

        Args:
            service_name: Name of service
            current_state: Current state of service
            **kwargs: Additional context
        """
        message = f"Cannot initialize service in {current_state} state"
        super().__init__(
            message=message,
            service_name=service_name,
            current_state=current_state,
            expected_state="CREATED, STOPPED, or FAILED",
            **kwargs,
        )


class ServiceInitializationFailedError(ServiceInitializationError):
    """Raised when service initialization fails."""

    def __init__(
        self,
        service_name: str | None = None,
        cause: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service initialization failed exception."""
        message = (
            f"Failed to initialize service: {cause}" if cause else "Failed to initialize service"
        )
        super().__init__(
            message=message,
            service_name=service_name,
            **kwargs,
        )


class ServiceStartStateError(ServiceStateError):
    """Raised when service cannot be started due to invalid state."""

    def __init__(
        self,
        service_name: str | None = None,
        current_state: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service start state exception."""
        message = f"Cannot start service in {current_state} state"
        super().__init__(
            message=message,
            service_name=service_name,
            current_state=current_state,
            expected_state="CREATED or STOPPED",
            **kwargs,
        )


class ServiceStartupTimeoutError(ServiceTimeoutError):
    """Raised when service startup times out."""

    def __init__(
        self,
        service_name: str | None = None,
        timeout_seconds: float | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service startup timeout exception."""
        message = (
            f"Service startup timed out after {timeout_seconds}s"
            if timeout_seconds
            else "Service startup timed out"
        )
        super().__init__(
            message=message,
            service_name=service_name,
            operation="startup",
            timeout_seconds=timeout_seconds,
            **kwargs,
        )


class ServiceStartFailedError(ServiceInitializationError):
    """Raised when service start fails."""

    def __init__(
        self,
        service_name: str | None = None,
        cause: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service start failed exception."""
        message = f"Failed to start service: {cause}" if cause else "Failed to start service"
        super().__init__(
            message=message,
            service_name=service_name,
            **kwargs,
        )


class ServiceShutdownTimeoutError(ServiceTimeoutError):
    """Raised when service shutdown times out."""

    def __init__(
        self,
        service_name: str | None = None,
        timeout_seconds: float | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service shutdown timeout exception."""
        message = (
            f"Service shutdown timed out after {timeout_seconds}s"
            if timeout_seconds
            else "Service shutdown timed out"
        )
        super().__init__(
            message=message,
            service_name=service_name,
            operation="shutdown",
            timeout_seconds=timeout_seconds,
            **kwargs,
        )


class ServiceShutdownFailedError(ServiceShutdownError):
    """Raised when service shutdown fails."""

    def __init__(
        self,
        service_name: str | None = None,
        cause: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service shutdown failed exception."""
        message = (
            f"Error during service shutdown: {cause}" if cause else "Error during service shutdown"
        )
        super().__init__(
            message=message,
            service_name=service_name,
            **kwargs,
        )


class ServiceCleanupError(ServiceShutdownError):
    """Raised when service cleanup fails."""

    def __init__(
        self,
        service_name: str | None = None,
        cause: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service cleanup exception."""
        message = (
            f"Error during service cleanup: {cause}" if cause else "Error during service cleanup"
        )
        super().__init__(
            message=message,
            service_name=service_name,
            cleanup_phase="cleanup",
            **kwargs,
        )


class ServiceNotFoundError(ServiceError):
    """Raised when requested service is not found."""

    def __init__(
        self,
        message: str,
        service_name: str | None = None,
        service_type: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize service not found exception.

        Args:
            message: Error message
            service_name: Name of service not found
            service_type: Type of service requested
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "service_name": service_name,
            "service_type": service_type,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "SERVICE_NOT_FOUND"
        super().__init__(message, **kwargs)
