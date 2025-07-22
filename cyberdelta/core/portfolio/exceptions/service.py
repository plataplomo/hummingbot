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


class ReconciliationValueError(ServiceError):
    """Raised when reconciliation values are invalid."""

    def __init__(
        self,
        value_type: str,
        actual_value: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize reconciliation value error."""
        message = f"Invalid {value_type} value"
        if actual_value:
            message += f": {actual_value}"
        context = kwargs.get("context") or {}
        context.update({
            "value_type": value_type,
            "actual_value": actual_value,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "RECONCILIATION_VALUE_ERROR"
        super().__init__(message, **kwargs)


class ReconciliationMetadataTypeError(ServiceError):
    """Raised when reconciliation metadata has invalid type."""

    def __init__(
        self,
        expected_type: str,
        actual_type: str,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize reconciliation metadata type error."""
        message = f"Invalid metadata type: expected {expected_type}, got {actual_type}"
        context = kwargs.get("context") or {}
        context.update({
            "expected_type": expected_type,
            "actual_type": actual_type,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "RECONCILIATION_METADATA_TYPE_ERROR"
        super().__init__(message, **kwargs)


class ReconciliationDiscrepancyTypeError(ServiceError):
    """Raised when reconciliation discrepancy type is invalid."""

    def __init__(
        self,
        invalid_type: str,
        valid_types: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize reconciliation discrepancy type error."""
        if valid_types:
            message = f"Type must be one of: {', '.join(valid_types)}, got '{invalid_type}'"
        else:
            message = f"Invalid discrepancy type: {invalid_type}"
        context = kwargs.get("context") or {}
        context.update({
            "invalid_type": invalid_type,
            "valid_types": valid_types,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "RECONCILIATION_DISCREPANCY_TYPE_ERROR"
        super().__init__(message, **kwargs)


class ReconciliationSeverityError(ServiceError):
    """Raised when reconciliation severity is invalid."""

    def __init__(
        self,
        invalid_severity: str,
        valid_severities: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize reconciliation severity error."""
        if valid_severities:
            message = (
                f"Severity must be one of: {', '.join(valid_severities)}, got '{invalid_severity}'"
            )
        else:
            message = f"Invalid severity: {invalid_severity}"
        context = kwargs.get("context") or {}
        context.update({
            "invalid_severity": invalid_severity,
            "valid_severities": valid_severities,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "RECONCILIATION_SEVERITY_ERROR"
        super().__init__(message, **kwargs)


class CircuitBreakerThresholdError(ServiceError):
    """Raised when circuit breaker thresholds are invalid."""

    def __init__(
        self,
        threshold_type: str,
        invalid_relationship: str,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize circuit breaker threshold error."""
        message = f"{threshold_type} threshold error: {invalid_relationship}"
        context = kwargs.get("context") or {}
        context.update({
            "threshold_type": threshold_type,
            "invalid_relationship": invalid_relationship,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CIRCUIT_BREAKER_THRESHOLD_ERROR"
        super().__init__(message, **kwargs)


class ResilienceConfigurationError(ServiceError):
    """Raised when resilience configuration is invalid."""

    def __init__(
        self,
        config_type: str,
        invalid_relationship: str,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize resilience configuration error."""
        message = f"{config_type} configuration error: {invalid_relationship}"
        context = kwargs.get("context") or {}
        context.update({
            "config_type": config_type,
            "invalid_relationship": invalid_relationship,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "RESILIENCE_CONFIGURATION_ERROR"
        super().__init__(message, **kwargs)


class AnalyticsValueError(ServiceError):
    """Raised when analytics values are invalid."""

    def __init__(
        self,
        value_type: str,
        value: str | None = None,
        requirement: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize analytics value error."""
        message = f"Invalid {value_type}"
        if requirement:
            message += f": {requirement}"
        if value:
            message += f" (value: {value})"
        context = kwargs.get("context") or {}
        context.update({
            "value_type": value_type,
            "value": value,
            "requirement": requirement,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "ANALYTICS_VALUE_ERROR"
        super().__init__(message, **kwargs)


class AnalyticsConfigurationError(ServiceError):
    """Raised when analytics configuration is invalid."""

    def __init__(
        self,
        config_type: str,
        invalid_value: str | None = None,
        valid_values: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize analytics configuration error."""
        if valid_values and invalid_value:
            message = (
                f"Invalid {config_type}: must be one of {', '.join(valid_values)}, "
                f"got '{invalid_value}'"
            )
        elif invalid_value:
            message = f"Invalid {config_type}: {invalid_value}"
        else:
            message = f"Invalid {config_type} configuration"
        context = kwargs.get("context") or {}
        context.update({
            "config_type": config_type,
            "invalid_value": invalid_value,
            "valid_values": valid_values,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "ANALYTICS_CONFIGURATION_ERROR"
        super().__init__(message, **kwargs)


class CurrencyConverterError(ServiceError):
    """Raised when currency conversion operations fail."""

    def __init__(
        self,
        operation_type: str,
        currency_pair: str | None = None,
        requirement: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize currency converter error."""
        message = f"Currency conversion error: {operation_type}"
        if requirement:
            message += f" - {requirement}"
        if currency_pair:
            message += f" (pair: {currency_pair})"
        context = kwargs.get("context") or {}
        context.update({
            "operation_type": operation_type,
            "currency_pair": currency_pair,
            "requirement": requirement,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CURRENCY_CONVERTER_ERROR"
        super().__init__(message, **kwargs)


class HealthCheckValidationError(ServiceError):
    """Raised when health check validation fails."""

    def __init__(
        self,
        metric_type: str,
        requirement: str,
        value: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize health check validation error."""
        message = f"Health check validation failed for {metric_type}: {requirement}"
        if value:
            message += f" (value: {value})"
        context = kwargs.get("context") or {}
        context.update({
            "metric_type": metric_type,
            "requirement": requirement,
            "value": value,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "HEALTH_CHECK_VALIDATION_ERROR"
        super().__init__(message, **kwargs)


class ExchangeRateUnavailableError(PriceServiceError):
    """Raised when exchange rate cannot be determined."""

    def __init__(
        self,
        from_currency: str,
        to_currency: str,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize exchange rate unavailable error."""
        message = "Unable to determine exchange rate"
        context = kwargs.get("context") or {}
        context.update({
            "from_currency": from_currency,
            "to_currency": to_currency,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "EXCHANGE_RATE_UNAVAILABLE"
        super().__init__(
            message=message,
            symbol=f"{from_currency}/{to_currency}",
            **kwargs,
        )


class ConfigManagerValidationError(ServiceError):
    """Raised when config manager validation fails."""

    def __init__(
        self,
        validation_type: str,
        requirement: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize config manager validation error."""
        message = f"Configuration validation failed: {validation_type}"
        if requirement:
            message += f" - {requirement}"
        context = kwargs.get("context") or {}
        context.update({
            "validation_type": validation_type,
            "requirement": requirement,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CONFIG_MANAGER_VALIDATION_ERROR"
        super().__init__(message, **kwargs)


class ConfigStringValidationError(ConfigManagerValidationError):
    """Raised when string fields in configuration are invalid."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize config string validation error."""
        super().__init__(
            validation_type="string_field",
            requirement="cannot be empty",
            **kwargs,
        )


class ConfigTimestampValidationError(ConfigManagerValidationError):
    """Raised when timestamp fields in configuration are invalid."""

    def __init__(
        self,
        requirement: str = "must be positive",
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize config timestamp validation error."""
        super().__init__(
            validation_type="timestamp_field",
            requirement=requirement,
            **kwargs,
        )


class ConfigChangeKeyValidationError(ConfigManagerValidationError):
    """Raised when change key fields are invalid."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize config change key validation error."""
        super().__init__(
            validation_type="change_key",
            requirement="cannot be empty",
            **kwargs,
        )


class ConfigProfileNameValidationError(ConfigManagerValidationError):
    """Raised when profile name fields are invalid."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize config profile name validation error."""
        super().__init__(
            validation_type="profile_name",
            requirement="cannot be empty",
            **kwargs,
        )


class ConfigTimestampNegativeError(ConfigManagerValidationError):
    """Raised when timestamp values are negative."""

    def __init__(
        self,
        field_type: str = "timestamp",
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize config timestamp negative error."""
        super().__init__(
            validation_type=field_type,
            requirement="cannot be negative",
            **kwargs,
        )


class AuditTrailValidationError(ServiceError):
    """Raised when audit trail validation fails."""

    def __init__(
        self,
        field_type: str,
        valid_values: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize audit trail validation error."""
        if valid_values:
            message = f"{field_type} must be one of: {', '.join(valid_values)}"
        else:
            message = f"Invalid {field_type}"
        context = kwargs.get("context") or {}
        context.update({
            "field_type": field_type,
            "valid_values": valid_values,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "AUDIT_TRAIL_VALIDATION_ERROR"
        super().__init__(message, **kwargs)


class BackupServiceValidationError(ServiceError):
    """Raised when backup service validation fails."""

    def __init__(
        self,
        value_type: str,
        requirement: str = "must be a valid number string",
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize backup service validation error."""
        message = f"{value_type} {requirement}"
        context = kwargs.get("context") or {}
        context.update({
            "value_type": value_type,
            "requirement": requirement,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "BACKUP_SERVICE_VALIDATION_ERROR"
        super().__init__(message, **kwargs)


class AnalyticsTypeError(ServiceError):
    """Raised when analytics type validation fails."""

    def __init__(
        self,
        field_type: str,
        valid_types: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize analytics type error."""
        if valid_types:
            message = f"{field_type} must be one of: {', '.join(valid_types)}"
        else:
            message = f"Invalid {field_type}"
        context = kwargs.get("context") or {}
        context.update({
            "field_type": field_type,
            "valid_types": valid_types,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "ANALYTICS_TYPE_ERROR"
        super().__init__(message, **kwargs)


class AnalyticsRequiredFieldError(ServiceError):
    """Raised when required analytics fields are missing or empty."""

    def __init__(
        self,
        field_name: str,
        requirement: str = "cannot be empty",
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize analytics required field error."""
        message = f"{field_name} {requirement}"
        context = kwargs.get("context") or {}
        context.update({
            "field_name": field_name,
            "requirement": requirement,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "ANALYTICS_REQUIRED_FIELD_ERROR"
        super().__init__(message, **kwargs)
