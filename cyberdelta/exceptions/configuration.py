"""Configuration-related exceptions for CyberDelta.

These exceptions handle configuration validation errors for exchange setup,
environment configuration, and required parameters.
"""

from typing import Any

from cyberdelta.apis.common import APIError, APIErrorCode


class ConfigurationError(APIError):
    """Base class for configuration-related errors."""

    def __init__(
        self,
        message: str,
        *,
        code: int | str | None = None,
        http_status: int | None = None,
        exchange_code: str | int | None = None,
        exchange_message: str | None = None,
        retry_after: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize configuration error with default code.

        Args:
            message: Human-readable error description
            code: Error code (defaults to INVALID_REQUEST)
            http_status: HTTP status code
            exchange_code: Exchange-specific error code
            exchange_message: Exchange-specific error message
            retry_after: Seconds to wait before retry
            metadata: Additional error context
            original_exception: The underlying exception
        """
        # Default to INVALID_REQUEST for configuration errors
        if code is None:
            code = APIErrorCode.INVALID_REQUEST.value
        super().__init__(
            message=message,
            code=code,
            http_status=http_status,
            exchange_code=exchange_code,
            exchange_message=exchange_message,
            retry_after=retry_after,
            metadata=metadata,
            original_exception=original_exception,
        )


class TestnetConfigurationError(ConfigurationError):
    """Raised when testnet configuration is missing or invalid."""

    def __init__(self, missing_config: str, config_type: str = "URL") -> None:
        """Initialize testnet configuration error.

        Args:
            missing_config: The configuration item that is missing
            config_type: Type of configuration (default: "URL")
        """
        self.missing_config = missing_config
        self.config_type = config_type

        message = f"Testnet {config_type} not configured but testnet environment requested"

        super().__init__(
            message=message,
            exchange_code="TESTNET_CONFIG_ERROR",
            metadata={
                "missing_config": missing_config,
                "config_type": config_type,
                "environment": "testnet",
            },
        )


class RateLimitConfigurationError(ConfigurationError):
    """Raised when rate limit configuration is missing or invalid."""

    def __init__(self, exchange: str, parameter_name: str = "rate_limit_per_minute") -> None:
        """Initialize rate limit configuration error.

        Args:
            exchange: Name of the exchange
            parameter_name: Name of the missing parameter (default: "rate_limit_per_minute")
        """
        self.exchange = exchange
        self.parameter_name = parameter_name

        super().__init__(
            message=f"{parameter_name} is required for {exchange}",
            exchange_code="RATE_LIMIT_CONFIG_ERROR",
            metadata={
                "exchange": exchange,
                "parameter": parameter_name,
                "error_type": "configuration",
            },
        )


class RequiredParameterError(ConfigurationError):
    """Raised when a required parameter is missing."""

    def __init__(self, parameter: str, context: str, exchange: str | None = None) -> None:
        """Initialize required parameter error.

        Args:
            parameter: Name of the missing parameter
            context: Context where the parameter is required
            exchange: Optional exchange name
        """
        self.parameter = parameter
        self.context = context
        self.exchange = exchange

        message = f"'{parameter}' parameter is required for {context}"
        if exchange:
            message = f"[{exchange}] {message}"

        super().__init__(
            message=message,
            exchange_code="MISSING_PARAMETER",
            metadata={"parameter": parameter, "context": context, "exchange": exchange},
        )
