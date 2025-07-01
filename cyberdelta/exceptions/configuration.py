"""Configuration-related exceptions for CyberDelta.

These exceptions handle configuration validation errors for exchange setup,
environment configuration, and required parameters.
"""

from typing import Any

from cyberdelta.apis.common import APIError, APIErrorCode


# Base ConfigurationError moved to cyberdelta.exceptions.base
# Import it here for API components that need APIError-based configuration errors


class ConfigurationError(APIError):
    """API-specific configuration error that requires API error handling.

    This version inherits from APIError for API components that need
    retry logic, error mapping, and other API-specific error handling.
    """

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
        """Initialize API configuration error with default code.

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


# TestnetConfigurationError moved to cyberdelta.apis.exceptions.configuration
# RateLimitConfigurationError moved to cyberdelta.apis.exceptions.configuration

# RequiredParameterError moved to cyberdelta.exceptions.base
# Import it here for backward compatibility


# HyperliquidRateLimitConfigError moved to cyberdelta.apis.exceptions.configuration
# ModelDefinitionError moved to cyberdelta.apis.exceptions.configuration

# ConfigurationInitializationError moved to cyberdelta.exceptions.base


# AppSettingsNotLoadedError moved to cyberdelta.exceptions.base


# SecretsNotLoadedError moved to cyberdelta.exceptions.base


# ConfigurationNotInitializedError moved to cyberdelta.exceptions.base


# ConfigFileNotFoundError moved to cyberdelta.exceptions.base


# ConfigFileInvalidError moved to cyberdelta.exceptions.base


# ConfigFileReadError moved to cyberdelta.exceptions.base


# ConfigValidationError moved to cyberdelta.exceptions.base


# InvalidAuthTypeError moved to cyberdelta.exceptions.base


# EmptySecretError moved to cyberdelta.exceptions.base


# StateFilePathError moved to cyberdelta.exceptions.base
