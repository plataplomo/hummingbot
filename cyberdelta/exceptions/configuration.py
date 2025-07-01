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


class HyperliquidRateLimitConfigError(ConfigurationError):
    """Raised when Hyperliquid rate limit configuration is incomplete."""

    def __init__(self, missing_fields: list[str]) -> None:
        """Initialize Hyperliquid rate limit configuration error.

        Args:
            missing_fields: List of missing configuration fields
        """
        self.missing_fields = missing_fields

        fields_str = " and ".join(missing_fields)
        message = f"HyperliquidRateLimitStrategy requires {fields_str} configuration"

        super().__init__(
            message=message,
            exchange_code="HL_RATE_LIMIT_CONFIG_ERROR",
            metadata={
                "exchange": "hyperliquid",
                "missing_fields": missing_fields,
                "error_type": "rate_limit_configuration",
            },
        )


class ModelDefinitionError(RuntimeError, ConfigurationError):
    """Raised when a model definition has incorrect structure."""

    def __init__(self, model_name: str, issue: str) -> None:
        """Initialize model definition error.

        Args:
            model_name: Name of the model with the issue
            issue: Description of the structural issue
        """
        self.model_name = model_name
        self.issue = issue

        message = f"{model_name} model definition error: {issue}"

        RuntimeError.__init__(self, message)
        ConfigurationError.__init__(
            self,
            message=message,
            exchange_code="MODEL_DEFINITION_ERROR",
            metadata={"model_name": model_name, "issue": issue},
        )


class ConfigurationInitializationError(RuntimeError):
    """Raised when configuration system initialization fails."""

    def __init__(self, reason: str, original_error: Exception) -> None:
        """Initialize configuration initialization error.

        Args:
            reason: Brief description of what failed
            original_error: The underlying exception that caused the failure
        """
        self.reason = reason
        self.original_error = original_error

        message = f"Configuration system initialization failed: {reason}"
        super().__init__(message)


class AppSettingsNotLoadedError(RuntimeError):
    """Raised when AppSettings fails to load from ConfigManager."""

    def __init__(self, config_path: str, config_loaded: bool) -> None:
        """Initialize app settings not loaded error.

        Args:
            config_path: Path where config was expected
            config_loaded: Whether config was marked as loaded
        """
        self.config_path = config_path
        self.config_loaded = config_loaded

        message = "AppSettings failed to load. Check logs for details from ConfigManager."
        super().__init__(message)


class SecretsNotLoadedError(RuntimeError):
    """Raised when SecretsConfig fails to load from SecretsManager."""

    def __init__(self, secrets_path: str, secrets_loaded: bool, env_path_set: bool) -> None:
        """Initialize secrets not loaded error.

        Args:
            secrets_path: Path where secrets were expected
            secrets_loaded: Whether secrets were marked as loaded
            env_path_set: Whether CYBERDELTA_SECRETS_PATH was set
        """
        self.secrets_path = secrets_path
        self.secrets_loaded = secrets_loaded
        self.env_path_set = env_path_set

        message = f"SecretsConfig failed to load. Check logs. Path used by manager: {secrets_path}"
        super().__init__(message)


class ConfigurationNotInitializedError(RuntimeError):
    """Raised when attempting to access configuration before initialization."""

    def __init__(self, config_type: str) -> None:
        """Initialize configuration not initialized error.

        Args:
            config_type: Type of configuration that wasn't initialized
        """
        self.config_type = config_type

        message = f"{config_type} not initialized. Call _initialize_config() first."
        super().__init__(message)


class ConfigFileNotFoundError(ConfigurationError):
    """Raised when configuration file is not found."""

    def __init__(self, config_path: str) -> None:
        """Initialize config file not found error.

        Args:
            config_path: Path where config file was expected
        """
        self.config_path = config_path

        message = f"Config file not found: {config_path}"
        super().__init__(
            message=message,
            exchange_code="CONFIG_FILE_NOT_FOUND",
            metadata={"config_path": config_path},
        )


class ConfigFileInvalidError(ConfigurationError):
    """Raised when configuration file has invalid content."""

    def __init__(self, config_path: str, data_type: str) -> None:
        """Initialize config file invalid error.

        Args:
            config_path: Path to the invalid config file
            data_type: Type of data found instead of dict
        """
        self.config_path = config_path
        self.data_type = data_type

        message = f"Invalid or empty content in config file: {config_path}"
        super().__init__(
            message=message,
            exchange_code="CONFIG_FILE_INVALID",
            metadata={"config_path": config_path, "data_type": data_type},
        )


class ConfigFileReadError(ConfigurationError):
    """Raised when configuration file cannot be read."""

    def __init__(self, config_path: str, error: Exception) -> None:
        """Initialize config file read error.

        Args:
            config_path: Path to the config file
            error: The underlying exception
        """
        self.config_path = config_path

        message = f"Error reading config file {config_path}: {error}"
        super().__init__(
            message=message,
            exchange_code="CONFIG_FILE_READ_ERROR",
            metadata={"config_path": config_path, "error_type": type(error).__name__},
            original_exception=error,
        )


class ConfigValidationError(ConfigurationError):
    """Raised when configuration validation fails."""

    def __init__(self, config_path: str, validation_error: Exception) -> None:
        """Initialize config validation error.

        Args:
            config_path: Path to the config file
            validation_error: The Pydantic validation error
        """
        self.config_path = config_path

        message = f"Invalid application configuration in {config_path}: {validation_error}"
        super().__init__(
            message=message,
            exchange_code="CONFIG_VALIDATION_ERROR",
            metadata={"config_path": config_path},
            original_exception=validation_error,
        )


class InvalidAuthTypeError(ValueError):
    """Raised when exchange has incorrect authentication type."""

    def __init__(
        self, exchange: str, expected_auth_type: str, actual_auth_type: str | None = None
    ) -> None:
        """Initialize invalid auth type error.

        Args:
            exchange: Name of the exchange
            expected_auth_type: Expected authentication type
            actual_auth_type: Actual authentication type found
        """
        self.exchange = exchange
        self.expected_auth_type = expected_auth_type
        self.actual_auth_type = actual_auth_type

        message = (
            f"{exchange} configuration in secrets must have auth_type '{expected_auth_type}' "
            f"and corresponding fields."
        )
        super().__init__(message)


class EmptySecretError(ValueError):
    """Raised when a required secret field is empty."""

    def __init__(
        self, exchange: str, field_name: str, field_description: str | None = None
    ) -> None:
        """Initialize empty secret error.

        Args:
            exchange: Name of the exchange
            field_name: Name of the empty field
            field_description: Optional description of the field
        """
        self.exchange = exchange
        self.field_name = field_name
        self.field_description = field_description

        if field_description:
            message = f"{exchange} '{field_name}' ({field_description}) cannot be empty."
        else:
            message = f"{exchange} '{field_name}' cannot be empty in secrets."
        super().__init__(message)


class StateFilePathError(ValueError):
    """Raised when state file path is None or invalid."""

    def __init__(self, operation: str = "save") -> None:
        """Initialize state file path error.

        Args:
            operation: The operation being performed (save/load)
        """
        self.operation = operation
        message = f"State file path cannot be None for {operation} operation"
        super().__init__(message)
