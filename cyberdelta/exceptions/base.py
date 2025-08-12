"""Base configuration exceptions for CyberDelta.

These exceptions handle configuration system errors that don't require
API error handling capabilities. They inherit from standard Python
exceptions to avoid circular dependencies with the API layer.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from cyberdelta.enums import ExchangeName


class ConfigurationError(RuntimeError):
    """Base class for configuration-related errors.

    Used for configuration system errors that don't need API retry logic
    or error mapping. This avoids circular dependencies with the API layer.
    """

    def __init__(
        self,
        message: str,
        *,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize configuration error.

        Args:
            message: Human-readable error description
            metadata: Additional error context
            original_exception: The underlying exception
        """
        self.metadata = metadata or {}
        self.original_exception = original_exception
        super().__init__(message)


class RequiredParameterError(ConfigurationError):
    """Raised when a required parameter is missing.

    Used for configuration validation errors that don't require
    API-specific error handling.
    """

    def __init__(self, parameter: str, context: str, exchange: ExchangeName | None = None) -> None:
        """Initialize required parameter error.

        Args:
            parameter: Name of the missing parameter
            context: Context where the parameter is required
            exchange: Optional exchange enum
        """
        self.parameter = parameter
        self.context = context
        self.exchange = exchange

        message = f"'{parameter}' parameter is required for {context}"
        if exchange:
            message = f"[{exchange.value}] {message}"

        super().__init__(
            message=message,
            metadata={
                "parameter": parameter,
                "context": context,
                "exchange": exchange.value if exchange else None,
            },
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
            metadata={"config_path": config_path},
            original_exception=validation_error,
        )


class InvalidAuthTypeError(ValueError):
    """Raised when exchange has incorrect authentication type."""

    def __init__(
        self, exchange: ExchangeName, expected_auth_type: str, actual_auth_type: str | None = None
    ) -> None:
        """Initialize invalid auth type error.

        Args:
            exchange: Exchange enum value
            expected_auth_type: Expected authentication type
            actual_auth_type: Actual authentication type found
        """
        self.exchange = exchange
        self.expected_auth_type = expected_auth_type
        self.actual_auth_type = actual_auth_type

        message = (
            f"{exchange.value} configuration in secrets must have auth_type '{expected_auth_type}' "
            f"and corresponding fields."
        )
        super().__init__(message)


class EmptySecretError(ValueError):
    """Raised when a required secret field is empty."""

    def __init__(
        self, exchange: ExchangeName, field_name: str, field_description: str | None = None
    ) -> None:
        """Initialize empty secret error.

        Args:
            exchange: Exchange enum value
            field_name: Name of the empty field
            field_description: Optional description of the field
        """
        self.exchange = exchange
        self.field_name = field_name
        self.field_description = field_description

        if field_description:
            message = f"{exchange.value} '{field_name}' ({field_description}) cannot be empty."
        else:
            message = f"{exchange.value} '{field_name}' cannot be empty in secrets."
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
            metadata={
                "missing_config": missing_config,
                "config_type": config_type,
                "environment": "testnet",
            },
        )
