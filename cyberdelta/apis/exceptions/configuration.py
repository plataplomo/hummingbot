"""API-specific configuration exceptions.

These configuration exceptions are used exclusively within API modules
and inherit from APIError to maintain proper error handling hierarchy.
"""

from cyberdelta.apis.common import APIError, APIErrorCode


class TestnetConfigurationError(APIError):
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
            code=APIErrorCode.INVALID_REQUEST.value,
            exchange_code="TESTNET_CONFIG_ERROR",
            metadata={
                "missing_config": missing_config,
                "config_type": config_type,
                "environment": "testnet",
            },
        )


class RateLimitConfigurationError(APIError):
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
            code=APIErrorCode.INVALID_REQUEST.value,
            exchange_code="RATE_LIMIT_CONFIG_ERROR",
            metadata={
                "exchange": exchange,
                "parameter": parameter_name,
                "error_type": "configuration",
            },
        )


class HyperliquidRateLimitConfigError(APIError):
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
            code=APIErrorCode.INVALID_REQUEST.value,
            exchange_code="HL_RATE_LIMIT_CONFIG_ERROR",
            metadata={
                "exchange": "hyperliquid",
                "missing_fields": missing_fields,
                "error_type": "rate_limit_configuration",
            },
        )


class ModelDefinitionError(RuntimeError, APIError):
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
        APIError.__init__(
            self,
            message=message,
            code=APIErrorCode.INVALID_REQUEST.value,
            exchange_code="MODEL_DEFINITION_ERROR",
            metadata={"model_name": model_name, "issue": issue},
        )
