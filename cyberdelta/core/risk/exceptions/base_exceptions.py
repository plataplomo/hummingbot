"""Base exceptions for the risk management module.

These exceptions provide structured error handling for risk management operations
with comprehensive context preservation and metadata support.
"""

from decimal import Decimal
from typing import Any


class RiskError(Exception):
    """Base exception for all risk-related errors.

    Provides structured error handling with metadata support and original
    exception tracking for comprehensive debugging and error reporting.
    """

    def __init__(
        self,
        message: str,
        *,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize risk error.

        Args:
            message: Human-readable error description
            metadata: Additional error context and debugging information
            original_exception: The underlying exception that caused this error
            symbol: Trading symbol associated with the error (e.g., "BTC-PERP")
            exchange: Exchange name associated with the error (e.g., "hyperliquid")
        """
        self.metadata = metadata or {}
        self.original_exception = original_exception
        self.symbol = symbol
        self.exchange = exchange

        # Add symbol and exchange to metadata if provided
        if symbol:
            self.metadata["symbol"] = symbol
        if exchange:
            self.metadata["exchange"] = exchange
        if original_exception:
            self.metadata["original_error_type"] = type(original_exception).__name__

        super().__init__(message)


class RiskCheckError(RiskError):
    """Base exception for check-related errors.

    Used for errors that occur during opportunity checking phase,
    including validation failures and check execution errors.
    """

    def __init__(
        self,
        message: str,
        *,
        checker_name: str | None = None,
        check_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize risk check error.

        Args:
            message: Human-readable error description
            checker_name: Name of the checker that failed (e.g., "ProfitabilityChecker")
            check_type: Type of check that failed (e.g., "profitability", "balance")
            metadata: Additional error context and debugging information
            original_exception: The underlying exception that caused this error
            symbol: Trading symbol associated with the error (e.g., "BTC-PERP")
            exchange: Exchange name associated with the error (e.g., "hyperliquid")
        """
        self.checker_name = checker_name
        self.check_type = check_type

        # Add checker context to metadata
        if metadata is None:
            metadata = {}
        if checker_name:
            metadata["checker_name"] = checker_name
        if check_type:
            metadata["check_type"] = check_type

        super().__init__(
            message,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class RiskSizingError(RiskError):
    """Base exception for sizing-related errors.

    Used for errors that occur during position sizing calculations,
    including Kelly criterion failures and capital allocation errors.
    """

    def __init__(
        self,
        message: str,
        *,
        sizing_method: str | None = None,
        position_size: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize risk sizing error.

        Args:
            message: Human-readable error description
            sizing_method: Sizing method used (e.g., "kelly", "simple")
            position_size: Position size that caused the error (in USD)
            metadata: Additional error context and debugging information
            original_exception: The underlying exception that caused this error
            symbol: Trading symbol associated with the error (e.g., "BTC-PERP")
            exchange: Exchange name associated with the error (e.g., "hyperliquid")
        """
        self.sizing_method = sizing_method
        self.position_size = position_size

        # Add sizing context to metadata
        if metadata is None:
            metadata = {}
        if sizing_method:
            metadata["sizing_method"] = sizing_method
        if position_size is not None:
            metadata["position_size"] = position_size

        super().__init__(
            message,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class RiskConstraintError(RiskError):
    """Base exception for constraint-related errors.

    Used for errors that occur during constraint validation,
    including position limits, portfolio limits, and leverage constraints.
    """

    def __init__(
        self,
        message: str,
        *,
        constraint_type: str | None = None,
        constraint_value: float | None = None,
        limit_value: float | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize risk constraint error.

        Args:
            message: Human-readable error description
            constraint_type: Type of constraint violated (e.g., "position", "portfolio")
            constraint_value: Value that violated the constraint
            limit_value: The constraint limit that was exceeded
            metadata: Additional error context and debugging information
            original_exception: The underlying exception that caused this error
            symbol: Trading symbol associated with the error (e.g., "BTC-PERP")
            exchange: Exchange name associated with the error (e.g., "hyperliquid")
        """
        self.constraint_type = constraint_type
        self.constraint_value = constraint_value
        self.limit_value = limit_value

        # Add constraint context to metadata
        if metadata is None:
            metadata = {}
        if constraint_type:
            metadata["constraint_type"] = constraint_type
        if constraint_value is not None:
            metadata["constraint_value"] = constraint_value
        if limit_value is not None:
            metadata["limit_value"] = limit_value

        super().__init__(
            message,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class RiskCalculationError(RiskError):
    """Base exception for calculation-related errors.

    Used for errors that occur during risk metric calculations,
    including volatility calculations, correlation analysis, and statistical operations.
    """

    # Predefined error messages
    INSUFFICIENT_DATA = "Insufficient data"
    NEGATIVE_RISK_FREE_RATE = "Risk-free rate cannot be negative"
    INVALID_VAR_METHOD = "Invalid VaR method"

    def __init__(
        self,
        message: str,
        *,
        calculation_type: str | None = None,
        input_data_size: int | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize risk calculation error.

        Args:
            message: Human-readable error description
            calculation_type: Type of calculation that failed (e.g., "volatility", "kelly")
            input_data_size: Size of input data that caused the error
            metadata: Additional error context and debugging information
            original_exception: The underlying exception that caused this error
            symbol: Trading symbol associated with the error (e.g., "BTC-PERP")
            exchange: Exchange name associated with the error (e.g., "hyperliquid")
        """
        self.calculation_type = calculation_type
        self.input_data_size = input_data_size

        # Add calculation context to metadata
        if metadata is None:
            metadata = {}
        if calculation_type:
            metadata["calculation_type"] = calculation_type
        if input_data_size is not None:
            metadata["input_data_size"] = input_data_size

        super().__init__(
            message,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )


class RiskConfigError(RiskError):
    """Base exception for configuration-related errors.

    Used for errors that occur during risk configuration validation
    and setup, including invalid parameters and configuration conflicts.
    """

    # Predefined error messages
    UNKNOWN_PRESET_CONFIGURATION = "Unknown preset configuration"
    UNSUPPORTED_STORAGE_TYPE = "Unsupported storage type"
    NO_SESSION_ID_PROVIDED = "No session ID provided"
    CANNOT_CREATE_STATE_MANAGER = "Cannot create state manager"

    def __init__(
        self,
        message: str,
        *,
        config_field: str | None = None,
        config_value: str | float | bool | Decimal | list[Any] | dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> None:
        """Initialize risk configuration error.

        Args:
            message: Human-readable error description
            config_field: Configuration field that caused the error
            config_value: Invalid configuration value
            metadata: Additional error context and debugging information
            original_exception: The underlying exception that caused this error
            symbol: Trading symbol associated with the error (e.g., "BTC-PERP")
            exchange: Exchange name associated with the error (e.g., "hyperliquid")
        """
        self.config_field = config_field
        self.config_value = config_value

        # Add configuration context to metadata
        if metadata is None:
            metadata = {}
        if config_field:
            metadata["config_field"] = config_field
        if config_value is not None:
            metadata["config_value"] = str(config_value)

        super().__init__(
            message,
            metadata=metadata,
            original_exception=original_exception,
            symbol=symbol,
            exchange=exchange,
        )
