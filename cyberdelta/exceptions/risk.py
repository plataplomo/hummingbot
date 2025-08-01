"""Risk management exceptions for CyberDelta.

These exceptions handle risk management errors including configuration,
validation, and portfolio constraint violations.
"""

from cyberdelta.core.symbols import Symbol


class RiskManagerError(Exception):
    """Base exception for all RiskManager errors."""


class RiskConfigError(RiskManagerError):
    """Raised when risk management configuration is missing or invalid."""

    def __init__(self, config_issue: str, original_error: Exception | None = None) -> None:
        """Initialize risk config error.

        Args:
            config_issue: Description of the configuration issue
            original_error: The underlying exception that caused the error
        """
        self.config_issue = config_issue
        self.original_error = original_error

        message = (
            f"Invalid or missing configuration value during RiskManager "
            f"initialization: {config_issue}"
        )
        super().__init__(message)


class RiskCheckError(RiskManagerError):
    """Raised when an opportunity or action fails risk validation checks."""

    def __init__(self, symbol: Symbol, validation_type: str, reason: str | None = None) -> None:
        """Initialize risk check error.

        Args:
            symbol: Trading Symbol object that failed validation
            validation_type: Type of validation that failed
            reason: Optional detailed reason for failure
        """
        self.symbol = symbol
        self.validation_type = validation_type
        self.reason = reason

        message = f"Validation failed at {validation_type} for {symbol.value}"
        if reason:
            message = f"{message}: {reason}"
        super().__init__(message)


class ConstraintViolationError(RiskManagerError):
    """Raised when a portfolio constraint is violated."""

    def __init__(
        self,
        constraint_type: str,
        current_value: float | None = None,
        limit_value: float | None = None,
        symbol: str | None = None,
    ) -> None:
        """Initialize constraint violation error.

        Args:
            constraint_type: Type of constraint violated
            current_value: Current value that violates the constraint
            limit_value: The limit that was exceeded
            symbol: Optional symbol related to the violation
        """
        self.constraint_type = constraint_type
        self.current_value = current_value
        self.limit_value = limit_value
        self.symbol = symbol

        message = f"Portfolio constraint violation: {constraint_type}"
        if current_value is not None and limit_value is not None:
            message = f"{message} (current: {current_value}, limit: {limit_value})"
        if symbol:
            message = f"{message} for {symbol}"
        super().__init__(message)
