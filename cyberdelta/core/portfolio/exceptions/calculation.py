"""Calculation-related exceptions for portfolio management."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, TypedDict, Unpack

from cyberdelta.core.portfolio.exceptions.base import PortfolioError


class ExceptionKwargs(TypedDict, total=False):
    """Type definition for exception keyword arguments."""

    error_code: str | None
    context: dict[str, Any] | None
    recoverable: bool


class CalculationError(PortfolioError):
    """Base exception for calculation errors."""

    def _get_default_error_code(self) -> str:
        """Get default error code for calculation exceptions."""
        return f"CALC_{self.__class__.__name__.upper()}"


class InsufficientDataError(CalculationError):
    """Raised when there's insufficient data for calculation."""

    def __init__(
        self,
        message: str,
        required_data: list[str] | None = None,
        available_data: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize insufficient data exception.

        Args:
            message: Error message
            required_data: List of required data elements
            available_data: List of available data elements
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "required_data": required_data or [],
            "available_data": available_data or [],
        })
        kwargs["context"] = context
        super().__init__(message, **kwargs)


class InvalidCalculationInputError(CalculationError):
    """Raised when calculation inputs are invalid."""

    def __init__(
        self,
        parameter: str | None = None,
        value: Decimal | float | str | None = None,
        expected: str | None = None,
        message: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize invalid input exception.

        Args:
            parameter: Parameter name
            value: Invalid value
            expected: Expected value or type
            message: Optional custom error message
            **kwargs: Additional context
        """
        # Build default message if not provided
        if message is None:
            if parameter and expected:
                message = f"Invalid input for {parameter}: expected {expected}, got {value}"
            elif parameter:
                message = f"Invalid input for {parameter}: {value}"
            else:
                message = f"Invalid calculation input: {value}"

        context = kwargs.get("context") or {}
        context.update({
            "parameter": parameter,
            "value": str(value) if value is not None else None,
            "expected": expected,
        })
        kwargs["context"] = context
        super().__init__(message, **kwargs)


class DivisionByZeroError(CalculationError):
    """Raised when division by zero is attempted."""

    def __init__(
        self,
        message: str = "Division by zero attempted",
        numerator: Decimal | None = None,
        denominator: Decimal | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize division by zero exception.

        Args:
            message: Error message
            numerator: Numerator value
            denominator: Denominator value (should be zero)
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "numerator": str(numerator) if numerator is not None else None,
            "denominator": str(denominator) if denominator is not None else None,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_DIVISION_BY_ZERO"
        super().__init__(message, **kwargs)


class PnLCalculationError(CalculationError):
    """Raised when P&L calculation fails."""

    def __init__(
        self,
        message: str,
        position_id: str | None = None,
        calculation_type: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize P&L calculation exception.

        Args:
            message: Error message
            position_id: Position ID involved
            calculation_type: Type of P&L calculation (realized/unrealized)
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "position_id": position_id,
            "calculation_type": calculation_type,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_PNL_ERROR"
        super().__init__(message, **kwargs)


class ExposureCalculationError(CalculationError):
    """Raised when exposure calculation fails."""

    def __init__(
        self,
        message: str,
        exposure_type: str | None = None,
        symbol: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize exposure calculation exception.

        Args:
            message: Error message
            exposure_type: Type of exposure calculation
            symbol: Symbol involved
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "exposure_type": exposure_type,
            "symbol": symbol,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_EXPOSURE_ERROR"
        super().__init__(message, **kwargs)


class CurrencyConversionError(CalculationError):
    """Raised when currency conversion fails."""

    def __init__(
        self,
        message: str,
        from_currency: str | None = None,
        to_currency: str | None = None,
        amount: Decimal | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize currency conversion exception.

        Args:
            message: Error message
            from_currency: Source currency
            to_currency: Target currency
            amount: Amount to convert
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "from_currency": from_currency,
            "to_currency": to_currency,
            "amount": str(amount) if amount is not None else None,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_CURRENCY_CONVERSION"
        super().__init__(message, **kwargs)


class RiskLimitExceededError(CalculationError):
    """Raised when risk limits are exceeded."""

    def __init__(
        self,
        message: str,
        limit_type: str,
        current_value: Decimal,
        limit_value: Decimal,
        severity: str = "HIGH",
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize risk limit exceeded exception.

        Args:
            message: Error message
            limit_type: Type of limit exceeded
            current_value: Current value
            limit_value: Limit value
            severity: Severity level
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "limit_type": limit_type,
            "current_value": str(current_value),
            "limit_value": str(limit_value),
            "severity": severity,
            "exceeded_by": str(current_value - limit_value),
        })
        kwargs["context"] = context
        kwargs["recoverable"] = severity != "CRITICAL"
        kwargs["error_code"] = "CALC_RISK_LIMIT_EXCEEDED"
        super().__init__(message, **kwargs)


class CalculatorConfigurationError(CalculationError):
    """Raised when calculator configuration is invalid."""

    def __init__(
        self,
        message: str,
        parameter: str | None = None,
        value: str | float | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize calculator configuration exception.

        Args:
            message: Error message
            parameter: Configuration parameter name
            value: Invalid configuration value
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "parameter": parameter,
            "value": str(value) if value is not None else None,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_CONFIGURATION_ERROR"
        super().__init__(message, **kwargs)


class CalculatorValidationError(CalculationError):
    """Raised when calculator input validation fails."""

    def __init__(
        self,
        field_name: str | None = None,
        constraint: str | None = None,
        message: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize calculator validation exception.

        Args:
            field_name: Field that failed validation
            constraint: Constraint that was violated
            message: Optional custom error message
            **kwargs: Additional context
        """
        # Build default message if not provided
        if message is None:
            if field_name and constraint:
                message = f"Validation failed for {field_name}: {constraint}"
            elif field_name:
                message = f"Validation failed for {field_name}"
            elif constraint:
                message = f"Validation constraint violated: {constraint}"
            else:
                message = "Calculator validation failed"

        context = kwargs.get("context") or {}
        context.update({
            "field_name": field_name,
            "constraint": constraint,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_VALIDATION_ERROR"
        super().__init__(message, **kwargs)


class InvalidCalculatorNameError(CalculatorConfigurationError):
    """Raised when calculator name is invalid."""

    def __init__(
        self,
        calculator_name: str,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize invalid calculator name exception.

        Args:
            calculator_name: The invalid calculator name
            **kwargs: Additional context
        """
        message = f"Invalid calculator name: '{calculator_name}'"
        context = kwargs.get("context") or {}
        context.update({
            "calculator_name": calculator_name,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_INVALID_NAME"
        super().__init__(message, **kwargs)


class NegativeExecutionTimeError(CalculatorConfigurationError):
    """Raised when execution time is negative."""

    def __init__(
        self,
        execution_time: float,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize negative execution time exception.

        Args:
            execution_time: The negative execution time value
            **kwargs: Additional context
        """
        message = f"Execution time cannot be negative: {execution_time}"
        context = kwargs.get("context") or {}
        context.update({
            "execution_time": execution_time,
            "parameter": "execution_time",
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_NEGATIVE_EXECUTION_TIME"
        super().__init__(message, **kwargs)


class InvalidRateRangeError(CalculatorConfigurationError):
    """Raised when rate is outside valid range [0, 1]."""

    def __init__(
        self,
        rate: float,
        rate_type: str,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize invalid rate range exception.

        Args:
            rate: The invalid rate value
            rate_type: Type of rate (e.g., 'profit_rate', 'loss_rate')
            **kwargs: Additional context
        """
        message = f"{rate_type} must be between 0 and 1, got {rate}"
        context = kwargs.get("context") or {}
        context.update({
            "rate": rate,
            "rate_type": rate_type,
            "valid_range": "[0, 1]",
            "parameter": rate_type,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_INVALID_RATE_RANGE"
        super().__init__(message, **kwargs)


class CurrencyExposureTypeError(CalculatorConfigurationError):
    """Raised when currency exposure type is invalid."""

    def __init__(
        self,
        exposure_type: str,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize currency exposure type exception.

        Args:
            exposure_type: The name of the invalid exposure type
            **kwargs: Additional context
        """
        message = f"Currency exposure must be a dictionary, got {exposure_type}"
        context = kwargs.get("context") or {}
        context.update({
            "exposure_type": str(type(exposure_type).__name__),
            "expected_type": "dict",
            "parameter": "currency_exposure",
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_CURRENCY_EXPOSURE_TYPE"
        super().__init__(message, **kwargs)


class CurrencyMismatchError(CalculatorConfigurationError):
    """Raised when currencies don't match where they should."""

    def __init__(
        self,
        expected_currency: str,
        actual_currency: str,
        context_info: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize currency mismatch exception.

        Args:
            expected_currency: The expected currency
            actual_currency: The actual currency found
            context_info: Additional context about where the mismatch occurred
            **kwargs: Additional context
        """
        if context_info:
            message = (
                f"Currency mismatch in {context_info}: "
                f"expected '{expected_currency}', got '{actual_currency}'"
            )
        else:
            message = f"Currency mismatch: expected '{expected_currency}', got '{actual_currency}'"

        context = kwargs.get("context") or {}
        context.update({
            "expected_currency": expected_currency,
            "actual_currency": actual_currency,
            "mismatch_context": context_info,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "CALC_CURRENCY_MISMATCH"
        super().__init__(message, **kwargs)
