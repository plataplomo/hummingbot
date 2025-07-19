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
        message: str,
        parameter: str | None = None,
        value: Decimal | float | str | None = None,
        expected: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize invalid input exception.

        Args:
            message: Error message
            parameter: Parameter name
            value: Invalid value
            expected: Expected value or type
            **kwargs: Additional context
        """
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
