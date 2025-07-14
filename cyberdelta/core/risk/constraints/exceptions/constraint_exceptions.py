"""Constraint-related exceptions."""

from cyberdelta.core.risk.exceptions.base_exceptions import RiskError


class ConstraintError(RiskError):
    """Base exception for constraint-related errors."""


class ConstraintViolationError(ConstraintError):
    """Exception for constraint violations."""


class PositionConstraintError(ConstraintError):
    """Exception for position constraint errors."""

    # Predefined error messages
    MIN_SIZE_MUST_BE_LESS_THAN_MAX_SIZE = "min_size must be less than max_size"
    MIN_ALLOCATION_MUST_BE_LESS_THAN_MAX_ALLOCATION = (
        "min_allocation must be less than max_allocation"
    )
    MAX_LEVERAGE_MUST_BE_POSITIVE = "max_leverage must be positive"
    POSITION_COUNT_LIMITS_MUST_BE_POSITIVE = "Position count limits must be positive"


class PortfolioConstraintError(ConstraintError):
    """Exception for portfolio constraint errors."""

    # Predefined error messages
    ALLOCATION_LIMITS_MUST_BE_POSITIVE = "Allocation limits must be positive"
    POSITION_LIMITS_MUST_BE_POSITIVE = "Position limits must be positive"
    DIVERSIFICATION_REQUIREMENTS_MUST_BE_POSITIVE = "Diversification requirements must be positive"


class ExchangeConstraintError(ConstraintError):
    """Exception for exchange constraint errors."""

    # Predefined error messages
    INVALID_ORDER_SIZE_RANGE = "Invalid order size range"
    INVALID_LEVERAGE_VALUE = "Invalid leverage value"
    INVALID_ALLOCATION_RANGE = "Invalid allocation range"
    INVALID_POSITION_LIMIT = "Invalid position limit"


class LeverageConstraintError(ConstraintError):
    """Exception for leverage constraint errors."""

    # Predefined error messages
    LEVERAGE_LIMITS_MUST_BE_POSITIVE = "Leverage limits must be positive"


class ConstraintConfigurationError(ConstraintError):
    """Exception for constraint configuration errors."""
