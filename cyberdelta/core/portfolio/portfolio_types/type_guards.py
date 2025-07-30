"""Type guards for runtime type checking and validation."""

from __future__ import annotations

from decimal import Decimal
from typing import Protocol, TypeGuard, runtime_checkable

# Import models for runtime isinstance checks
from cyberdelta.core.models import DerivativePosition, Order, SpotBalance, Trade
from cyberdelta.core.portfolio.portfolio_types.portfolio_data_models import (
    BalanceUpdateRequest,
    CacheStatistics,
    ExchangeBalances,
    ExchangeOrders,
    ExchangePositions,
    OrderUpdateRequest,
    PortfolioMetrics,
    PortfolioState,
    PositionUpdateRequest,
    ValidationStatistics,
)
from cyberdelta.core.portfolio.portfolio_types.result_types import (
    PortfolioResultError,
    Result,
)


# Constants for validation
MIN_SYMBOL_LENGTH = 2
MAX_SYMBOL_LENGTH = 20
MIN_ASSET_LENGTH = 2
MAX_ASSET_LENGTH = 10


@runtime_checkable
class Sized(Protocol):
    """Protocol for objects that have a length."""

    def __len__(self) -> int:
        """Return the length of the object."""
        ...


# All required imports are now at runtime level for isinstance checks


# Basic type guards
def is_string(value: object) -> TypeGuard[str]:
    """Type guard for string values.

    Args:
        value: Value to check

    Returns:
        True if value is a string, False otherwise
    """
    return isinstance(value, str)


def is_int(value: object) -> TypeGuard[int]:
    """Type guard for integer values.

    Args:
        value: Value to check

    Returns:
        True if value is an integer, False otherwise
    """
    return isinstance(value, int)


def is_float(value: object) -> TypeGuard[float]:
    """Type guard for float values.

    Args:
        value: Value to check

    Returns:
        True if value is a float or int, False otherwise
    """
    return isinstance(value, (int, float))


def is_decimal(value: object) -> TypeGuard[Decimal]:
    """Type guard for Decimal values.

    Args:
        value: Value to check

    Returns:
        True if value is a Decimal, False otherwise
    """
    return isinstance(value, Decimal)


def is_numeric(value: object) -> TypeGuard[float | int]:
    """Type guard for numeric values.

    Args:
        value: Value to check

    Returns:
        True if value is numeric (int or float), False otherwise
    """
    return isinstance(value, (int, float))


def is_positive_number(value: object) -> TypeGuard[float]:
    """Type guard for positive numeric values.

    Args:
        value: Value to check

    Returns:
        True if value is a positive number, False otherwise
    """
    return is_numeric(value) and value > 0


def is_non_negative_number(value: object) -> TypeGuard[float]:
    """Type guard for non-negative numeric values.

    Args:
        value: Value to check

    Returns:
        True if value is a non-negative number, False otherwise
    """
    return is_numeric(value) and value >= 0


def is_dict(value: object) -> TypeGuard[dict[str, object]]:
    """Type guard for dictionary values.

    Args:
        value: Value to check

    Returns:
        True if value is a dictionary, False otherwise
    """
    return isinstance(value, dict)


def is_list(value: object) -> TypeGuard[list[object]]:
    """Type guard for list values.

    Args:
        value: Value to check

    Returns:
        True if value is a list, False otherwise
    """
    return isinstance(value, list)


# Portfolio model type guards
def is_spot_balance(value: object) -> TypeGuard[SpotBalance]:
    """Type guard for SpotBalance objects.

    Args:
        value: Value to check

    Returns:
        True if value is a SpotBalance, False otherwise
    """
    return isinstance(value, SpotBalance)


def is_derivative_position(value: object) -> TypeGuard[DerivativePosition]:
    """Type guard for DerivativePosition objects.

    Args:
        value: Value to check

    Returns:
        True if value is a DerivativePosition, False otherwise
    """
    return isinstance(value, DerivativePosition)


def is_order(value: object) -> TypeGuard[Order]:
    """Type guard for Order objects.

    Args:
        value: Value to check

    Returns:
        True if value is an Order, False otherwise
    """
    return isinstance(value, Order)


def is_trade(value: object) -> TypeGuard[Trade]:
    """Type guard for Trade objects.

    Args:
        value: Value to check

    Returns:
        True if value is a Trade, False otherwise
    """
    return isinstance(value, Trade)


# Portfolio data model type guards
def is_portfolio_metrics(value: object) -> TypeGuard[PortfolioMetrics]:
    """Type guard for PortfolioMetrics objects.

    Args:
        value: Value to check

    Returns:
        True if value is a PortfolioMetrics, False otherwise
    """
    return isinstance(value, PortfolioMetrics)


def is_exchange_balances(value: object) -> TypeGuard[ExchangeBalances]:
    """Type guard for ExchangeBalances objects.

    Args:
        value: Value to check

    Returns:
        True if value is an ExchangeBalances, False otherwise
    """
    return isinstance(value, ExchangeBalances)


def is_exchange_positions(value: object) -> TypeGuard[ExchangePositions]:
    """Type guard for ExchangePositions objects.

    Args:
        value: Value to check

    Returns:
        True if value is an ExchangePositions, False otherwise
    """
    return isinstance(value, ExchangePositions)


def is_exchange_orders(value: object) -> TypeGuard[ExchangeOrders]:
    """Type guard for ExchangeOrders objects.

    Args:
        value: Value to check

    Returns:
        True if value is an ExchangeOrders, False otherwise
    """
    return isinstance(value, ExchangeOrders)


def is_portfolio_state(value: object) -> TypeGuard[PortfolioState]:
    """Type guard for PortfolioState objects.

    Args:
        value: Value to check

    Returns:
        True if value is a PortfolioState, False otherwise
    """
    return isinstance(value, PortfolioState)


def is_cache_statistics(value: object) -> TypeGuard[CacheStatistics]:
    """Type guard for CacheStatistics objects.

    Args:
        value: Value to check

    Returns:
        True if value is a CacheStatistics, False otherwise
    """
    return isinstance(value, CacheStatistics)


def is_validation_statistics(value: object) -> TypeGuard[ValidationStatistics]:
    """Type guard for ValidationStatistics objects.

    Args:
        value: Value to check

    Returns:
        True if value is a ValidationStatistics, False otherwise
    """
    return isinstance(value, ValidationStatistics)


# Update request type guards
def is_balance_update_request(value: object) -> TypeGuard[BalanceUpdateRequest]:
    """Type guard for BalanceUpdateRequest objects.

    Args:
        value: Value to check

    Returns:
        True if value is a BalanceUpdateRequest, False otherwise
    """
    return isinstance(value, BalanceUpdateRequest)


def is_position_update_request(value: object) -> TypeGuard[PositionUpdateRequest]:
    """Type guard for PositionUpdateRequest objects.

    Args:
        value: Value to check

    Returns:
        True if value is a PositionUpdateRequest, False otherwise
    """
    return isinstance(value, PositionUpdateRequest)


def is_order_update_request(value: object) -> TypeGuard[OrderUpdateRequest]:
    """Type guard for OrderUpdateRequest objects.

    Args:
        value: Value to check

    Returns:
        True if value is an OrderUpdateRequest, False otherwise
    """
    return isinstance(value, OrderUpdateRequest)


# Result type guards
def is_portfolio_error(value: object) -> TypeGuard[PortfolioResultError]:
    """Type guard for PortfolioResultError objects.

    Args:
        value: Value to check

    Returns:
        True if value is a PortfolioResultError, False otherwise
    """
    return isinstance(value, PortfolioResultError)


def is_portfolio_result_ok(value: object) -> bool:
    """Type guard for successful PortfolioResult objects.

    Note: PortfolioResult is a type alias for Result[T, PortfolioResultError].
    We check for Result base class and error type.

    Args:
        value: Value to check

    Returns:
        True if value is a successful Result, False otherwise
    """
    return isinstance(value, Result) and value.is_ok


def is_portfolio_result_error(value: object) -> bool:
    """Type guard for error PortfolioResult objects.

    Note: PortfolioResult is a type alias for Result[T, PortfolioResultError].
    We check for Result base class and error type.

    Args:
        value: Value to check

    Returns:
        True if value is an error Result, False otherwise
    """
    return isinstance(value, Result) and value.is_error


# Exchange-specific type guards
def is_valid_exchange_name(value: object) -> TypeGuard[str]:
    """Type guard for valid exchange names.

    Args:
        value: Value to check

    Returns:
        True if value is a valid exchange name, False otherwise
    """
    valid_exchanges = {"hyperliquid", "backpack"}
    return is_string(value) and value.lower() in valid_exchanges


def is_valid_symbol(value: object) -> TypeGuard[str]:
    """Type guard for valid trading symbols.

    Args:
        value: Value to check

    Returns:
        True if value is a valid symbol, False otherwise
    """
    return (
        is_string(value)
        and len(value) >= MIN_SYMBOL_LENGTH
        and len(value) <= MAX_SYMBOL_LENGTH
        and value.replace("-", "").replace("/", "").replace("_", "").isalnum()
    )


def is_valid_asset(value: object) -> TypeGuard[str]:
    """Type guard for valid asset names.

    Args:
        value: Value to check

    Returns:
        True if value is a valid asset name, False otherwise
    """
    return (
        is_string(value)
        and len(value) >= MIN_ASSET_LENGTH
        and len(value) <= MAX_ASSET_LENGTH
        and value.isalnum()
    )


# Validation helpers
def validate_numeric_range(
    value: object, min_val: float | None = None, max_val: float | None = None
) -> bool:
    """Validate that a numeric value is within a specified range.

    Args:
        value: Value to check
        min_val: Minimum allowed value (inclusive)
        max_val: Maximum allowed value (inclusive)

    Returns:
        True if value is numeric and within range, False otherwise
    """
    if not is_numeric(value):
        return False

    return not (
        (min_val is not None and value < min_val) or (max_val is not None and value > max_val)
    )


def validate_string_length(
    value: object, min_len: int | None = None, max_len: int | None = None
) -> bool:
    """Validate that a string is within specified length bounds.

    Args:
        value: Value to check
        min_len: Minimum allowed length
        max_len: Maximum allowed length

    Returns:
        True if value is a string within bounds, False otherwise
    """
    if not is_string(value):
        return False

    return not (
        (min_len is not None and len(value) < min_len)
        or (max_len is not None and len(value) > max_len)
    )


def validate_collection_size(
    value: object, min_size: int | None = None, max_size: int | None = None
) -> bool:
    """Validate that a collection is within specified size bounds.

    Args:
        value: Value to check
        min_size: Minimum allowed size
        max_size: Maximum allowed size

    Returns:
        True if value is a collection within bounds, False otherwise
    """
    if not isinstance(value, Sized):
        return False

    # Use the Sized protocol to safely call len()
    size = len(value)
    return not (
        (min_size is not None and size < min_size) or (max_size is not None and size > max_size)
    )


# Comprehensive type validation - broken down into helper functions
def _validate_required_fields(data: dict[str, object]) -> list[str]:
    """Validate that required fields are present.

    Args:
        data: Dictionary to validate

    Returns:
        List of validation error messages
    """
    required_fields = ["balances", "positions", "metrics"]
    return [f"Missing required field: {field}" for field in required_fields if field not in data]


def _validate_balances_data(data: dict[str, object]) -> list[str]:
    """Validate balances data structure.

    Args:
        data: Dictionary containing balances data

    Returns:
        List of validation error messages
    """
    issues: list[str] = []
    if "balances" not in data:
        return issues

    if not is_dict(data["balances"]):
        issues.append("Balances must be a dictionary")
    else:
        for exchange, balance_data in data["balances"].items():
            if not is_string(exchange):
                issues.append(f"Exchange name must be string: {exchange}")
            if not is_exchange_balances(balance_data):
                issues.append(f"Invalid balance data for exchange: {exchange}")
    return issues


def _validate_positions_data(data: dict[str, object]) -> list[str]:
    """Validate positions data structure.

    Args:
        data: Dictionary containing positions data

    Returns:
        List of validation error messages
    """
    issues: list[str] = []
    if "positions" not in data:
        return issues

    if not is_dict(data["positions"]):
        issues.append("Positions must be a dictionary")
    else:
        for exchange, position_data in data["positions"].items():
            if not is_string(exchange):
                issues.append(f"Exchange name must be string: {exchange}")
            if not is_exchange_positions(position_data):
                issues.append(f"Invalid position data for exchange: {exchange}")
    return issues


def _validate_metrics_data(data: dict[str, object]) -> list[str]:
    """Validate metrics data structure.

    Args:
        data: Dictionary containing metrics data

    Returns:
        List of validation error messages
    """
    issues: list[str] = []
    if "metrics" in data and not is_portfolio_metrics(data["metrics"]):
        issues.append("Invalid portfolio metrics data")
    return issues


def validate_portfolio_data_integrity(data: object) -> list[str]:
    """Validate portfolio data integrity and return list of issues.

    Args:
        data: Data to validate

    Returns:
        List of validation error messages, empty if valid
    """
    issues: list[str] = []

    if not is_dict(data):
        issues.append("Data must be a dictionary")
        return issues

    # Use helper functions to validate different aspects
    issues.extend(_validate_required_fields(data))
    issues.extend(_validate_balances_data(data))
    issues.extend(_validate_positions_data(data))
    issues.extend(_validate_metrics_data(data))

    return issues


def is_valid_portfolio_data(data: object) -> TypeGuard[dict[str, object]]:
    """Type guard for valid portfolio data with comprehensive validation.

    Args:
        data: Data to validate

    Returns:
        True if data is valid portfolio data, False otherwise
    """
    issues = validate_portfolio_data_integrity(data)
    return len(issues) == 0
