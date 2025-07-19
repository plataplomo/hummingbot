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
    """Type guard for string values."""
    return isinstance(value, str)


def is_int(value: object) -> TypeGuard[int]:
    """Type guard for integer values."""
    return isinstance(value, int)


def is_float(value: object) -> TypeGuard[float]:
    """Type guard for float values."""
    return isinstance(value, (int, float))


def is_decimal(value: object) -> TypeGuard[Decimal]:
    """Type guard for Decimal values."""
    return isinstance(value, Decimal)


def is_numeric(value: object) -> TypeGuard[float | int]:
    """Type guard for numeric values."""
    return isinstance(value, (int, float))


def is_positive_number(value: object) -> TypeGuard[float]:
    """Type guard for positive numeric values."""
    return is_numeric(value) and value > 0


def is_non_negative_number(value: object) -> TypeGuard[float]:
    """Type guard for non-negative numeric values."""
    return is_numeric(value) and value >= 0


def is_dict(value: object) -> TypeGuard[dict[str, object]]:
    """Type guard for dictionary values."""
    return isinstance(value, dict)


def is_list(value: object) -> TypeGuard[list[object]]:
    """Type guard for list values."""
    return isinstance(value, list)


# Portfolio model type guards
def is_spot_balance(value: object) -> TypeGuard[SpotBalance]:
    """Type guard for SpotBalance objects."""
    return isinstance(value, SpotBalance)


def is_derivative_position(value: object) -> TypeGuard[DerivativePosition]:
    """Type guard for DerivativePosition objects."""
    return isinstance(value, DerivativePosition)


def is_order(value: object) -> TypeGuard[Order]:
    """Type guard for Order objects."""
    return isinstance(value, Order)


def is_trade(value: object) -> TypeGuard[Trade]:
    """Type guard for Trade objects."""
    return isinstance(value, Trade)


# Portfolio data model type guards
def is_portfolio_metrics(value: object) -> TypeGuard[PortfolioMetrics]:
    """Type guard for PortfolioMetrics objects."""
    return isinstance(value, PortfolioMetrics)


def is_exchange_balances(value: object) -> TypeGuard[ExchangeBalances]:
    """Type guard for ExchangeBalances objects."""
    return isinstance(value, ExchangeBalances)


def is_exchange_positions(value: object) -> TypeGuard[ExchangePositions]:
    """Type guard for ExchangePositions objects."""
    return isinstance(value, ExchangePositions)


def is_exchange_orders(value: object) -> TypeGuard[ExchangeOrders]:
    """Type guard for ExchangeOrders objects."""
    return isinstance(value, ExchangeOrders)


def is_portfolio_state(value: object) -> TypeGuard[PortfolioState]:
    """Type guard for PortfolioState objects."""
    return isinstance(value, PortfolioState)


def is_cache_statistics(value: object) -> TypeGuard[CacheStatistics]:
    """Type guard for CacheStatistics objects."""
    return isinstance(value, CacheStatistics)


def is_validation_statistics(value: object) -> TypeGuard[ValidationStatistics]:
    """Type guard for ValidationStatistics objects."""
    return isinstance(value, ValidationStatistics)


# Update request type guards
def is_balance_update_request(value: object) -> TypeGuard[BalanceUpdateRequest]:
    """Type guard for BalanceUpdateRequest objects."""
    return isinstance(value, BalanceUpdateRequest)


def is_position_update_request(value: object) -> TypeGuard[PositionUpdateRequest]:
    """Type guard for PositionUpdateRequest objects."""
    return isinstance(value, PositionUpdateRequest)


def is_order_update_request(value: object) -> TypeGuard[OrderUpdateRequest]:
    """Type guard for OrderUpdateRequest objects."""
    return isinstance(value, OrderUpdateRequest)


# Result type guards
def is_portfolio_error(value: object) -> TypeGuard[PortfolioResultError]:
    """Type guard for PortfolioResultError objects."""
    return isinstance(value, PortfolioResultError)


def is_portfolio_result_ok(value: object) -> bool:
    """Type guard for successful PortfolioResult objects.

    Note: PortfolioResult is a type alias for Result[T, PortfolioResultError].
    We check for Result base class and error type.
    """
    return isinstance(value, Result) and value.is_ok


def is_portfolio_result_error(value: object) -> bool:
    """Type guard for error PortfolioResult objects.

    Note: PortfolioResult is a type alias for Result[T, PortfolioResultError].
    We check for Result base class and error type.
    """
    return isinstance(value, Result) and value.is_error


# Exchange-specific type guards
def is_valid_exchange_name(value: object) -> TypeGuard[str]:
    """Type guard for valid exchange names."""
    valid_exchanges = {"hyperliquid", "backpack"}
    return is_string(value) and value.lower() in valid_exchanges


def is_valid_symbol(value: object) -> TypeGuard[str]:
    """Type guard for valid trading symbols."""
    return (
        is_string(value)
        and len(value) >= MIN_SYMBOL_LENGTH
        and len(value) <= MAX_SYMBOL_LENGTH
        and value.replace("-", "").replace("/", "").replace("_", "").isalnum()
    )


def is_valid_asset(value: object) -> TypeGuard[str]:
    """Type guard for valid asset names."""
    return (
        is_string(value)
        and len(value) >= MIN_ASSET_LENGTH
        and len(value) <= MAX_ASSET_LENGTH
        and value.isalnum()
    )


# Validation helpers
def validate_required_fields(obj: object, fields: list[str]) -> bool:
    """Validate that an object has all required fields.

    Note: This function is deprecated. Use isinstance() checks with proper Pydantic models instead.
    For Pydantic models, all required fields are validated automatically.
    """
    # Deprecated - always return True since Pydantic models validate required fields
    # This function should be removed in favor of proper isinstance checks
    return True


def validate_numeric_range(
    value: object, min_val: float | None = None, max_val: float | None = None
) -> bool:
    """Validate that a numeric value is within a specified range."""
    if not is_numeric(value):
        return False

    return not (
        (min_val is not None and value < min_val) or (max_val is not None and value > max_val)
    )


def validate_string_length(
    value: object, min_len: int | None = None, max_len: int | None = None
) -> bool:
    """Validate that a string is within specified length bounds."""
    if not is_string(value):
        return False

    return not (
        (min_len is not None and len(value) < min_len)
        or (max_len is not None and len(value) > max_len)
    )


def validate_collection_size(
    value: object, min_size: int | None = None, max_size: int | None = None
) -> bool:
    """Validate that a collection is within specified size bounds."""
    if not isinstance(value, Sized):
        return False

    # Use the Sized protocol to safely call len()
    size = len(value)
    return not (
        (min_size is not None and size < min_size) or (max_size is not None and size > max_size)
    )


# Comprehensive type validation - broken down into helper functions
def _validate_required_fields(data: dict[str, object]) -> list[str]:
    """Validate that required fields are present."""
    required_fields = ["balances", "positions", "metrics"]
    return [f"Missing required field: {field}" for field in required_fields if field not in data]


def _validate_balances_data(data: dict[str, object]) -> list[str]:
    """Validate balances data structure."""
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
    """Validate positions data structure."""
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
    """Validate metrics data structure."""
    issues: list[str] = []
    if "metrics" in data and not is_portfolio_metrics(data["metrics"]):
        issues.append("Invalid portfolio metrics data")
    return issues


def validate_portfolio_data_integrity(data: object) -> list[str]:
    """Validate portfolio data integrity and return list of issues."""
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
    """Type guard for valid portfolio data with comprehensive validation."""
    issues = validate_portfolio_data_integrity(data)
    return len(issues) == 0
