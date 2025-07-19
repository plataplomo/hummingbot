"""Data integrity and consistency exceptions for portfolio management."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Unpack

from cyberdelta.core.portfolio.exceptions.base import PortfolioError


if TYPE_CHECKING:
    from typing_extensions import TypedDict

    class ExceptionKwargs(TypedDict, total=False):
        """Typed dictionary for exception kwargs."""

        error_code: str | None
        context: dict[str, Any] | None
        recoverable: bool


class PortfolioIntegrityError(PortfolioError):
    """Base exception for portfolio data integrity and consistency errors."""

    def _get_default_error_code(self) -> str:
        """Get default error code for validation exceptions."""
        return f"VALID_{self.__class__.__name__.upper()}"


class MalformedTradeError(PortfolioIntegrityError):
    """Raised when trade data is malformed or inconsistent."""

    def __init__(
        self,
        message: str,
        trade_id: str | None = None,
        field: str | None = None,
        value: Decimal | float | str | None = None,
        integrity_errors: list[dict[str, Any]] | None = None,
        field_name: str | None = None,
        field_value: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize trade validation exception.

        Args:
            message: Error message
            trade_id: Trade ID
            field: Field that failed validation
            value: Invalid value
            integrity_errors: List of integrity validation errors
            field_name: Alternative field name (for validators)
            field_value: Alternative field value (for validators)
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "trade_id": trade_id,
            "field": field or field_name,
            "value": str(value) if value is not None else field_value,
            "integrity_errors": integrity_errors or [],
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_TRADE_FAILED"
        super().__init__(message, **kwargs)


class InvalidPositionError(PortfolioIntegrityError):
    """Raised when position data is invalid or inconsistent."""

    def __init__(
        self,
        message: str,
        position_id: str | None = None,
        exchange_id: str | None = None,
        symbol: str | None = None,
        reason: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize position validation exception.

        Args:
            message: Error message
            position_id: Position ID
            exchange_id: Exchange ID
            symbol: Trading symbol
            reason: Validation failure reason
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "position_id": position_id,
            "exchange_id": exchange_id,
            "symbol": symbol,
            "reason": reason,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_POSITION_FAILED"
        super().__init__(message, **kwargs)


class BalanceDiscrepancyError(PortfolioIntegrityError):
    """Raised when balance data shows discrepancies or inconsistencies."""

    def __init__(
        self,
        message: str,
        exchange_id: str | None = None,
        asset: str | None = None,
        expected_balance: str | None = None,
        actual_balance: str | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize balance validation exception.

        Args:
            message: Error message
            exchange_id: Exchange ID
            asset: Asset/currency
            expected_balance: Expected balance
            actual_balance: Actual balance
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "exchange_id": exchange_id,
            "asset": asset,
            "expected_balance": expected_balance,
            "actual_balance": actual_balance,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_BALANCE_FAILED"
        super().__init__(message, **kwargs)


class OrderConstraintError(PortfolioIntegrityError):
    """Raised when order violates constraints or business rules."""

    def __init__(
        self,
        message: str,
        order_id: str | None = None,
        order_type: str | None = None,
        constraint_violations: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize order validation exception.

        Args:
            message: Error message
            order_id: Order ID
            order_type: Type of order
            constraint_violations: List of constraint violations
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "order_id": order_id,
            "order_type": order_type,
            "constraint_violations": constraint_violations or [],
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_ORDER_FAILED"
        super().__init__(message, **kwargs)


class DataCorruptionError(PortfolioIntegrityError):
    """Raised when data corruption or integrity violations are detected."""

    def __init__(
        self,
        message: str,
        data_type: str | None = None,
        integrity_check: str | None = None,
        details: dict[str, Any] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize data integrity exception.

        Args:
            message: Error message
            data_type: Type of data
            integrity_check: Type of integrity check
            details: Additional details
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "data_type": data_type,
            "integrity_check": integrity_check,
            "details": details or {},
        })
        kwargs["context"] = context
        kwargs["recoverable"] = False  # Data integrity issues are critical
        kwargs["error_code"] = "VALID_DATA_INTEGRITY"
        super().__init__(message, **kwargs)


class ConfigurationError(PortfolioIntegrityError):
    """Raised when configuration is invalid or incompatible."""

    def __init__(
        self,
        message: str,
        config_section: str | None = None,
        invalid_fields: list[str] | None = None,
        key: str | None = None,
        value: str | None = None,
        valid_values: list[str] | None = None,
        **kwargs: Unpack[ExceptionKwargs],
    ) -> None:
        """Initialize configuration validation exception.

        Args:
            message: Error message
            config_section: Configuration section
            invalid_fields: List of invalid fields
            key: Configuration key
            value: Invalid value
            valid_values: List of valid values
            **kwargs: Additional context
        """
        context = kwargs.get("context") or {}
        context.update({
            "config_section": config_section,
            "invalid_fields": invalid_fields or [],
            "key": key,
            "value": value,
            "valid_values": valid_values,
        })
        kwargs["context"] = context
        kwargs["error_code"] = "VALID_CONFIG_FAILED"
        super().__init__(message, **kwargs)


# Specific validation exceptions without message parameters to comply with TRY003
class EmptySymbolError(MalformedTradeError):
    """Raised when symbol is empty."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize empty symbol exception."""
        super().__init__("Symbol cannot be empty", field_name="symbol", field_value="", **kwargs)


class InvalidSymbolLengthError(MalformedTradeError):
    """Raised when symbol length is invalid."""

    def __init__(self, symbol: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid symbol length exception."""
        super().__init__(
            "Symbol must be 2-20 characters", field_name="symbol", field_value=symbol, **kwargs
        )


class InvalidSymbolCharactersError(MalformedTradeError):
    """Raised when symbol contains invalid characters."""

    def __init__(self, symbol: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid symbol characters exception."""
        super().__init__(
            "Symbol contains invalid characters", field_name="symbol", field_value=symbol, **kwargs
        )


class EmptyAssetError(MalformedTradeError):
    """Raised when asset is empty."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize empty asset exception."""
        super().__init__("Asset cannot be empty", field_name="asset", field_value="", **kwargs)


class InvalidAssetLengthError(MalformedTradeError):
    """Raised when asset length is invalid."""

    def __init__(self, asset: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid asset length exception."""
        super().__init__(
            "Asset must be 1-10 characters", field_name="asset", field_value=asset, **kwargs
        )


class InvalidAssetCharactersError(MalformedTradeError):
    """Raised when asset is not alphanumeric."""

    def __init__(self, asset: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid asset characters exception."""
        super().__init__(
            "Asset must be alphanumeric", field_name="asset", field_value=asset, **kwargs
        )


class InvalidExchangeError(ConfigurationError):
    """Raised when exchange name is invalid."""

    def __init__(
        self, exchange: str, valid_exchanges: set[str], **kwargs: Unpack[ExceptionKwargs]
    ) -> None:
        """Initialize invalid exchange exception."""
        super().__init__(
            f"Invalid exchange: {exchange}. Must be one of {valid_exchanges}",
            key="exchange",
            value=exchange,
            valid_values=list(valid_exchanges),
            **kwargs,
        )


class InvalidTimestampRangeError(MalformedTradeError):
    """Raised when timestamp is outside valid range."""

    def __init__(self, timestamp: float, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid timestamp exception."""
        super().__init__(
            f"Timestamp {timestamp} is outside valid range",
            field_name="timestamp",
            field_value=str(timestamp),
            **kwargs,
        )


class EmptyListError(ConfigurationError):
    """Raised when a list that should not be empty is empty."""

    def __init__(self, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize empty list exception."""
        super().__init__("List cannot be empty", key="list", value="[]", **kwargs)


class NonUniqueListError(ConfigurationError):
    """Raised when a list contains non-unique elements."""

    def __init__(self, items: list[Any], **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize non-unique list exception."""
        super().__init__(
            "List must contain unique elements", key="list", value=str(items), **kwargs
        )


class PositionSizeTooLargeError(MalformedTradeError):
    """Raised when position size exceeds maximum."""

    def __init__(self, size: float, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize position size too large exception."""
        super().__init__(
            "Position size too large", field_name="position_size", field_value=str(size), **kwargs
        )


class InvalidCacheSizeError(ConfigurationError):
    """Raised when cache size is out of bounds."""

    def __init__(self, size: int, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid cache size exception."""
        super().__init__(
            "Cache size must be between 10 and 1,000,000",
            key="cache_size",
            value=str(size),
            **kwargs,
        )


class InvalidTTLError(ConfigurationError):
    """Raised when TTL is out of bounds."""

    def __init__(self, ttl: float, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid TTL exception."""
        super().__init__(
            "TTL must be between 1 second and 30 days", key="ttl_seconds", value=str(ttl), **kwargs
        )


class InvalidURLFormatError(ConfigurationError):
    """Raised when URL format is invalid."""

    def __init__(self, url: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid URL format exception."""
        super().__init__(
            "URL must start with http://, https://, ws://, or wss://",
            key="url",
            value=url,
            **kwargs,
        )


class InvalidMarketHoursFormatError(ConfigurationError):
    """Raised when market hours format is invalid."""

    def __init__(self, hours: str, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize invalid market hours format exception."""
        super().__init__(
            "Market hours must be in format HH:MM-HH:MM", key="market_hours", value=hours, **kwargs
        )


class CacheSizeMustBePositiveError(ConfigurationError):
    """Raised when cache size is not positive."""

    def __init__(self, size: int, **kwargs: Unpack[ExceptionKwargs]) -> None:
        """Initialize cache size must be positive exception."""
        super().__init__("Cache size must be positive", key="max_size", value=str(size), **kwargs)
