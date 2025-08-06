"""Abstract mapper protocol interfaces and shared utilities for all exchanges.

This module provides mixin classes with common utilities and abstract protocols
that define conceptual transformation interfaces. Each protocol represents a
transformation domain (balance, position, etc.) while providing shared utilities
that reduce code duplication across exchanges.
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import NoReturn, Protocol, cast, runtime_checkable

from cyberdelta.apis.exceptions import MissingRequiredFieldError
from cyberdelta.models import (
    SpotBalance,
)
from cyberdelta.symbols.models import Symbol


# Constants for timestamp parsing
_MILLISECOND_THRESHOLD = 1e10  # Timestamps > this are assumed to be in milliseconds


class BalanceMapperMixin:
    """Shared utilities for all balance mappers.

    Provides common functionality that balance mappers across all exchanges
    can use to reduce code duplication and ensure consistent behavior.
    """

    def create_zero_balance(self, asset: Symbol, exchange: str) -> SpotBalance:
        """Create a zero balance for missing or unavailable assets.

        Args:
            asset: Asset symbol/name
            exchange: Exchange name

        Returns:
            SpotBalance with zero quantities
        """
        # Use properly typed Symbol from core.symbols
        return SpotBalance(
            asset=asset,
            exchange=exchange,
            total_quantity=Decimal(0),
            available_quantity=Decimal(0),
            timestamp=datetime.now(UTC),
        )

    def validate_balance_amount(self, amount: str | float | Decimal | None) -> Decimal:
        """Safely convert and validate balance amounts to Decimal.

        Args:
            amount: Balance amount in various formats (str, int, float, Decimal)

        Returns:
            Decimal representation of the amount, or Decimal(0) if invalid
        """
        if amount is None:
            return Decimal(0)

        try:
            if isinstance(amount, str):
                return Decimal(amount) if amount.strip() else Decimal(0)
            if isinstance(amount, (int, float)):
                return Decimal(str(amount))
            # Convert other types to Decimal
            return Decimal(str(amount))
        except (InvalidOperation, ValueError):
            return Decimal(0)

    def calculate_available_from_total_and_locked(self, total: Decimal, locked: Decimal) -> Decimal:
        """Calculate available balance from total and locked amounts.

        Args:
            total: Total balance amount
            locked: Locked/reserved balance amount

        Returns:
            Available balance (total - locked), minimum 0
        """
        available = total - locked
        return max(available, Decimal(0))


@runtime_checkable
class AbstractBalanceMapperProtocol(Protocol):
    """Abstract protocol for balance transformation mappers.

    Defines the conceptual interface for transforming exchange-specific
    balance data into internal SpotBalance models.

    Expected Transformations:
    - Raw balance data -> SpotBalance
    - WebSocket balance updates -> SpotBalance
    - Collateral data -> SpotBalance (if applicable)

    Use BalanceMapperMixin for shared utility methods like:
    - create_zero_balance(): Create zero balances for missing assets
    - validate_balance_amount(): Safely convert amounts to Decimal
    - calculate_available_from_total_and_locked(): Calculate available balances
    """


class PositionMapperMixin:
    """Shared utilities for all position mappers."""

    def calculate_unrealized_pnl(
        self, entry_price: Decimal, current_price: Decimal, size: Decimal, is_long: bool
    ) -> Decimal:
        """Calculate unrealized PnL for a position.

        Args:
            entry_price: Position entry price
            current_price: Current market price
            size: Position size (absolute value)
            is_long: True for long positions, False for short

        Returns:
            Unrealized PnL
        """
        if is_long:
            return (current_price - entry_price) * size
        return (entry_price - current_price) * size

    def calculate_position_value(self, price: Decimal, size: Decimal) -> Decimal:
        """Calculate the total value of a position.

        Args:
            price: Position price
            size: Position size (absolute value)

        Returns:
            Position value (price * size)
        """
        return price * abs(size)


@runtime_checkable
class AbstractPositionMapperProtocol(Protocol):
    """Abstract protocol for position transformation mappers.

    Defines the conceptual interface for transforming exchange-specific
    position data into internal DerivativePosition models.

    Expected Transformations:
    - Raw position data -> DerivativePosition
    - WebSocket position updates -> DerivativePosition
    - Position arrays -> list[DerivativePosition]

    Use PositionMapperMixin for shared utility methods like:
    - calculate_unrealized_pnl(): Calculate position PnL
    - calculate_position_value(): Calculate position notional value
    """


class ValidationMixin:
    """Common validation utilities for all mappers."""

    def ensure_not_none(self, value: object | None, field_name: str, context: str = "") -> object:
        """Ensure a value is not None.

        Args:
            value: Value to check
            field_name: Field name for error reporting
            context: Additional context for error

        Returns:
            The non-None value

        Raises:
            MissingRequiredFieldError: If value is None
        """
        if value is None:
            raise MissingRequiredFieldError(
                field_names=field_name,
                context=context or "validation",
            )
        return value

    def ensure_decimal_not_none(
        self, value: Decimal | None, field_name: str, context: str = ""
    ) -> Decimal:
        """Ensure a decimal value is not None.

        Args:
            value: Decimal value to check
            field_name: Field name for error reporting
            context: Additional context for error

        Returns:
            The non-None decimal value

        Raises:
            MissingRequiredFieldError: If value is None
        """
        if value is None:
            raise MissingRequiredFieldError(
                field_names=field_name,
                context=context or "decimal validation",
            )
        return value

    def _raise_missing_ticker_field_error(self, field_name: str, raw_data: object) -> NoReturn:
        """Raise error for missing ticker field.

        Args:
            field_name: Name of the missing field
            raw_data: Raw data object for context

        Raises:
            MissingRequiredFieldError: Always raises this error
        """
        raise MissingRequiredFieldError(
            field_names=field_name,
            context=f"ticker transformation: {raw_data}",
        )

    def _raise_missing_mid_price_field_error(self, field_name: str, raw_data: object) -> NoReturn:
        """Raise error for missing mid price field.

        Args:
            field_name: Name of the missing field
            raw_data: Raw data object for context

        Raises:
            MissingRequiredFieldError: Always raises this error
        """
        raise MissingRequiredFieldError(
            field_names=field_name,
            context=f"mid price transformation: {raw_data}",
        )

    def _raise_missing_candle_field_error(self, field_name: str, raw_data: object) -> NoReturn:
        """Raise error for missing candle field.

        Args:
            field_name: Name of the missing field
            raw_data: Raw data object for context

        Raises:
            MissingRequiredFieldError: Always raises this error
        """
        raise MissingRequiredFieldError(
            field_names=field_name,
            context=f"candle transformation: {raw_data}",
        )

    def _raise_missing_funding_history_field_error(
        self, field_name: str, raw_data: object
    ) -> NoReturn:
        """Raise error for missing funding history field.

        Args:
            field_name: Name of the missing field
            raw_data: Raw data object for context

        Raises:
            MissingRequiredFieldError: Always raises this error
        """
        raise MissingRequiredFieldError(
            field_names=field_name,
            context=f"funding history transformation: {raw_data}",
        )


class CommonDataParserMixin:
    """Common data parsing utilities for all mappers."""

    def parse_decimal_safely(
        self, value: str | float | Decimal | None, default: Decimal | None = Decimal(0)
    ) -> Decimal | None:
        """Safely parse decimal values with fallback.

        Args:
            value: The value to parse as a decimal
            default: Default value to return if parsing fails (can be None for optional values)

        Returns:
            Parsed decimal value or default
        """
        if value is None:
            return default
        try:
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation):
            return default

    def timestamp_ms_to_datetime(self, timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to datetime.

        Args:
            timestamp_ms: Millisecond timestamp

        Returns:
            Converted datetime or None
        """
        if timestamp_ms is None:
            return None
        try:
            return datetime.fromtimestamp(float(timestamp_ms) / 1000, tz=UTC)
        except (ValueError, TypeError, OSError):
            return None

    def parse_timestamp(self, timestamp: datetime | float | str | None) -> datetime | None:
        """Parse various timestamp formats to datetime.

        Args:
            timestamp: Timestamp in various formats (int, float, str, datetime)

        Returns:
            Parsed datetime or None if invalid
        """
        if timestamp is None:
            return None

        try:
            if isinstance(timestamp, datetime):
                return timestamp.replace(tzinfo=UTC) if timestamp.tzinfo is None else timestamp
            if isinstance(timestamp, (int, float)):
                # Assume milliseconds if > threshold, otherwise seconds
                ts_seconds = timestamp / 1000 if timestamp > _MILLISECOND_THRESHOLD else timestamp
                return datetime.fromtimestamp(ts_seconds, tz=UTC)
            # Handle string timestamps
            try:
                return datetime.fromisoformat(timestamp)
            except ValueError:
                ts_float = float(timestamp)
                ts_seconds = ts_float / 1000 if ts_float > _MILLISECOND_THRESHOLD else ts_float
                return datetime.fromtimestamp(ts_seconds, tz=UTC)
        except (ValueError, OSError, OverflowError):
            pass

        return None

    def safe_get_nested(
        self, data: dict[str, object], *keys: str, default: str | None = None
    ) -> str | None:
        """Safely get nested dictionary values as strings.

        Args:
            data: Dictionary to traverse
            *keys: Sequence of keys to traverse
            default: Default value if key path doesn't exist

        Returns:
            String value at the key path or default
        """
        current_dict: dict[str, object] = data
        for key in keys[:-1]:  # All keys except the last one
            if key not in current_dict:
                return default
            value = current_dict[key]
            if not isinstance(value, dict):
                return default
            # Safe cast since we've verified it's a dict
            current_dict = cast(dict[str, object], value)

        # Handle the final key
        if not keys:
            return default
        final_key = keys[-1]
        if final_key not in current_dict:
            return default

        final_value = current_dict[final_key]
        return str(final_value) if final_value is not None else default


@runtime_checkable
class AbstractAccountSummaryMapperProtocol(Protocol):
    """Abstract protocol for account summary transformation mappers.

    Defines the conceptual interface for transforming exchange-specific
    account data into internal MarginAccountSummary models.

    Expected Transformations:
    - Raw account state -> MarginAccountSummary
    - Multi-source account data -> MarginAccountSummary
    - Account settings updates -> MarginAccountSummary

    Use CommonDataParserMixin for shared utility methods like:
    - parse_timestamp(): Parse various timestamp formats
    - safe_get_nested(): Safely extract nested dictionary values
    """


@runtime_checkable
class AbstractOrderMapperProtocol(Protocol):
    """Abstract protocol for order transformation mappers.

    Combines shared parsing utilities with the conceptual interface for
    transforming exchange-specific order data into internal Order models.

    Expected Transformations:
    - Raw order data -> Order
    - WebSocket order updates -> Order
    - Order history data -> Order
    - Fill/execution data -> Order (with fill info)

    Shared Utilities:
    - parse_timestamp(): Parse various timestamp formats
    - safe_get_nested(): Safely extract nested dictionary values
    """


@runtime_checkable
class AbstractTickerMapperProtocol(Protocol):
    """Abstract protocol for ticker transformation mappers.

    Combines shared parsing utilities with the conceptual interface for
    transforming exchange-specific ticker data into internal Ticker models.

    Expected Transformations:
    - Raw ticker data -> Ticker
    - WebSocket ticker updates -> Ticker
    - Asset context data -> Ticker

    Shared Utilities:
    - parse_timestamp(): Parse various timestamp formats
    - safe_get_nested(): Safely extract nested dictionary values
    """


@runtime_checkable
class AbstractOrderBookMapperProtocol(Protocol):
    """Abstract protocol for order book transformation mappers.

    Combines shared parsing utilities with the conceptual interface for
    transforming exchange-specific order book data into internal OrderBook models.

    Expected Transformations:
    - Raw order book data -> OrderBook
    - WebSocket depth updates -> OrderBook
    - L2 book data -> OrderBook

    Shared Utilities:
    - parse_timestamp(): Parse various timestamp formats
    - safe_get_nested(): Safely extract nested dictionary values
    """


@runtime_checkable
class AbstractTradeMapperProtocol(Protocol):
    """Abstract protocol for trade transformation mappers.

    Combines shared parsing utilities with the conceptual interface for
    transforming exchange-specific trade data into internal Trade models.

    Expected Transformations:
    - Raw trade/fill data -> Trade
    - WebSocket trade events -> Trade
    - Public trade data -> Trade
    - Trade history -> Trade

    Shared Utilities:
    - parse_timestamp(): Parse various timestamp formats
    - safe_get_nested(): Safely extract nested dictionary values
    """


@runtime_checkable
class AbstractCandleMapperProtocol(Protocol):
    """Abstract protocol for candle transformation mappers.

    Combines shared parsing utilities with the conceptual interface for
    transforming exchange-specific candle/OHLCV data into internal Candle models.

    Expected Transformations:
    - Raw candle data -> Candle
    - WebSocket candle updates -> Candle
    - Historical candle arrays -> list[Candle]

    Shared Utilities:
    - parse_timestamp(): Parse various timestamp formats
    - safe_get_nested(): Safely extract nested dictionary values
    """


@runtime_checkable
class AbstractFundingRateMapperProtocol(Protocol):
    """Abstract protocol for funding rate transformation mappers.

    Combines shared parsing utilities with the conceptual interface for
    transforming exchange-specific funding rate data into internal FundingRate models.

    Expected Transformations:
    - Raw funding rate data -> FundingRate
    - Historical funding data -> FundingRate
    - Funding interval data -> FundingRate

    Shared Utilities:
    - parse_timestamp(): Parse various timestamp formats
    - safe_get_nested(): Safely extract nested dictionary values
    """


@runtime_checkable
class AbstractMarketMapperProtocol(Protocol):
    """Abstract protocol for market transformation mappers.

    Combines shared parsing utilities with the conceptual interface for
    transforming exchange-specific market metadata into internal Market models.

    Expected Transformations:
    - Raw market data -> Market
    - Asset definitions -> Market
    - Market configuration -> Market

    Shared Utilities:
    - parse_timestamp(): Parse various timestamp formats
    - safe_get_nested(): Safely extract nested dictionary values
    """


# Export mixin classes and abstract protocols
__all__ = [
    # Abstract protocols (combine mixins with Protocol)
    "AbstractAccountSummaryMapperProtocol",
    "AbstractBalanceMapperProtocol",
    "AbstractCandleMapperProtocol",
    "AbstractFundingRateMapperProtocol",
    "AbstractMarketMapperProtocol",
    "AbstractOrderBookMapperProtocol",
    "AbstractOrderMapperProtocol",
    "AbstractPositionMapperProtocol",
    "AbstractTickerMapperProtocol",
    "AbstractTradeMapperProtocol",
    # Mixin classes with shared utilities
    "BalanceMapperMixin",
    "CommonDataParserMixin",
    "PositionMapperMixin",
    "ValidationMixin",
]
