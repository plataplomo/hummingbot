from __future__ import annotations  # Enable postponed evaluation

import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

# Set Decimal precision globally if desired, or manage context locally
# getcontext().prec = 28


class OrderSide(Enum):
    """Enum representing the side of an order (BUY or SELL)."""

    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Enum representing the type of an order."""

    LIMIT = "limit"
    MARKET = "market"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TAKE_PROFIT = "take_profit"
    TAKE_PROFIT_LIMIT = "take_profit_limit"


class OrderStatus(Enum):
    """Enum representing the status of an order."""

    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    OPEN = "OPEN"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class SignalType(Enum):
    """Enum representing the type of a trading signal."""

    ENTER_LONG = "ENTER_LONG"
    EXIT_LONG = "EXIT_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT_SHORT = "EXIT_SHORT"
    HOLD = "HOLD"
    REBALANCE = "REBALANCE"


class TimeInForce(Enum):
    """Enum representing the time in force for an order."""

    GTC = "GTC"  # Good 'Til Canceled
    IOC = "IOC"  # Immediate Or Cancel
    FOK = "FOK"  # Fill Or Kill


@dataclass
class MarketData:
    """Represents market data for a symbol, including OHLCV information."""

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal("0.0")
    # Add ticker_data field to store ticker information for multiple symbols
    ticker_data: dict[str, dict[str, Ticker]] | None = None  # symbol -> exchange -> Ticker

    def __post_init__(self) -> None:
        """Ensure all numeric fields are Decimals, converting safely."""
        # For MarketData fields, the type annotation guarantees they should be Decimal
        # and _safe_decimal_convert will always return a Decimal or raise an error
        self.open = self._safe_decimal_convert(self.open, "open", self.symbol)
        self.high = self._safe_decimal_convert(self.high, "high", self.symbol)
        self.low = self._safe_decimal_convert(self.low, "low", self.symbol)
        self.close = self._safe_decimal_convert(self.close, "close", self.symbol)
        self.volume = self._safe_decimal_convert(self.volume, "volume", self.symbol)

        # Initialize ticker_data as an empty dict if it's None
        if self.ticker_data is None:
            self.ticker_data = {}

    @staticmethod
    def _safe_decimal_convert(
        value: str | int | float | Decimal, field_name: str, symbol: str
    ) -> Decimal:
        """Safely convert a value to Decimal, logging errors."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            # For MarketData, we don't allow None values - raise an error
            raise ValueError(
                f"MarketData field '{field_name}' for symbol '{symbol}' cannot be None"
            )
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            # Log error or raise? Let's raise for critical market data.
            raise ValueError(
                f"Invalid value '{value}' for MarketData field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err


@dataclass
class Balance:
    """Represents an account balance for a single asset."""

    asset: str
    total: Decimal
    available: Decimal | None = None
    free: Decimal | None = None
    locked: Decimal | None = None

    def __post_init__(self) -> None:
        """Ensure fields are Decimal, handle None for optional fields."""
        # Since total is non-optional, we know _safe_decimal_convert won't return None
        # when allow_none=False
        decimal_value = self._safe_decimal_convert(
            self.total, "total", self.asset, allow_none=False
        )
        if decimal_value is not None:  # This is just for type checking, it will always be non-None
            self.total = decimal_value
        else:
            # This should never happen due to the allow_none=False, but satisfies the type checker
            self.total = Decimal("0.0")

        # Handle available field
        if self.available is None:
            # Default available to total if not provided
            self.available = self.total
        else:
            decimal_value = self._safe_decimal_convert(
                self.available, "available", self.asset, allow_none=False
            )
            if decimal_value is not None:  # This is just for type checking
                self.available = decimal_value
            else:
                # This should never happen due to the allow_none=False
                self.available = Decimal("0.0")

        # Handle free field
        if self.free is None:
            # Default to zero
            self.free = Decimal("0.0")
        else:
            decimal_value = self._safe_decimal_convert(
                self.free, "free", self.asset, allow_none=False
            )
            if decimal_value is not None:  # This is just for type checking
                self.free = decimal_value
            else:
                # This should never happen due to the allow_none=False
                self.free = Decimal("0.0")

        # Handle locked field
        if self.locked is None:
            # Default to zero
            self.locked = Decimal("0.0")
        else:
            decimal_value = self._safe_decimal_convert(
                self.locked, "locked", self.asset, allow_none=False
            )
            if decimal_value is not None:  # This is just for type checking
                self.locked = decimal_value
            else:
                # This should never happen due to the allow_none=False
                self.locked = Decimal("0.0")

    @staticmethod
    def _safe_decimal_convert(
        value: str | int | float | Decimal | None,
        field_name: str,
        asset: str,
        allow_none: bool = False,
        default: Decimal | None = None,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, handling None and defaults."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            if allow_none and default is not None:
                return default
            elif allow_none:
                return None
            else:
                raise ValueError(f"Balance field '{field_name}' for asset '{asset}' cannot be None")
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            raise ValueError(
                f"Invalid value '{value}' for Balance field '{field_name}' "
                f"for asset '{asset}'. Cannot convert to Decimal."
            ) from err

    def to_dict(self) -> dict[str, Any]:
        """Convert Balance to dictionary, ensuring Decimals are strings."""
        d = asdict(self)
        for key, value in d.items():
            if isinstance(value, Decimal):
                d[key] = str(value)
        return d

    @property
    def quantity(self) -> Decimal:
        """Return total balance quantity as Decimal."""
        return self.total

    def is_active(self) -> bool:
        """Check if the balance represents a non-zero quantity."""
        return self.total > Decimal("0.0")

    def calculate_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """Calculate unrealized PnL (mainly for non-quote assets)."""
        # This method seems more applicable to Positions.
        # For Balance, it might represent the change in value if it's not the quote currency.
        # Let's assume it's intended for non-quote assets.
        # Requires an assumed entry price which isn't stored in Balance.
        # Returning 0 as the current implementation is unclear/likely incorrect for 'Balance'.
        # Consider removing or clarifying the purpose of this method for Balance.
        # If self.total represents quantity and needs valuation:
        # Requires average entry cost, which is not part of Balance.
        # Returning 0 for now.
        if self.asset in ["USD", "USDC", "USDT"]:  # Example quote assets
            return Decimal("0.0")
        # Without entry price, cannot calculate PNL accurately.
        # Returning 0. Implement if Balance needs valuation logic.
        return Decimal("0.0")


@dataclass
class Position:
    """Represents an open position."""

    symbol: str
    side: OrderSide
    size: Decimal
    entry_price: Decimal
    leverage: Decimal | None = None  # Often optional or derived
    id: str | None = None
    status: str | None = None
    mark_price: Decimal | None = None
    liquidation_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    realized_pnl: Decimal | None = None
    margin_type: str | None = None
    margin_used: Decimal | None = None
    timestamp: int | None = None  # Consider datetime instead?
    strategy_name: str | None = None
    close_price: Decimal | None = None
    close_time: datetime | None = None
    pnl: Decimal | None = None  # Realized PNL?

    def __post_init__(self) -> None:
        """Ensure numeric fields are Decimal, handle None."""
        # These fields are required and can't be None per the type annotation
        if self.size is None:
            raise ValueError(f"Position field 'size' for symbol '{self.symbol}' cannot be None")
        if self.entry_price is None:
            raise ValueError(
                f"Position field 'entry_price' for symbol '{self.symbol}' cannot be None"
            )

        # For required fields, we know _safe_decimal_convert won't return None
        # when allow_none=False
        decimal_value = self._safe_decimal_convert(self.size, "size", self.symbol, allow_none=False)
        if decimal_value is not None:  # This is just for type checking
            self.size = decimal_value
        else:
            # This should never happen due to the allow_none=False, but satisfies the type checker
            raise ValueError(f"Failed to convert size to Decimal for symbol '{self.symbol}'")

        decimal_value = self._safe_decimal_convert(
            self.entry_price, "entry_price", self.symbol, allow_none=False
        )
        if decimal_value is not None:  # This is just for type checking
            self.entry_price = decimal_value
        else:
            # This should never happen due to the allow_none=False, but satisfies the type checker
            raise ValueError(f"Failed to convert entry_price to Decimal for symbol '{self.symbol}'")

        # Handle Optional Decimal fields - these can remain as is since they're allowed to be None
        self.leverage = self._safe_decimal_convert(
            self.leverage, "leverage", self.symbol, allow_none=True
        )
        self.mark_price = self._safe_decimal_convert(
            self.mark_price, "mark_price", self.symbol, allow_none=True
        )
        self.liquidation_price = self._safe_decimal_convert(
            self.liquidation_price, "liquidation_price", self.symbol, allow_none=True
        )
        self.unrealized_pnl = self._safe_decimal_convert(
            self.unrealized_pnl, "unrealized_pnl", self.symbol, allow_none=True
        )
        self.realized_pnl = self._safe_decimal_convert(
            self.realized_pnl, "realized_pnl", self.symbol, allow_none=True
        )
        self.margin_used = self._safe_decimal_convert(
            self.margin_used, "margin_used", self.symbol, allow_none=True
        )
        self.close_price = self._safe_decimal_convert(
            self.close_price, "close_price", self.symbol, allow_none=True
        )
        self.pnl = self._safe_decimal_convert(self.pnl, "pnl", self.symbol, allow_none=True)

    @staticmethod
    def _safe_decimal_convert(
        value: Any,
        field_name: str,
        symbol: str,
        allow_none: bool = False,
        default: Decimal | None = None,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, handling None and defaults."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            if allow_none:
                return default
            else:
                raise ValueError(
                    f"Position field '{field_name}' for symbol '{symbol}' cannot be None"
                )
        try:
            # Force string conversion first for robustness against float inputs
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            raise ValueError(
                f"Invalid value '{value}' for Position field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err

    def is_active(self) -> bool:
        """Check if the position is actively held (size is non-zero)."""
        return self.size is not None and self.size != Decimal("0")

    def calculate_unrealized_pnl(self, current_mark_price: Decimal | None = None) -> Decimal | None:
        """
        Calculate the current unrealized PNL for this position.

        Args:
            current_mark_price: The current mark price to use for calculation

        Returns:
            The calculated unrealized PNL as a Decimal, or None if calculation is not possible
        """
        # First check if we have valid data to calculate PNL
        if current_mark_price is None or self.entry_price is None:
            return self.unrealized_pnl

        # Check for zero size - we know self.size is never None due to __post_init__ validation
        if self.size == Decimal("0"):
            return Decimal("0.0")  # Return zero for zero-sized positions

        # Calculate PNL based on side
        if self.side == OrderSide.BUY:
            pnl = (current_mark_price - self.entry_price) * self.size
        else:  # Assume OrderSide.SELL
            pnl = (self.entry_price - current_mark_price) * self.size

        # Update the stored unrealized PnL
        self.unrealized_pnl = pnl
        return pnl

    def to_dict(self) -> dict[str, Any]:
        """Convert Position to dictionary, ensuring Decimals are strings."""
        d = asdict(self)
        for key, value in d.items():
            if isinstance(value, Decimal):
                d[key] = str(value)
            elif isinstance(value, OrderSide):
                d[key] = value.value
            elif isinstance(value, datetime):
                d[key] = value.isoformat()
        return d


@dataclass
class Order:
    """Represents an order on an exchange."""

    symbol: str  # Trading pair symbol (e.g., "BTC-USDT")
    order_id: str  # Exchange-assigned order ID
    side: OrderSide  # Buy or sell
    order_type: OrderType  # Limit, market, etc.
    quantity: Decimal  # Original order quantity
    status: OrderStatus  # Status of the order (open, filled, canceled, etc.)
    client_order_id: str | None = None  # Client-assigned order ID (optional)
    price: Decimal | None = None  # Limit price (None for market orders)
    filled_quantity: Decimal | None = None  # Amount of the order that has been filled
    remaining_quantity: Decimal | None = None  # Amount of the order that remains to be filled
    timestamp: datetime | None = None  # When the order was created or last updated
    leverage: Decimal | None = None  # Leverage used for the order (if applicable)
    time_in_force: TimeInForce | None = None  # Time in force for the order
    post_only: bool = False  # Whether the order is post-only (maker-only)
    reduce_only: bool = False  # Whether the order is reduce-only (cannot increase position)
    associated_signal_id: str | None = None  # ID of the signal that generated this order
    metadata: dict[str, Any] = field(default_factory=dict)  # Additional exchange-specific data

    def __post_init__(self) -> None:
        """Ensure numeric fields are Decimal and validate fields."""
        # Convert numeric fields to Decimal
        self.price = self._safe_decimal_convert(self.price, "price", self.symbol, allow_none=True)

        # For required fields, we use allow_none=False to ensure we get a Decimal
        # When allow_none=False, _safe_decimal_convert will raise ValueError if value is None
        quantity_decimal = self._safe_decimal_convert(
            self.quantity, "quantity", self.symbol, allow_none=False
        )
        if (
            quantity_decimal is not None
        ):  # This is for type checking; should never be None with allow_none=False
            self.quantity = quantity_decimal
        else:
            # This should never happen as _safe_decimal_convert should raise an error when allow_none=False
            raise ValueError(f"Failed to convert quantity to Decimal for symbol '{self.symbol}'")

        self.filled_quantity = self._safe_decimal_convert(
            self.filled_quantity, "filled_quantity", self.symbol, allow_none=True
        )
        self.remaining_quantity = self._safe_decimal_convert(
            self.remaining_quantity, "remaining_quantity", self.symbol, allow_none=True
        )
        self.leverage = self._safe_decimal_convert(
            self.leverage, "leverage", self.symbol, allow_none=True
        )

        # Validate required fields based on order type
        if self.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) and self.price is None:
            raise ValueError(f"Price is required for {self.order_type} orders")

        # Ensure timestamp is timezone-aware
        if self.timestamp is not None and self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=UTC)

    def _safe_decimal_convert(
        self, value: Any, field_name: str, symbol: str, allow_none: bool = False
    ) -> Decimal | None:
        """Safely convert a value to Decimal, with appropriate error handling."""
        if value is None:
            if not allow_none:
                raise ValueError(f"Order field '{field_name}' for {symbol} cannot be None")
            return None

        try:
            if isinstance(value, str):
                # Handle potential commas in string representation
                value = value.replace(",", "")
            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation) as e:
            raise ValueError(
                f"Failed to convert {field_name} value '{value}' to Decimal for {symbol}: {str(e)}"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert order to dictionary, with proper formatting for serialization."""
        result = {
            "symbol": self.symbol,
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "order_type": self.order_type.value
            if isinstance(self.order_type, OrderType)
            else self.order_type,
            "price": str(self.price) if self.price is not None else None,
            "quantity": str(self.quantity),
            "filled_quantity": str(self.filled_quantity)
            if self.filled_quantity is not None
            else None,
            "remaining_quantity": str(self.remaining_quantity)
            if self.remaining_quantity is not None
            else None,
            "status": self.status.value if isinstance(self.status, OrderStatus) else self.status,
            "timestamp": self.timestamp.isoformat() if self.timestamp is not None else None,
            "leverage": str(self.leverage) if self.leverage is not None else None,
            "time_in_force": self.time_in_force.value if self.time_in_force is not None else None,
            "post_only": self.post_only,
            "reduce_only": self.reduce_only,
            "associated_signal_id": self.associated_signal_id,
            "metadata": self.metadata,
        }
        return result


@dataclass
class Trade:
    """Represents a trade execution."""

    id: str
    symbol: str
    timestamp: int  # Usually ms from epoch
    price: Decimal
    quantity: Decimal
    side: OrderSide | None = None  # Can sometimes be inferred or missing
    order_id: str | None = None
    exchange: str | None = None
    datetime: datetime | None = None
    fee: Decimal | None = None
    fee_asset: str | None = None
    is_maker: bool | None = None
    client_order_id: str | None = None
    cost: Decimal | None = None

    def __post_init__(self) -> None:
        """Ensure numeric fields are Decimal and calculate cost/datetime if needed."""
        # Handle required fields
        self.price = self._safe_decimal_convert(self.price, "price", self.symbol)
        self.quantity = self._safe_decimal_convert(self.quantity, "quantity", self.symbol)

        # Handle optional fee field
        if self.fee is not None:
            fee_decimal = self._safe_decimal_convert_optional(self.fee, "fee", self.symbol)
            # Ensure we only assign non-None values to self.fee
            if fee_decimal is not None:
                self.fee = fee_decimal

        # Calculate cost if not provided
        if self.cost is None:
            # We already ensured price and quantity are Decimal above
            self.cost = self.price * self.quantity
        else:
            # If cost is provided, convert it to Decimal
            cost_decimal = self._safe_decimal_convert_optional(self.cost, "cost", self.symbol)
            # Ensure we only assign non-None values to self.cost (which is Decimal | None)
            if cost_decimal is not None:
                self.cost = cost_decimal

        # Create datetime from timestamp if needed
        if self.timestamp and self.datetime is None:
            try:
                # Assume timestamp is in milliseconds
                self.datetime = datetime.fromtimestamp(self.timestamp / 1000, tz=UTC)
            except (TypeError, ValueError, OSError):
                # Log warning if timestamp is invalid
                logger.warning(
                    f"Could not convert timestamp {self.timestamp} to datetime for trade {self.id}"
                )
                self.datetime = None

    @staticmethod
    def _safe_decimal_convert(
        value: Any,
        field_name: str,
        symbol: str,
    ) -> Decimal:
        """Safely convert a value to Decimal, raising errors for None."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            raise ValueError(f"Trade field '{field_name}' for symbol '{symbol}' cannot be None")
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            raise ValueError(
                f"Invalid value '{value}' for Trade field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err

    @staticmethod
    def _safe_decimal_convert_optional(
        value: Any,
        field_name: str,
        symbol: str,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, allowing None values."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            raise ValueError(
                f"Invalid value '{value}' for Trade field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err


@dataclass
class Ticker:
    """Represents ticker information for a symbol."""

    symbol: str
    price: Decimal | None = None  # Make price optional, might be missing sometimes
    bid: Decimal | None = None  # Make optional
    ask: Decimal | None = None  # Make optional
    volume: Decimal | None = None  # Make optional
    timestamp: int | None = None  # Consider datetime? Usually ms

    def __post_init__(self) -> None:
        """Ensure numeric fields are Decimal, handle None."""
        self.price = self._safe_decimal_convert(self.price, "price", self.symbol, allow_none=True)
        self.bid = self._safe_decimal_convert(self.bid, "bid", self.symbol, allow_none=True)
        self.ask = self._safe_decimal_convert(self.ask, "ask", self.symbol, allow_none=True)
        self.volume = self._safe_decimal_convert(
            self.volume, "volume", self.symbol, allow_none=True
        )

    @staticmethod
    def _safe_decimal_convert(
        value: Any,
        field_name: str,
        symbol: str,
        allow_none: bool = False,
        default: Decimal | None = None,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, handling None and defaults."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            if allow_none:
                return default
            else:
                raise ValueError(
                    f"Ticker field '{field_name}' for symbol '{symbol}' cannot be None"
                )
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            # Log warning or use default if allowed
            if allow_none:
                logger.warning(
                    f"Invalid value '{value}' for Ticker field '{field_name}'"
                    f" for symbol '{symbol}'. Using default: {default}"
                )
                return default

            # If not allowed to be None, raise the error
            raise ValueError(
                f"Invalid value '{value}' for Ticker field '{field_name}'"
                f" for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err


@dataclass
class OrderBook:
    """Represents an order book for a symbol."""

    symbol: str
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]
    timestamp: int | None = None  # Usually ms

    def __post_init__(self) -> None:
        """Ensure bids and asks contain Decimal tuples, converting safely."""
        # Convert both bids and asks to ensure they are in the correct format
        # Use fresh lists to avoid type checking issues with the original lists
        self.bids = self._safe_convert_level_list(self.bids, "bids", self.symbol)
        self.asks = self._safe_convert_level_list(self.asks, "asks", self.symbol)

    @staticmethod
    def _safe_convert_level_list(
        levels: list[Any], field_name: str, symbol: str
    ) -> list[tuple[Decimal, Decimal]]:
        """Safely convert a list of price/quantity pairs to Decimal tuples."""
        if not isinstance(levels, list):
            raise ValueError(
                f"OrderBook field '{field_name}' for symbol '{symbol}' must be a list."
            )

        converted_levels: list[tuple[Decimal, Decimal]] = []
        for i, level in enumerate(levels):
            if not isinstance(level, list | tuple) or len(level) != 2:
                raise ValueError(
                    f"Invalid item format in OrderBook field '{field_name}'"
                    f" for symbol '{symbol}' at index {i}. "
                    f"Expected tuple/list of length 2, got: {level}"
                )
            try:
                price = Decimal(str(level[0]))
                quantity = Decimal(str(level[1]))
                converted_levels.append((price, quantity))
            except (InvalidOperation, TypeError, IndexError) as err:
                raise ValueError(
                    f"Invalid price/quantity in OrderBook field '{field_name}'"
                    f" for symbol '{symbol}' at index {i}. "
                    f"Cannot convert {level} to Decimal tuple."
                ) from err
        return converted_levels


@dataclass
class FundingRate:
    """Represents funding rate information for a perpetual contract."""

    symbol: str
    funding_rate: Decimal | None = None  # Allow None, might be missing
    predicted_rate: Decimal | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    next_funding_time: int | None = None  # Timestamp in ms
    timestamp: int | None = None  # Timestamp of data receipt?
    historical_rates: list[dict[str, Any]] | None = None  # Keep as Any for flexibility

    def __post_init__(self) -> None:
        """Ensure numeric fields are Decimal, handle None."""
        self.funding_rate = self._safe_decimal_convert(
            self.funding_rate, "funding_rate", self.symbol, allow_none=True
        )
        self.predicted_rate = self._safe_decimal_convert(
            self.predicted_rate, "predicted_rate", self.symbol, allow_none=True
        )
        self.mark_price = self._safe_decimal_convert(
            self.mark_price, "mark_price", self.symbol, allow_none=True
        )
        self.index_price = self._safe_decimal_convert(
            self.index_price, "index_price", self.symbol, allow_none=True
        )

    @staticmethod
    def _safe_decimal_convert(
        value: Any,
        field_name: str,
        symbol: str,
        allow_none: bool = False,
        default: Decimal | None = None,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, handling None and defaults."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            if allow_none:
                return None
            else:
                raise ValueError(
                    f"FundingRate field '{field_name}' for symbol '{symbol}' cannot be None"
                )
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            # Log warning or use default if allowed
            if allow_none:
                logger.warning(
                    f"Invalid value '{value}' for FundingRate field '{field_name}'"
                    f" for symbol '{symbol}'. Using None as default."
                )
                return None

            # If not allowed to be None, raise the error
            raise ValueError(
                f"Invalid value '{value}' for FundingRate field '{field_name}'"
                f" for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err


# ArbitrageOpportunity is not a dataclass, handle conversion in __init__
class ArbitrageOpportunity:
    """Represents a funding rate arbitrage opportunity."""

    def __init__(
        self,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        long_price: str | int | float | Decimal | None,
        short_price: str | int | float | Decimal | None,
        long_funding_rate: str | int | float | Decimal | None,
        short_funding_rate: str | int | float | Decimal | None,
        net_funding_differential: str | int | float | Decimal | None,
        timestamp: datetime,
        optimal_size: str | int | float | Decimal | None = None,
        expected_profit: str | int | float | Decimal | None = None,
        confidence: str | int | float | Decimal | None = None,
        basis_volatility: str | int | float | Decimal | None = None,
        utility_score: str | int | float | Decimal | None = None,
    ) -> None:
        self.symbol = symbol
        self.long_exchange = long_exchange
        self.short_exchange = short_exchange
        self.timestamp = timestamp

        # Perform safe Decimal conversions for financial values
        self.long_price = self._safe_decimal_convert(
            long_price, "long_price", symbol, allow_none=False
        )
        self.short_price = self._safe_decimal_convert(
            short_price, "short_price", symbol, allow_none=False
        )
        self.long_funding_rate = self._safe_decimal_convert(
            long_funding_rate, "long_funding_rate", symbol, allow_none=False
        )
        self.short_funding_rate = self._safe_decimal_convert(
            short_funding_rate, "short_funding_rate", symbol, allow_none=False
        )
        self.net_funding_differential = self._safe_decimal_convert(
            net_funding_differential, "net_funding_differential", symbol, allow_none=False
        )
        self.optimal_size = self._safe_decimal_convert(
            optimal_size, "optimal_size", symbol, allow_none=True
        )
        self.expected_profit = self._safe_decimal_convert(
            expected_profit, "expected_profit", symbol, allow_none=True
        )

        # Use float for statistical/ranking metrics that don't require financial precision
        self.confidence = self._safe_float_convert(confidence, "confidence", symbol)
        self.basis_volatility = self._safe_float_convert(
            basis_volatility, "basis_volatility", symbol
        )
        self.utility_score = self._safe_float_convert(utility_score, "utility_score", symbol)

        # Validate required fields are not None after conversion
        self.validate_required_fields()

        # Expiration calculation
        # Use timezone-aware comparison if timestamp has timezone
        if timestamp.tzinfo:
            self.expiration_timestamp = timestamp.timestamp() + 3600  # 1 hour
        else:
            # Assume UTC if no timezone provided for timestamp()
            self.expiration_timestamp = timestamp.replace(tzinfo=UTC).timestamp() + 3600

    @staticmethod
    def _safe_decimal_convert(
        value: str | int | float | Decimal | None,
        field_name: str,
        symbol: str,
        allow_none: bool = False,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, handling None values."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            if allow_none:
                return None
            else:
                raise ValueError(
                    f"ArbitrageOpportunity field '{field_name}' for symbol '{symbol}' cannot be None"
                )
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            raise ValueError(
                f"Invalid value '{value}' for ArbitrageOpportunity field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err

    @staticmethod
    def _safe_float_convert(
        value: str | int | float | Decimal | None,
        field_name: str,
        symbol: str,
    ) -> float | None:
        """Safely convert a value to float for non-financial metrics, handling None values."""
        if value is None:
            return None
        if isinstance(value, float):
            return value
        try:
            if isinstance(value, Decimal):
                # Convert via string to avoid float precision issues with direct float(Decimal)
                return float(str(value))
            return float(value)
        except (ValueError, TypeError):
            logger.warning(
                f"Invalid value '{value}' for ArbitrageOpportunity field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to float. Using None."
            )
            return None

    def validate_required_fields(self) -> None:
        """Check if all required fields are set and not None."""
        # Define required fields (critical for an arbitrage opportunity)
        required_fields = [
            "long_price",
            "short_price",
            "long_funding_rate",
            "short_funding_rate",
            "net_funding_differential",
        ]

        # Check which fields are None
        missing_fields = [field for field in required_fields if getattr(self, field) is None]

        if missing_fields:
            raise ValueError(
                f"Required fields {', '.join(missing_fields)} are None after conversion"
                f" for ArbitrageOpportunity (symbol: {self.symbol})"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert ArbitrageOpportunity to dictionary."""
        return {
            "symbol": self.symbol,
            "long_exchange": self.long_exchange,
            "short_exchange": self.short_exchange,
            "long_price": str(self.long_price) if self.long_price is not None else None,
            "short_price": str(self.short_price) if self.short_price is not None else None,
            "long_funding_rate": str(self.long_funding_rate)
            if self.long_funding_rate is not None
            else None,
            "short_funding_rate": str(self.short_funding_rate)
            if self.short_funding_rate is not None
            else None,
            "net_funding_differential": str(self.net_funding_differential)
            if self.net_funding_differential is not None
            else None,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "optimal_size": str(self.optimal_size) if self.optimal_size is not None else None,
            "expected_profit": str(self.expected_profit)
            if self.expected_profit is not None
            else None,
            # Float fields don't need str() conversion
            "confidence": self.confidence,
            "basis_volatility": self.basis_volatility,
            "utility_score": self.utility_score,
            "expiration_timestamp": self.expiration_timestamp,
            "id": str(uuid.uuid4()),
        }

    @property
    def is_expired(self) -> bool:
        """Check if the opportunity has expired."""
        # Compare current UTC time with expiration timestamp
        return datetime.now(UTC).timestamp() > self.expiration_timestamp


@dataclass(order=True)  # order=True needed for sorting? Depends on usage.
class TradeSignal:
    """Represents a decision signal generated by a strategy."""

    symbol: str
    signal_type: SignalType
    side: OrderSide
    price: Decimal | None = None
    quantity: Decimal | None = None
    timestamp: datetime | None = None
    confidence: float | None = None  # Changed from Decimal to float for statistical measure
    source_strategy: str | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    expiration: datetime | None = None
    metadata: dict[str, Any] | None = None
    signal_id: str | None = None  # Unique identifier, should be added if not in dataclass already

    def __post_init__(self) -> None:
        """Ensure numeric fields are Decimal and handle None values."""
        # Convert Decimal fields safely - all are optional in TradeSignal
        self.price = self._safe_decimal_convert_optional(self.price, "price", self.symbol)
        self.quantity = self._safe_decimal_convert_optional(self.quantity, "quantity", self.symbol)
        self.stop_loss = self._safe_decimal_convert_optional(
            self.stop_loss, "stop_loss", self.symbol
        )
        self.take_profit = self._safe_decimal_convert_optional(
            self.take_profit, "take_profit", self.symbol
        )

        # Convert confidence to float | None (statistical measure, not financial)
        confidence_val = self.confidence
        final_confidence: float | None = None  # Explicitly type the target variable

        if isinstance(confidence_val, float):
            final_confidence = confidence_val
        # The following elif blocks were removed as they were deemed unreachable by Mypy
        # due to the type hint `confidence: float | None`.
        # The float case is handled above, and the None case is handled by the default value.

        # Assign the final processed value back
        self.confidence = final_confidence

        # Ensure timestamp and expiration are timezone-aware (UTC) if provided
        if self.timestamp and self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=UTC)
        if self.expiration and self.expiration.tzinfo is None:
            self.expiration = self.expiration.replace(tzinfo=UTC)

        # Generate unique signal_id if not provided
        if self.signal_id is None:
            self.signal_id = str(uuid.uuid4())

    @staticmethod
    def _safe_decimal_convert_optional(
        value: Any,
        field_name: str,
        symbol: str,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, handling None values."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as err:
            raise ValueError(
                f"Invalid value '{value}' for TradeSignal field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            ) from err

    def is_valid(self) -> bool:
        """Check if the signal is still valid (e.g., not expired)."""
        if self.expiration is None:
            return True
        # Ensure comparison uses timezone-aware datetime
        return datetime.now(UTC) < self.expiration

    def to_dict(self) -> dict[str, Any]:
        """Convert TradeSignal to dictionary."""
        result = {
            "symbol": self.symbol,
            "signal_type": self.signal_type.name
            if isinstance(self.signal_type, SignalType)
            else self.signal_type,
            "side": self.side.name if isinstance(self.side, OrderSide) else self.side,
            "price": str(self.price) if self.price is not None else None,
            "quantity": str(self.quantity) if self.quantity is not None else None,
            "timestamp": self.timestamp.isoformat() if self.timestamp is not None else None,
            # Float values don't need str() conversion like Decimal
            "confidence": self.confidence,  # Now a float
            "source_strategy": self.source_strategy,
            "stop_loss": str(self.stop_loss) if self.stop_loss is not None else None,
            "take_profit": str(self.take_profit) if self.take_profit is not None else None,
            "expiration": self.expiration.isoformat() if self.expiration is not None else None,
            "metadata": self.metadata if self.metadata is not None else {},
            "signal_id": self.signal_id,
        }
        return result
