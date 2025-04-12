from __future__ import annotations  # Enable postponed evaluation

import logging
from dataclasses import asdict, dataclass
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

    def __post_init__(self) -> None:
        """Ensure all numeric fields are Decimals, converting safely."""
        self.open = self._safe_decimal_convert(self.open, "open", self.symbol)
        self.high = self._safe_decimal_convert(self.high, "high", self.symbol)
        self.low = self._safe_decimal_convert(self.low, "low", self.symbol)
        self.close = self._safe_decimal_convert(self.close, "close", self.symbol)
        self.volume = self._safe_decimal_convert(self.volume, "volume", self.symbol)

    @staticmethod
    def _safe_decimal_convert(value: Any, field_name: str, symbol: str) -> Decimal:
        """Safely convert a value to Decimal, logging errors."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            # Decide handling: raise error, return 0, or log and return 0?
            # Assuming non-nullable based on type hint, raise error.
            raise ValueError(
                f"MarketData field '{field_name}' for symbol '{symbol}' cannot be None"
            )
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError):
            # Log error or raise? Let's raise for critical market data.
            raise ValueError(
                f"Invalid value '{value}' for MarketData field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            )


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
        self.total = self._safe_decimal_convert(self.total, "total", self.asset, allow_none=False)

        # Handle available field - ensure it's never None after initialization
        if self.available is None:
            self.available = self.total  # Default available to total if not provided
        else:
            available_decimal = self._safe_decimal_convert(
                self.available, "available", self.asset, allow_none=False
            )
            # This is safe now because _safe_decimal_convert will raise an error if it can't convert
            self.available = available_decimal

        # Handle free field - ensure it's never None after initialization
        if self.free is None:
            self.free = Decimal("0.0")  # Default to zero
        else:
            free_decimal = self._safe_decimal_convert(
                self.free, "free", self.asset, allow_none=False
            )
            self.free = free_decimal

        # Handle locked field - ensure it's never None after initialization
        if self.locked is None:
            self.locked = Decimal("0.0")  # Default to zero
        else:
            locked_decimal = self._safe_decimal_convert(
                self.locked, "locked", self.asset, allow_none=False
            )
            self.locked = locked_decimal

    @staticmethod
    def _safe_decimal_convert(
        value: Any,
        field_name: str,
        asset: str,
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
                raise ValueError(f"Balance field '{field_name}' for asset '{asset}' cannot be None")
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError):
            raise ValueError(
                f"Invalid value '{value}' for Balance field '{field_name}' "
                f"for asset '{asset}'. Cannot convert to Decimal."
            )

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
        self.size = self._safe_decimal_convert(self.size, "size", self.symbol, allow_none=False)
        self.entry_price = self._safe_decimal_convert(
            self.entry_price, "entry_price", self.symbol, allow_none=False
        )

        # Handle Optional Decimal fields
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
        except (InvalidOperation, TypeError):
            raise ValueError(
                f"Invalid value '{value}' for Position field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            )

    def is_active(self) -> bool:
        """Check if the position is actively held (size is non-zero)."""
        return self.size is not None and self.size != Decimal("0")

    def calculate_unrealized_pnl(self, current_mark_price: Decimal | None) -> Decimal | None:
        """Calculates the unrealized PNL based on a provided mark price.

        Args:
            current_mark_price: The current mark price to use for calculation

        Returns:
            The calculated unrealized PNL as a Decimal, or None if calculation is not possible
        """
        # First check if we have valid data to calculate PNL
        if current_mark_price is None:
            return self.unrealized_pnl

        # Check for other required values
        if self.entry_price is None or self.size is None:
            return self.unrealized_pnl

        # Check for zero size
        if self.size == Decimal("0"):
            return Decimal("0.0")  # Return zero instead of None for zero size

        # Convert current_mark_price to Decimal (this is safe since we checked for None above)
        mark_price_decimal = self._safe_decimal_convert(
            current_mark_price, "current_mark_price", self.symbol, allow_none=False
        )

        # Calculate PNL based on side
        if self.side == OrderSide.BUY:
            pnl = (mark_price_decimal - self.entry_price) * self.size
        elif self.side == OrderSide.SELL:
            pnl = (self.entry_price - mark_price_decimal) * self.size
        else:
            # Handle unexpected OrderSide (shouldn't happen with enum)
            logger.warning(f"Unexpected OrderSide value: {self.side} for {self.symbol}")
            pnl = Decimal("0.0")

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
    """Represents an order."""

    id: str
    symbol: str
    side: OrderSide
    type: OrderType
    quantity: Decimal
    price: Decimal | None = None
    filled_quantity: Decimal = Decimal("0.0")
    status: OrderStatus = OrderStatus.UNKNOWN
    time: int | None = None  # Consider datetime? Timestamp in ms is common
    client_order_id: str | None = None  # Make optional
    reduce_only: bool | None = None  # Make optional
    avg_fill_price: Decimal | None = None

    def __post_init__(self) -> None:
        """Ensure numeric fields are Decimal, handle None."""
        self.quantity = self._safe_decimal_convert(
            self.quantity, "quantity", self.symbol, allow_none=False
        )

        # Handle price field - may be None for market orders
        if self.price is None and self.type != OrderType.MARKET:
            # For non-market orders, we need a price
            raise ValueError(f"Price cannot be None for {self.type} orders")
        elif self.price is not None:
            # Convert price to Decimal if provided
            self.price = self._safe_decimal_convert(
                self.price, "price", self.symbol, allow_none=False
            )

        # Handle filled_quantity - ensure it's never None
        if self.filled_quantity is None:
            self.filled_quantity = Decimal("0.0")
        else:
            filled_qty = self._safe_decimal_convert(
                self.filled_quantity, "filled_quantity", self.symbol, allow_none=False
            )
            self.filled_quantity = filled_qty

        # Handle avg_fill_price - may be None if order not filled at all
        if self.avg_fill_price is not None:
            self.avg_fill_price = self._safe_decimal_convert(
                self.avg_fill_price, "avg_fill_price", self.symbol, allow_none=False
            )

        # Set defaults for optional fields if they are None after init
        if self.status is None:
            self.status = OrderStatus.UNKNOWN
        if self.client_order_id is None:
            self.client_order_id = ""  # Default empty string
        if self.reduce_only is None:
            self.reduce_only = False  # Default False

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
                # Consider if quantity=None should raise error or be 0? Raising for now.
                raise ValueError(f"Order field '{field_name}' for symbol '{symbol}' cannot be None")
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError):
            raise ValueError(
                f"Invalid value '{value}' for Order field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert order to dictionary representation."""
        d = asdict(self)
        # Convert enums and Decimals for serialization
        for key, value in d.items():
            if isinstance(value, Decimal):
                d[key] = str(value)
            elif isinstance(value, (OrderSide, OrderType, OrderStatus)):
                d[key] = value.value
            elif isinstance(value, datetime):  # If time becomes datetime
                d[key] = value.isoformat()
        # Ensure None values are represented correctly (as null in JSON)
        # asdict handles this, but explicit check might be needed depending on target format
        return d


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
        self.price = self._safe_decimal_convert(self.price, "price", self.symbol, allow_none=False)
        self.quantity = self._safe_decimal_convert(
            self.quantity, "quantity", self.symbol, allow_none=False
        )

        # Handle optional fee field
        if self.fee is not None:
            self.fee = self._safe_decimal_convert(self.fee, "fee", self.symbol, allow_none=False)

        # Calculate cost if not provided
        if self.cost is None:
            # We already ensured price and quantity are Decimal above
            self.cost = self.price * self.quantity
        else:
            # If cost is provided, convert it to Decimal
            self.cost = self._safe_decimal_convert(self.cost, "cost", self.symbol, allow_none=False)

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
                raise ValueError(f"Trade field '{field_name}' for symbol '{symbol}' cannot be None")
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError):
            raise ValueError(
                f"Invalid value '{value}' for Trade field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            )


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
        except (InvalidOperation, TypeError):
            # Log warning or use default if allowed
            if allow_none:
                logger.warning(
                    f"Invalid value '{value}' for Ticker field '{field_name}' for symbol '{symbol}'. "
                    f"Using default: {default}"
                )
                return default

            # If not allowed to be None, raise the error
            raise ValueError(
                f"Invalid value '{value}' for Ticker field '{field_name}' for symbol '{symbol}'. "
                f"Cannot convert to Decimal."
            )


@dataclass
class OrderBook:
    """Represents an order book for a symbol."""

    symbol: str
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]
    timestamp: int | None = None  # Usually ms

    def __post_init__(self) -> None:
        """Ensure bids and asks contain Decimal tuples, converting safely."""
        self.bids = self._safe_convert_level_list(self.bids, "bids", self.symbol)
        self.asks = self._safe_convert_level_list(self.asks, "asks", self.symbol)

    @staticmethod
    def _safe_convert_level_list(
        levels: Any, field_name: str, symbol: str
    ) -> list[tuple[Decimal, Decimal]]:
        """Safely convert a list of price/quantity pairs to Decimal tuples."""
        if not isinstance(levels, list):
            raise ValueError(
                f"OrderBook field '{field_name}' for symbol '{symbol}' must be a list."
            )

        converted_levels = []
        for i, level in enumerate(levels):
            if not isinstance(level, (list, tuple)) or len(level) != 2:
                raise ValueError(
                    f"Invalid item format in OrderBook field '{field_name}' for symbol '{symbol}' at index {i}. "
                    f"Expected tuple/list of length 2, got: {level}"
                )
            try:
                price = Decimal(str(level[0]))
                quantity = Decimal(str(level[1]))
                converted_levels.append((price, quantity))
            except (InvalidOperation, TypeError, IndexError):
                raise ValueError(
                    f"Invalid price/quantity in OrderBook field '{field_name}' for symbol '{symbol}' at index {i}. "
                    f"Cannot convert {level} to Decimal tuple."
                )
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
                return default
            else:
                raise ValueError(
                    f"FundingRate field '{field_name}' for symbol '{symbol}' cannot be None"
                )
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError):
            # Log warning or use default if allowed
            if allow_none:
                logger.warning(
                    f"Invalid value '{value}' for FundingRate field '{field_name}' for symbol '{symbol}'. "
                    f"Using default: {default}"
                )
                return default

            # If not allowed to be None, raise the error
            raise ValueError(
                f"Invalid value '{value}' for FundingRate field '{field_name}' for symbol '{symbol}'. "
                f"Cannot convert to Decimal."
            )


# ArbitrageOpportunity is not a dataclass, handle conversion in __init__
class ArbitrageOpportunity:
    """Represents a funding rate arbitrage opportunity."""

    def __init__(
        self,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        long_price: Any,  # Accept Any initially
        short_price: Any,
        long_funding_rate: Any,
        short_funding_rate: Any,
        net_funding_differential: Any,
        timestamp: datetime,
        optimal_size: Any | None = None,
        expected_profit: Any | None = None,
        confidence: float | None = None,
        basis_volatility: float | None = None,
        utility_score: float | None = None,
    ):
        self.symbol = symbol
        self.long_exchange = long_exchange
        self.short_exchange = short_exchange
        self.timestamp = timestamp
        self.confidence = confidence  # float or None
        self.basis_volatility = basis_volatility  # float or None
        self.utility_score = utility_score  # float or None

        # Perform safe Decimal conversions
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

        # Validate required fields are not None after conversion
        if None in [
            self.long_price,
            self.short_price,
            self.long_funding_rate,
            self.short_funding_rate,
            self.net_funding_differential,
        ]:
            raise ValueError(
                f"Required Decimal field is None after conversion for ArbitrageOpportunity (symbol: {symbol})"
            )

        # Expiration calculation
        # Use timezone-aware comparison if timestamp has timezone
        if timestamp.tzinfo:
            self.expiration_timestamp = timestamp.timestamp() + 3600  # 1 hour
        else:
            # Assume UTC if no timezone provided for timestamp()
            self.expiration_timestamp = timestamp.replace(tzinfo=UTC).timestamp() + 3600

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
                    f"ArbitrageOpportunity field '{field_name}' for symbol '{symbol}' cannot be None"
                )
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError):
            raise ValueError(
                f"Invalid value '{value}' for ArbitrageOpportunity field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
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
            "timestamp": self.timestamp.isoformat(),
            "optimal_size": str(self.optimal_size) if self.optimal_size is not None else None,
            "expected_profit": str(self.expected_profit)
            if self.expected_profit is not None
            else None,
            "confidence": self.confidence,
            "basis_volatility": self.basis_volatility,
            "utility_score": self.utility_score,
            # Add expiration_timestamp if needed in dict
            "expiration_timestamp": self.expiration_timestamp,
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
    confidence: float | None = None
    source_strategy: str | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    expiration: datetime | None = None
    metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Ensure numeric fields are Decimal, handle None."""
        self.price = self._safe_decimal_convert(self.price, "price", self.symbol, allow_none=True)
        self.quantity = self._safe_decimal_convert(
            self.quantity, "quantity", self.symbol, allow_none=True
        )
        self.stop_loss = self._safe_decimal_convert(
            self.stop_loss, "stop_loss", self.symbol, allow_none=True
        )
        self.take_profit = self._safe_decimal_convert(
            self.take_profit, "take_profit", self.symbol, allow_none=True
        )

        # Ensure timestamp and expiration are timezone-aware (UTC) if provided
        if self.timestamp and self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=UTC)
        if self.expiration and self.expiration.tzinfo is None:
            self.expiration = self.expiration.replace(tzinfo=UTC)

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
                    f"TradeSignal field '{field_name}' for symbol '{symbol}' cannot be None"
                )
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError):
            raise ValueError(
                f"Invalid value '{value}' for TradeSignal field '{field_name}' "
                f"for symbol '{symbol}'. Cannot convert to Decimal."
            )

    def is_valid(self) -> bool:
        """Check if the signal is still valid (e.g., not expired)."""
        if self.expiration is None:
            return True
        # Ensure comparison uses timezone-aware datetime
        return datetime.now(UTC) < self.expiration

    def to_dict(self) -> dict[str, Any]:
        """Convert TradeSignal to dictionary representation."""
        d = asdict(self)
        # Convert enums, Decimals, and datetimes for serialization
        for key, value in d.items():
            if isinstance(value, Decimal):
                d[key] = str(value)
            elif isinstance(value, (SignalType, OrderSide)):
                d[key] = value.value
            elif isinstance(value, datetime):
                d[key] = value.isoformat()
        return d


# Helper function placed outside classes if used by multiple, or as staticmethod if only one
# def _safe_decimal_convert(value: Any, field_name: str, identifier: str, allow_none: bool = False, default: Optional[Decimal] = None) -> Optional[Decimal]:
#     """Safely convert a value to Decimal, handling None and defaults."""
#     if isinstance(value, Decimal):
#         return value
#     if value is None:
#         if allow_none:
#             return default
#         else:
#              raise ValueError(f"Field '{field_name}' for '{identifier}' cannot be None")
#     try:
#         # Force string conversion first for robustness against float inputs
#         return Decimal(str(value))
#     except (InvalidOperation, TypeError):
#          # Log warning or raise? Raising for now.
#          raise ValueError(
#              f"Invalid value '{value}' for field '{field_name}' "
#              f"for '{identifier}'. Cannot convert to Decimal."
#          )
