from __future__ import annotations  # Enable postponed evaluation

import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


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
        # Ensure numeric fields are Decimal
        self.open = Decimal(str(self.open)) if not isinstance(self.open, Decimal) else self.open
        self.high = Decimal(str(self.high)) if not isinstance(self.high, Decimal) else self.high
        self.low = Decimal(str(self.low)) if not isinstance(self.low, Decimal) else self.low
        self.close = Decimal(str(self.close)) if not isinstance(self.close, Decimal) else self.close
        self.volume = (
            Decimal(str(self.volume)) if not isinstance(self.volume, Decimal) else self.volume
        )


@dataclass
class Balance:
    """Represents an account balance for a single asset."""

    asset: str
    total: Decimal
    available: Decimal | None = None
    free: Decimal | None = None
    locked: Decimal | None = None

    def __post_init__(self):
        # If available is not provided, default it to total
        if self.available is None:
            self.available = self.total
        # Ensure fields are Decimal
        self.total = Decimal(str(self.total)) if not isinstance(self.total, Decimal) else self.total
        self.available = (
            Decimal(str(self.available))
            if not isinstance(self.available, Decimal)
            else self.available
        )

        # --- ADD INITIALIZATION FOR free and locked ---
        if self.free is None:
            self.free = Decimal("0.0")
        else:
            self.free = Decimal(str(self.free)) if not isinstance(self.free, Decimal) else self.free

        if self.locked is None:
            self.locked = Decimal("0.0")
        else:
            self.locked = (
                Decimal(str(self.locked)) if not isinstance(self.locked, Decimal) else self.locked
            )
        # --- END INITIALIZATION ---

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def quantity(self) -> Decimal:
        """Return position size (for API compatibility). Returns Decimal."""
        return self.total

    def is_active(self) -> bool:
        """Check if position is active."""
        return self.total > Decimal("0.0")

    def calculate_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """Calculate unrealized profit/loss based on current price. Uses Decimal."""
        if not isinstance(current_price, Decimal):
            current_price = Decimal(str(current_price))

        # Use OrderSide enum for comparison
        if self.asset == "USDT":
            return Decimal("0.0")  # Assuming USDT is not involved in PNL calculations
        else:
            return (current_price - self.total) * self.quantity


@dataclass
class Position:
    """Represents an open position."""

    symbol: str
    side: OrderSide
    size: Decimal
    entry_price: Decimal
    leverage: Decimal
    id: str | None = None  # Exchange position ID (Optional)
    status: str | None = None  # Position status (Optional, consider enum later)
    mark_price: Decimal | None = None  # Current mark price (Optional)
    liquidation_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    realized_pnl: Decimal | None = None
    margin_type: str | None = None  # Added
    margin_used: Decimal | None = None  # Added
    timestamp: int | None = None
    # Added fields based on Engine usage:
    strategy_name: str | None = None
    close_price: Decimal | None = None
    close_time: datetime | None = None
    pnl: Decimal | None = None  # Assuming this represents realized PNL upon close

    def is_active(self) -> bool:
        """Check if the position is actively held (size is non-zero)."""
        return self.size is not None and self.size != Decimal("0")

    def calculate_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """Calculates the unrealized PNL."""
        if self.side == OrderSide.BUY:
            pnl = (current_price - self.entry_price) * self.size
        elif self.side == OrderSide.SELL:
            pnl = (self.entry_price - current_price) * self.size
        else:
            pnl = Decimal("0.0")  # Or raise an error for unexpected side
        self.unrealized_pnl = pnl
        return pnl

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        # Convert Decimal fields back to strings for serialization if necessary
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, OrderSide):  # Handle OrderSide enum
                data[key] = value.value
        return data


@dataclass
class Order:
    """Represents an order."""

    id: str  # Exchange order ID
    symbol: str  # Trading pair symbol
    side: OrderSide  # BUY or SELL
    type: OrderType  # LIMIT, MARKET, etc.
    quantity: Decimal  # Order quantity (Decimal) - Moved before price
    price: Decimal | None = None  # Order price (Decimal, Optional for MARKET)
    filled_quantity: Decimal = Decimal("0.0")  # Executed quantity (Decimal)
    status: OrderStatus = OrderStatus.UNKNOWN  # Change type to OrderStatus enum, default UNKNOWN
    time: int = 0  # Order creation time (timestamp)
    client_order_id: str = ""  # Custom client order ID
    reduce_only: bool = False  # Whether the order is reduce-only
    avg_fill_price: Decimal | None = None  # Average fill price (Decimal)

    def __post_init__(self) -> None:
        # Ensure numeric fields are Decimal
        if self.price is not None and not isinstance(self.price, Decimal):
            self.price = Decimal(str(self.price))
        if not isinstance(self.quantity, Decimal):
            self.quantity = Decimal(str(self.quantity))
        if not isinstance(self.filled_quantity, Decimal):
            self.filled_quantity = Decimal(str(self.filled_quantity))
        if self.avg_fill_price is not None and not isinstance(self.avg_fill_price, Decimal):
            self.avg_fill_price = Decimal(str(self.avg_fill_price))

    def to_dict(self) -> dict[str, Any]:
        """Convert order to dictionary representation."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "type": self.type.value if isinstance(self.type, OrderType) else self.type,
            "quantity": str(self.quantity),
            "price": str(self.price) if self.price is not None else None,
            "filled_quantity": str(self.filled_quantity),
            "status": self.status,
            "time": self.time,
            "client_order_id": self.client_order_id,
            "reduce_only": self.reduce_only,
            "avg_fill_price": str(self.avg_fill_price) if self.avg_fill_price is not None else None,
        }


@dataclass
class Trade:
    """Represents a trade execution."""

    # Required fields (no defaults) first
    id: str
    symbol: str
    timestamp: int
    # side: OrderSide # Changed to optional
    price: Decimal
    quantity: Decimal

    # Optional fields (with defaults) last - REORDERED
    side: OrderSide | None = None  # CHANGED: Made optional
    order_id: str | None = None
    exchange: str | None = None
    datetime: datetime | None = None
    fee: Decimal | None = None
    fee_asset: str | None = None
    is_maker: bool | None = None
    client_order_id: str | None = None
    cost: Decimal | None = None  # Moved cost here as it's calculated in post_init if None

    def __post_init__(self) -> None:
        # Ensure numeric fields are Decimal
        self.price = Decimal(str(self.price)) if not isinstance(self.price, Decimal) else self.price
        self.quantity = (
            Decimal(str(self.quantity)) if not isinstance(self.quantity, Decimal) else self.quantity
        )
        if self.fee is not None:
            self.fee = Decimal(str(self.fee)) if not isinstance(self.fee, Decimal) else self.fee
        if self.cost is None:
            self.cost = self.price * self.quantity
        else:
            self.cost = Decimal(str(self.cost)) if not isinstance(self.cost, Decimal) else self.cost

        # Convert timestamp to datetime if not already done
        if self.timestamp and self.datetime is None:
            try:
                # Assume timestamp is in milliseconds
                self.datetime = datetime.fromtimestamp(self.timestamp / 1000, tz=UTC)
            except Exception:
                # Handle potential errors if timestamp format is unexpected
                pass  # Or log a warning


@dataclass
class Ticker:
    """Represents ticker information for a symbol."""

    # Required fields (no defaults) first
    symbol: str
    price: Decimal  # Last price (Decimal)

    # Optional fields (with defaults) last - REORDERED
    bid: Decimal = Decimal("0.0")  # Best bid price (Decimal)
    ask: Decimal = Decimal("0.0")  # Best ask price (Decimal)
    volume: Decimal = Decimal("0.0")  # 24h volume (Decimal)
    timestamp: int = 0  # Ticker timestamp

    def __post_init__(self) -> None:
        # Ensure numeric fields are Decimal
        if self.price is not None and not isinstance(self.price, Decimal):
            try:
                self.price = Decimal(str(self.price))
            except InvalidOperation:
                # Handle potential conversion error, e.g., log and set to None or raise
                self.price = None  # Or raise appropriate error
        if self.bid is not None and not isinstance(self.bid, Decimal):
            try:
                self.bid = Decimal(str(self.bid))
            except InvalidOperation:
                self.bid = None
        if self.ask is not None and not isinstance(self.ask, Decimal):
            try:
                self.ask = Decimal(str(self.ask))
            except InvalidOperation:
                self.ask = None
        if self.volume is not None and not isinstance(self.volume, Decimal):
            try:
                self.volume = Decimal(str(self.volume))
            except InvalidOperation:
                self.volume = None
        # Ensure timestamp is datetime if provided
        # if self.timestamp is not None and not isinstance(self.timestamp, datetime):
        #     # Attempt conversion or raise error based on expected input format
        #     # e.g., self.timestamp = datetime.fromisoformat(self.timestamp)
        #     pass # Add appropriate timestamp handling if needed
        return


@dataclass
class OrderBook:
    """Represents an order book for a symbol."""

    symbol: str
    bids: list[tuple[Decimal, Decimal]]  # List of [price, quantity] for bids (Decimal)
    asks: list[tuple[Decimal, Decimal]]  # List of [price, quantity] for asks (Decimal)
    timestamp: int = 0  # Order book timestamp

    def __post_init__(self) -> None:
        # Ensure bids and asks contain Decimal tuples
        self.bids = [(Decimal(str(p)), Decimal(str(q))) for p, q in self.bids]
        self.asks = [(Decimal(str(p)), Decimal(str(q))) for p, q in self.asks]


@dataclass
class FundingRate:
    """Represents funding rate information for a perpetual contract."""

    symbol: str
    funding_rate: Decimal  # Current funding rate (Decimal)
    predicted_rate: Decimal | None = None  # Made optional
    mark_price: Decimal | None = None  # Made optional
    index_price: Decimal | None = None  # Made optional
    next_funding_time: int | None = None  # Made optional
    timestamp: int | None = None  # Added Optional timestamp field (Fix 13)
    historical_rates: list[dict[str, Any]] | None = None  # Historical funding rates (Optional)

    def __post_init__(self) -> None:
        # Ensure numeric fields are Decimal, handle None explicitly
        if self.funding_rate is not None and not isinstance(self.funding_rate, Decimal):
            try:
                self.funding_rate = Decimal(str(self.funding_rate))
            except InvalidOperation:
                self.funding_rate = None  # Handle conversion error

        if self.predicted_rate is not None and not isinstance(self.predicted_rate, Decimal):
            try:
                self.predicted_rate = Decimal(str(self.predicted_rate))
            except InvalidOperation:
                self.predicted_rate = None  # Handle conversion error

        if self.mark_price is not None and not isinstance(self.mark_price, Decimal):
            try:
                self.mark_price = Decimal(str(self.mark_price))
            except InvalidOperation:
                self.mark_price = None  # Handle conversion error

        if self.index_price is not None and not isinstance(self.index_price, Decimal):
            try:
                self.index_price = Decimal(str(self.index_price))
            except InvalidOperation:
                self.index_price = None  # Handle conversion error


class ArbitrageOpportunity:
    """Represents a funding rate arbitrage opportunity."""

    def __init__(
        self,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        long_price: Decimal,
        short_price: Decimal,
        long_funding_rate: Decimal,
        short_funding_rate: Decimal,
        net_funding_differential: Decimal,
        timestamp: datetime,
        # Optional fields can be added if needed later
        optimal_size: Decimal | None = None,
        expected_profit: Decimal | None = None,
        confidence: float | None = None,
        # Added missing fields based on errors in callers
        basis_volatility: float | None = None,  # Optional volatility measure
        utility_score: float | None = None,  # Optional score
    ):
        self.symbol = symbol
        self.long_exchange = long_exchange
        self.short_exchange = short_exchange
        self.long_price = long_price
        self.short_price = short_price
        self.long_funding_rate = long_funding_rate
        self.short_funding_rate = short_funding_rate
        self.net_funding_differential = net_funding_differential
        self.timestamp = timestamp
        self.optimal_size = optimal_size
        self.expected_profit = expected_profit
        self.confidence = confidence
        self.basis_volatility = basis_volatility
        self.utility_score = utility_score
        # Keep expiration logic or adapt if needed
        self.expiration = timestamp.timestamp() + 3600  # 1 hour expiration, maybe adjust

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "long_exchange": self.long_exchange,
            "short_exchange": self.short_exchange,
            "long_price": self.long_price,
            "short_price": self.short_price,
            "long_funding_rate": self.long_funding_rate,
            "short_funding_rate": self.short_funding_rate,
            "net_funding_differential": self.net_funding_differential,
            "timestamp": self.timestamp.isoformat(),
            "optimal_size": self.optimal_size,
            "expected_profit": self.expected_profit,
            "confidence": self.confidence,
            "basis_volatility": self.basis_volatility,
            "utility_score": self.utility_score,
        }

    @property
    def is_expired(self) -> bool:
        """Check if the opportunity has expired."""
        return time.time() > self.expiration


@dataclass(order=True)
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
    # Added missing fields based on errors in callers
    stop_loss: Decimal | None = None  # Optional stop loss price
    take_profit: Decimal | None = None  # Optional take profit price
    expiration: datetime | None = None  # Optional signal expiration time
    metadata: dict[str, Any] | None = None  # Optional additional data

    def is_valid(self) -> bool:
        """Check if the signal is still valid (e.g., not expired)."""
        if self.expiration is None:
            return True  # No expiration set, always valid
        return datetime.now(UTC) < self.expiration

    def to_dict(self) -> dict[str, Any]:
        """Convert TradeSignal to dictionary representation."""
        data = asdict(self)
        # Convert enums and Decimals for serialization
        if isinstance(data.get("signal_type"), SignalType):
            data["signal_type"] = data["signal_type"].value
        if isinstance(data.get("side"), OrderSide):
            data["side"] = data["side"].value
        if isinstance(data.get("price"), Decimal):
            data["price"] = str(data["price"])
        if isinstance(data.get("quantity"), Decimal):
            data["quantity"] = str(data["quantity"])
        if isinstance(data.get("timestamp"), datetime):
            data["timestamp"] = data["timestamp"].isoformat()
        return data
