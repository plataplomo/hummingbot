import time
from dataclasses import dataclass, field, asdict
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, List, Optional, Dict
# noqa: F821


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
    UNKNOWN = "UNKNOWN"


class SignalType(Enum):
    """Enum representing the type of a trading signal."""
    ENTRY = "ENTRY"
    EXIT = "EXIT"
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

    def __post_init__(self):
        # Ensure numeric fields are Decimal
        self.open = Decimal(str(self.open)) if not isinstance(self.open, Decimal) else self.open
        self.high = Decimal(str(self.high)) if not isinstance(self.high, Decimal) else self.high
        self.low = Decimal(str(self.low)) if not isinstance(self.low, Decimal) else self.low
        self.close = Decimal(str(self.close)) if not isinstance(self.close, Decimal) else self.close
        self.volume = Decimal(str(self.volume)) if not isinstance(self.volume, Decimal) else self.volume


@dataclass
class Balance:
    """Represents an account balance for a single asset."""

    asset: str
    total: Decimal
    available: Decimal

    def to_dict(self) -> Dict[str, Any]:
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
    side: OrderSide  # Changed from str to OrderSide
    size: Decimal
    entry_price: Decimal
    liquidation_price: Optional[Decimal] = None
    leverage: Decimal # Changed from float
    unrealized_pnl: Optional[Decimal] = None
    realized_pnl: Optional[Decimal] = None
    margin_type: Optional[str] = None # Added
    margin_used: Optional[Decimal] = None # Added
    timestamp: Optional[int] = None

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

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        # Convert Decimal fields back to strings for serialization if necessary
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, OrderSide): # Handle OrderSide enum
                data[key] = value.value
        return data


@dataclass
class Order:
    """Represents an order."""

    id: str  # Exchange order ID
    symbol: str  # Trading pair symbol
    side: OrderSide  # BUY or SELL
    type: OrderType  # LIMIT, MARKET, etc.
    price: Optional[Decimal] = None # Order price (Decimal, Optional for MARKET)
    quantity: Decimal # Order quantity (Decimal)
    filled_quantity: Decimal = Decimal("0.0") # Executed quantity (Decimal)
    status: OrderStatus = OrderStatus.UNKNOWN # Change type to OrderStatus enum, default UNKNOWN
    time: int = 0  # Order creation time (timestamp)
    client_order_id: str = ""  # Custom client order ID
    reduce_only: bool = False  # Whether the order is reduce-only
    avg_fill_price: Optional[Decimal] = None # Average fill price (Decimal)

    def __post_init__(self):
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
            "price": str(self.price) if self.price is not None else None,
            "quantity": str(self.quantity),
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
    side: OrderSide
    price: Decimal
    quantity: Decimal

    # Optional fields (with defaults) last
    order_id: str | None = None
    exchange: str | None = None
    datetime: datetime | None = None
    cost: Decimal | None = None
    fee: Decimal | None = None
    fee_asset: str | None = None
    is_maker: bool | None = None
    client_order_id: str | None = None

    def __post_init__(self):
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

    symbol: str
    price: Decimal  # Last price (Decimal)
    bid: Decimal = Decimal("0.0")  # Best bid price (Decimal)
    ask: Decimal = Decimal("0.0")  # Best ask price (Decimal)
    volume: Decimal = Decimal("0.0")  # 24h volume (Decimal)
    timestamp: int = 0  # Ticker timestamp

    def __post_init__(self):
        # Ensure numeric fields are Decimal
        if not isinstance(self.price, Decimal):
            self.price = Decimal(str(self.price))
        if not isinstance(self.bid, Decimal):
            self.bid = Decimal(str(self.bid))
        if not isinstance(self.ask, Decimal):
            self.ask = Decimal(str(self.ask))
        if not isinstance(self.volume, Decimal):
            self.volume = Decimal(str(self.volume))


@dataclass
class OrderBook:
    """Represents an order book for a symbol."""

    symbol: str
    bids: list[tuple[Decimal, Decimal]]  # List of [price, quantity] for bids (Decimal)
    asks: list[tuple[Decimal, Decimal]]  # List of [price, quantity] for asks (Decimal)
    timestamp: int = 0  # Order book timestamp

    def __post_init__(self):
        # Ensure bids and asks contain Decimal tuples
        self.bids = [
            (Decimal(str(p)), Decimal(str(q))) for p, q in self.bids
        ]
        self.asks = [
            (Decimal(str(p)), Decimal(str(q))) for p, q in self.asks
        ]


@dataclass
class FundingRate:
    """Represents funding rate information for a perpetual contract."""

    symbol: str
    funding_rate: Decimal  # Current funding rate (Decimal)
    predicted_rate: Decimal = Decimal("0.0")  # Predicted next funding rate (Decimal)
    mark_price: Decimal = Decimal("0.0")  # Current mark price (Decimal)
    index_price: Decimal = Decimal("0.0")  # Current index price (Decimal)
    next_funding_time: int = 0  # Next funding timestamp
    historical_rates: list[dict[str, Any]] | None = None # Historical funding rates (Optional)

    def __post_init__(self):
        # Ensure numeric fields are Decimal
        if not isinstance(self.funding_rate, Decimal):
            self.funding_rate = Decimal(str(self.funding_rate))
        if not isinstance(self.predicted_rate, Decimal):
            self.predicted_rate = Decimal(str(self.predicted_rate))
        if not isinstance(self.mark_price, Decimal):
            self.mark_price = Decimal(str(self.mark_price))
        if not isinstance(self.index_price, Decimal):
            self.index_price = Decimal(str(self.index_price))


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
        # Keep expiration logic or adapt if needed
        self.expiration = timestamp.timestamp() + 3600  # 1 hour expiration, maybe adjust

    @property
    def is_expired(self) -> bool:
        """Check if the opportunity has expired."""
        return time.time() > self.expiration
