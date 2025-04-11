from enum import Enum
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Dict, Any, Union
import time
from datetime import datetime, timezone
from decimal import Decimal


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


@dataclass
class MarketData:
    """Represents market data for a symbol, including OHLCV information."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class Balance:
    """Represents an account balance for a single asset."""
    asset: str
    total: Decimal  # Changed to Decimal
    free: Decimal   # Changed to Decimal
    locked: Decimal = Decimal("0.0")  # Changed to Decimal, default 0
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        """Post-initialization method to ensure total is sum of free and locked."""
        if self.total != self.free + self.locked:
            raise ValueError("Total must be sum of free and locked.")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert balance to dictionary representation."""
        return {
            "asset": self.asset,
            "free": self.free,
            "locked": self.locked,
            "total": self.total
        }


@dataclass
class Position:
    """Represents an open position."""
    symbol: str
    size: Decimal  # Position size (absolute value)
    entry_price: Decimal  # Average entry price
    mark_price: Decimal  # Current mark price
    liquidation_price: Decimal = Decimal("0.0")  # Liquidation price
    unrealized_pnl: Decimal = Decimal("0.0")  # Unrealized profit/loss
    leverage: float = 1.0  # Position leverage
    side: OrderSide = OrderSide.BUY  # Position side (long/short)
    id: str = ""  # Position ID
    status: str = "OPEN"  # Position status
    
    @property
    def quantity(self) -> Decimal:
        """Return position size (for API compatibility). Returns Decimal."""
        return self.size
    
    def is_active(self) -> bool:
        """Check if position is active."""
        return self.status in ["OPEN", "PENDING", "PARTIAL"]
    
    def calculate_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """Calculate unrealized profit/loss based on current price. Uses Decimal."""
        if not isinstance(current_price, Decimal):
            current_price = Decimal(str(current_price))
            
        if self.side == OrderSide.BUY or self.side == "LONG":
            return (current_price - self.entry_price) * self.size
        else:  # SELL/SHORT
            return (self.entry_price - current_price) * self.size
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary representation. Converts Decimals to strings for JSON."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "size": str(self.size),  # Convert Decimal to string
            "entry_price": str(self.entry_price),  # Convert Decimal to string
            "mark_price": str(self.mark_price),  # Convert Decimal to string
            "liquidation_price": str(self.liquidation_price),  # Convert Decimal to string
            "unrealized_pnl": str(self.unrealized_pnl),  # Convert Decimal to string
            "leverage": self.leverage,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "status": self.status
        }


@dataclass
class Order:
    """Represents an order."""
    id: str  # Exchange order ID
    symbol: str  # Trading pair symbol
    side: OrderSide  # BUY or SELL
    type: OrderType  # LIMIT, MARKET, etc.
    price: float  # Order price
    quantity: float  # Order quantity
    filled_quantity: float = 0.0  # Executed quantity
    status: str = ""  # Order status (NEW, FILLED, CANCELED, etc.)
    time: int = 0  # Order creation time (timestamp)
    client_order_id: str = ""  # Custom client order ID
    reduce_only: bool = False  # Whether the order is reduce-only
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert order to dictionary representation."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "side": self.side.value if isinstance(self.side, OrderSide) else self.side,
            "type": self.type.value if isinstance(self.type, OrderType) else self.type,
            "price": self.price,
            "quantity": self.quantity,
            "filled_quantity": self.filled_quantity,
            "status": self.status,
            "time": self.time,
            "client_order_id": self.client_order_id,
            "reduce_only": self.reduce_only
        }


@dataclass
class Trade:
    """Represents a trade execution."""
    id: str  # Trade ID
    order_id: str  # Add this field
    exchange: str
    symbol: str  # Trading pair symbol
    timestamp: int  # Unix timestamp in milliseconds
    side: OrderSide  # BUY or SELL
    price: Decimal  # Execution price
    quantity: Decimal  # Executed quantity
    fee: Decimal  # Fee paid
    fee_asset: str  # Asset in which fee was paid
    is_maker: bool = False


@dataclass
class Ticker:
    """Represents ticker information for a symbol."""
    symbol: str
    price: float  # Last price
    bid: float = 0.0  # Best bid price
    ask: float = 0.0  # Best ask price
    volume: float = 0.0  # 24h volume
    timestamp: int = 0  # Ticker timestamp


@dataclass
class OrderBook:
    """Represents an order book for a symbol."""
    symbol: str
    bids: List[Tuple[float, float]]  # List of [price, quantity] for bids
    asks: List[Tuple[float, float]]  # List of [price, quantity] for asks
    timestamp: int = 0  # Order book timestamp


@dataclass
class FundingRate:
    """Represents funding rate information for a perpetual contract."""
    symbol: str
    funding_rate: float  # Current funding rate
    predicted_rate: float = 0.0  # Predicted next funding rate
    mark_price: float = 0.0  # Current mark price
    index_price: float = 0.0  # Current index price
    next_funding_time: int = 0  # Next funding timestamp
    historical_rates: List[Dict[str, Any]] = None  # Historical funding rates


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
        optimal_size: Optional[Decimal] = None, 
        expected_profit: Optional[Decimal] = None, 
        confidence: Optional[float] = None,
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