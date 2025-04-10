from enum import Enum
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
import time
from datetime import datetime


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
    free: float = 0.0  # Amount available for use
    locked: float = 0.0  # Amount locked in orders
    total: float = 0.0  # Total balance (free + locked)
    
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
    size: float  # Position size (absolute value)
    entry_price: float  # Average entry price
    mark_price: float  # Current mark price
    liquidation_price: float = 0.0  # Liquidation price
    unrealized_pnl: float = 0.0  # Unrealized profit/loss
    leverage: float = 1.0  # Position leverage
    side: OrderSide = OrderSide.BUY  # Position side (long/short)
    id: str = ""  # Position ID
    status: str = "OPEN"  # Position status
    
    @property
    def quantity(self) -> float:
        """Return position size (for API compatibility)."""
        return self.size
    
    def is_active(self) -> bool:
        """Check if position is active."""
        return self.status in ["OPEN", "PENDING", "PARTIAL"]
    
    def calculate_unrealized_pnl(self, current_price: float) -> float:
        """Calculate unrealized profit/loss based on current price."""
        if self.side == OrderSide.BUY or self.side == "LONG":
            return (current_price - self.entry_price) * self.size
        else:  # SELL/SHORT
            return (self.entry_price - current_price) * self.size
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert position to dictionary representation."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "size": self.size,
            "entry_price": self.entry_price,
            "mark_price": self.mark_price,
            "liquidation_price": self.liquidation_price,
            "unrealized_pnl": self.unrealized_pnl,
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
    symbol: str  # Trading pair symbol
    price: float  # Execution price
    quantity: float  # Executed quantity
    time: int  # Execution time (timestamp)
    side: OrderSide  # BUY or SELL
    fee: float = 0.0  # Fee paid
    fee_asset: str = ""  # Asset in which fee was paid


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
        asset: Dict[str, str],  # Contains symbol and exchange information
        funding_rate: float,  # Current funding rate
        expected_return: float,  # Expected return (annualized)
        optimal_size: float,  # Optimal position size
        side: OrderSide,  # Position side (BUY or SELL)
        confidence: float,  # Confidence level (0-1)
        timestamp: datetime  # When the opportunity was identified
    ):
        self.asset = asset
        self.funding_rate = funding_rate
        self.expected_return = expected_return
        self.optimal_size = optimal_size
        self.side = side
        self.confidence = confidence
        self.timestamp = timestamp
        self.expiration = timestamp.timestamp() + 3600  # 1 hour expiration
    
    @property
    def is_expired(self) -> bool:
        """Check if the opportunity has expired."""
        return time.time() > self.expiration 