import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, List

class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"

class OrderStatus(Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"

class TimeInForce(Enum):
    GTC = "GTC"  # Good 'Til Canceled
    IOC = "IOC"  # Immediate or Cancel
    FOK = "FOK"  # Fill or Kill

@dataclass
class Ticker:
    symbol: str
    timestamp: float = field(default_factory=time.time)
    last_price: Optional[float] = None
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    volume_24h: Optional[float] = None

@dataclass
class OrderBookLevel:
    price: float
    quantity: float

@dataclass
class OrderBook:
    symbol: str
    timestamp: float = field(default_factory=time.time)
    bids: List[OrderBookLevel] = field(default_factory=list)
    asks: List[OrderBookLevel] = field(default_factory=list)

@dataclass
class Trade:
    symbol: str
    trade_id: str
    timestamp: float # Exchange timestamp if available, otherwise reception time
    price: float
    quantity: float
    side: OrderSide
    is_taker: Optional[bool] = None

@dataclass
class FundingRate:
    symbol: str
    timestamp: float = field(default_factory=time.time)
    predicted_rate: Optional[float] = None # Rate for the next interval
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    time_to_next_funding: Optional[float] = None # Seconds

@dataclass
class Balance:
    asset: str
    total: float
    available: float # Total - Locked in orders/positions

@dataclass
class Position:
    symbol: str
    side: OrderSide # Long (BUY) or Short (SELL)
    quantity: float
    entry_price: float
    mark_price: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    leverage: Optional[float] = None
    liquidation_price: Optional[float] = None
    margin: Optional[float] = None

@dataclass
class Order:
    order_id: str # Exchange-provided ID
    client_order_id: Optional[str] = None # Optional ID provided by us
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float] = None # Required for LIMIT orders
    status: OrderStatus
    time_in_force: Optional[TimeInForce] = None
    filled_quantity: float = 0.0
    average_fill_price: Optional[float] = None
    created_at: float = field(default_factory=time.time) # Timestamp when created by our system
    updated_at: float = field(default_factory=time.time) # Timestamp of last update from exchange

@dataclass
class ArbitrageOpportunity:
    # Identifies the specific opportunity (e.g., HL_vs_BP_BTC-PERP)
    opportunity_id: str
    # Cross-exchange or single-exchange
    opportunity_type: str # 'cross' or 'single'
    # List of symbols involved (e.g., ['BTC-PERP@HL', 'BTC-PERP@BP'])
    legs: List[str]
    # Calculated expected profit (net of estimated costs)
    expected_profit_adj: float
    # Calculated basis volatility relevant to this opportunity
    basis_volatility: float
    # Calculated Utility Score
    utility_score: float
    # Recommended size (constrained by risk, collateral, Kelly)
    recommended_size: float
    # Details for execution (e.g., side per leg)
    execution_details: Dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

@dataclass
class TransferInfo:
    transfer_id: str # Our internal ID or bridge/exchange ID
    asset: str
    amount: float
    source_exchange: str
    destination_exchange: str
    status: str # PENDING, CONFIRMING, COMPLETED, FAILED
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    tx_hash: Optional[str] = None
    path_details: Dict = field(default_factory=dict) # e.g., {'type': 'bridge', 'name': 'across', 'steps': ...}

# Add more models as needed (e.g., OraclePrice, MarketVolatility) 