"""Core msgspec event structures for CyberDeltaEngine.

These 7 event structures replace the 33 EventType enums with type-safe,
high-performance msgspec structures. They provide 25x faster serialization
compared to Pydantic-based DomainEvent.
"""

import time
from decimal import Decimal

import msgspec

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.enums.monitoring import (
    BalanceEventType,
    HealthStatus,
    MarketDataType,
    RiskSeverity,
    RiskType,
    SystemEventType,
)
from cyberdelta.enums.trading import OrderEventType, PositionEventType, TradingAction


# 1. Market Data Event
class MarketData(msgspec.Struct, tag="market", array_like=True, gc=False):
    """ALL market data in one structure.

    Uses array_like=True and gc=False for optimal performance with high-frequency data.
    The data_type field determines which optional fields are populated.
    """

    symbol: str  # Symbol name as string (Pydantic Symbol not supported)
    exchange: ExchangeName  # Enum works directly in msgspec
    data_type: MarketDataType
    price: Decimal | None = None
    volume: int | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    bids: list[tuple[Decimal, Decimal]] | None = None  # [(price, size), ...]
    asks: list[tuple[Decimal, Decimal]] | None = None  # [(price, size), ...]
    timestamp: float = msgspec.field(default_factory=time.time)


# 2. Order Event
class OrderEvent(msgspec.Struct, tag="order"):
    """ALL order lifecycle events.

    The event_type field determines which optional fields are populated.
    Handlers convert string representations to domain enums/models.
    """

    order_id: str
    exchange: ExchangeName
    symbol: str
    event_type: OrderEventType
    price: Decimal | None = None
    quantity: Decimal | None = None
    fill_price: Decimal | None = None
    fill_quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    commission: Decimal | None = None
    reason: str | None = None
    error_code: str | None = None
    timestamp: float = msgspec.field(default_factory=time.time)


# 3. Position Event
class PositionEvent(msgspec.Struct, tag="position"):
    """ALL position changes.

    Tracks position lifecycle from opening through closing/liquidation.
    """

    position_id: str
    symbol: str
    exchange: ExchangeName
    event_type: PositionEventType
    size: Decimal
    average_price: Decimal
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    close_price: Decimal | None = None
    timestamp: float = msgspec.field(default_factory=time.time)


# 4. Signal Event
class SignalEvent(msgspec.Struct, tag="signal"):
    """Trading signals from strategies.

    Confidence is a float between 0.0 and 1.0.
    """

    signal_id: str
    strategy_name: str
    symbol: str
    exchange: ExchangeName  # REQUIRED - no defaults for critical operations
    action: TradingAction
    confidence: float  # 0.0 to 1.0
    target_price: Decimal | None = None
    target_quantity: Decimal | None = None
    timestamp: float = msgspec.field(default_factory=time.time)


# 5. Risk Event
class RiskEvent(msgspec.Struct, tag="risk"):
    """Risk management events.

    Severity determines urgency of response.
    CRITICAL and EMERGENCY events are handled with highest priority.
    """

    risk_type: RiskType
    severity: RiskSeverity
    current_value: Decimal
    limit_value: Decimal
    message: str
    symbol: str | None = None
    exchange: ExchangeName | None = None
    timestamp: float = msgspec.field(default_factory=time.time)


# 6. Balance Event
class BalanceEvent(msgspec.Struct, tag="balance"):
    """Account balance updates.

    Tracks balance changes including locks for pending operations.
    """

    account_id: str
    exchange: ExchangeName
    currency: str
    event_type: BalanceEventType
    old_balance: Decimal
    new_balance: Decimal
    locked_amount: Decimal | None = None
    timestamp: float = msgspec.field(default_factory=time.time)


# 7. System Event
class SystemEvent(msgspec.Struct, tag="system"):
    """System-level events.

    Used for monitoring component health and system status.
    """

    component: str
    event_type: SystemEventType
    status: HealthStatus
    message: str
    # Metadata should be strongly typed per component type
    error_count: int | None = None
    uptime_seconds: int | None = None
    timestamp: float = msgspec.field(default_factory=time.time)
