"""Strategy and signal domain events.

Events related to signal generation, processing, market data updates,
and strategy execution lifecycle.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.models.events.base_event import DomainEvent
from cyberdelta.symbols.models import Symbol


class SignalProcessedEvent(DomainEvent):
    """Event raised when a trading signal has been processed."""

    signal_id: str
    order_id: str
    symbol: Symbol
    exchange: ExchangeName
    success: bool
    rejection_reason: str | None = None


class SignalExecutionFailedEvent(DomainEvent):
    """Event raised when signal execution fails."""

    signal_id: str
    symbol: Symbol
    exchange: ExchangeName
    error_message: str
    error_type: str


class StrategySignalGeneratedEvent(DomainEvent):
    """Event raised when a strategy generates a trading signal."""

    strategy_name: str
    signal_id: str
    symbol: Symbol
    exchange: ExchangeName
    side: OrderSide
    confidence: float
    expected_profit: Decimal | None = None


class MarketDataUpdatedEvent(DomainEvent):
    """Event raised when market data is updated."""

    symbol: Symbol
    exchange: ExchangeName
    data_type: str  # "ticker", "orderbook", "trade"
    last_price: Decimal | None = None
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    volume_24h: Decimal | None = None
