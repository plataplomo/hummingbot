"""Event system enumerations for domain events.

This module provides enumerations for the event system including
event types and entity types used throughout the trading engine.
"""

from enum import StrEnum


class EventType(StrEnum):
    """Enumeration of all domain event types."""

    # Trading events
    ORDER_PLACED = "order.placed"
    ORDER_EXECUTED = "order.executed"
    ORDER_FILLED = "order.filled"
    ORDER_PARTIALLY_FILLED = "order.partially_filled"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_REJECTED = "order.rejected"
    ORDER_MODIFIED = "order.modified"
    ORDER_EXPIRED = "order.expired"

    # Portfolio events
    BALANCE_UPDATED = "balance.updated"
    POSITION_OPENED = "position.opened"
    POSITION_CLOSED = "position.closed"
    POSITION_UPDATED = "position.updated"
    PNL_REALIZED = "pnl.realized"
    PNL_UPDATED = "pnl.updated"

    # Risk events
    RISK_LIMIT_BREACHED = "risk.limit_breached"
    RISK_LIMIT_WARNING = "risk.limit_warning"
    DRAWDOWN_ALERT = "risk.drawdown_alert"
    EXPOSURE_LIMIT_REACHED = "risk.exposure_limit"

    # System events
    CONNECTION_ESTABLISHED = "system.connection_established"
    CONNECTION_LOST = "system.connection_lost"
    HEARTBEAT_MISSED = "system.heartbeat_missed"
    RATE_LIMIT_EXCEEDED = "system.rate_limit_exceeded"
    CIRCUIT_BREAKER_TRIGGERED = "system.circuit_breaker"

    # Strategy events
    SIGNAL_GENERATED = "strategy.signal_generated"
    SIGNAL_PROCESSED = "strategy.signal_processed"
    STRATEGY_STARTED = "strategy.started"
    STRATEGY_STOPPED = "strategy.stopped"
    STRATEGY_ERROR = "strategy.error"

    # Market data events
    MARKET_DATA_UPDATED = "market.data_updated"
    TICKER_UPDATED = "market.ticker_updated"
    ORDERBOOK_UPDATED = "market.orderbook_updated"


class EntityType(StrEnum):
    """Types of entities that events can relate to."""

    ORDER = "order"
    FILL = "fill"
    POSITION = "position"
    BALANCE = "balance"
    STRATEGY = "strategy"
    CONNECTION = "connection"
    RISK_LIMIT = "risk_limit"
    SYSTEM = "system"
