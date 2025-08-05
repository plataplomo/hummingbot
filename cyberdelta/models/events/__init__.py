"""Event models for the trading system.

This package contains all domain events that represent business facts
and state changes in the trading system.

Events are organized by domain:
- base_event: Base DomainEvent class
- trading_events: Order and execution events
- portfolio_events: Balance and position events
- risk_events: Risk management events
- system_events: System health and monitoring events
- strategy_events: Strategy and signal events
"""

# Base event class
from .base_event import DomainEvent

# Portfolio domain events
from .portfolio_events import (
    BalanceUpdatedEvent,
    PositionUpdatedEvent,
)

# Risk domain events
from .risk_events import (
    RiskLimitViolationEvent,
)

# Strategy domain events
from .strategy_events import (
    MarketDataUpdatedEvent,
    SignalExecutionFailedEvent,
    SignalProcessedEvent,
    StrategySignalGeneratedEvent,
)

# System domain events
from .system_events import (
    CircuitBreakerTrippedEvent,
    ReconciliationDiscrepancyEvent,
    SystemHealthCheckEvent,
    TradingSessionStartedEvent,
    TradingSessionStoppedEvent,
)

# Trading domain events
from .trading_events import (
    OrderCancelledEvent,
    OrderExecutedEvent,
    OrderFilledEvent,
)


__all__ = [
    # Base
    "DomainEvent",
    # Trading
    "OrderExecutedEvent",
    "OrderFilledEvent",
    "OrderCancelledEvent",
    # Portfolio
    "PositionUpdatedEvent",
    "BalanceUpdatedEvent",
    # Risk
    "RiskLimitViolationEvent",
    # System
    "CircuitBreakerTrippedEvent",
    "ReconciliationDiscrepancyEvent",
    "SystemHealthCheckEvent",
    "TradingSessionStartedEvent",
    "TradingSessionStoppedEvent",
    # Strategy
    "SignalProcessedEvent",
    "SignalExecutionFailedEvent",
    "StrategySignalGeneratedEvent",
    "MarketDataUpdatedEvent",
]
