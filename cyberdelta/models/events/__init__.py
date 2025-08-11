"""Event models for the trading system.

This module provides the msgspec-based event system for all events.
All events use msgspec.Struct for high-performance serialization.

BREAKING CHANGE: DomainEvent has been removed. Use msgspec events only.
"""

# msgspec event structures ONLY
from cyberdelta.models.events.core import (
    BalanceEvent,
    MarketData,
    OrderEvent,
    PositionEvent,
    RiskEvent,
    SignalEvent,
    SystemEvent,
)

# msgspec models for event system
from cyberdelta.models.events.handler_health import HandlerHealthModel
from cyberdelta.models.events.health_status import EventBusHealthStatus
from cyberdelta.models.events.system_health import SystemHealthReport

# workflow event models
from cyberdelta.models.events.workflow import (
    BaseWorkflowEvent,
    EmergencyLiquidationEvent,
    GracefulShutdownEvent,
    PlaceOrderWorkflowEvent,
    RebalanceWorkflowEvent,
)
from cyberdelta.models.events.workflow_context import WorkflowAuditEntry, WorkflowContextModel


__all__ = [
    # msgspec event structures (sorted)
    "BalanceEvent",
    "BaseWorkflowEvent",
    "EmergencyLiquidationEvent",
    "EventBusHealthStatus",
    "GracefulShutdownEvent",
    "HandlerHealthModel",
    "MarketData",
    "OrderEvent",
    "PlaceOrderWorkflowEvent",
    "PositionEvent",
    "RebalanceWorkflowEvent",
    "RiskEvent",
    "SignalEvent",
    "SystemEvent",
    "SystemHealthReport",
    "WorkflowAuditEntry",
    "WorkflowContextModel",
]
