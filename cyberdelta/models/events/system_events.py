"""System domain events.

Events related to system health, circuit breakers, reconciliation,
and overall system lifecycle.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import Field

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import ExchangeName
from cyberdelta.models.events.base_event import DomainEvent


class CircuitBreakerTrippedEvent(DomainEvent):
    """Event raised when a circuit breaker is tripped."""

    breaker_type: str  # "global", "exchange", "symbol"
    breaker_name: str
    failure_count: int
    cooldown_seconds: int
    affected_exchange: ExchangeName | None = None
    affected_symbol: Symbol | None = None


class ReconciliationDiscrepancyEvent(DomainEvent):
    """Event raised when portfolio reconciliation finds a discrepancy."""

    discrepancy_type: str  # "balance", "position"
    exchange: ExchangeName
    symbol: Symbol | None = None
    expected_value: Decimal
    actual_value: Decimal
    discrepancy_pct: Decimal
    action_taken: str  # "corrected", "alert_sent", "ignored"


class SystemHealthCheckEvent(DomainEvent):
    """Event raised during system health checks."""

    service_name: str
    health_status: str  # "healthy", "degraded", "unhealthy"
    metrics: dict[str, Any] = Field(default_factory=dict)
    issues: list[str] = Field(default_factory=list)


class TradingSessionStartedEvent(DomainEvent):
    """Event raised when a trading session starts."""

    session_id: str
    safe_mode: bool
    enabled_exchanges: list[str]
    enabled_strategies: list[str]
    risk_limits: dict[str, Any] = Field(default_factory=dict)


class TradingSessionStoppedEvent(DomainEvent):
    """Event raised when a trading session stops."""

    session_id: str
    stop_reason: str  # "user_requested", "error", "maintenance"
    total_orders: int
    successful_orders: int
    total_pnl: Decimal | None = None
