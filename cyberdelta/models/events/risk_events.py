"""Risk management domain events.

Events related to risk limits, violations, and risk assessment outcomes.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import ExchangeName
from cyberdelta.models.events.base_event import DomainEvent


class RiskLimitViolationEvent(DomainEvent):
    """Event raised when a risk limit is violated."""

    limit_type: str  # "position_size", "exposure", "drawdown", etc.
    current_value: Decimal
    limit_value: Decimal
    symbol: Symbol | None = None
    exchange: ExchangeName | None = None
    action_taken: str  # "order_rejected", "position_reduced", etc.
