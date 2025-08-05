"""Portfolio domain events.

Events related to balance updates, position changes, and portfolio state.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import ExchangeName
from cyberdelta.models.events.base_event import DomainEvent


class PositionUpdatedEvent(DomainEvent):
    """Event raised when a position is updated."""

    symbol: Symbol
    exchange: ExchangeName
    previous_quantity: Decimal
    new_quantity: Decimal
    average_price: Decimal
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None


class BalanceUpdatedEvent(DomainEvent):
    """Event raised when a balance is updated."""

    asset: Symbol
    exchange: ExchangeName
    previous_balance: Decimal
    new_balance: Decimal
    available_balance: Decimal
    change_reason: str  # "trade", "deposit", "withdrawal", "fee"
