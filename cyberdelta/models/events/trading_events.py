"""Trading domain events.

Events related to order execution, fills, and trading operations.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import ExchangeName, OrderSide, OrderType
from cyberdelta.models.events.base_event import DomainEvent
from cyberdelta.symbols.models import Symbol


class OrderExecutedEvent(DomainEvent):
    """Event raised when an order is executed."""

    order_id: str
    symbol: Symbol
    exchange: ExchangeName
    side: OrderSide
    order_type: OrderType
    price: Decimal | None
    quantity: Decimal
    status: OrderStatus


class OrderFilledEvent(DomainEvent):
    """Event raised when an order is filled (partially or completely)."""

    order_id: str
    symbol: Symbol
    exchange: ExchangeName
    side: OrderSide
    fill_price: Decimal
    fill_quantity: Decimal
    commission: Decimal
    remaining_quantity: Decimal
    is_partial: bool = False


class OrderCancelledEvent(DomainEvent):
    """Event raised when an order is cancelled."""

    order_id: str
    symbol: Symbol
    exchange: ExchangeName
    cancellation_reason: str
    was_partially_filled: bool = False
    filled_quantity: Decimal | None = None
