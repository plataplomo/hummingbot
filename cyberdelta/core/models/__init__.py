"""
CyberDeltaEngine models package: aggregates all core trading, API, and enum models for
convenient import.

- Enums (OrderSide, OrderType, etc.) are only re-exported from .enums to avoid type conflicts.
- All models are imported from .trading and .api as needed.
- Star imports are avoided for clarity and type safety.
"""

from cyberdelta.validation.funding_data import ArbitrageOpportunity

from .api import OrderUpdateEvent, PlaceOrderRequest, TradeFillEvent
from .enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    SignalType,
    TimeInForce,
)
from .market import Order, Trade
from .portfolio import Position

__all__ = [
    # Explicitly re-export enums
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "SignalType",
    "TimeInForce",
    # Explicitly re-export key models
    "Order",
    "Trade",
    "Position",
    "ArbitrageOpportunity",
    # API models
    "PlaceOrderRequest",
    "OrderUpdateEvent",
    "TradeFillEvent",
]
