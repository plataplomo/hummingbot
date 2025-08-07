"""Domain event system for CyberDeltaEngine.

This module provides a generic event pattern that reduces the need for
30+ individual event classes while maintaining type safety and clarity.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pydantic import Field

from cyberdelta.enums import ExchangeName
from cyberdelta.enums.events import EntityType, EventType
from cyberdelta.models.base_validators import StandardModel
from cyberdelta.symbols.models import Symbol


class DomainEvent(StandardModel):
    """Generic domain event that can represent any business event.

    This approach reduces code duplication while maintaining
    type safety and clear event semantics. The payload contains
    event-specific data that would have been individual fields
    in separate event classes.

    Attributes:
        event_id: Unique identifier for this event
        event_type: Type of event (from EventType enum)
        entity_type: Type of entity this event relates to
        entity_id: ID of the specific entity (order_id, position_id, etc.)
        timestamp: UTC timestamp when event occurred
        exchange: Exchange where event occurred (optional)
        symbol: Trading symbol involved (optional)
        payload: Event-specific data as a dictionary
        metadata: Additional metadata (source, version, etc.)
    """

    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique identifier for this event",
    )
    event_type: EventType = Field(description="Type of domain event")
    entity_type: EntityType = Field(description="Type of entity this event relates to")
    entity_id: str = Field(description="ID of the entity (order_id, position_id, etc.)")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when event occurred",
    )
    exchange: ExchangeName | None = Field(default=None, description="Exchange if applicable")
    symbol: Symbol | None = Field(default=None, description="Trading symbol if applicable")
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Event-specific data",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata (source, version, etc.)",
    )

    def get_decimal(self, key: str, default: Decimal | None = None) -> Decimal | None:
        """Get a Decimal value from payload safely.

        Args:
            key: Key to retrieve from payload
            default: Default value if key not found

        Returns:
            Decimal value or default
        """
        value = self.payload.get(key, default)
        if value is None:
            return default
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get a boolean value from payload safely.

        Args:
            key: Key to retrieve from payload
            default: Default value if key not found

        Returns:
            Boolean value or default
        """
        return bool(self.payload.get(key, default))

    def get_str(self, key: str, default: str | None = None) -> str | None:
        """Get a string value from payload safely.

        Args:
            key: Key to retrieve from payload
            default: Default value if key not found

        Returns:
            String value or default
        """
        value = self.payload.get(key, default)
        if value is None:
            return default
        return str(value)

    @classmethod
    def create_order_filled(
        cls,
        order_id: str,
        exchange: ExchangeName,
        symbol: Symbol,
        fill_price: Decimal,
        fill_quantity: Decimal,
        remaining_quantity: Decimal,
        commission: Decimal | None = None,
        is_partial: bool = False,
    ) -> DomainEvent:
        """Factory method to create an order filled event.

        Args:
            order_id: ID of the order
            exchange: Exchange where order was filled
            symbol: Trading symbol
            fill_price: Price at which order was filled
            fill_quantity: Quantity filled
            remaining_quantity: Quantity remaining unfilled
            commission: Commission charged (optional)
            is_partial: Whether this is a partial fill

        Returns:
            DomainEvent configured as an order filled event
        """
        event_type = EventType.ORDER_PARTIALLY_FILLED if is_partial else EventType.ORDER_FILLED
        return cls(
            event_type=event_type,
            entity_type=EntityType.ORDER,
            entity_id=order_id,
            exchange=exchange,
            symbol=symbol,
            payload={
                "fill_price": str(fill_price),
                "fill_quantity": str(fill_quantity),
                "remaining_quantity": str(remaining_quantity),
                "commission": str(commission) if commission else None,
                "is_partial": is_partial,
            },
        )

    @classmethod
    def create_position_updated(
        cls,
        position_id: str,
        exchange: ExchangeName,
        symbol: Symbol,
        new_size: Decimal,
        average_price: Decimal,
        realized_pnl: Decimal | None = None,
    ) -> DomainEvent:
        """Factory method to create a position updated event.

        Args:
            position_id: ID of the position
            exchange: Exchange where position exists
            symbol: Trading symbol
            new_size: New position size
            average_price: New average entry price
            realized_pnl: Realized PnL if position was reduced

        Returns:
            DomainEvent configured as a position updated event
        """
        return cls(
            event_type=EventType.POSITION_UPDATED,
            entity_type=EntityType.POSITION,
            entity_id=position_id,
            exchange=exchange,
            symbol=symbol,
            payload={
                "new_size": str(new_size),
                "average_price": str(average_price),
                "realized_pnl": str(realized_pnl) if realized_pnl else None,
            },
        )

    @classmethod
    def create_risk_limit_breached(
        cls,
        limit_type: str,
        current_value: Decimal,
        limit_value: Decimal,
        exchange: ExchangeName | None = None,
        symbol: Symbol | None = None,
    ) -> DomainEvent:
        """Factory method to create a risk limit breached event.

        Args:
            limit_type: Type of limit breached (e.g., "position_size", "drawdown")
            current_value: Current value that breached the limit
            limit_value: The limit that was breached
            exchange: Exchange if limit is exchange-specific
            symbol: Symbol if limit is symbol-specific

        Returns:
            DomainEvent configured as a risk limit breached event
        """
        return cls(
            event_type=EventType.RISK_LIMIT_BREACHED,
            entity_type=EntityType.RISK_LIMIT,
            entity_id=limit_type,
            exchange=exchange,
            symbol=symbol,
            payload={
                "limit_type": limit_type,
                "current_value": str(current_value),
                "limit_value": str(limit_value),
                "breach_percentage": str((current_value / limit_value - 1) * 100),
            },
        )
