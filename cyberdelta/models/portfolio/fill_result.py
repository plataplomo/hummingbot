"""Result models for fill application to positions.

These models provide type-safe return values for position updates
following CODING_STANDARDS.md - no None returns, explicit data.
"""

from decimal import Decimal

from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class FillApplicationResult:
    """Result of applying a fill to a position.

    Provides explicit, type-safe data about the fill application
    without using None values or implicit behaviors.
    """

    realized_pnl: Decimal
    """Realized PnL from the fill (Decimal(0) if position opened/increased)."""

    new_average_price: Decimal
    """New average price after applying the fill."""

    was_reducing_position: bool
    """Whether the fill reduced the position size."""

    position_closed: bool
    """Whether the position was fully closed by this fill."""


@dataclass(frozen=True)
class PositionChangeResult:
    """Result of calculating position change from fill.

    Used for tracking how a fill changes a position's quantity
    and any realized PnL from the change.
    """

    new_quantity: Decimal
    """New position quantity after applying the fill."""

    realized_pnl: Decimal
    """Realized PnL if position was reduced (Decimal(0) otherwise)."""

    was_reducing: bool
    """Whether the fill reduced the position."""
