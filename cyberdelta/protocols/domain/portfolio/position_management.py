"""Position management protocols for portfolio operations."""

from __future__ import annotations

from typing import Protocol

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import DerivativePosition


class PositionManagerProtocol(Protocol):
    """Protocol for position management operations.

    Defines the interface for managing derivative positions including
    retrieval, updates, and reconciliation across exchanges.
    """

    async def get_exchange_positions(self, exchange: ExchangeName) -> dict[str, DerivativePosition]:
        """Get all positions for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            Dictionary of symbol -> DerivativePosition for the exchange
        """
        ...

    async def update_position_directly(
        self, symbol: Symbol, exchange: ExchangeName, new_position: DerivativePosition | None
    ) -> None:
        """Update position directly (for reconciliation).

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            new_position: New position to set, None to remove
        """
        ...
