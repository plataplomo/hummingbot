"""Position management protocols for portfolio operations."""

from __future__ import annotations

from typing import Protocol

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import DerivativePosition
from cyberdelta.symbols.models import Symbol


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

    async def set_position(
        self, symbol: Symbol, exchange: ExchangeName, position: DerivativePosition
    ) -> None:
        """Set a position to a specific state.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            position: Position to set
        """
        ...

    async def remove_position(self, symbol: Symbol, exchange: ExchangeName) -> None:
        """Remove a position completely.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
        """
        ...
