"""State management protocols for portfolio operations."""

from __future__ import annotations

from typing import Protocol

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import DerivativePosition, SpotBalance, Trade
from cyberdelta.models.portfolio.state import PortfolioState


class PortfolioStateManagerProtocol(Protocol):
    """Protocol for portfolio state management operations.

    Defines the interface for managing portfolio state including
    initialization, access, persistence, and trade updates.
    """

    async def initialize_state(self) -> PortfolioState:
        """Initialize portfolio state.

        Returns:
            Initialized portfolio state
        """
        ...

    async def get_state(self) -> PortfolioState:
        """Get current portfolio state.

        Returns:
            Current portfolio state
        """
        ...

    async def save_state(self) -> None:
        """Save current portfolio state."""
        ...

    async def get_balance(self, asset: Symbol, exchange: ExchangeName) -> SpotBalance | None:
        """Get balance for specific asset on exchange.

        Args:
            asset: Asset symbol
            exchange: Exchange name

        Returns:
            SpotBalance if found, None otherwise
        """
        ...

    async def get_position(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> DerivativePosition | None:
        """Get position for specific symbol on exchange.

        Args:
            symbol: Trading symbol
            exchange: Exchange name

        Returns:
            DerivativePosition if found, None otherwise
        """
        ...

    async def update_from_trade(self, trade: Trade) -> None:
        """Update portfolio state from trade execution.

        Args:
            trade: Trade execution details
        """
        ...
