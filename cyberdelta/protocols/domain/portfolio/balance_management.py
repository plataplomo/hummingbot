"""Balance management protocols for portfolio operations."""

from __future__ import annotations

from typing import Protocol

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import SpotBalance


class BalanceManagerProtocol(Protocol):
    """Protocol for balance management operations.

    Defines the interface for managing spot balances including
    retrieval, updates, and reconciliation across exchanges.
    """

    async def get_exchange_balances(self, exchange: ExchangeName) -> dict[str, SpotBalance]:
        """Get all balances for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            Dictionary of asset -> SpotBalance for the exchange
        """
        ...

    async def update_balance_directly(
        self, asset: Symbol, exchange: ExchangeName, new_balance: SpotBalance
    ) -> None:
        """Update balance directly (for reconciliation).

        Args:
            asset: Asset symbol
            exchange: Exchange name
            new_balance: New balance to set
        """
        ...
