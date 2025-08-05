"""PnL calculation protocols for portfolio operations."""

from __future__ import annotations

from typing import Any, Protocol

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName


class PnLCalculatorProtocol(Protocol):
    """Protocol for PnL calculation operations.

    Defines the interface for calculating profit and loss metrics
    including comprehensive reports and position-specific calculations.
    """

    async def calculate_pnl(self) -> dict[str, Any]:
        """Calculate comprehensive PnL report.

        Returns:
            Dictionary containing PnL calculations and metrics
        """
        ...

    async def calculate_position_pnl(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> dict[str, Any] | None:
        """Calculate PnL for a specific position.

        Args:
            symbol: Symbol of the position
            exchange: Exchange where position is held

        Returns:
            Dictionary with position PnL details or None if position not found
        """
        ...
