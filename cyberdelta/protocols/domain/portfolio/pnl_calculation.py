"""PnL calculation protocols for portfolio operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:
    from cyberdelta.models.portfolio.pnl_report import PnLReport, PositionPnLDetail

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.symbols.models import Symbol


class PnLCalculatorProtocol(Protocol):
    """Protocol for PnL calculation operations.

    Defines the interface for calculating profit and loss metrics
    including comprehensive reports and position-specific calculations.
    """

    async def calculate_pnl(self) -> PnLReport:
        """Calculate comprehensive PnL report.

        Returns:
            Typed PnL report with calculations and metrics
        """
        ...

    async def calculate_position_pnl(
        self, symbol: Symbol, exchange: ExchangeName
    ) -> PositionPnLDetail | None:
        """Calculate PnL for a specific position.

        Args:
            symbol: Symbol of the position
            exchange: Exchange where position is held

        Returns:
            Typed position PnL details or None if position not found
        """
        ...
