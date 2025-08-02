"""Focused P&L metrics calculation service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData as PortfolioState
from cyberdelta.core.portfolio.portfolio_types.calculations import PnLBreakdown
from cyberdelta.core.symbols import Symbol


class PnLMetricsService(BaseModel):
    """Calculates P&L metrics only."""

    base_currency: str = Field(default="USDC", description="Base currency for calculations")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def calculate_pnl_breakdown(
        self,
        portfolio_state: PortfolioState
    ) -> PnLBreakdown:
        """Calculate comprehensive P&L breakdown."""
        
        # Calculate realized P&L
        realized_pnl = await self._calculate_realized_pnl(portfolio_state)
        
        # Calculate unrealized P&L
        unrealized_pnl = await self._calculate_unrealized_pnl(portfolio_state)
        
        # Calculate breakdowns by exchange and symbol
        by_exchange = await self._calculate_pnl_by_exchange(portfolio_state)
        by_symbol = await self._calculate_pnl_by_symbol(portfolio_state)

        return PnLBreakdown(
            realized=realized_pnl,
            unrealized=unrealized_pnl,
            total=realized_pnl + unrealized_pnl,
            by_exchange=by_exchange,
            by_symbol=by_symbol
        )

    async def calculate_daily_pnl(
        self,
        portfolio_state: PortfolioState
    ) -> Decimal:
        """Calculate daily P&L change."""
        # Use total unrealized P&L as proxy for daily change
        return portfolio_state.total_unrealized_pnl

    async def calculate_session_pnl(
        self,
        portfolio_state: PortfolioState
    ) -> dict[str, Decimal]:
        """Calculate P&L for current trading session."""
        session_realized = await self._calculate_realized_pnl(portfolio_state)
        session_unrealized = await self._calculate_unrealized_pnl(portfolio_state)
        
        return {
            "session_realized": session_realized,
            "session_unrealized": session_unrealized,
            "session_total": session_realized + session_unrealized,
        }

    async def get_pnl_history(
        self,
        portfolio_state: PortfolioState,
        days: int = 30
    ) -> list[dict[str, Any]]:
        """Get historical P&L data."""
        # Would need historical data storage
        # Return current state as single data point
        current_pnl = await self.calculate_pnl_breakdown(portfolio_state)
        return [{
            "date": str(portfolio_state.updated_at),
            "realized_pnl": str(current_pnl.realized),
            "unrealized_pnl": str(current_pnl.unrealized),
            "total_pnl": str(current_pnl.total),
        }]

    async def _calculate_realized_pnl(self, state: PortfolioState) -> Decimal:
        """Calculate total realized P&L."""
        return state.total_realized_pnl

    async def _calculate_unrealized_pnl(self, state: PortfolioState) -> Decimal:
        """Calculate total unrealized P&L."""
        return state.total_unrealized_pnl

    async def _calculate_pnl_by_exchange(self, state: PortfolioState) -> dict[str, Decimal]:
        """Calculate P&L breakdown by exchange."""
        breakdown = {}
        for exchange_id, summary in state.exchange_summaries.items():
            exchange_pnl = summary.realized_pnl + summary.unrealized_pnl
            breakdown[exchange_id] = exchange_pnl
        return breakdown

    async def _calculate_pnl_by_symbol(self, state: PortfolioState) -> dict[Symbol, Decimal]:
        """Calculate P&L breakdown by symbol."""
        # Without position-level data, return empty breakdown
        # In production, this would query position data separately
        return {}