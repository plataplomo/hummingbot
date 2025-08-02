"""Focused P&L metrics calculation service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState
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
        # Use unrealized P&L as proxy for daily change
        daily_pnl = Decimal(0)
        for positions in portfolio_state.positions.values():
            for position in positions:
                if position.unrealized_pnl:
                    daily_pnl += position.unrealized_pnl
        return daily_pnl

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
            "date": str(portfolio_state.timestamp),
            "realized_pnl": str(current_pnl.realized),
            "unrealized_pnl": str(current_pnl.unrealized),
            "total_pnl": str(current_pnl.total),
        }]

    async def _calculate_realized_pnl(self, state: PortfolioState) -> Decimal:
        """Calculate total realized P&L."""
        total_realized = Decimal(0)
        for exchange, positions in state.positions.items():
            for position in positions:
                if position.realized_pnl:
                    total_realized += position.realized_pnl
        return total_realized

    async def _calculate_unrealized_pnl(self, state: PortfolioState) -> Decimal:
        """Calculate total unrealized P&L."""
        total_unrealized = Decimal(0)
        for exchange, positions in state.positions.items():
            for position in positions:
                if position.unrealized_pnl:
                    total_unrealized += position.unrealized_pnl
        return total_unrealized

    async def _calculate_pnl_by_exchange(self, state: PortfolioState) -> dict[str, Decimal]:
        """Calculate P&L breakdown by exchange."""
        breakdown = {}
        for exchange, positions in state.positions.items():
            exchange_pnl = Decimal(0)
            for position in positions:
                if position.realized_pnl:
                    exchange_pnl += position.realized_pnl
                if position.unrealized_pnl:
                    exchange_pnl += position.unrealized_pnl
            breakdown[exchange] = exchange_pnl
        return breakdown

    async def _calculate_pnl_by_symbol(self, state: PortfolioState) -> dict[Symbol, Decimal]:
        """Calculate P&L breakdown by symbol."""
        breakdown = {}
        for exchange, positions in state.positions.items():
            for position in positions:
                symbol_key = position.symbol
                if symbol_key not in breakdown:
                    breakdown[symbol_key] = Decimal(0)
                
                # Add position P&L to symbol total
                if position.realized_pnl:
                    breakdown[symbol_key] += position.realized_pnl
                if position.unrealized_pnl:
                    breakdown[symbol_key] += position.unrealized_pnl
        return breakdown