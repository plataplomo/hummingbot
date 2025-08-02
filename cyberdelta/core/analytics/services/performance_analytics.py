"""Focused performance analytics service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData as PortfolioState
from cyberdelta.core.portfolio.portfolio_types.calculations import (
    PerformanceInput,
    PerformanceResult
)
from cyberdelta.core.portfolio.portfolio_types.protocols import CalculatorProtocol


class PerformanceAnalyticsService(BaseModel):
    """Calculates portfolio performance metrics only."""

    base_currency: str = Field(default="USDC", description="Base currency for calculations")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def calculate_performance(
        self,
        portfolio_state: PortfolioState
    ) -> PerformanceResult:
        """Calculate performance metrics from portfolio state."""

        # Calculate total capital
        total_capital = await self._calculate_total_capital(portfolio_state)

        # Calculate P&L
        realized_pnl, unrealized_pnl = await self._calculate_pnl(portfolio_state)

        # Calculate drawdown if high watermark exists
        drawdown = await self._calculate_drawdown(total_capital)

        return PerformanceResult(
            total_capital=total_capital,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            drawdown=drawdown,
        )

    async def calculate_returns(
        self,
        portfolio_state: PortfolioState,
        period_days: int = 30
    ) -> dict[str, Decimal]:
        """Calculate portfolio returns over specified period."""
        # Calculate returns from current P&L
        realized_pnl, unrealized_pnl = await self._calculate_pnl(portfolio_state)
        total_pnl = realized_pnl + unrealized_pnl
        
        total_return = Decimal(0)
        if portfolio_state.total_account_value > 0:
            total_return = total_pnl / portfolio_state.total_account_value
        
        # Estimate annualized return (simplified)
        annualized_return = total_return * Decimal(365) / Decimal(period_days)
        
        return {
            "total_return": total_return,
            "annualized_return": annualized_return,
            "period_return": total_return,  # Same as total for current snapshot
        }

    async def calculate_attribution(
        self,
        portfolio_state: PortfolioState
    ) -> dict[str, Any]:
        """Calculate performance attribution by exchange/symbol."""
        by_exchange = {}
        by_symbol = {}
        
        # Calculate P&L attribution by exchange using exchange summaries
        for exchange_id, summary in portfolio_state.exchange_summaries.items():
            exchange_pnl = summary.realized_pnl + summary.unrealized_pnl
            by_exchange[exchange_id] = exchange_pnl
        
        return {
            "by_exchange": by_exchange,
            "by_symbol": by_symbol,
            "by_asset_class": {"crypto": sum(by_symbol.values())},
        }

    async def _calculate_total_capital(self, state: PortfolioState) -> Decimal:
        """Calculate total portfolio capital."""
        return state.total_account_value

    async def _calculate_pnl(self, state: PortfolioState) -> tuple[Decimal, Decimal]:
        """Calculate realized and unrealized P&L."""
        # Use aggregated P&L from portfolio state
        return state.total_realized_pnl, state.total_unrealized_pnl

    async def _calculate_drawdown(self, current_capital: Decimal) -> Decimal | None:
        """Calculate current drawdown from high watermark."""
        # Would need historical high watermark data
        # For now, return None indicating no drawdown calculation available
        return None