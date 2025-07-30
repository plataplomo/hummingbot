"""Focused performance analytics service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState
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
        if portfolio_state.total_capital > 0:
            total_return = total_pnl / portfolio_state.total_capital
        
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
        
        # Calculate P&L attribution by exchange
        for exchange, positions in portfolio_state.positions.items():
            exchange_pnl = Decimal(0)
            for position in positions:
                position_pnl = Decimal(0)
                if position.realized_pnl:
                    position_pnl += position.realized_pnl
                if position.unrealized_pnl:
                    position_pnl += position.unrealized_pnl
                
                exchange_pnl += position_pnl
                
                # Add to symbol attribution
                if position.symbol not in by_symbol:
                    by_symbol[position.symbol] = Decimal(0)
                by_symbol[position.symbol] += position_pnl
            
            by_exchange[exchange] = exchange_pnl
        
        return {
            "by_exchange": by_exchange,
            "by_symbol": by_symbol,
            "by_asset_class": {"crypto": sum(by_symbol.values())},
        }

    async def _calculate_total_capital(self, state: PortfolioState) -> Decimal:
        """Calculate total portfolio capital."""
        return state.total_capital

    async def _calculate_pnl(self, state: PortfolioState) -> tuple[Decimal, Decimal]:
        """Calculate realized and unrealized P&L."""
        realized_pnl = Decimal(0)
        unrealized_pnl = Decimal(0)
        
        # Sum P&L from all positions
        for exchange, positions in state.positions.items():
            for position in positions:
                if position.realized_pnl:
                    realized_pnl += position.realized_pnl
                if position.unrealized_pnl:
                    unrealized_pnl += position.unrealized_pnl
        
        return realized_pnl, unrealized_pnl

    async def _calculate_drawdown(self, current_capital: Decimal) -> Decimal | None:
        """Calculate current drawdown from high watermark."""
        # Would need historical high watermark data
        # For now, return None indicating no drawdown calculation available
        return None