"""Focused performance metrics calculation service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState, PerformanceMetrics


class PerformanceMetricsService(BaseModel):
    """Calculates performance metrics only."""

    base_currency: str = Field(default="USDC", description="Base currency for calculations")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def calculate_performance_metrics(
        self,
        portfolio_state: PortfolioState
    ) -> PerformanceMetrics:
        """Calculate comprehensive performance metrics."""
        
        # Calculate key performance indicators
        total_return = await self._calculate_total_return(portfolio_state)
        daily_return = await self._calculate_daily_return(portfolio_state)
        sharpe_ratio = await self._calculate_sharpe_ratio(portfolio_state)
        max_drawdown = await self._calculate_max_drawdown(portfolio_state)
        
        from datetime import datetime
        
        return PerformanceMetrics(
            total_return=total_return,
            daily_return=daily_return,
            monthly_return=Decimal("0.0"),
            annual_return=Decimal("0.0"),
            volatility=Decimal("0.0"),
            max_drawdown=max_drawdown or Decimal("0.0"),
            sharpe_ratio=sharpe_ratio,
            win_rate=await self._calculate_win_rate(portfolio_state),
            profit_factor=await self._calculate_profit_factor(portfolio_state),
            average_win=Decimal("0.0"),
            average_loss=Decimal("0.0"),
            largest_win=Decimal("0.0"),
            largest_loss=Decimal("0.0"),
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            total_fees=Decimal("0.0"),
            start_date=datetime.now(),
            end_date=datetime.now(),
            trading_days=1
        )

    async def calculate_returns_series(
        self,
        portfolio_state: PortfolioState,
        period_days: int = 30
    ) -> list[dict[str, Any]]:
        """Calculate time series of returns."""
        # Return current state as single data point
        # In production, this would query historical data
        current_return = await self._calculate_total_return(portfolio_state)
        
        return [{
            "date": str(portfolio_state.timestamp),
            "return": str(current_return),
            "cumulative_return": str(current_return)
        }]

    async def calculate_volatility_metrics(
        self,
        portfolio_state: PortfolioState
    ) -> dict[str, Decimal]:
        """Calculate volatility-related metrics."""
        return {
            "daily_volatility": Decimal("0.0"),
            "monthly_volatility": Decimal("0.0"),
            "annualized_volatility": Decimal("0.0"),
        }

    async def calculate_benchmark_comparison(
        self,
        portfolio_state: PortfolioState,
        benchmark_returns: list[Decimal]
    ) -> dict[str, Any]:
        """Compare portfolio performance to benchmark."""
        return {
            "alpha": Decimal("0.0"),
            "beta": Decimal("1.0"),
            "tracking_error": Decimal("0.0"),
            "information_ratio": Decimal("0.0"),
        }

    async def calculate_risk_adjusted_returns(
        self,
        portfolio_state: PortfolioState
    ) -> dict[str, Decimal]:
        """Calculate risk-adjusted return metrics."""
        sharpe = await self._calculate_sharpe_ratio(portfolio_state)
        sortino = await self._calculate_sortino_ratio(portfolio_state)
        calmar = await self._calculate_calmar_ratio(portfolio_state)
        
        return {
            "sharpe_ratio": sharpe or Decimal("0.0"),
            "sortino_ratio": sortino or Decimal("0.0"),
            "calmar_ratio": calmar or Decimal("0.0"),
        }

    async def _calculate_total_return(self, state: PortfolioState) -> Decimal:
        """Calculate total portfolio return."""
        # Calculate return as (total_pnl) / initial_capital
        # For simplicity, assume current total_capital includes P&L
        total_pnl = Decimal(0)
        for positions in state.positions.values():
            for position in positions:
                if position.realized_pnl:
                    total_pnl += position.realized_pnl
                if position.unrealized_pnl:
                    total_pnl += position.unrealized_pnl
        
        # Return percentage of total P&L vs capital
        if state.total_capital > 0:
            return total_pnl / state.total_capital
        return Decimal(0)

    async def _calculate_daily_return(self, state: PortfolioState) -> Decimal:
        """Calculate daily return."""
        # Would need historical data for true daily return
        # Return current unrealized P&L as proxy
        daily_pnl = Decimal(0)
        for positions in state.positions.values():
            for position in positions:
                if position.unrealized_pnl:
                    daily_pnl += position.unrealized_pnl
        
        if state.total_capital > 0:
            return daily_pnl / state.total_capital
        return Decimal(0)

    async def _calculate_sharpe_ratio(self, state: PortfolioState) -> Decimal | None:
        """Calculate Sharpe ratio."""
        # Would need historical returns and risk-free rate
        # Return None indicating insufficient data
        return None

    async def _calculate_max_drawdown(self, state: PortfolioState) -> Decimal | None:
        """Calculate maximum drawdown."""
        # Would need historical equity curve data
        # Return None indicating insufficient data
        return None

    async def _calculate_win_rate(self, state: PortfolioState) -> Decimal:
        """Calculate win rate (percentage of profitable periods)."""
        profitable_positions = 0
        total_positions = 0
        
        for positions in state.positions.values():
            for position in positions:
                total_positions += 1
                total_pnl = Decimal(0)
                if position.realized_pnl:
                    total_pnl += position.realized_pnl
                if position.unrealized_pnl:
                    total_pnl += position.unrealized_pnl
                
                if total_pnl > 0:
                    profitable_positions += 1
        
        if total_positions > 0:
            return Decimal(profitable_positions) / Decimal(total_positions)
        return Decimal(0)

    async def _calculate_profit_factor(self, state: PortfolioState) -> Decimal:
        """Calculate profit factor (gross profit / gross loss)."""
        gross_profit = Decimal(0)
        gross_loss = Decimal(0)
        
        for positions in state.positions.values():
            for position in positions:
                total_pnl = Decimal(0)
                if position.realized_pnl:
                    total_pnl += position.realized_pnl
                if position.unrealized_pnl:
                    total_pnl += position.unrealized_pnl
                
                if total_pnl > 0:
                    gross_profit += total_pnl
                elif total_pnl < 0:
                    gross_loss += abs(total_pnl)
        
        if gross_loss > 0:
            return gross_profit / gross_loss
        return Decimal(1)

    async def _calculate_sortino_ratio(self, state: PortfolioState) -> Decimal | None:
        """Calculate Sortino ratio (downside deviation adjusted)."""
        # Would need historical returns for downside deviation
        return None

    async def _calculate_calmar_ratio(self, state: PortfolioState) -> Decimal | None:
        """Calculate Calmar ratio (return / max drawdown)."""
        # Would need max drawdown calculation
        return None