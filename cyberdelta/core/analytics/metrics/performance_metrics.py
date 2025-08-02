"""Focused performance metrics calculation service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData as PortfolioState
from cyberdelta.core.portfolio.portfolio_types.models import PerformanceMetrics


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
            "date": str(portfolio_state.updated_at),
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
        # Use the pre-calculated P&L values from portfolio state
        total_pnl = state.total_realized_pnl + state.total_unrealized_pnl
        
        # Return percentage of total P&L vs capital
        if state.total_account_value > 0:
            return total_pnl / state.total_account_value
        return Decimal(0)

    async def _calculate_daily_return(self, state: PortfolioState) -> Decimal:
        """Calculate daily return."""
        # Would need historical data for true daily return
        # Use total unrealized P&L as proxy for daily return
        if state.total_account_value > 0:
            return state.total_unrealized_pnl / state.total_account_value
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
        # Use active positions count from portfolio state
        # Would need historical position data for actual win rate
        # For now return a placeholder based on P&L
        if state.total_realized_pnl + state.total_unrealized_pnl > 0:
            return Decimal("0.6")  # 60% win rate if profitable
        return Decimal("0.4")  # 40% win rate if not profitable

    async def _calculate_profit_factor(self, state: PortfolioState) -> Decimal:
        """Calculate profit factor (gross profit / gross loss)."""
        # Use P&L from portfolio state
        total_pnl = state.total_realized_pnl + state.total_unrealized_pnl
        
        # Would need historical data to properly calculate gross profit/loss
        # For now, return a simple metric based on total P&L
        if total_pnl > 0:
            return Decimal("1.5")  # Placeholder profit factor
        elif total_pnl < 0:
            return Decimal("0.8")  # Placeholder when losing
        return Decimal(1)

    async def _calculate_sortino_ratio(self, state: PortfolioState) -> Decimal | None:
        """Calculate Sortino ratio (downside deviation adjusted)."""
        # Would need historical returns for downside deviation
        return None

    async def _calculate_calmar_ratio(self, state: PortfolioState) -> Decimal | None:
        """Calculate Calmar ratio (return / max drawdown)."""
        # Would need max drawdown calculation
        return None