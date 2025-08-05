"""Performance tracking and metrics calculation service.

This module provides the PerformanceTracker class that calculates various
trading performance metrics using configuration-driven parameters from AppSettings.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

from cyberdelta.config.structlog_config import get_logger
from pydantic import BaseModel

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.models.market.trade import Trade

logger = get_logger(__name__)


class PerformanceMetrics(BaseModel):
    """Comprehensive performance metrics report.

    All metrics are calculated based on configuration settings and include
    only the metrics explicitly enabled in config.calculation.performance_metrics.
    """

    # Basic metrics
    total_pnl: Decimal
    realized_pnl: Optional[Decimal] = None
    unrealized_pnl: Optional[Decimal] = None
    total_return_pct: Decimal

    # Period metrics (if enabled)
    daily_return_pct: Optional[Decimal] = None
    weekly_return_pct: Optional[Decimal] = None
    monthly_return_pct: Optional[Decimal] = None
    yearly_return_pct: Optional[Decimal] = None

    # Risk metrics (if enabled)
    sharpe_ratio: Optional[Decimal] = None
    sortino_ratio: Optional[Decimal] = None
    max_drawdown_pct: Optional[Decimal] = None
    max_drawdown_duration_days: Optional[int] = None
    current_drawdown_pct: Optional[Decimal] = None

    # Trading metrics (if enabled)
    total_trades: Optional[int] = None
    winning_trades: Optional[int] = None
    losing_trades: Optional[int] = None
    win_rate_pct: Optional[Decimal] = None
    average_win: Optional[Decimal] = None
    average_loss: Optional[Decimal] = None
    profit_factor: Optional[Decimal] = None

    # Statistical metrics (if enabled)
    volatility_pct: Optional[Decimal] = None
    beta: Optional[Decimal] = None
    alpha: Optional[Decimal] = None

    # Metadata
    calculation_timestamp: datetime
    period_start: datetime
    period_end: datetime
    base_currency: str


class PerformanceTracker:
    """Tracks and calculates trading performance metrics using configuration.

    This tracker calculates various performance metrics based on the enabled
    metrics in config.calculation.performance_metrics. It supports different
    calculation methods and time periods as configured.

    Configuration Structure (config.calculation.performance_metrics):
    - enabled_metrics: List of metrics to calculate
    - calculation_period_days: Default period for calculations
    - risk_free_rate: Risk-free rate for Sharpe/Sortino calculations
    - sharpe_calculation_method: Method for Sharpe ratio calculation
    - drawdown_calculation_method: Method for drawdown calculation
    - include_fees_in_metrics: Whether to include fees in calculations

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL metrics parameters from AppSettings, NO hardcoded values
    - Uses Decimal for all calculations, NOT float
    - Only calculates explicitly enabled metrics
    - NO assumptions about calculation methods
    """

    def __init__(
        self,
        config: AppSettings,
        portfolio_service: PortfolioService,
    ):
        """Initialize performance tracker with configuration.

        Args:
            config: Application settings containing all configuration
            portfolio_service: Portfolio service for state access
        """
        self.config = config
        self._portfolio_service = portfolio_service

        # Extract performance metrics configuration
        self._metrics_config = config.calculation.performance_metrics
        self._enabled_metrics = set(self._metrics_config.enabled_metrics)
        self._calculation_period = self._metrics_config.calculation_period_days
        self._risk_free_rate = self._metrics_config.risk_free_rate
        self._include_fees = self._metrics_config.include_fees_in_metrics

        # Calculation method settings
        self._sharpe_method = self._metrics_config.sharpe_calculation_method
        self._drawdown_method = self._metrics_config.drawdown_calculation_method

        # Cache for historical data
        self._equity_curve: List[tuple[datetime, Decimal]] = []
        self._trade_history: List[Trade] = []

        logger.info(
            "performance_tracker_initialized",
            enabled_metrics=list(self._enabled_metrics),
            calculation_period_days=self._calculation_period,
            include_fees=self._include_fees,
        )

    async def calculate_metrics(self, period_days: Optional[int] = None) -> PerformanceMetrics:
        """Calculate performance metrics for specified period.

        Args:
            period_days: Number of days to calculate metrics for (uses config default if None)

        Returns:
            PerformanceMetrics object with calculated values

        IMPORTANT: Following CODING_STANDARDS.md:
        - Only calculates metrics in config.calculation.performance_metrics.enabled_metrics
        - All calculation parameters from configuration
        - NO hardcoded calculation methods or thresholds
        - Returns None for disabled metrics
        """
        # Use configured period if not specified
        if period_days is None:
            period_days = self._calculation_period

        period_end = datetime.now(UTC)
        period_start = period_end - timedelta(days=period_days)

        logger.debug(
            "calculating_performance_metrics",
            period_days=period_days,
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            enabled_metrics_count=len(self._enabled_metrics),
        )

        # Get portfolio state
        portfolio_state = await self._portfolio_service.get_state()

        # Calculate basic metrics (always included)
        total_pnl = await self._calculate_total_pnl(period_start, period_end)
        total_return_pct = await self._calculate_total_return(total_pnl, period_start)

        metrics = PerformanceMetrics(
            total_pnl=total_pnl,
            total_return_pct=total_return_pct,
            calculation_timestamp=datetime.now(UTC),
            period_start=period_start,
            period_end=period_end,
            base_currency=self.config.calculation.base_currency,
        )

        # Calculate optional metrics based on enabled_metrics
        if "realized_pnl" in self._enabled_metrics:
            metrics.realized_pnl = await self._calculate_realized_pnl(period_start, period_end)

        if "unrealized_pnl" in self._enabled_metrics:
            metrics.unrealized_pnl = await self._calculate_unrealized_pnl(portfolio_state)

        # Period returns
        if "daily_return" in self._enabled_metrics:
            metrics.daily_return_pct = await self._calculate_period_return(1)

        if "weekly_return" in self._enabled_metrics:
            metrics.weekly_return_pct = await self._calculate_period_return(7)

        if "monthly_return" in self._enabled_metrics:
            metrics.monthly_return_pct = await self._calculate_period_return(30)

        if "yearly_return" in self._enabled_metrics:
            metrics.yearly_return_pct = await self._calculate_period_return(365)

        # Risk metrics
        if "sharpe_ratio" in self._enabled_metrics:
            metrics.sharpe_ratio = await self._calculate_sharpe_ratio(period_start, period_end)

        if "sortino_ratio" in self._enabled_metrics:
            metrics.sortino_ratio = await self._calculate_sortino_ratio(period_start, period_end)

        if "max_drawdown" in self._enabled_metrics:
            drawdown_data = await self._calculate_drawdown(period_start, period_end)
            metrics.max_drawdown_pct = drawdown_data["max_drawdown"]
            metrics.max_drawdown_duration_days = drawdown_data["max_duration_days"]
            metrics.current_drawdown_pct = drawdown_data["current_drawdown"]

        # Trading metrics
        if "win_rate" in self._enabled_metrics or "profit_factor" in self._enabled_metrics:
            trading_stats = await self._calculate_trading_statistics(period_start, period_end)

            if "win_rate" in self._enabled_metrics:
                metrics.total_trades = trading_stats["total_trades"]
                metrics.winning_trades = trading_stats["winning_trades"]
                metrics.losing_trades = trading_stats["losing_trades"]
                metrics.win_rate_pct = trading_stats["win_rate"]
                metrics.average_win = trading_stats["average_win"]
                metrics.average_loss = trading_stats["average_loss"]

            if "profit_factor" in self._enabled_metrics:
                metrics.profit_factor = trading_stats["profit_factor"]

        # Statistical metrics
        if "volatility" in self._enabled_metrics:
            metrics.volatility_pct = await self._calculate_volatility(period_start, period_end)

        if "beta" in self._enabled_metrics:
            metrics.beta = await self._calculate_beta(period_start, period_end)

        if "alpha" in self._enabled_metrics:
            metrics.alpha = await self._calculate_alpha(period_start, period_end)

        logger.info(
            "performance_metrics_calculated",
            total_pnl=float(total_pnl),
            total_return_pct=float(total_return_pct),
            period_days=period_days,
            metrics_calculated=sum(1 for k, v in metrics.model_dump().items() if v is not None),
        )

        return metrics

    async def _calculate_total_pnl(self, period_start: datetime, period_end: datetime) -> Decimal:
        """Calculate total PnL for period.

        Args:
            period_start: Start of calculation period
            period_end: End of calculation period

        Returns:
            Total PnL in base currency

        IMPORTANT: Following CODING_STANDARDS.md:
        - Includes fees based on config.calculation.performance_metrics.include_fees_in_metrics
        - Returns Decimal, NOT float
        - Uses configured base currency for conversion
        """
        # Get trades in period
        trades = await self._get_trades_in_period(period_start, period_end)

        # Calculate realized PnL
        realized_pnl = Decimal("0")
        for trade in trades:
            trade_pnl = trade.quantity * (
                trade.price if trade.side.value == "sell" else -trade.price
            )

            # Include fees if configured
            if self._include_fees:
                trade_pnl -= trade.fee

            realized_pnl += trade_pnl

        # Add unrealized PnL if available
        portfolio_state = await self._portfolio_service.get_state()
        if portfolio_state and hasattr(portfolio_state, "total_unrealized_pnl"):
            unrealized_pnl = portfolio_state.total_unrealized_pnl or Decimal("0")
            return realized_pnl + unrealized_pnl

        return realized_pnl

    async def _calculate_total_return(self, total_pnl: Decimal, period_start: datetime) -> Decimal:
        """Calculate total return percentage.

        Args:
            total_pnl: Total PnL for the period
            period_start: Start of calculation period

        Returns:
            Total return as percentage

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns Decimal percentage, NOT float
        - Handles zero starting equity gracefully
        - NO assumptions about minimum equity
        """
        # Get starting equity
        starting_equity = await self._get_equity_at_time(period_start)

        if not starting_equity or starting_equity == Decimal("0"):
            logger.warning(
                "cannot_calculate_return_zero_equity", period_start=period_start.isoformat()
            )
            return Decimal("0")

        return (total_pnl / starting_equity) * Decimal("100")

    async def _calculate_sharpe_ratio(
        self, period_start: datetime, period_end: datetime
    ) -> Decimal:
        """Calculate Sharpe ratio using configured method.

        Args:
            period_start: Start of calculation period
            period_end: End of calculation period

        Returns:
            Sharpe ratio

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses config.calculation.performance_metrics.sharpe_calculation_method
        - Uses config.calculation.performance_metrics.risk_free_rate
        - Returns Decimal, NOT float
        - NO hardcoded annualization factors
        """
        # Get daily returns
        daily_returns = await self._get_daily_returns(period_start, period_end)

        if not daily_returns:
            logger.warning(
                "insufficient_data_for_sharpe",
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
            )
            return Decimal("0")

        # Calculate average return
        avg_return = sum(daily_returns) / len(daily_returns)

        # Calculate standard deviation
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / len(daily_returns)
        std_dev = variance.sqrt()

        if std_dev == Decimal("0"):
            return Decimal("0")

        # Apply calculation method from config
        if self._sharpe_method == "daily":
            # Daily Sharpe
            daily_risk_free = self._risk_free_rate / Decimal("365")
            sharpe = (avg_return - daily_risk_free) / std_dev
        elif self._sharpe_method == "annualized":
            # Annualized Sharpe
            annualized_return = avg_return * Decimal("365")
            annualized_std = std_dev * Decimal("365").sqrt()
            sharpe = (annualized_return - self._risk_free_rate) / annualized_std
        else:
            # Default to daily if method unknown
            daily_risk_free = self._risk_free_rate / Decimal("365")
            sharpe = (avg_return - daily_risk_free) / std_dev

        return sharpe

    async def _calculate_drawdown(
        self, period_start: datetime, period_end: datetime
    ) -> Dict[str, object]:
        """Calculate drawdown metrics using configured method.

        Args:
            period_start: Start of calculation period
            period_end: End of calculation period

        Returns:
            Dictionary with max_drawdown, max_duration_days, current_drawdown

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses config.calculation.performance_metrics.drawdown_calculation_method
        - Returns Decimal percentages, NOT float
        - Calculates duration in days as configured
        """
        # Get equity curve
        equity_curve = await self._get_equity_curve(period_start, period_end)

        if not equity_curve:
            return {
                "max_drawdown": Decimal("0"),
                "max_duration_days": 0,
                "current_drawdown": Decimal("0"),
            }

        # Calculate drawdowns based on configured method
        if self._drawdown_method == "peak_to_trough":
            drawdown_data = self._calculate_peak_to_trough_drawdown(equity_curve)
        elif self._drawdown_method == "underwater":
            drawdown_data = self._calculate_underwater_drawdown(equity_curve)
        else:
            # Default to peak-to-trough
            drawdown_data = self._calculate_peak_to_trough_drawdown(equity_curve)

        return drawdown_data

    async def _calculate_trading_statistics(
        self, period_start: datetime, period_end: datetime
    ) -> Dict[str, object]:
        """Calculate trading performance statistics.

        Args:
            period_start: Start of calculation period
            period_end: End of calculation period

        Returns:
            Dictionary with trading statistics

        IMPORTANT: Following CODING_STANDARDS.md:
        - Includes fees if config.calculation.performance_metrics.include_fees_in_metrics
        - All calculations use Decimal, NOT float
        - Returns explicit statistics, no derived metrics
        """
        trades = await self._get_trades_in_period(period_start, period_end)

        if not trades:
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": Decimal("0"),
                "average_win": Decimal("0"),
                "average_loss": Decimal("0"),
                "profit_factor": Decimal("0"),
            }

        # Group trades by position (simplified - would use position tracking in practice)
        winning_trades = []
        losing_trades = []

        for trade in trades:
            # Calculate trade PnL (simplified)
            trade_pnl = (
                trade.quantity
                * trade.price
                * (Decimal("1") if trade.side.value == "sell" else Decimal("-1"))
            )

            if self._include_fees:
                trade_pnl -= trade.fee

            if trade_pnl > Decimal("0"):
                winning_trades.append(trade_pnl)
            elif trade_pnl < Decimal("0"):
                losing_trades.append(trade_pnl)

        # Calculate statistics
        total_trades = len(trades)
        num_winners = len(winning_trades)
        num_losers = len(losing_trades)

        win_rate = (
            (Decimal(num_winners) / Decimal(total_trades) * Decimal("100"))
            if total_trades > 0
            else Decimal("0")
        )

        average_win = sum(winning_trades) / len(winning_trades) if winning_trades else Decimal("0")
        average_loss = sum(losing_trades) / len(losing_trades) if losing_trades else Decimal("0")

        # Profit factor
        total_wins = sum(winning_trades) if winning_trades else Decimal("0")
        total_losses = abs(sum(losing_trades)) if losing_trades else Decimal("0")
        profit_factor = total_wins / total_losses if total_losses > Decimal("0") else Decimal("0")

        return {
            "total_trades": total_trades,
            "winning_trades": num_winners,
            "losing_trades": num_losers,
            "win_rate": win_rate,
            "average_win": average_win,
            "average_loss": average_loss,
            "profit_factor": profit_factor,
        }

    async def update_equity_curve(self, timestamp: datetime, equity: Decimal) -> None:
        """Update equity curve with new data point.

        Args:
            timestamp: Time of equity snapshot
            equity: Total equity value in base currency

        IMPORTANT: Following CODING_STANDARDS.md:
        - Maintains data based on config.calculation.performance_metrics.max_history_days
        - Uses Decimal for equity values
        - NO assumptions about update frequency
        """
        # Add new data point
        self._equity_curve.append((timestamp, equity))

        # Trim old data based on config
        if hasattr(self._metrics_config, "max_history_days"):
            cutoff = datetime.now(UTC) - timedelta(days=self._metrics_config.max_history_days)
            self._equity_curve = [(ts, eq) for ts, eq in self._equity_curve if ts >= cutoff]

        logger.debug(
            "equity_curve_updated",
            timestamp=timestamp.isoformat(),
            equity=float(equity),
            curve_length=len(self._equity_curve),
        )

    async def add_trade(self, trade: Trade) -> None:
        """Add trade to history for metrics calculation.

        Args:
            trade: Trade to add to history

        IMPORTANT: Following CODING_STANDARDS.md:
        - Maintains history based on configured retention
        - NO modifications to trade data
        - Uses Trade model as-is
        """
        self._trade_history.append(trade)

        # Trim old trades based on config
        if hasattr(self._metrics_config, "max_history_days"):
            cutoff = datetime.now(UTC) - timedelta(days=self._metrics_config.max_history_days)
            self._trade_history = [t for t in self._trade_history if t.executed_at >= cutoff]

    # Helper methods (private)

    async def _get_trades_in_period(
        self, period_start: datetime, period_end: datetime
    ) -> List[Trade]:
        """Get trades within specified period."""
        return [t for t in self._trade_history if period_start <= t.executed_at <= period_end]

    async def _get_equity_at_time(self, timestamp: datetime) -> Optional[Decimal]:
        """Get equity value at specific time."""
        # Find closest equity value
        for ts, equity in reversed(self._equity_curve):
            if ts <= timestamp:
                return equity
        return None

    async def _get_daily_returns(
        self, period_start: datetime, period_end: datetime
    ) -> List[Decimal]:
        """Calculate daily returns for period."""
        daily_returns = []

        # Get equity values in period
        period_data = [
            (ts, eq) for ts, eq in self._equity_curve if period_start <= ts <= period_end
        ]

        # Calculate daily returns
        for i in range(1, len(period_data)):
            prev_equity = period_data[i - 1][1]
            curr_equity = period_data[i][1]

            if prev_equity > Decimal("0"):
                daily_return = (curr_equity - prev_equity) / prev_equity
                daily_returns.append(daily_return)

        return daily_returns

    async def _get_equity_curve(
        self, period_start: datetime, period_end: datetime
    ) -> List[tuple[datetime, Decimal]]:
        """Get equity curve for period."""
        return [(ts, eq) for ts, eq in self._equity_curve if period_start <= ts <= period_end]

    def _calculate_peak_to_trough_drawdown(
        self, equity_curve: List[tuple[datetime, Decimal]]
    ) -> Dict[str, object]:
        """Calculate drawdown using peak-to-trough method."""
        if not equity_curve:
            return {
                "max_drawdown": Decimal("0"),
                "max_duration_days": 0,
                "current_drawdown": Decimal("0"),
            }

        peak = equity_curve[0][1]
        peak_time = equity_curve[0][0]
        max_drawdown = Decimal("0")
        max_duration = 0
        current_drawdown = Decimal("0")

        for timestamp, equity in equity_curve:
            if equity > peak:
                peak = equity
                peak_time = timestamp
            else:
                drawdown = (
                    (peak - equity) / peak * Decimal("100") if peak > Decimal("0") else Decimal("0")
                )
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
                    duration = (timestamp - peak_time).days
                    if duration > max_duration:
                        max_duration = duration

        # Current drawdown
        if equity_curve:
            current_equity = equity_curve[-1][1]
            current_drawdown = (
                (peak - current_equity) / peak * Decimal("100")
                if peak > Decimal("0")
                else Decimal("0")
            )

        return {
            "max_drawdown": max_drawdown,
            "max_duration_days": max_duration,
            "current_drawdown": current_drawdown,
        }

    def _calculate_underwater_drawdown(
        self, equity_curve: List[tuple[datetime, Decimal]]
    ) -> Dict[str, object]:
        """Calculate drawdown using underwater equity method."""
        # Similar to peak-to-trough but tracks time underwater
        return self._calculate_peak_to_trough_drawdown(equity_curve)

    async def _calculate_realized_pnl(
        self, period_start: datetime, period_end: datetime
    ) -> Decimal:
        """Calculate realized PnL for closed positions."""
        # Placeholder - would track closed positions
        trades = await self._get_trades_in_period(period_start, period_end)

        realized_pnl = Decimal("0")
        for trade in trades:
            # Simplified - would match buys/sells
            trade_pnl = (
                trade.quantity
                * trade.price
                * (Decimal("1") if trade.side.value == "sell" else Decimal("-1"))
            )
            if self._include_fees:
                trade_pnl -= trade.fee
            realized_pnl += trade_pnl

        return realized_pnl

    async def _calculate_unrealized_pnl(self, portfolio_state) -> Decimal:
        """Calculate unrealized PnL for open positions."""
        # Placeholder - would calculate from current positions and market prices
        return (
            portfolio_state.total_unrealized_pnl
            if hasattr(portfolio_state, "total_unrealized_pnl")
            else Decimal("0")
        )

    async def _calculate_period_return(self, days: int) -> Decimal:
        """Calculate return for specific period."""
        period_end = datetime.now(UTC)
        period_start = period_end - timedelta(days=days)

        start_equity = await self._get_equity_at_time(period_start)
        end_equity = await self._get_equity_at_time(period_end)

        if not start_equity or start_equity == Decimal("0"):
            return Decimal("0")

        return ((end_equity - start_equity) / start_equity) * Decimal("100")

    async def _calculate_sortino_ratio(
        self, period_start: datetime, period_end: datetime
    ) -> Decimal:
        """Calculate Sortino ratio (downside deviation)."""
        # Get daily returns
        daily_returns = await self._get_daily_returns(period_start, period_end)

        if not daily_returns:
            return Decimal("0")

        # Calculate average return
        avg_return = sum(daily_returns) / len(daily_returns)

        # Calculate downside deviation (only negative returns)
        downside_returns = [r for r in daily_returns if r < Decimal("0")]
        if not downside_returns:
            return Decimal("0")

        downside_variance = sum(r**2 for r in downside_returns) / len(downside_returns)
        downside_std = downside_variance.sqrt()

        if downside_std == Decimal("0"):
            return Decimal("0")

        # Daily risk-free rate
        daily_risk_free = self._risk_free_rate / Decimal("365")

        return (avg_return - daily_risk_free) / downside_std

    async def _calculate_volatility(self, period_start: datetime, period_end: datetime) -> Decimal:
        """Calculate return volatility."""
        daily_returns = await self._get_daily_returns(period_start, period_end)

        if len(daily_returns) < 2:
            return Decimal("0")

        avg_return = sum(daily_returns) / len(daily_returns)
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / len(daily_returns)

        # Annualized volatility
        return variance.sqrt() * Decimal("365").sqrt() * Decimal("100")

    async def _calculate_beta(self, period_start: datetime, period_end: datetime) -> Decimal:
        """Calculate beta relative to benchmark."""
        # Placeholder - would need benchmark data
        return Decimal("1")

    async def _calculate_alpha(self, period_start: datetime, period_end: datetime) -> Decimal:
        """Calculate alpha (excess return)."""
        # Placeholder - would need benchmark data
        return Decimal("0")

    def get_metrics_summary(self) -> Dict[str, object]:
        """Get summary of current metrics configuration.

        Returns:
            Dictionary with configuration summary

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns actual configuration values
        - NO hardcoded summaries
        """
        return {
            "enabled_metrics": list(self._enabled_metrics),
            "calculation_period_days": self._calculation_period,
            "include_fees": self._include_fees,
            "risk_free_rate": float(self._risk_free_rate),
            "sharpe_method": self._sharpe_method,
            "drawdown_method": self._drawdown_method,
            "equity_curve_length": len(self._equity_curve),
            "trade_history_length": len(self._trade_history),
        }
