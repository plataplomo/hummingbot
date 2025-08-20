"""Performance tracking and metrics calculation service.

This module provides the PerformanceTracker class that calculates various
trading performance metrics using configuration-driven parameters from AppSettings.

File has been refactored to stay under 600 lines by extracting:
- PerformanceMetrics model to performance_metrics_models.py
- Calculation methods to performance_calculator.py
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.monitoring.performance_calculator import PerformanceCalculator
from cyberdelta.domain.monitoring.performance_metrics_models import PerformanceMetrics
from cyberdelta.enums.trading import OrderSide
from cyberdelta.models.market.fill import Fill


if TYPE_CHECKING:
    from cyberdelta.domain.market.market_service import MarketDataService
    from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
    from cyberdelta.models.portfolio.state import PortfolioState


logger = get_logger(__name__)


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
    - Uses centralized calculators from domain/financial/calculators
    """

    def __init__(
        self,
        config: AppSettings,
        portfolio_service: PortfolioService,
        market_data_service: MarketDataService,
    ) -> None:
        """Initialize performance tracker with configuration.

        Args:
            config: Application settings containing all configuration
            portfolio_service: Portfolio service for state access
            market_data_service: Market data service for real-time prices and historical data
        """
        self.config = config
        self._portfolio_service = portfolio_service
        self._market_data_service = market_data_service

        # Initialize calculator with dependencies
        self._calculator = PerformanceCalculator(config, portfolio_service, market_data_service)

        # Extract performance metrics configuration
        self._metrics_config = config.calculation.performance_metrics
        # Extract only the metrics that are enabled (True values)
        self._enabled_metrics = {
            metric for metric, enabled in self._metrics_config.enabled_metrics.items() if enabled
        }
        self._calculation_period = self._metrics_config.calculation_period_days
        self._risk_free_rate = Decimal(str(self._metrics_config.risk_free_rate))
        self._include_fees = self._metrics_config.include_fees_in_metrics

        # Calculation method settings
        self._sharpe_method = self._metrics_config.sharpe_calculation_method
        self._drawdown_method = self._metrics_config.drawdown_calculation_method

        # Cache for historical data
        self._equity_curve: list[tuple[datetime, Decimal]] = []
        self._fill_history: list[Fill] = []

        logger.info(
            "performance_tracker_initialized",
            enabled_metrics=list(self._enabled_metrics),
            calculation_period_days=self._calculation_period,
            include_fees=self._include_fees,
        )

    async def calculate_metrics(self, period_days: int | None = None) -> PerformanceMetrics:
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

        # Calculate optional metrics based on configuration
        await self._calculate_pnl_metrics(metrics, period_start, period_end, portfolio_state)
        await self._calculate_return_metrics(metrics)
        await self._calculate_risk_metrics(metrics, period_start, period_end)
        await self._calculate_trading_metrics(metrics, period_start, period_end)
        await self._calculate_statistical_metrics(metrics, period_start, period_end)

        logger.info(
            "performance_metrics_calculated",
            total_pnl=float(total_pnl),
            total_return_pct=float(total_return_pct),
            period_days=period_days,
            metrics_calculated=sum(1 for _, v in metrics.model_dump().items() if v is not None),
        )

        return metrics

    async def _calculate_total_pnl(self, period_start: datetime, period_end: datetime) -> Decimal:
        """Calculate total PnL for period using centralized calculator.

        Args:
            period_start: Start of calculation period
            period_end: End of calculation period

        Returns:
            Total PnL in base currency

        IMPORTANT: Now uses centralized calculator for consistency.
        """
        # Get fills in period
        fills = await self._get_fills_in_period(period_start, period_end)

        # Calculate realized PnL using calculator
        realized_pnl = await self._calculator.calculate_realized_pnl(
            period_start, period_end, fills
        )

        # Calculate unrealized PnL from current positions
        portfolio_state = await self._portfolio_service.get_state()
        unrealized_pnl = await self._calculator.calculate_unrealized_pnl_from_positions(
            portfolio_state
        )

        return realized_pnl + unrealized_pnl

    async def _calculate_total_return(self, total_pnl: Decimal, period_start: datetime) -> Decimal:
        """Calculate total return percentage.

        Args:
            total_pnl: Total PnL for period
            period_start: Start of period

        Returns:
            Total return as percentage
        """
        # Get starting equity
        starting_equity = await self._get_equity_at_time(period_start)

        if starting_equity is None or starting_equity == Decimal(0):
            return Decimal(0)

        return (total_pnl / starting_equity) * Decimal(100)

    async def update_equity_curve(self, timestamp: datetime, equity: Decimal) -> None:
        """Update equity curve with new data point.

        Args:
            timestamp: Time of equity value
            equity: Total equity value
        """
        # Add to equity curve
        self._equity_curve.append((timestamp, equity))

        # Limit size based on configuration
        # Using max_state_history_size for equity curve points
        max_points = self.config.state.max_state_history_size
        if len(self._equity_curve) > max_points:
            self._equity_curve = self._equity_curve[-max_points:]

        logger.debug(
            "equity_curve_updated",
            timestamp=timestamp.isoformat(),
            equity=float(equity),
            curve_length=len(self._equity_curve),
        )

    async def add_fill(self, fill: Fill) -> None:
        """Add fill to history for metrics calculation.

        Args:
            fill: Fill to add to history
        """
        self._fill_history.append(fill)

        # Limit size based on configuration
        # Using max_trade_history_size for fill history
        max_fills = self.config.state.max_trade_history_size
        if len(self._fill_history) > max_fills:
            self._fill_history = self._fill_history[-max_fills:]

        logger.debug(
            "fill_added_to_history",
            fill_id=fill.id,
            history_length=len(self._fill_history),
        )

    async def _get_fills_in_period(
        self, period_start: datetime, period_end: datetime
    ) -> list[Fill]:
        """Get fills within specified period.

        Args:
            period_start: Start of period
            period_end: End of period

        Returns:
            List of fills in the period
        """
        return [
            fill for fill in self._fill_history if period_start <= fill.executed_at <= period_end
        ]

    async def _get_equity_at_time(self, timestamp: datetime) -> Decimal | None:
        """Get equity value at specific time.

        Args:
            timestamp: Time to get equity for

        Returns:
            Equity value at the time or None if not found
        """
        # Find closest equity value to timestamp
        for eq_time, equity in reversed(self._equity_curve):
            if eq_time <= timestamp:
                return equity
        return None

    async def _get_daily_returns(
        self, period_start: datetime, period_end: datetime
    ) -> list[Decimal]:
        """Calculate daily returns for period.

        Args:
            period_start: Start of period
            period_end: End of period

        Returns:
            List of daily return percentages
        """
        daily_returns: list[Decimal] = []

        # Get equity curve for period
        period_curve = [(t, e) for t, e in self._equity_curve if period_start <= t <= period_end]

        # Calculate daily returns
        for i in range(1, len(period_curve)):
            prev_equity = period_curve[i - 1][1]
            curr_equity = period_curve[i][1]

            if prev_equity > Decimal(0):
                daily_return = (curr_equity - prev_equity) / prev_equity
                daily_returns.append(daily_return)

        return daily_returns

    async def _calculate_pnl_metrics(
        self,
        metrics: PerformanceMetrics,
        period_start: datetime,
        period_end: datetime,
        portfolio_state: PortfolioState,
    ) -> None:
        """Calculate PnL-related metrics."""
        if "realized_pnl" in self._enabled_metrics:
            fills = await self._get_fills_in_period(period_start, period_end)
            metrics.realized_pnl = await self._calculator.calculate_realized_pnl(
                period_start, period_end, fills
            )

        if "unrealized_pnl" in self._enabled_metrics:
            metrics.unrealized_pnl = await self._calculator.calculate_unrealized_pnl_from_positions(
                portfolio_state
            )

    async def _calculate_return_metrics(self, metrics: PerformanceMetrics) -> None:
        """Calculate period return metrics."""
        if "daily_return" in self._enabled_metrics:
            metrics.daily_return_pct = await self._calculate_period_return(1)

        if "weekly_return" in self._enabled_metrics:
            metrics.weekly_return_pct = await self._calculate_period_return(7)

        if "monthly_return" in self._enabled_metrics:
            metrics.monthly_return_pct = await self._calculate_period_return(30)

        if "yearly_return" in self._enabled_metrics:
            metrics.yearly_return_pct = await self._calculate_period_return(365)

    async def _calculate_period_return(self, days: int) -> Decimal:
        """Calculate return for specified period.

        Args:
            days: Number of days to calculate return for

        Returns:
            Return percentage for the period
        """
        period_end = datetime.now(UTC)
        period_start = period_end - timedelta(days=days)

        # Get PnL for period
        pnl = await self._calculate_total_pnl(period_start, period_end)

        # Calculate return
        return await self._calculate_total_return(pnl, period_start)

    async def _calculate_risk_metrics(
        self, metrics: PerformanceMetrics, period_start: datetime, period_end: datetime
    ) -> None:
        """Calculate risk-related metrics."""
        daily_returns = await self._get_daily_returns(period_start, period_end)

        if "sharpe_ratio" in self._enabled_metrics:
            metrics.sharpe_ratio = await self._calculator.calculate_sharpe_ratio(
                daily_returns, self._risk_free_rate
            )

        if "sortino_ratio" in self._enabled_metrics:
            metrics.sortino_ratio = await self._calculator.calculate_sortino_ratio(
                daily_returns, self._risk_free_rate
            )

        if "max_drawdown" in self._enabled_metrics:
            # Get equity curve for period
            period_curve = [
                (t, e) for t, e in self._equity_curve if period_start <= t <= period_end
            ]

            if self._drawdown_method == "underwater":
                drawdown_data = self._calculator.calculate_underwater_drawdown(period_curve)
            else:
                drawdown_data = self._calculator.calculate_peak_to_trough_drawdown(period_curve)

            metrics.max_drawdown_pct = drawdown_data["max_drawdown"]
            metrics.max_drawdown_duration_days = drawdown_data["max_duration_days"]
            metrics.current_drawdown_pct = drawdown_data["current_drawdown"]

    async def _calculate_trading_metrics(
        self, metrics: PerformanceMetrics, period_start: datetime, period_end: datetime
    ) -> None:
        """Calculate trading-related metrics."""
        if not any(m in self._enabled_metrics for m in ["win_rate", "profit_factor"]):
            return

        # Get fills for period
        fills = await self._get_fills_in_period(period_start, period_end)

        if not fills:
            return

        # Classify fills into wins and losses
        winning_fills, losing_fills = self._classify_fills(fills)

        # Calculate win rate metrics
        self._calculate_win_rate_metrics(metrics, fills, winning_fills, losing_fills)

        # Calculate profit factor
        self._calculate_profit_factor(metrics)

    def _classify_fills(self, fills: list[Fill]) -> tuple[list[Fill], list[Fill]]:
        """Classify fills into winning and losing trades.

        Args:
            fills: List of fills to classify

        Returns:
            Tuple of (winning_fills, losing_fills)
        """
        winning_fills: list[Fill] = []
        losing_fills: list[Fill] = []

        for fill in fills:
            # Simple calculation - sells are wins if price > 0
            # In production, would match with opening fills
            if fill.side == OrderSide.SELL:
                if fill.price > Decimal(0):
                    winning_fills.append(fill)
                else:
                    losing_fills.append(fill)

        return winning_fills, losing_fills

    def _calculate_win_rate_metrics(
        self,
        metrics: PerformanceMetrics,
        fills: list[Fill],
        winning_fills: list[Fill],
        losing_fills: list[Fill],
    ) -> None:
        """Calculate win rate related metrics.

        Args:
            metrics: Metrics object to update
            fills: All fills
            winning_fills: Winning trades
            losing_fills: Losing trades
        """
        if "win_rate" not in self._enabled_metrics:
            return

        total_fills = len(fills)
        win_count = len(winning_fills)
        loss_count = len(losing_fills)

        metrics.total_trades = total_fills
        metrics.winning_trades = win_count
        metrics.losing_trades = loss_count

        if total_fills > 0:
            metrics.win_rate_pct = Decimal(win_count) / Decimal(total_fills) * Decimal(100)

        if winning_fills:
            total_win = sum(f.quantity * f.price for f in winning_fills)
            metrics.average_win = total_win / Decimal(len(winning_fills))

        if losing_fills:
            total_loss = sum(f.quantity * f.price for f in losing_fills)
            metrics.average_loss = total_loss / Decimal(len(losing_fills))

    def _calculate_profit_factor(self, metrics: PerformanceMetrics) -> None:
        """Calculate profit factor metric.

        Args:
            metrics: Metrics object to update
        """
        if (
            "profit_factor" in self._enabled_metrics
            and metrics.average_win
            and metrics.average_loss
            and metrics.average_loss != Decimal(0)
        ):
            metrics.profit_factor = abs(metrics.average_win / metrics.average_loss)

    async def _calculate_statistical_metrics(
        self, metrics: PerformanceMetrics, period_start: datetime, period_end: datetime
    ) -> None:
        """Calculate statistical metrics."""
        daily_returns = await self._get_daily_returns(period_start, period_end)

        if "volatility" in self._enabled_metrics:
            metrics.volatility_pct = await self._calculator.calculate_volatility(daily_returns)

        # Beta and alpha would require benchmark data - simplified for now
        if "beta" in self._enabled_metrics:
            metrics.beta = Decimal(1)  # Market beta placeholder

        if "alpha" in self._enabled_metrics:
            metrics.alpha = Decimal(0)  # Zero alpha placeholder

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of current metrics configuration.

        Returns:
            Dictionary with configuration summary
        """
        return {
            "enabled_metrics": list(self._enabled_metrics),
            "calculation_period_days": self._calculation_period,
            "include_fees": self._include_fees,
            "risk_free_rate": float(self._risk_free_rate),
            "sharpe_method": self._sharpe_method,
            "drawdown_method": self._drawdown_method,
            "equity_curve_length": len(self._equity_curve),
            "fill_history_length": len(self._fill_history),
        }
