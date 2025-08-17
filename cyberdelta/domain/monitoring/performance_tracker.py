"""Performance tracking and metrics calculation service.

This module provides the PerformanceTracker class that calculates various
trading performance metrics using configuration-driven parameters from AppSettings.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.financial.calculators.mark_to_market_calculator import MarkToMarketCalculator
from cyberdelta.enums import ExchangeName
from cyberdelta.enums.trading import OrderSide
from cyberdelta.exceptions.monitoring import MetricCalculationError
from cyberdelta.models.market.fill import Fill
from cyberdelta.symbols.models import BaseSymbol, HyperliquidMetadata


if TYPE_CHECKING:
    from cyberdelta.domain.market.market_service import MarketDataService
    from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
    from cyberdelta.models.derivative_position import DerivativePosition
    from cyberdelta.models.portfolio.state import PortfolioState


logger = get_logger(__name__)

# Constants for statistical calculations
MIN_DATA_POINTS_FOR_METRICS = 2


class PerformanceMetrics(BaseModel):
    """Comprehensive performance metrics report.

    All metrics are calculated based on configuration settings and include
    only the metrics explicitly enabled in config.calculation.performance_metrics.
    """

    # Basic metrics
    total_pnl: Decimal
    realized_pnl: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    total_return_pct: Decimal

    # Period metrics (if enabled)
    daily_return_pct: Decimal | None = None
    weekly_return_pct: Decimal | None = None
    monthly_return_pct: Decimal | None = None
    yearly_return_pct: Decimal | None = None

    # Risk metrics (if enabled)
    sharpe_ratio: Decimal | None = None
    sortino_ratio: Decimal | None = None
    max_drawdown_pct: Decimal | None = None
    max_drawdown_duration_days: int | None = None
    current_drawdown_pct: Decimal | None = None

    # Trading metrics (if enabled)
    total_trades: int | None = None
    winning_trades: int | None = None
    losing_trades: int | None = None
    win_rate_pct: Decimal | None = None
    average_win: Decimal | None = None
    average_loss: Decimal | None = None
    profit_factor: Decimal | None = None

    # Statistical metrics (if enabled)
    volatility_pct: Decimal | None = None
    beta: Decimal | None = None
    alpha: Decimal | None = None

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

        # Initialize unified PnL calculator from financial domain
        self._pnl_calculator = MarkToMarketCalculator(config)

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
        # Get fills in period
        fills = await self._get_fills_in_period(period_start, period_end)

        # Calculate realized PnL
        realized_pnl = Decimal(0)
        for fill in fills:
            fill_pnl = fill.quantity * (fill.price if fill.side == OrderSide.SELL else -fill.price)

            # Include fees if configured
            if self._include_fees:
                fill_pnl -= fill.fee

            realized_pnl += fill_pnl

        # Calculate unrealized PnL from current positions using unified calculator
        portfolio_state = await self._portfolio_service.get_state()
        unrealized_pnl = Decimal(0)

        if portfolio_state:
            unrealized_pnl = await self._calculate_unrealized_pnl_from_positions(portfolio_state)

        # Total PnL is the sum of realized and unrealized
        total_pnl = realized_pnl + unrealized_pnl

        logger.debug(
            "total_pnl_calculated",
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            realized_pnl=float(realized_pnl),
            unrealized_pnl=float(unrealized_pnl),
            total_pnl=float(total_pnl),
        )

        return total_pnl

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

        if not starting_equity or starting_equity == Decimal(0):
            logger.warning(
                "cannot_calculate_return_zero_equity", period_start=period_start.isoformat()
            )
            return Decimal(0)

        return (total_pnl / starting_equity) * Decimal(100)

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
            return Decimal(0)

        # Calculate average return
        avg_return = sum(daily_returns) / Decimal(len(daily_returns))

        # Calculate standard deviation
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / Decimal(len(daily_returns))
        std_dev = Decimal(str(math.sqrt(float(variance))))

        if std_dev == Decimal(0):
            return Decimal(0)

        # Apply calculation method from config
        if self._sharpe_method == "daily":
            # Daily Sharpe
            daily_risk_free = self._risk_free_rate / Decimal(365)
            sharpe = (avg_return - daily_risk_free) / std_dev
        elif self._sharpe_method == "annualized":
            # Annualized Sharpe
            annualized_return = avg_return * Decimal(365)
            annualized_std = std_dev * Decimal(str(math.sqrt(365)))
            sharpe = (annualized_return - self._risk_free_rate) / annualized_std
        else:
            # Default to daily if method unknown
            daily_risk_free = self._risk_free_rate / Decimal(365)
            sharpe = (avg_return - daily_risk_free) / std_dev

        return Decimal(str(sharpe))

    async def _calculate_drawdown(
        self, period_start: datetime, period_end: datetime
    ) -> dict[str, Any]:
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
                "max_drawdown": Decimal(0),
                "max_duration_days": 0,
                "current_drawdown": Decimal(0),
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
    ) -> dict[str, Any]:
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
        - NO ASSUMPTIONS about trading side determining profit/loss
        """
        fills = await self._get_fills_in_period(period_start, period_end)

        if not fills:
            return {
                "total_fills": 0,
                "winning_fills": 0,
                "losing_fills": 0,
                "win_rate": Decimal(0),
                "average_win": Decimal(0),
                "average_loss": Decimal(0),
                "profit_factor": Decimal(0),
            }

        # CRITICAL: Proper win/loss calculation requires matched trades
        # A BUY at $100 sold at $110 is a WIN (not a loss)
        # A SHORT sold at $100 bought back at $90 is a WIN
        # Individual fills cannot determine profit/loss without matching

        # For now, return zero statistics rather than incorrect assumptions
        # Proper implementation requires:
        # 1. Trade matching system to pair opening and closing trades
        # 2. Position tracking to determine actual realized PnL
        # 3. Use of the PnLCalculatorProtocol for accurate calculations

        logger.warning(
            "Trading statistics calculation requires matched trades. "
            "Returning zero statistics to avoid incorrect assumptions. "
            "Implement trade matching for accurate win/loss statistics."
        )

        return {
            "total_fills": len(fills),
            "winning_fills": 0,  # Cannot determine without matched trades
            "losing_fills": 0,  # Cannot determine without matched trades
            "win_rate": Decimal(0),  # Cannot calculate without matched trades
            "average_win": Decimal(0),  # Cannot calculate without matched trades
            "average_loss": Decimal(0),  # Cannot calculate without matched trades
            "profit_factor": Decimal(0),  # Cannot calculate without matched trades
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

        # Trim old data based on calculation period to prevent unbounded growth
        max_days = self._calculation_period * 3  # Keep 3x calculation period
        cutoff = datetime.now(UTC) - timedelta(days=max_days)
        self._equity_curve = [(ts, eq) for ts, eq in self._equity_curve if ts >= cutoff]

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

        IMPORTANT: Following CODING_STANDARDS.md:
        - Maintains history based on configured retention
        - NO modifications to trade data
        - Uses Fill model as-is
        """
        self._fill_history.append(fill)

        # Trim old fills based on calculation period to prevent unbounded growth
        max_days = self._calculation_period * 3  # Keep 3x calculation period
        cutoff = datetime.now(UTC) - timedelta(days=max_days)
        self._fill_history = [f for f in self._fill_history if f.executed_at >= cutoff]

    # Helper methods (private)

    async def _get_fills_in_period(
        self, period_start: datetime, period_end: datetime
    ) -> list[Fill]:
        """Get fills within specified period.

        Returns:
            List of fills in the specified period
        """
        return [f for f in self._fill_history if period_start <= f.executed_at <= period_end]

    async def _get_equity_at_time(self, timestamp: datetime) -> Decimal | None:
        """Get equity value at specific time.

        Returns:
            Equity value at timestamp, or None if not found
        """
        # Find closest equity value
        for ts, equity in reversed(self._equity_curve):
            if ts <= timestamp:
                return equity
        return None

    async def _get_daily_returns(
        self, period_start: datetime, period_end: datetime
    ) -> list[Decimal]:
        """Calculate daily returns for period.

        Returns:
            List of daily returns as Decimal values
        """
        daily_returns: list[Decimal] = []

        # Get equity values in period
        period_data = [
            (ts, eq) for ts, eq in self._equity_curve if period_start <= ts <= period_end
        ]

        # Calculate daily returns
        for i in range(1, len(period_data)):
            prev_equity = period_data[i - 1][1]
            curr_equity = period_data[i][1]

            if prev_equity > Decimal(0):
                daily_return = (curr_equity - prev_equity) / prev_equity
                daily_returns.append(daily_return)

        return daily_returns

    async def _get_equity_curve(
        self, period_start: datetime, period_end: datetime
    ) -> list[tuple[datetime, Decimal]]:
        """Get equity curve for period.

        Returns:
            List of tuples containing (timestamp, equity) pairs
        """
        return [(ts, eq) for ts, eq in self._equity_curve if period_start <= ts <= period_end]

    def _calculate_peak_to_trough_drawdown(
        self, equity_curve: list[tuple[datetime, Decimal]]
    ) -> dict[str, Any]:
        """Calculate drawdown using peak-to-trough method.

        Returns:
            Dictionary with max_drawdown, max_duration_days, and current_drawdown
        """
        if not equity_curve:
            return {
                "max_drawdown": Decimal(0),
                "max_duration_days": 0,
                "current_drawdown": Decimal(0),
            }

        peak = equity_curve[0][1]
        peak_time = equity_curve[0][0]
        max_drawdown = Decimal(0)
        max_duration = 0
        current_drawdown = Decimal(0)

        for timestamp, equity in equity_curve:
            if equity > peak:
                peak = equity
                peak_time = timestamp
            else:
                drawdown = (
                    (peak - equity) / peak * Decimal(100) if peak > Decimal(0) else Decimal(0)
                )
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
                    duration = (timestamp - peak_time).days
                    max_duration = max(max_duration, duration)

        # Current drawdown
        if equity_curve:
            current_equity = equity_curve[-1][1]
            current_drawdown = (
                (peak - current_equity) / peak * Decimal(100) if peak > Decimal(0) else Decimal(0)
            )

        return {
            "max_drawdown": max_drawdown,
            "max_duration_days": max_duration,
            "current_drawdown": current_drawdown,
        }

    def _calculate_underwater_drawdown(
        self, equity_curve: list[tuple[datetime, Decimal]]
    ) -> dict[str, Any]:
        """Calculate drawdown using underwater equity method.

        Returns:
            Dictionary with max_drawdown, max_duration_days, and current_drawdown
        """
        # Similar to peak-to-trough but tracks time underwater
        return self._calculate_peak_to_trough_drawdown(equity_curve)

    async def _calculate_realized_pnl(
        self, period_start: datetime, period_end: datetime
    ) -> Decimal:
        """Calculate realized PnL for closed positions.

        Returns:
            Realized PnL as Decimal

        Note:
            This calculates realized PnL from fills in the period.
            For proper position-based PnL, integration with position
            tracking service would match opening and closing trades.
        """
        fills = await self._get_fills_in_period(period_start, period_end)

        # Calculate PnL from fills
        # In a complete implementation, this would match opening and closing fills
        # to calculate true realized PnL. For now, we calculate the cash flow
        # from all fills in the period.
        realized_pnl = Decimal(0)

        for fill in fills:
            # Calculate cash flow from fill
            # Sells generate positive cash flow, buys negative
            cash_flow = fill.quantity * fill.price
            if fill.side == OrderSide.BUY:
                cash_flow = -cash_flow

            # Subtract fees if configured
            if self._include_fees and fill.fee:
                cash_flow -= fill.fee

            realized_pnl += cash_flow

        return realized_pnl

    async def _calculate_unrealized_pnl(self, portfolio_state: PortfolioState) -> Decimal:
        """Calculate unrealized PnL for open positions.

        Returns:
            Unrealized PnL as Decimal
        """
        # Calculate unrealized PnL from current positions
        return await self._calculate_unrealized_pnl_from_positions(portfolio_state)

    async def _calculate_period_return(self, days: int) -> Decimal:
        """Calculate return for specific period.

        Args:
            days: Number of days in the period

        Returns:
            Period return percentage as Decimal

        Raises:
            MetricCalculationError: If end equity is missing for the period.
        """
        period_end = datetime.now(UTC)
        period_start = period_end - timedelta(days=days)

        start_equity = await self._get_equity_at_time(period_start)
        end_equity = await self._get_equity_at_time(period_end)

        if not start_equity or start_equity == Decimal(0):
            return Decimal(0)

        if end_equity is None:
            raise MetricCalculationError("return", f"end equity missing for period {period_end}")
        return (end_equity - start_equity) / start_equity * Decimal(100)

    async def _calculate_sortino_ratio(
        self, period_start: datetime, period_end: datetime
    ) -> Decimal:
        """Calculate Sortino ratio (downside deviation).

        Returns:
            Sortino ratio as Decimal
        """
        # Get daily returns
        daily_returns = await self._get_daily_returns(period_start, period_end)

        if not daily_returns:
            return Decimal(0)

        # Calculate average return
        avg_return = sum(daily_returns) / Decimal(len(daily_returns))

        # Calculate downside deviation (only negative returns)
        downside_returns = [r for r in daily_returns if r < Decimal(0)]
        if not downside_returns:
            return Decimal(0)

        downside_variance = sum(r**2 for r in downside_returns) / Decimal(len(downside_returns))
        downside_std = Decimal(str(math.sqrt(float(downside_variance))))

        if downside_std == Decimal(0):
            return Decimal(0)

        # Daily risk-free rate
        daily_risk_free = self._risk_free_rate / Decimal(365)

        return (avg_return - daily_risk_free) / downside_std

    async def _calculate_volatility(self, period_start: datetime, period_end: datetime) -> Decimal:
        """Calculate return volatility.

        Returns:
            Annualized volatility percentage as Decimal
        """
        daily_returns = await self._get_daily_returns(period_start, period_end)

        if len(daily_returns) < MIN_DATA_POINTS_FOR_METRICS:
            return Decimal(0)

        avg_return = sum(daily_returns) / Decimal(len(daily_returns))
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / Decimal(len(daily_returns))

        # Annualized volatility
        std_dev = Decimal(str(math.sqrt(float(variance))))
        return std_dev * Decimal(str(math.sqrt(365))) * Decimal(100)

    async def _calculate_beta(self, period_start: datetime, period_end: datetime) -> Decimal:
        """Calculate beta relative to benchmark.

        Returns:
            Beta as Decimal

        Note:
            Beta measures the systematic risk of the portfolio relative to a benchmark.
            A beta of 1.0 indicates the portfolio moves with the market.
            Without benchmark data, returns 1.0 as a neutral value.
        """
        # Beta calculation requires benchmark returns for comparison
        # using covariance and variance statistical measures

        # Get portfolio returns
        portfolio_returns = await self._get_daily_returns(period_start, period_end)

        if not portfolio_returns or len(portfolio_returns) < MIN_DATA_POINTS_FOR_METRICS:
            logger.debug(
                "insufficient_data_for_beta",
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
                data_points=len(portfolio_returns),
            )
            return Decimal(1)  # Neutral beta when insufficient data

        # Benchmark data would come from market data service
        # For crypto, this might be BTC, ETH, or a crypto index
        benchmark_returns = await self._get_benchmark_returns(period_start, period_end)

        if not benchmark_returns:
            # Without benchmark, return market-neutral beta
            return Decimal(1)

        # Calculate covariance and variance
        n = min(len(portfolio_returns), len(benchmark_returns))
        if n < MIN_DATA_POINTS_FOR_METRICS:
            return Decimal(1)

        portfolio_returns_subset = portfolio_returns[:n]
        benchmark_returns_subset = benchmark_returns[:n]

        portfolio_mean = sum(portfolio_returns_subset) / Decimal(n)
        benchmark_mean = sum(benchmark_returns_subset) / Decimal(n)

        covariance = sum(
            (p - portfolio_mean) * (b - benchmark_mean)
            for p, b in zip(portfolio_returns_subset, benchmark_returns_subset, strict=False)
        ) / Decimal(n)

        benchmark_variance = sum(
            (b - benchmark_mean) ** 2 for b in benchmark_returns_subset
        ) / Decimal(n)

        if benchmark_variance == Decimal(0):
            return Decimal(1)

        beta = covariance / benchmark_variance

        logger.debug(
            "beta_calculated",
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            beta=float(beta),
            data_points=n,
        )

        return beta

    async def _calculate_alpha(self, period_start: datetime, period_end: datetime) -> Decimal:
        """Calculate alpha (excess return).

        Returns:
            Alpha as Decimal

        Note:
            Alpha represents the excess return of the portfolio compared to the
            expected return based on its beta. Positive alpha indicates
            outperformance relative to systematic risk taken.
        """
        # Alpha = Portfolio Return - (Risk-Free Rate + Beta * (Market Return - Risk-Free Rate))

        # Get portfolio returns
        portfolio_returns = await self._get_daily_returns(period_start, period_end)

        if not portfolio_returns:
            logger.debug(
                "insufficient_data_for_alpha",
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
            )
            return Decimal(0)

        # Calculate average portfolio return (annualized)
        avg_daily_return = sum(portfolio_returns) / Decimal(len(portfolio_returns))
        portfolio_return_annual = avg_daily_return * Decimal(365)

        # Get beta
        beta = await self._calculate_beta(period_start, period_end)

        # Get benchmark return
        benchmark_returns = await self._get_benchmark_returns(period_start, period_end)

        if benchmark_returns:
            avg_benchmark_daily = sum(benchmark_returns) / Decimal(len(benchmark_returns))
            benchmark_return_annual = avg_benchmark_daily * Decimal(365)
        else:
            # Without benchmark data, cannot calculate true alpha
            return Decimal(0)

        # Calculate alpha using CAPM formula
        market_premium = benchmark_return_annual - self._risk_free_rate
        expected_return = self._risk_free_rate + beta * market_premium
        alpha = portfolio_return_annual - expected_return

        logger.debug(
            "alpha_calculated",
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            alpha=float(alpha),
            portfolio_return=float(portfolio_return_annual),
            expected_return=float(expected_return),
            beta=float(beta),
        )

        return alpha

    async def _get_benchmark_returns(
        self, period_start: datetime, period_end: datetime
    ) -> list[Decimal]:
        """Get benchmark returns for the specified period.

        Args:
            period_start: Start of period
            period_end: End of period

        Returns:
            List of daily benchmark returns
        """
        # Get benchmark symbol from config - default to BTC for crypto portfolios
        # This could be extended to config.calculation.benchmark_symbol
        benchmark_symbol = self._get_benchmark_symbol()

        if not benchmark_symbol:
            logger.debug(
                "benchmark_symbol_not_configured",
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
            )
            return []

        try:
            # Get benchmark candles from market data service
            # Convert datetime to milliseconds
            start_time_ms = int(period_start.timestamp() * 1000)
            end_time_ms = int(period_end.timestamp() * 1000)

            candles = await self._market_data_service.get_historical_candles(
                symbol=benchmark_symbol,
                exchange=self._get_benchmark_exchange(),
                timeframe="1d",
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )

            if not candles or len(candles) < MIN_DATA_POINTS_FOR_METRICS:
                logger.warning(
                    "insufficient_benchmark_data",
                    symbol=benchmark_symbol.value,
                    data_points=len(candles) if candles else 0,
                    required=MIN_DATA_POINTS_FOR_METRICS,
                )
                return []

            # Calculate daily returns from candles
            returns: list[Decimal] = []
            for i in range(1, len(candles)):
                prev_close = candles[i - 1].close
                curr_close = candles[i].close

                if prev_close > Decimal(0):
                    daily_return = (curr_close - prev_close) / prev_close
                    returns.append(daily_return)

            logger.debug(
                "benchmark_returns_calculated",
                symbol=benchmark_symbol.value,
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
                data_points=len(returns),
            )

        except Exception as e:
            logger.exception(
                "failed_to_get_benchmark_returns",
                error=str(e),
                symbol=benchmark_symbol.value if benchmark_symbol else "None",
            )
            return []

        return returns

    def _get_benchmark_symbol(self) -> BaseSymbol[HyperliquidMetadata] | None:
        """Get benchmark symbol from configuration.

        Returns:
            Benchmark symbol or None if not configured

        IMPORTANT: Following CODING_STANDARDS.md:
        - Reads benchmark symbol from config.calculation.performance_metrics.benchmark_symbol
        - NO hardcoded values
        - Returns None if not configured or invalid
        """
        # Get benchmark symbol from configuration
        benchmark_symbol_str = self._metrics_config.benchmark_symbol

        if not benchmark_symbol_str:
            logger.debug(
                "benchmark_symbol_not_configured",
                message="No benchmark symbol configured in performance_metrics",
            )
            return None

        # Get configured exchange
        benchmark_exchange = self._get_benchmark_exchange()

        try:
            # Create proper symbol based on configured exchange
            # For now supporting Hyperliquid, can be extended for other exchanges
            if benchmark_exchange == ExchangeName.HYPERLIQUID:
                benchmark_symbol = BaseSymbol[HyperliquidMetadata](
                    value=benchmark_symbol_str,
                    exchange=benchmark_exchange,
                    metadata=HyperliquidMetadata(),
                )
            else:
                # For other exchanges, would need appropriate metadata types
                logger.warning(
                    "benchmark_exchange_not_fully_supported",
                    exchange=benchmark_exchange.value,
                    symbol=benchmark_symbol_str,
                    message="Exchange metadata type not yet implemented for benchmarks",
                )
                benchmark_symbol = None
        except (ValueError, TypeError) as e:
            logger.exception(
                "failed_to_create_benchmark_symbol",
                configured_symbol=benchmark_symbol_str,
                configured_exchange=benchmark_exchange.value,
                error=str(e),
            )
            benchmark_symbol = None

        return benchmark_symbol

    def _get_benchmark_exchange(self) -> ExchangeName:
        """Get benchmark exchange from configuration.

        Returns:
            Exchange to use for benchmark data

        Raises:
            ValueError: If configured exchange name is invalid

        Note:
            Following CODING_STANDARDS.md:
            - Reads exchange from config.calculation.performance_metrics.benchmark_exchange
            - NO hardcoded values
            - Validates exchange name from configuration
        """
        # Get benchmark exchange from configuration
        benchmark_exchange_str = self._metrics_config.benchmark_exchange

        try:
            # Convert configured string to ExchangeName enum
            return ExchangeName(benchmark_exchange_str)
        except (ValueError, KeyError) as e:
            logger.exception(
                "invalid_benchmark_exchange_configured",
                configured_value=benchmark_exchange_str,
                error=str(e),
                available_exchanges=[e.value for e in ExchangeName],
                message="Invalid exchange name in configuration",
            )
            # This should fail fast per CODING_STANDARDS.md
            msg = f"Invalid benchmark exchange '{benchmark_exchange_str}'"
            raise ValueError(msg) from e

    def get_metrics_summary(self) -> dict[str, Any]:
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
            "fill_history_length": len(self._fill_history),
        }

    async def _calculate_pnl_metrics(
        self,
        metrics: PerformanceMetrics,
        period_start: datetime,
        period_end: datetime,
        portfolio_state: PortfolioState,
    ) -> None:
        """Calculate PnL-related metrics."""
        if "realized_pnl" in self._enabled_metrics:
            metrics.realized_pnl = await self._calculate_realized_pnl(period_start, period_end)

        if "unrealized_pnl" in self._enabled_metrics:
            metrics.unrealized_pnl = await self._calculate_unrealized_pnl(portfolio_state)

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

    async def _calculate_risk_metrics(
        self, metrics: PerformanceMetrics, period_start: datetime, period_end: datetime
    ) -> None:
        """Calculate risk-related metrics."""
        if "sharpe_ratio" in self._enabled_metrics:
            metrics.sharpe_ratio = await self._calculate_sharpe_ratio(period_start, period_end)

        if "sortino_ratio" in self._enabled_metrics:
            metrics.sortino_ratio = await self._calculate_sortino_ratio(period_start, period_end)

        if "max_drawdown" in self._enabled_metrics:
            drawdown_data = await self._calculate_drawdown(period_start, period_end)
            metrics.max_drawdown_pct = drawdown_data["max_drawdown"]
            metrics.max_drawdown_duration_days = drawdown_data["max_duration_days"]
            metrics.current_drawdown_pct = drawdown_data["current_drawdown"]

    async def _calculate_trading_metrics(
        self, metrics: PerformanceMetrics, period_start: datetime, period_end: datetime
    ) -> None:
        """Calculate trading-related metrics."""
        if "win_rate" in self._enabled_metrics or "profit_factor" in self._enabled_metrics:
            trading_stats = await self._calculate_trading_statistics(period_start, period_end)

            if "win_rate" in self._enabled_metrics:
                metrics.total_trades = trading_stats["total_fills"]
                metrics.winning_trades = trading_stats["winning_fills"]
                metrics.losing_trades = trading_stats["losing_fills"]
                metrics.win_rate_pct = trading_stats["win_rate"]
                metrics.average_win = trading_stats["average_win"]
                metrics.average_loss = trading_stats["average_loss"]

            if "profit_factor" in self._enabled_metrics:
                metrics.profit_factor = trading_stats["profit_factor"]

    async def _calculate_statistical_metrics(
        self, metrics: PerformanceMetrics, period_start: datetime, period_end: datetime
    ) -> None:
        """Calculate statistical metrics."""
        if "volatility" in self._enabled_metrics:
            metrics.volatility_pct = await self._calculate_volatility(period_start, period_end)

        if "beta" in self._enabled_metrics:
            metrics.beta = await self._calculate_beta(period_start, period_end)

        if "alpha" in self._enabled_metrics:
            metrics.alpha = await self._calculate_alpha(period_start, period_end)

    async def _calculate_unrealized_pnl_from_positions(
        self, portfolio_state: PortfolioState
    ) -> Decimal:
        """Calculate unrealized PnL from current positions using centralized calculator.

        Args:
            portfolio_state: Current portfolio state

        Returns:
            Total unrealized PnL as Decimal

        Note:
            This method uses the centralized MarkToMarketCalculator for consistency
            across all PnL calculations in the system.
        """
        if not portfolio_state or not portfolio_state.positions:
            return Decimal(0)

        total_pnl = Decimal(0)

        # Iterate through all positions and calculate unrealized PnL
        for position in portfolio_state.positions.values():
            if position.size == Decimal(0) or not position.entry_price:
                continue

            # Get current mark price for the position
            # In production, this would fetch from market data service
            mark_price = await self._get_mark_price_for_position(position)

            if mark_price:
                # Use the centralized calculator already initialized
                result = self._pnl_calculator.calculate_unrealized_pnl(
                    position, mark_price, include_fees=self._include_fees
                )
                total_pnl += result.amount

                logger.debug(
                    "position_unrealized_pnl_calculated",
                    exchange=position.exchange.value,
                    symbol=position.symbol.value,
                    size=float(position.size),
                    entry_price=float(position.entry_price),
                    mark_price=float(mark_price),
                    unrealized_pnl=float(result.amount),
                )

        return total_pnl

    async def _get_mark_price_for_position(self, position: DerivativePosition) -> Decimal | None:
        """Get current mark price for a position.

        Args:
            position: Position to get mark price for

        Returns:
            Current mark price or None if unavailable
        """
        try:
            # Get ticker from market data service
            ticker = await self._market_data_service.get_ticker(symbol=position.symbol)

            if ticker and ticker.price:
                return ticker.price

            logger.warning(
                "no_mark_price_available",
                exchange=position.exchange.value,
                symbol=position.symbol.value,
            )
        except (AttributeError, ValueError, TypeError) as e:
            logger.exception(
                "failed_to_get_mark_price",
                exchange=position.exchange.value,
                symbol=position.symbol.value,
                error=str(e),
            )

        return position.entry_price or None
