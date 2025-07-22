"""Portfolio metrics aggregation service for comprehensive analytics."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import defaultdict
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import Field
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.calculators.position_exposure_calculator import PositionExposure
from cyberdelta.core.portfolio.exceptions import ServiceUnavailableError
from cyberdelta.core.portfolio.exceptions.calculation import (
    ExposureCalculationError,
    InsufficientDataError,
    PnLCalculationError,
)
from cyberdelta.core.portfolio.portfolio_types.domain_models import MetricsMetadata
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService
from cyberdelta.enums.exchange_names import ExchangeName


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition
    from cyberdelta.core.portfolio.calculators.performance_calculator import (
        PerformanceCalculator,
        PerformanceInput,
    )
    from cyberdelta.core.portfolio.calculators.pnl.realized_pnl_calculator import (
        RealizedPnLCalculator,
    )
    from cyberdelta.core.portfolio.calculators.portfolio_exposure_calculator import (
        PortfolioExposureCalculator,
    )
    from cyberdelta.core.portfolio.calculators.position_exposure_calculator import PositionExposure
    from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
    from cyberdelta.core.portfolio.services.currency_converter import CurrencyConverter


logger = get_logger(__name__)


# Type-preserving factory functions for dataclass fields
def _str_any_dict_factory() -> dict[str, Any]:
    """Factory function that preserves dict[str, Any] type information."""
    return {}


def _str_decimal_dict_factory() -> dict[str, Decimal]:
    """Factory function that preserves dict[str, Decimal] type information."""
    return {}


def _nested_decimal_dict_factory() -> dict[str, dict[str, Decimal]]:
    """Factory function that preserves dict[str, dict[str, Decimal]] type information."""
    return {}


def _metric_snapshot_list_factory() -> list[Any]:
    """Factory function that preserves list[MetricSnapshot] type information."""
    return []


def _str_float_dict_factory() -> dict[str, float]:
    """Factory function that preserves dict[str, float] type information."""
    return {}


def _str_metrics_trend_dict_factory() -> dict[str, Any]:
    """Factory function that preserves dict[str, MetricsTrend] type information."""
    return {}


def _dict_any_list_factory() -> list[dict[str, Any]]:
    """Factory function that preserves list[dict[str, Any]] type information."""
    return []


def _aggregated_metrics_list_factory() -> list[Any]:
    """Factory function that preserves list[AggregatedMetrics] type information."""
    return []


# Constants
HOURS_IN_DAY = 24
SECONDS_IN_HOUR = 3600
MAX_CALCULATION_TIMES = 1000


class NoPositionsDataError(InsufficientDataError):
    """Raised when no positions data is available for calculations."""

    def __init__(self) -> None:
        """Initialize with default error details."""
        super().__init__(
            "No positions data",
            error_code="METRICS_NO_POSITIONS",
            context={"operation": "pnl_calculation"},
        )


class NoBalanceManagerError(ServiceUnavailableError):
    """Raised when balance manager is not available."""

    def __init__(self) -> None:
        """Initialize with default service details."""
        super().__init__(
            "No balance manager",
            service_name="portfolio_state_manager",
        )


class PortfolioStateManagerUnavailableError(ServiceUnavailableError):
    """Raised when portfolio state manager is not available."""

    def __init__(self) -> None:
        """Initialize with standard message."""
        super().__init__("Portfolio state manager not available")


class PnLCalculationFailedError(PnLCalculationError):
    """Raised when P&L calculation fails."""

    def __init__(self, cause: Exception) -> None:
        """Initialize with cause."""
        super().__init__("Failed to calculate P&L metrics")
        self.__cause__ = cause


class InsufficientCalculationDataError(InsufficientDataError):
    """Raised when insufficient data for calculation."""

    def __init__(self, cause: Exception) -> None:
        """Initialize with cause."""
        super().__init__("Insufficient data for calculation")
        self.__cause__ = cause


class ExposureCalculationFailedError(ExposureCalculationError):
    """Raised when exposure calculation fails."""

    def __init__(self, cause: Exception) -> None:
        """Initialize with cause."""
        super().__init__("Failed to calculate exposure metrics")
        self.__cause__ = cause


class MetricType(Enum):
    """Types of portfolio metrics."""

    PNL = "pnl"
    EXPOSURE = "exposure"
    PERFORMANCE = "performance"
    RISK = "risk"
    VOLUME = "volume"
    ALLOCATION = "allocation"
    CORRELATION = "correlation"
    LIQUIDITY = "liquidity"


class AggregationPeriod(Enum):
    """Time periods for metric aggregation."""

    REALTIME = "realtime"
    MINUTE = "minute"
    HOUR = "hour"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


@dataclass
class MetricSnapshot:
    """Snapshot of portfolio metrics at a specific time."""

    timestamp: float
    metric_type: MetricType
    period: AggregationPeriod
    exchange_id: str | None = None
    symbol: str | None = None
    base_currency: str = "USD"
    metrics: dict[str, Any] = Field(default_factory=_str_any_dict_factory)
    metadata: MetricsMetadata | None = None


@dataclass
class MetricSeries:
    """Time series of metric snapshots."""

    metric_type: MetricType
    period: AggregationPeriod
    snapshots: list[MetricSnapshot] = Field(default_factory=_metric_snapshot_list_factory)
    start_time: float | None = None
    end_time: float | None = None

    def add_snapshot(self, snapshot: MetricSnapshot) -> None:
        """Add a snapshot to the series."""
        self.snapshots.append(snapshot)
        self.snapshots.sort(key=lambda x: x.timestamp)

        if self.start_time is None or snapshot.timestamp < self.start_time:
            self.start_time = snapshot.timestamp
        if self.end_time is None or snapshot.timestamp > self.end_time:
            self.end_time = snapshot.timestamp


@dataclass
class AggregatedMetrics:
    """Aggregated portfolio metrics."""

    timestamp: float
    period: AggregationPeriod
    base_currency: str = "USD"

    # P&L Metrics
    realized_pnl: Decimal = Decimal(0)
    unrealized_pnl: Decimal = Decimal(0)
    total_pnl: Decimal = Decimal(0)
    pnl_change: Decimal = Decimal(0)
    pnl_change_pct: Decimal = Decimal(0)

    # Performance Metrics
    portfolio_return: Decimal = Decimal(0)
    benchmark_return: Decimal = Decimal(0)
    alpha: Decimal = Decimal(0)
    beta: Decimal = Decimal(0)
    sharpe_ratio: Decimal = Decimal(0)
    sortino_ratio: Decimal = Decimal(0)
    calmar_ratio: Decimal = Decimal(0)
    max_drawdown: Decimal = Decimal(0)
    volatility: Decimal = Decimal(0)

    # Risk Metrics
    var_95: Decimal = Decimal(0)
    var_99: Decimal = Decimal(0)
    expected_shortfall: Decimal = Decimal(0)
    leverage_ratio: Decimal = Decimal(0)
    concentration_risk: Decimal = Decimal(0)

    # Exposure Metrics
    total_exposure: Decimal = Decimal(0)
    net_exposure: Decimal = Decimal(0)
    gross_exposure: Decimal = Decimal(0)
    long_exposure: Decimal = Decimal(0)
    short_exposure: Decimal = Decimal(0)

    # Volume Metrics
    trading_volume: Decimal = Decimal(0)
    trade_count: int = 0
    avg_trade_size: Decimal = Decimal(0)
    turnover_ratio: Decimal = Decimal(0)

    # Allocation Metrics
    exchange_allocation: dict[str, Decimal] = Field(default_factory=_str_decimal_dict_factory)
    symbol_allocation: dict[str, Decimal] = Field(default_factory=_str_decimal_dict_factory)
    currency_allocation: dict[str, Decimal] = Field(default_factory=_str_decimal_dict_factory)

    # Correlation Metrics
    portfolio_correlation: dict[str, Decimal] = Field(default_factory=_str_decimal_dict_factory)
    exchange_correlation: dict[str, dict[str, Decimal]] = Field(
        default_factory=_nested_decimal_dict_factory
    )

    # Liquidity Metrics
    liquidity_score: Decimal = Decimal(0)
    bid_ask_spread: Decimal = Decimal(0)
    market_impact: Decimal = Decimal(0)

    # Portfolio value
    total_account_value: Decimal = Decimal(0)

    # Metadata
    metrics_count: int = 0
    calculation_time_ms: float = 0.0
    data_quality_score: Decimal = Decimal("1.0")
    metadata: MetricsMetadata | None = None


@dataclass
class MetricsTrend:
    """Trend analysis for portfolio metrics."""

    metric_name: str
    period: AggregationPeriod
    trend_direction: str  # "up", "down", "flat"
    trend_strength: Decimal  # 0-1 scale
    trend_duration: int  # Number of periods
    trend_confidence: Decimal  # 0-1 scale
    regression_slope: Decimal = Decimal(0)
    regression_r_squared: Decimal = Decimal(0)
    volatility: Decimal = Decimal(0)
    momentum: Decimal = Decimal(0)
    recent_change: Decimal = Decimal(0)
    recent_change_pct: Decimal = Decimal(0)


@dataclass
class MetricsReport:
    """Comprehensive portfolio metrics report."""

    report_id: str
    generated_at: float
    period: AggregationPeriod
    start_time: float
    end_time: float
    base_currency: str = "USD"

    # Current snapshot
    current_metrics: AggregatedMetrics = Field(
        default_factory=lambda: AggregatedMetrics(
            timestamp=time.time(), period=AggregationPeriod.REALTIME
        )
    )

    # Historical data
    historical_metrics: list[AggregatedMetrics] = Field(
        default_factory=_aggregated_metrics_list_factory
    )

    # Trend analysis
    trends: dict[str, MetricsTrend] = Field(default_factory=_str_metrics_trend_dict_factory)

    # Performance attribution
    attribution_by_exchange: dict[str, dict[str, Decimal]] = Field(
        default_factory=_nested_decimal_dict_factory
    )
    attribution_by_symbol: dict[str, dict[str, Decimal]] = Field(
        default_factory=_nested_decimal_dict_factory
    )
    attribution_by_strategy: dict[str, dict[str, Decimal]] = Field(
        default_factory=_nested_decimal_dict_factory
    )

    # Risk analysis
    risk_metrics: dict[str, Any] = Field(default_factory=_str_any_dict_factory)
    risk_alerts: list[dict[str, Any]] = Field(default_factory=_dict_any_list_factory)

    # Quality metrics
    data_quality: dict[str, Any] = Field(default_factory=_str_any_dict_factory)
    coverage: dict[str, float] = Field(default_factory=_str_float_dict_factory)

    # Summary statistics
    summary: dict[str, Any] = Field(default_factory=_str_any_dict_factory)

    # Metadata
    metadata: MetricsMetadata | None = None


class PortfolioMetricsAggregationService(BasePortfolioService):
    """Service for aggregating and analyzing portfolio metrics."""

    def __init__(
        self, name: str = "PortfolioMetricsAggregationService", config: dict[str, Any] | None = None
    ) -> None:
        """Initialize the portfolio metrics aggregation service.

        Args:
            name: Service name
            config: Configuration dictionary
        """
        super().__init__(name, config)

        # Configuration
        cfg = config or {}
        self.aggregation_periods = cfg.get(
            "aggregation_periods",
            [
                AggregationPeriod.REALTIME,
                AggregationPeriod.MINUTE,
                AggregationPeriod.HOUR,
                AggregationPeriod.DAILY,
            ],
        )
        self.max_snapshots_per_period = cfg.get("max_snapshots_per_period", 1000)
        self.retention_days = cfg.get("retention_days", 90)
        self.calculation_interval = cfg.get("calculation_interval", 60)  # seconds
        self.trend_analysis_enabled = cfg.get("trend_analysis_enabled", True)
        self.performance_attribution_enabled = cfg.get("performance_attribution_enabled", True)
        self.risk_analysis_enabled = cfg.get("risk_analysis_enabled", True)
        self.auto_cleanup_enabled = cfg.get("auto_cleanup_enabled", True)

        # Storage
        self.metric_series: dict[tuple[MetricType, AggregationPeriod], MetricSeries] = {}
        self.current_snapshots: dict[MetricType, MetricSnapshot] = {}
        self.aggregated_metrics: dict[AggregationPeriod, AggregatedMetrics] = {}

        # Performance tracking
        self.calculation_times: dict[str, list[float]] = defaultdict(list)
        self.last_calculation_time: dict[AggregationPeriod, float] = {}

        # Dependencies (will be injected)
        self.pnl_calculator: RealizedPnLCalculator | None = None
        self.exposure_calculator: PortfolioExposureCalculator | None = None
        self.performance_calculator: PerformanceCalculator | None = None
        self.currency_converter: CurrencyConverter | None = None
        self.portfolio_state_manager: PortfolioStateManager | None = None

        # Background tasks
        self.calculation_task: asyncio.Task[None] | None = None
        self.cleanup_task: asyncio.Task[None] | None = None

        logger.info(
            "portfolio_metrics_aggregation_service_initialized",
            name=name,
            aggregation_periods=[p.value for p in self.aggregation_periods],
            max_snapshots_per_period=self.max_snapshots_per_period,
            retention_days=self.retention_days,
        )

    async def _initialize_internal(self) -> None:
        """Initialize the metrics aggregation service."""
        # Initialize metric series for each type and period
        for metric_type in MetricType:
            for period in self.aggregation_periods:
                key = (metric_type, period)
                self.metric_series[key] = MetricSeries(metric_type=metric_type, period=period)

        # Initialize current snapshots
        for metric_type in MetricType:
            self.current_snapshots[metric_type] = MetricSnapshot(
                timestamp=time.time(), metric_type=metric_type, period=AggregationPeriod.REALTIME
            )

        # Initialize aggregated metrics
        for period in self.aggregation_periods:
            self.aggregated_metrics[period] = AggregatedMetrics(
                timestamp=time.time(), period=period
            )

        # Start background tasks
        self.calculation_task = asyncio.create_task(self._run_calculation_loop())
        if self.auto_cleanup_enabled:
            self.cleanup_task = asyncio.create_task(self._run_cleanup_loop())

        logger.info("portfolio_metrics_aggregation_service_initialized_internal")

    async def _shutdown_internal(self) -> None:
        """Shutdown the metrics aggregation service."""
        # Cancel background tasks
        if self.calculation_task:
            self.calculation_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.calculation_task

        if self.cleanup_task:
            self.cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.cleanup_task

        logger.info("portfolio_metrics_aggregation_service_shutdown_internal")

    def set_dependencies(
        self,
        pnl_calculator: RealizedPnLCalculator | None = None,
        exposure_calculator: PortfolioExposureCalculator | None = None,
        performance_calculator: PerformanceCalculator | None = None,
        currency_converter: CurrencyConverter | None = None,
        portfolio_state_manager: PortfolioStateManager | None = None,
    ) -> None:
        """Set service dependencies."""
        self.pnl_calculator = pnl_calculator
        self.exposure_calculator = exposure_calculator
        self.performance_calculator = performance_calculator
        self.currency_converter = currency_converter
        self.portfolio_state_manager = portfolio_state_manager

    async def calculate_aggregated_metrics(
        self, period: AggregationPeriod = AggregationPeriod.REALTIME, base_currency: str = "USD"
    ) -> AggregatedMetrics:
        """Calculate comprehensive aggregated metrics."""
        start_time = time.time()

        try:
            # Create new aggregated metrics
            metrics = AggregatedMetrics(
                timestamp=start_time, period=period, base_currency=base_currency
            )

            # Calculate total account value first (needed by other calculations)
            metrics.total_account_value = await self._calculate_total_account_value(base_currency)

            # Calculate P&L metrics
            await self._calculate_pnl_metrics(metrics, base_currency)

            # Calculate exposure metrics
            await self._calculate_exposure_metrics(metrics, base_currency)

            # Calculate performance metrics
            await self._calculate_performance_metrics(metrics, base_currency)

            # Calculate risk metrics
            await self._calculate_risk_metrics(metrics, base_currency)

            # Calculate volume metrics
            await self._calculate_volume_metrics(metrics, base_currency)

            # Calculate allocation metrics
            await self._calculate_allocation_metrics(metrics, base_currency)

            # Calculate correlation metrics
            await self._calculate_correlation_metrics(metrics, base_currency)

            # Calculate liquidity metrics
            await self._calculate_liquidity_metrics(metrics, base_currency)

            # Update calculation timing
            calculation_time = (time.time() - start_time) * 1000
            metrics.calculation_time_ms = calculation_time
            self.calculation_times[period.value].append(calculation_time)

            # Store metrics
            self.aggregated_metrics[period] = metrics
            self.last_calculation_time[period] = time.time()

            # Create snapshot
            snapshot = MetricSnapshot(
                timestamp=metrics.timestamp,
                metric_type=MetricType.PERFORMANCE,  # Combined metrics
                period=period,
                base_currency=base_currency,
                metrics=self._metrics_to_dict(metrics),
                metadata=MetricsMetadata(
                    calculation_method="aggregated",
                    data_quality_score=1.0,
                ),
            )

            # Store snapshot
            series_key = (MetricType.PERFORMANCE, period)
            if series_key in self.metric_series:
                self.metric_series[series_key].add_snapshot(snapshot)

            logger.debug(
                "aggregated_metrics_calculated",
                period=period.value,
                calculation_time_ms=calculation_time,
                total_pnl=float(metrics.total_pnl),
                total_exposure=float(metrics.total_exposure),
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception(
                "aggregated_metrics_calculation_failed",
                period=period.value,
                base_currency=base_currency,
            )
            raise
        else:
            return metrics

    async def get_metrics_report(
        self,
        period: AggregationPeriod = AggregationPeriod.DAILY,
        lookback_days: int = 30,
        base_currency: str = "USD",
    ) -> MetricsReport:
        """Generate comprehensive metrics report."""
        start_time = time.time()
        end_time = start_time
        report_start_time = start_time - (lookback_days * HOURS_IN_DAY * SECONDS_IN_HOUR)

        try:
            # Create report
            report = MetricsReport(
                report_id=f"metrics_report_{int(start_time)}",
                generated_at=start_time,
                period=period,
                start_time=report_start_time,
                end_time=end_time,
                base_currency=base_currency,
            )

            # Get current metrics
            report.current_metrics = await self.calculate_aggregated_metrics(period, base_currency)

            # Get historical metrics
            report.historical_metrics = await self._get_historical_metrics(
                period, report_start_time, end_time, base_currency
            )

            # Calculate trends
            if self.trend_analysis_enabled:
                report.trends = await self._calculate_trends(report.historical_metrics)

            # Calculate performance attribution
            if self.performance_attribution_enabled:
                report.attribution_by_exchange = await self._calculate_exchange_attribution(
                    report.historical_metrics
                )
                report.attribution_by_symbol = await self._calculate_symbol_attribution(
                    report.historical_metrics
                )

            # Calculate risk metrics
            if self.risk_analysis_enabled:
                report.risk_metrics = await self._calculate_risk_analysis(report.historical_metrics)
                report.risk_alerts = await self._generate_risk_alerts(report.current_metrics)

            # Calculate data quality metrics
            report.data_quality = await self._calculate_data_quality(report.historical_metrics)

            # Calculate coverage metrics
            report.coverage = await self._calculate_coverage(report.historical_metrics)

            # Generate summary
            report.summary = await self._generate_summary(report)

            logger.info(
                "metrics_report_generated",
                report_id=report.report_id,
                period=period.value,
                lookback_days=lookback_days,
                historical_points=len(report.historical_metrics),
                trends_count=len(report.trends),
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception(
                "metrics_report_generation_failed", period=period.value, lookback_days=lookback_days
            )
            raise
        else:
            return report

    async def get_metric_series(
        self,
        metric_type: MetricType,
        period: AggregationPeriod,
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> MetricSeries:
        """Get metric series for specific type and period."""
        key = (metric_type, period)

        if key not in self.metric_series:
            raise ValueError

        series = self.metric_series[key]

        # Filter by time range if specified
        if start_time is not None or end_time is not None:
            filtered_snapshots: list[MetricSnapshot] = []
            for snapshot in series.snapshots:
                if start_time is not None and snapshot.timestamp < start_time:
                    continue
                if end_time is not None and snapshot.timestamp > end_time:
                    continue
                filtered_snapshots.append(snapshot)

            # Create filtered series
            filtered_series = MetricSeries(
                metric_type=metric_type, period=period, snapshots=filtered_snapshots
            )

            if filtered_snapshots:
                filtered_series.start_time = min(s.timestamp for s in filtered_snapshots)
                filtered_series.end_time = max(s.timestamp for s in filtered_snapshots)

            return filtered_series

        return series

    async def get_current_metrics(self, base_currency: str = "USD") -> AggregatedMetrics:
        """Get current real-time metrics."""
        return await self.calculate_aggregated_metrics(AggregationPeriod.REALTIME, base_currency)

    async def get_metrics_statistics(self) -> dict[str, Any]:
        """Get service statistics."""
        return {
            "metric_series_count": len(self.metric_series),
            "total_snapshots": sum(len(series.snapshots) for series in self.metric_series.values()),
            "aggregation_periods": [p.value for p in self.aggregation_periods],
            "last_calculation_times": {
                period.value: timestamp for period, timestamp in self.last_calculation_time.items()
            },
            "avg_calculation_times": {
                period: sum(times) / len(times) if times else 0
                for period, times in self.calculation_times.items()
            },
            "current_snapshot_count": len(self.current_snapshots),
        }

    async def _run_calculation_loop(self) -> None:
        """Background task for periodic metric calculations."""
        while True:
            try:
                await asyncio.sleep(self.calculation_interval)

                # Calculate metrics for all periods
                for period in self.aggregation_periods:
                    try:
                        await self.calculate_aggregated_metrics(period)
                    except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                        logger.exception("periodic_calculation_failed", period=period.value)

            except asyncio.CancelledError:
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("calculation_loop_error")
                await asyncio.sleep(60)  # Wait before retrying

    async def _run_cleanup_loop(self) -> None:
        """Background task for cleaning up old data."""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                await self._cleanup_old_data()

            except asyncio.CancelledError:
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("cleanup_loop_error")
                await asyncio.sleep(3600)  # Wait before retrying

    async def _cleanup_old_data(self) -> None:
        """Clean up old metric data."""
        current_time = time.time()
        retention_cutoff = current_time - (self.retention_days * HOURS_IN_DAY * SECONDS_IN_HOUR)

        total_removed = 0

        for series in self.metric_series.values():
            initial_count = len(series.snapshots)

            # Remove old snapshots
            series.snapshots = [
                snapshot for snapshot in series.snapshots if snapshot.timestamp >= retention_cutoff
            ]

            # Update time range
            if series.snapshots:
                series.start_time = min(s.timestamp for s in series.snapshots)
                series.end_time = max(s.timestamp for s in series.snapshots)
            else:
                series.start_time = None
                series.end_time = None

            removed = initial_count - len(series.snapshots)
            total_removed += removed

        # Clean up calculation times
        for period, times in self.calculation_times.items():
            if len(times) > MAX_CALCULATION_TIMES:  # Keep last measurements
                self.calculation_times[period] = times[-MAX_CALCULATION_TIMES:]

        if total_removed > 0:
            logger.info(
                "old_metrics_cleaned_up",
                total_removed=total_removed,
                retention_days=self.retention_days,
            )

    def _validate_positions_data(self, positions: list[Any]) -> None:
        """Validate that positions data is sufficient for calculations."""
        if not positions:
            raise NoPositionsDataError

    def _validate_balance_manager_available(self) -> None:
        """Validate that balance manager is available."""
        if not self.portfolio_state_manager:
            raise PortfolioStateManagerUnavailableError

    async def _calculate_pnl_metrics(self, metrics: AggregatedMetrics, base_currency: str) -> None:
        """Calculate P&L metrics."""
        if not self.portfolio_state_manager:
            raise PortfolioStateManagerUnavailableError

        try:
            # Calculate realized and unrealized P&L separately
            all_positions = await self._get_all_positions()

            # Calculate simple unrealized PnL based on position mark prices
            total_unrealized = Decimal(0)
            for position in all_positions:
                if position.mark_price and position.entry_price:
                    # Calculate unrealized PnL for each position
                    price_diff = position.mark_price - position.entry_price
                    if position.size < 0:  # Short position
                        price_diff = -price_diff
                    unrealized = position.size * price_diff
                    total_unrealized += unrealized

            metrics.unrealized_pnl = total_unrealized

            # For realized P&L, we would need trade history or cumulative tracking
            # For now, set to zero as this requires more complex state management
            metrics.realized_pnl = Decimal(0)
            metrics.total_pnl = metrics.realized_pnl + metrics.unrealized_pnl

            # Calculate changes (would need historical data for proper calculation)
            # For now, using placeholder logic
            metrics.pnl_change = Decimal(0)  # Would calculate from previous period
            metrics.pnl_change_pct = Decimal(0)  # Would calculate percentage change

        except (ServiceUnavailableError, InsufficientDataError):
            raise  # Re-raise service exceptions
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            raise PnLCalculationFailedError(e) from e

    async def _calculate_exposure_metrics(
        self, metrics: AggregatedMetrics, base_currency: str
    ) -> None:
        """Calculate exposure metrics."""
        if not self.exposure_calculator:
            raise PortfolioStateManagerUnavailableError

        if not self.portfolio_state_manager:
            raise PortfolioStateManagerUnavailableError

        try:
            # Get actual positions from portfolio state manager
            all_positions = await self._get_all_positions()
            self._validate_positions_data(all_positions)

            # Calculate position exposures using position exposure calculator
            position_exposures: list[PositionExposure] = []

            # Create PositionExposure objects for each position
            for position in all_positions:
                if position.size != Decimal(0) and position.entry_price is not None:
                    current_price = position.mark_price or position.entry_price
                    notional_value = abs(position.size) * current_price

                    # Create a simplified PositionExposure object
                    position_exposure = PositionExposure(
                        position_id=f"{position.exchange}_{position.symbol}",
                        exchange_id=position.exchange,
                        symbol=position.symbol,
                        side="LONG" if position.size > Decimal(0) else "SHORT",
                        size=position.size,
                        notional_value=notional_value,
                        market_value=notional_value,  # Simplified
                        gross_exposure=notional_value,
                        net_exposure=(
                            notional_value if position.size > Decimal(0) else -notional_value
                        ),
                        margin_requirement=Decimal(0),  # Placeholder
                        leverage=Decimal(1),  # Placeholder
                        liquidation_price=None,
                        distance_to_liquidation=None,
                        delta=Decimal(1),  # Simplified for futures
                        gamma=Decimal(0),
                        vega=Decimal(0),
                        theta=Decimal(0),
                    )
                    position_exposures.append(position_exposure)

            # Get total account value (sum of balance values)
            total_account_value = await self._calculate_total_account_value(base_currency)

            # Calculate portfolio exposure
            exposure_result = await self.exposure_calculator.calculate_portfolio_exposure(
                position_exposures, total_account_value
            )

            metrics.total_exposure = exposure_result.gross_exposure
            metrics.net_exposure = exposure_result.net_exposure
            metrics.gross_exposure = exposure_result.gross_exposure
            metrics.long_exposure = exposure_result.long_exposure
            metrics.short_exposure = exposure_result.short_exposure

        except (ServiceUnavailableError, InsufficientDataError):
            raise  # Re-raise service exceptions
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            raise ExposureCalculationFailedError(e) from e

    async def _calculate_performance_metrics(
        self, metrics: AggregatedMetrics, base_currency: str
    ) -> None:
        """Calculate performance metrics."""
        try:
            if self.performance_calculator and metrics.total_account_value > 0:
                # Create proper input for performance calculator

                # Create a simple portfolio value series based on current state
                # In reality, we'd need historical data for proper performance metrics
                current_value = metrics.total_account_value
                initial_value = current_value - metrics.total_pnl  # Estimate initial value

                if initial_value <= 0:
                    initial_value = current_value  # Avoid negative/zero initial value

                performance_input = PerformanceInput(
                    portfolio_values=[initial_value, current_value],
                    timestamps=[int(time.time() - 86400), int(time.time())],  # 24h period
                )

                performance_result = await self.performance_calculator.calculate(performance_input)

                if performance_result.success and performance_result.result:
                    result_metrics = performance_result.result
                    metrics.portfolio_return = result_metrics.total_return_percent
                    metrics.sharpe_ratio = result_metrics.sharpe_ratio or Decimal(0)
                    metrics.sortino_ratio = result_metrics.sortino_ratio or Decimal(0)
                    metrics.max_drawdown = result_metrics.max_drawdown_percent
                    metrics.volatility = result_metrics.volatility

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            logger.exception("performance_metrics_calculation_failed")

    async def _calculate_risk_metrics(self, metrics: AggregatedMetrics, base_currency: str) -> None:
        """Calculate risk metrics."""
        # Placeholder implementation
        metrics.var_95 = Decimal(0)
        metrics.var_99 = Decimal(0)
        metrics.expected_shortfall = Decimal(0)
        metrics.leverage_ratio = Decimal(0)
        metrics.concentration_risk = Decimal(0)

    async def _calculate_volume_metrics(
        self, metrics: AggregatedMetrics, base_currency: str
    ) -> None:
        """Calculate volume metrics."""
        if not self.portfolio_state_manager:
            raise PortfolioStateManagerUnavailableError

        # Volume metrics require trade history tracking which is not available
        # in the current PortfolioStateManager structure
        try:
            # Set placeholder values since trade history is not available
            metrics.trade_count = 0
            metrics.trading_volume = Decimal(0)
            metrics.avg_trade_size = Decimal(0)

            # Calculate turnover ratio (trading volume / total account value)
            total_account_value = await self._calculate_total_account_value(base_currency)
            if total_account_value > Decimal(0):
                metrics.turnover_ratio = metrics.trading_volume / total_account_value
            else:
                metrics.turnover_ratio = Decimal(0)

        except (ServiceUnavailableError, InsufficientDataError):
            raise  # Re-raise service exceptions
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            raise PnLCalculationFailedError(e) from e

    async def _calculate_allocation_metrics(
        self, metrics: AggregatedMetrics, base_currency: str
    ) -> None:
        """Calculate allocation metrics."""
        if not self.portfolio_state_manager:
            raise PortfolioStateManagerUnavailableError

        try:
            # Get all positions for allocation calculations
            all_positions = await self._get_all_positions()
            total_account_value = await self._calculate_total_account_value(base_currency)

            # Calculate allocations
            exchange_allocation = self._calculate_exchange_allocation(all_positions)
            symbol_allocation = self._calculate_symbol_allocation(all_positions)
            currency_allocation = await self._calculate_currency_allocation()

            # Convert to percentages
            self._convert_allocations_to_percentages(
                exchange_allocation, symbol_allocation, total_account_value
            )

            metrics.exchange_allocation = exchange_allocation
            metrics.symbol_allocation = symbol_allocation
            metrics.currency_allocation = currency_allocation

        except (ServiceUnavailableError, InsufficientDataError):
            raise  # Re-raise service exceptions
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            raise PnLCalculationFailedError(e) from e

    def _calculate_exchange_allocation(
        self, positions: list[DerivativePosition]
    ) -> dict[str, Decimal]:
        """Calculate allocation by exchange."""
        exchange_allocation: dict[str, Decimal] = {}

        for position in positions:
            exchange_id = position.exchange
            if position.entry_price is not None:
                position_value = abs(position.size) * position.entry_price
                exchange_allocation[exchange_id] = (
                    exchange_allocation.get(exchange_id, Decimal(0)) + position_value
                )

        return exchange_allocation

    def _calculate_symbol_allocation(
        self, positions: list[DerivativePosition]
    ) -> dict[str, Decimal]:
        """Calculate allocation by symbol."""
        symbol_allocation: dict[str, Decimal] = {}

        for position in positions:
            symbol = position.symbol
            if position.entry_price is not None:
                position_value = abs(position.size) * position.entry_price
                symbol_allocation[symbol] = (
                    symbol_allocation.get(symbol, Decimal(0)) + position_value
                )

        return symbol_allocation

    async def _calculate_currency_allocation(self) -> dict[str, Decimal]:
        """Calculate allocation by currency."""
        currency_allocation: dict[str, Decimal] = {}

        self._validate_balance_manager_available()

        if self.portfolio_state_manager is None:
            return currency_allocation

        # Get all balances using the proper method
        for exchange in [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]:
            try:
                balances = await self.portfolio_state_manager.get_balances(exchange)
                for asset, balance in balances.items():
                    # Use balance total_quantity as value (price conversion needed)
                    currency_allocation[asset] = (
                        currency_allocation.get(asset, Decimal(0)) + balance.total_quantity
                    )
            except (ValueError, TypeError, KeyError, AttributeError):
                # Skip if exchange not available
                logger.debug("Skipping exchange", exchange=exchange)
                continue

        return currency_allocation

    def _convert_allocations_to_percentages(
        self,
        exchange_allocation: dict[str, Decimal],
        symbol_allocation: dict[str, Decimal],
        total_account_value: Decimal,
    ) -> None:
        """Convert allocation values to percentages."""
        if total_account_value > Decimal(0):
            for exchange_id, value in exchange_allocation.items():
                exchange_allocation[exchange_id] = (value / total_account_value) * Decimal(100)

            total_position_value = sum(exchange_allocation.values())
            if total_position_value > Decimal(0):
                for symbol, value in symbol_allocation.items():
                    symbol_allocation[symbol] = (value / total_position_value) * Decimal(100)

    async def _calculate_correlation_metrics(
        self, metrics: AggregatedMetrics, base_currency: str
    ) -> None:
        """Calculate correlation metrics."""
        # Placeholder implementation
        metrics.portfolio_correlation = {}
        metrics.exchange_correlation = {}

    async def _calculate_liquidity_metrics(
        self, metrics: AggregatedMetrics, base_currency: str
    ) -> None:
        """Calculate liquidity metrics."""
        # Placeholder implementation
        metrics.liquidity_score = Decimal(0)
        metrics.bid_ask_spread = Decimal(0)
        metrics.market_impact = Decimal(0)

    async def _get_historical_metrics(
        self, period: AggregationPeriod, start_time: float, end_time: float, base_currency: str
    ) -> list[AggregatedMetrics]:
        """Get historical aggregated metrics."""
        # Placeholder implementation
        return []

    async def _calculate_trends(
        self, historical_metrics: list[AggregatedMetrics]
    ) -> dict[str, MetricsTrend]:
        """Calculate trend analysis."""
        # Placeholder implementation
        return {}

    async def _calculate_exchange_attribution(
        self, historical_metrics: list[AggregatedMetrics]
    ) -> dict[str, dict[str, Decimal]]:
        """Calculate performance attribution by exchange."""
        # Placeholder implementation
        return {}

    async def _calculate_symbol_attribution(
        self, historical_metrics: list[AggregatedMetrics]
    ) -> dict[str, dict[str, Decimal]]:
        """Calculate performance attribution by symbol."""
        # Placeholder implementation
        return {}

    async def _calculate_risk_analysis(
        self, historical_metrics: list[AggregatedMetrics]
    ) -> dict[str, Any]:
        """Calculate comprehensive risk analysis."""
        # Placeholder implementation
        return {}

    async def _generate_risk_alerts(
        self, current_metrics: AggregatedMetrics
    ) -> list[dict[str, Any]]:
        """Generate risk alerts based on current metrics."""
        # Placeholder implementation
        return []

    async def _calculate_data_quality(
        self, historical_metrics: list[AggregatedMetrics]
    ) -> dict[str, Any]:
        """Calculate data quality metrics."""
        # Placeholder implementation
        return {"quality_score": 1.0, "completeness": 1.0, "timeliness": 1.0}

    async def _get_all_positions(self) -> list[DerivativePosition]:
        """Get all positions from portfolio state manager."""
        if not self.portfolio_state_manager:
            raise PortfolioStateManagerUnavailableError

        try:
            all_positions: list[DerivativePosition] = []
            # Get positions from all exchanges using the proper method
            for exchange in [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]:
                try:
                    positions_dict = await self.portfolio_state_manager.get_positions(exchange)
                    # Only include non-zero positions
                    all_positions.extend(
                        position
                        for position in positions_dict.values()
                        if position.size != Decimal(0)
                    )
                except (ValueError, TypeError, KeyError, AttributeError):
                    # Skip if exchange not available
                    continue
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            raise InsufficientCalculationDataError(e) from e
        else:
            return all_positions

    async def _calculate_total_account_value(self, base_currency: str) -> Decimal:
        """Calculate total account value across all exchanges."""
        if not self.portfolio_state_manager:
            raise PortfolioStateManagerUnavailableError

        try:
            total_value = Decimal(0)
            # Get balances from all exchanges using the proper method
            for exchange in [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]:
                try:
                    balances = await self.portfolio_state_manager.get_balances(exchange)
                    for asset, balance in balances.items():
                        # For now, assume 1:1 conversion (currency conversion needed)
                        if asset == base_currency:
                            total_value += balance.total_quantity
                        else:
                            # Skip non-base currency assets until price service is implemented
                            logger.warning(
                                "currency_conversion_skipped",
                                asset=asset,
                                base_currency=base_currency,
                                exchange=exchange.value,
                                reason="price_service_not_implemented",
                            )
                except (ValueError, TypeError, KeyError, AttributeError):
                    # Skip if exchange not available
                    continue
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            raise InsufficientCalculationDataError(e) from e
        else:
            return total_value

    async def _calculate_coverage(
        self, historical_metrics: list[AggregatedMetrics]
    ) -> dict[str, float]:
        """Calculate coverage metrics."""
        # Placeholder implementation
        return {"temporal_coverage": 1.0, "exchange_coverage": 1.0, "symbol_coverage": 1.0}

    async def _generate_summary(self, report: MetricsReport) -> dict[str, Any]:
        """Generate report summary."""
        return {
            "report_type": "portfolio_metrics",
            "period": report.period.value,
            "data_points": len(report.historical_metrics),
            "trends_analyzed": len(report.trends),
            "risk_alerts": len(report.risk_alerts),
            "current_pnl": float(report.current_metrics.total_pnl),
            "current_exposure": float(report.current_metrics.total_exposure),
        }

    def _metrics_to_dict(self, metrics: AggregatedMetrics) -> dict[str, Any]:
        """Convert aggregated metrics to dictionary."""
        return {
            "timestamp": metrics.timestamp,
            "period": metrics.period.value,
            "base_currency": metrics.base_currency,
            "realized_pnl": float(metrics.realized_pnl),
            "unrealized_pnl": float(metrics.unrealized_pnl),
            "total_pnl": float(metrics.total_pnl),
            "total_exposure": float(metrics.total_exposure),
            "net_exposure": float(metrics.net_exposure),
            "sharpe_ratio": float(metrics.sharpe_ratio),
            "max_drawdown": float(metrics.max_drawdown),
            "volatility": float(metrics.volatility),
            "var_95": float(metrics.var_95),
            "leverage_ratio": float(metrics.leverage_ratio),
            "trading_volume": float(metrics.trading_volume),
            "trade_count": metrics.trade_count,
            "calculation_time_ms": metrics.calculation_time_ms,
            "data_quality_score": float(metrics.data_quality_score),
        }
