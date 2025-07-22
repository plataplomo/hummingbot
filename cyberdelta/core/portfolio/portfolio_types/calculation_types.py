"""Type definitions for portfolio calculations."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class CalculationMetadata(BaseModel):
    """Metadata for calculation results."""

    model_config = ConfigDict(frozen=True)

    calculation_timestamp: float = 0.0
    calculation_method: str = ""
    data_source: str = ""
    notes: str = ""


class RealizedPnLResult(BaseModel):
    """Result of realized P&L calculation."""

    model_config = ConfigDict(frozen=True)

    pnl: Decimal
    position_size_change: Decimal
    average_entry_price: Decimal | None
    calculation_method: str
    metadata: CalculationMetadata | None = None


class UnrealizedPnLResult(BaseModel):
    """Result of unrealized P&L calculation."""

    model_config = ConfigDict(frozen=True)

    pnl: Decimal
    current_price: Decimal
    entry_price: Decimal
    position_size: Decimal
    currency: str
    metadata: CalculationMetadata | None = None


class PortfolioUnrealizedPnLResult(BaseModel):
    """Result of portfolio-wide unrealized P&L calculation."""

    model_config = ConfigDict(frozen=True)

    total_pnl: Decimal
    currency: str
    position_results: list[UnrealizedPnLResult]
    metadata: CalculationMetadata | None = None


class ExposureResult(BaseModel):
    """Result of exposure calculation."""

    model_config = ConfigDict(frozen=True)

    long_exposure: Decimal
    short_exposure: Decimal
    net_exposure: Decimal
    gross_exposure: Decimal
    currency: str
    metadata: CalculationMetadata | None = None


class PortfolioExposureResult(BaseModel):
    """Result of portfolio-wide exposure calculation."""

    model_config = ConfigDict(frozen=True)

    total_long_exposure: Decimal
    total_short_exposure: Decimal
    total_net_exposure: Decimal
    total_gross_exposure: Decimal
    currency: str
    by_symbol: dict[str, ExposureResult]
    by_exchange: dict[str, ExposureResult]
    metadata: CalculationMetadata | None = None


class PerformanceMetrics(BaseModel):
    """Portfolio performance metrics."""

    model_config = ConfigDict(frozen=True)

    total_pnl: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_return_pct: Decimal
    sharpe_ratio: Decimal | None
    max_drawdown: Decimal | None
    currency: str
    calculation_timestamp: float
    metadata: CalculationMetadata | None = None


class DrawdownMetrics(BaseModel):
    """Drawdown calculation metrics."""

    model_config = ConfigDict(frozen=True)

    current_drawdown: Decimal
    max_drawdown: Decimal
    max_drawdown_duration_seconds: float | None
    recovery_factor: Decimal | None
    metadata: CalculationMetadata | None = None


class CalculatorConfiguration(BaseModel):
    """Base configuration for all calculators."""

    model_config = ConfigDict(frozen=True)

    name: str = "Calculator"
    precision: int = 8
    currency: str = "USD"
    enabled: bool = True
    cache_results: bool = False


class UnrealizedPnLConfiguration(CalculatorConfiguration):
    """Configuration for unrealized P&L calculator."""

    name: str = "UnrealizedPnLCalculator"
    use_mark_to_market: bool = True
    price_staleness_threshold_seconds: int = 300
    fallback_to_last_price: bool = True


class PnLAggregatorConfiguration(CalculatorConfiguration):
    """Configuration for P&L aggregator."""

    name: str = "PnLAggregator"
    max_trade_history_size: int = 1000
    trade_history_retention_size: int = 500
    min_data_points_for_metrics: int = 2
    calculate_sharpe_ratio: bool = True
    calculate_max_drawdown: bool = True


class PortfolioSummaryMetrics(BaseModel):
    """Comprehensive portfolio summary with all metrics."""

    model_config = ConfigDict(frozen=True)

    # Core performance metrics
    performance_metrics: PerformanceMetrics
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_pnl: Decimal
    total_return_pct: Decimal
    sharpe_ratio: Decimal | None
    max_drawdown: Decimal | None

    # Portfolio composition
    currency: str
    position_count: int
    trade_count: int
    calculation_timestamp: float

    # Breakdown metrics
    breakdown: BreakdownMetrics


class BreakdownMetrics(BaseModel):
    """Breakdown metrics by exchange, symbol, and position type."""

    model_config = ConfigDict(frozen=True)

    by_exchange: dict[str, ExchangeBreakdown]
    by_symbol: dict[str, UnrealizedPnLResult]
    long_positions: list[UnrealizedPnLResult]
    short_positions: list[UnrealizedPnLResult]


class ExchangeBreakdown(BaseModel):
    """Breakdown metrics for a specific exchange."""

    model_config = ConfigDict(frozen=True)

    positions: list[UnrealizedPnLResult]
    total_pnl: Decimal
    position_count: int


class RealizedPnLSummaryMetrics(BaseModel):
    """Summary metrics for realized P&L from trade history."""

    model_config = ConfigDict(frozen=True)

    cumulative_realized_pnl: Decimal
    trade_count: int
    winning_trades: int
    losing_trades: int
    average_win: Decimal
    average_loss: Decimal
    win_rate: Decimal
    profit_factor: Decimal | None
