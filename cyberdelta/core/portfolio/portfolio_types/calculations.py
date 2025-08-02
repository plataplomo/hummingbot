"""Financial calculation types and results.

This module consolidates all calculation-related types from:
- calculation_types.py
- result_types.py
- Financial calculation inputs/outputs
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from .models import Position
from cyberdelta.core.symbols import Symbol


# Type variables
T = TypeVar("T")
E = TypeVar("E")


# ==================== Calculation Metadata ====================

class CalculationMetadata(BaseModel):
    """Metadata for calculation results."""
    model_config = ConfigDict(frozen=True)
    
    calculation_timestamp: float = 0.0
    calculation_method: str = ""
    data_source: str = ""
    notes: str = ""
    precision: int = 8
    confidence_level: float | None = None


# ==================== P&L Calculation Types ====================

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


# ==================== Exposure Calculation Types ====================

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
    by_symbol: dict[Symbol, ExposureResult]
    by_exchange: dict[str, ExposureResult]
    metadata: CalculationMetadata | None = None


# ==================== Performance Metrics ====================

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


# ==================== Calculator Configuration ====================

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


# ==================== Summary Metrics ====================

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
    by_symbol: dict[Symbol, UnrealizedPnLResult]
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


# ==================== Calculation Inputs ====================

class PerformanceInput(BaseModel):
    """Input for performance calculations."""
    positions: list[Position]
    balances: dict[str, Any]  # Will be SpotBalance type
    base_currency: str = "USDC"
    include_fees: bool = True
    period_start: float | None = None
    period_end: float | None = None


class ExposureInput(BaseModel):
    """Input for exposure calculations."""
    positions: list[Position]
    valuation_currency: str = "USDC"
    include_pending_orders: bool = False
    price_data: dict[str, Decimal] | None = None


class RiskInput(BaseModel):
    """Input for risk calculations."""
    positions: list[Position]
    historical_returns: list[Decimal] | None = None
    confidence_level: float = 0.95
    time_horizon_days: int = 1
    use_parametric_var: bool = True


# ==================== Calculation Results ====================

class PerformanceResult(BaseModel):
    """Performance calculation result."""
    total_capital: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    high_watermark: Decimal | None = None
    drawdown: Decimal | None = None
    sharpe_ratio: Decimal | None = None
    sortino_ratio: Decimal | None = None
    calmar_ratio: Decimal | None = None
    return_metrics: dict[str, Decimal] = Field(default_factory=dict)


class RiskResult(BaseModel):
    """Risk calculation result."""
    var_95: Decimal
    cvar_95: Decimal | None = None
    volatility: Decimal
    beta: Decimal | None = None
    correlation_matrix: dict[str, dict[str, Decimal]] | None = None
    stress_test_results: dict[str, Decimal] | None = None


# ==================== Financial Types ====================

class PnLBreakdown(BaseModel):
    """Profit and loss breakdown."""
    realized: Decimal
    unrealized: Decimal
    total: Decimal
    by_exchange: dict[str, Decimal]
    by_symbol: dict[Symbol, Decimal]
    by_strategy: dict[str, Decimal] | None = None
    fees_paid: Decimal = Decimal(0)
    net_pnl: Decimal = Decimal(0)


class CapitalAllocation(BaseModel):
    """Capital allocation breakdown."""
    total_capital: Decimal
    allocated_capital: Decimal
    free_capital: Decimal
    margin_used: Decimal
    by_exchange: dict[str, Decimal]
    by_strategy: dict[str, Decimal] | None = None
    utilization_pct: Decimal


class FeeBreakdown(BaseModel):
    """Fee breakdown by type and exchange."""
    total_fees: Decimal
    trading_fees: Decimal
    funding_fees: Decimal | None = None
    withdrawal_fees: Decimal | None = None
    by_exchange: dict[str, Decimal]
    by_type: dict[str, Decimal]
    as_percentage_of_volume: Decimal | None = None


# ==================== Result Types (from result_types.py) ====================

class Result[T, E](BaseModel):
    """Generic result type for operations that can succeed or fail."""
    model_config = ConfigDict(frozen=True)
    
    value: T | None = Field(default=None)
    error: E | None = Field(default=None)
    is_success: bool = Field(default=True)
    
    @classmethod
    def ok(cls, value: T) -> Result[T, E]:
        """Create a successful result."""
        return cls(value=value, error=None, is_success=True)
    
    @classmethod
    def err(cls, error: E) -> Result[T, E]:
        """Create an error result."""
        return cls(value=None, error=error, is_success=False)
    
    def is_ok(self) -> bool:
        """Check if result is successful."""
        return self.is_success
    
    def is_err(self) -> bool:
        """Check if result is an error."""
        return not self.is_success
    
    def unwrap(self) -> T:
        """Get the value, raising if error."""
        if self.is_success and self.value is not None:
            return self.value
        raise ValueError("Called unwrap on an error result")
    
    def unwrap_err(self) -> E:
        """Get the error, raising if ok."""
        if not self.is_success and self.error is not None:
            return self.error
        raise ValueError("Called unwrap_err on an ok result")
    
    def unwrap_or(self, default: T) -> T:
        """Get the value or return default."""
        if self.is_success and self.value is not None:
            return self.value
        return default
    
    def map[U](self, f: Any) -> Result[U, E]:
        """Map the value if ok."""
        if self.is_success and self.value is not None:
            return Result.ok(f(self.value))
        return Result.err(self.error)  # type: ignore
    
    def map_err[F](self, f: Any) -> Result[T, F]:
        """Map the error if err."""
        if not self.is_success and self.error is not None:
            return Result.err(f(self.error))
        return Result.ok(self.value)  # type: ignore


class PortfolioResultError(BaseModel):
    """Error type for portfolio operations."""
    model_config = ConfigDict(frozen=True)
    
    code: str
    message: str
    details: dict[str, Any] | None = None
    timestamp: float = Field(default_factory=lambda: 0.0)
    component: str | None = None
    operation: str | None = None
    recoverable: bool = False
    retry_after: float | None = None


class OperationMetrics(BaseModel):
    """Metrics for an operation."""
    model_config = ConfigDict(frozen=True)
    
    start_time: float
    end_time: float
    duration_ms: float
    success: bool
    retry_count: int = 0
    error_count: int = 0
    
    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        return self.duration_ms / 1000


class AsyncOperationResult[T](BaseModel):
    """Result of an async operation with metrics."""
    model_config = ConfigDict(frozen=True)
    
    result: Result[T, PortfolioResultError]
    metrics: OperationMetrics
    correlation_id: str | None = None
    
    def is_success(self) -> bool:
        """Check if operation succeeded."""
        return self.result.is_ok()
    
    def get_value(self) -> T | None:
        """Get the value if successful."""
        if self.result.is_ok():
            return self.result.unwrap()
        return None
    
    def get_error(self) -> PortfolioResultError | None:
        """Get the error if failed."""
        if self.result.is_err():
            return self.result.unwrap_err()
        return None


# ==================== Calculation Specific Results ====================

class VaRCalculationResult(BaseModel):
    """Value at Risk calculation result."""
    model_config = ConfigDict(frozen=True)
    
    var_95: Decimal
    var_99: Decimal | None = None
    cvar_95: Decimal | None = None
    cvar_99: Decimal | None = None
    methodology: Literal["historical", "parametric", "monte_carlo"]
    confidence_level: float
    time_horizon_days: int
    sample_size: int | None = None
    metadata: CalculationMetadata | None = None


class CorrelationResult(BaseModel):
    """Correlation calculation result."""
    model_config = ConfigDict(frozen=True)
    
    correlation_matrix: dict[str, dict[str, Decimal]]
    period_days: int
    data_points: int
    methodology: str
    metadata: CalculationMetadata | None = None


class OptimizationResult(BaseModel):
    """Portfolio optimization result."""
    model_config = ConfigDict(frozen=True)
    
    optimal_weights: dict[str, Decimal]
    expected_return: Decimal
    expected_risk: Decimal
    sharpe_ratio: Decimal
    constraints_satisfied: bool
    optimization_method: str
    iterations: int | None = None
    metadata: CalculationMetadata | None = None


# ==================== Aggregation Results ====================

class AggregatedMetrics(BaseModel):
    """Aggregated metrics across multiple calculations."""
    model_config = ConfigDict(frozen=True)
    
    # P&L aggregation
    total_pnl: PnLBreakdown
    
    # Exposure aggregation
    total_exposure: PortfolioExposureResult
    
    # Risk aggregation
    portfolio_risk: RiskResult
    
    # Performance
    performance: PerformanceMetrics
    
    # Capital
    capital: CapitalAllocation
    
    # Fees
    fees: FeeBreakdown
    
    # Metadata
    calculation_timestamp: float
    aggregation_period_seconds: float
    data_completeness_pct: float