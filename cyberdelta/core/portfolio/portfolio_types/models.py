"""Portfolio data models and domain objects.

This module consolidates all data models from:
- domain_models.py
- portfolio_data_models.py
- portfolio_models.py
- data_transfer_objects.py (if exists)
- annotated_types.py (if exists)
- update_models.py
- models/*.py
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.core.models import Order, SpotBalance, Trade
from cyberdelta.core.symbols import Symbol


# ==================== Enums ====================

class PortfolioComponentType(Enum):
    """Types of portfolio components."""
    MANAGER = "manager"
    CALCULATOR = "calculator"
    SERVICE = "service"
    VALIDATOR = "validator"
    CACHE = "cache"
    EVENT_HANDLER = "event_handler"
    PERSISTENCE = "persistence"
    MONITORING = "monitoring"


class HealthStatus(Enum):
    """Component health status."""
    HEALTHY = "healthy"
    WARNING = "warning"
    ERROR = "error"
    UNKNOWN = "unknown"


# ==================== Core Portfolio Models ====================

class Position(BaseModel):
    """Position data model."""
    exchange: str
    symbol: Symbol
    size: Decimal
    entry_price: Decimal | None = None
    mark_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    realized_pnl: Decimal = Field(default=Decimal(0))
    side: str | None = None  # "long" or "short"
    leverage: Decimal | None = None
    liquidation_price: Decimal | None = None
    margin_used: Decimal | None = None
    timestamp: datetime | None = None


class PortfolioConfig(BaseModel):
    """Portfolio configuration."""
    base_currency: str = "USDC"
    enable_pnl_tracking: bool = True
    enable_exposure_monitoring: bool = True
    enable_risk_limits: bool = True
    max_leverage: Decimal = Decimal(10)
    max_position_size: Decimal | None = None
    max_exposure: Decimal | None = None
    update_interval_seconds: int = 60
    cache_ttl_seconds: int = 300
    enable_persistence: bool = True
    enable_event_logging: bool = True


# ==================== State Models ====================

class ExposureMetrics(BaseModel):
    """Portfolio exposure metrics."""
    total_exposure: Decimal
    long_exposure: Decimal = Decimal(0)
    short_exposure: Decimal = Decimal(0)
    net_exposure: Decimal = Decimal(0)
    gross_exposure: Decimal = Decimal(0)
    currency_exposures: dict[str, Decimal] = Field(default_factory=dict)
    position_count: int = 0
    leverage_ratio: Decimal | None = None
    max_leverage_used: Decimal | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PortfolioState(BaseModel):
    """Complete portfolio state snapshot."""
    portfolio_id: str
    total_capital: Decimal
    free_capital: Decimal = Decimal(0)
    positions: dict[str, list[Position]] = Field(default_factory=dict)
    balances: dict[str, dict[str, SpotBalance]] = Field(default_factory=dict)
    exposure_metrics: ExposureMetrics | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # P&L metrics
    total_realized_pnl: Decimal = Decimal(0)
    total_unrealized_pnl: Decimal = Decimal(0)
    daily_pnl: Decimal = Decimal(0)
    
    # Risk metrics
    portfolio_var_95: Decimal | None = None
    max_drawdown: Decimal | None = None
    sharpe_ratio: Decimal | None = None
    
    # Counts
    active_positions: int = 0
    open_orders: int = 0
    total_trades: int = 0
    
    # Exchange summaries
    exchange_summaries: dict[str, ExchangeSummary] = Field(default_factory=dict)
    
    # Component health
    component_health: dict[str, ComponentHealth] = Field(default_factory=dict)
    
    # Metadata
    metadata: dict[str, str | int | float | bool] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ExchangeSummary(BaseModel):
    """Exchange-specific portfolio summary."""
    model_config = ConfigDict(frozen=True)
    
    exchange_id: str
    account_value: Decimal = Decimal(0)
    collateral: Decimal = Decimal(0)
    margin_used: Decimal = Decimal(0)
    free_margin: Decimal = Decimal(0)
    
    # P&L
    realized_pnl: Decimal = Decimal(0)
    unrealized_pnl: Decimal = Decimal(0)
    
    # Positions
    position_count: int = 0
    long_positions: int = 0
    short_positions: int = 0
    
    # Orders
    open_orders: int = 0
    buy_orders: int = 0
    sell_orders: int = 0
    
    # Risk
    exposure: Decimal = Decimal(0)
    leverage: Decimal = Decimal(0)
    margin_ratio: Decimal = Decimal(0)
    
    # Balances by asset
    spot_balances: dict[str, Decimal] = Field(default_factory=dict)
    
    # Timestamp
    last_update: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ComponentHealth(BaseModel):
    """Component health information."""
    model_config = ConfigDict(frozen=True)
    
    component_type: PortfolioComponentType
    component_name: str
    status: HealthStatus
    last_update: float
    metrics: dict[str, str | int | float | bool] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    uptime_seconds: float | None = None
    last_error_time: float | None = None
    error_count: int = 0
    
    @property
    def is_healthy(self) -> bool:
        """Check if component is healthy."""
        return self.status == HealthStatus.HEALTHY


# ==================== Trading Session Models ====================

class TradingSession(BaseModel):
    """Trading session tracking."""
    model_config = ConfigDict(frozen=True)
    
    session_id: UUID = Field(default_factory=uuid4)
    start_time: float
    end_time: float | None = None
    
    # Starting values
    starting_account_value: Decimal = Decimal(0)
    starting_positions: int = 0
    
    # Current values
    current_account_value: Decimal = Decimal(0)
    current_positions: int = 0
    
    # Session metrics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    
    # P&L
    session_realized_pnl: Decimal = Decimal(0)
    session_unrealized_pnl: Decimal = Decimal(0)
    max_profit: Decimal = Decimal(0)
    max_loss: Decimal = Decimal(0)
    
    # Risk metrics
    max_exposure: Decimal = Decimal(0)
    max_leverage: Decimal = Decimal(0)
    max_drawdown: Decimal = Decimal(0)
    
    # Fee tracking
    total_fees_paid: Decimal = Decimal(0)
    fees_by_exchange: dict[str, Decimal] = Field(default_factory=dict)
    
    @property
    def win_rate(self) -> Decimal:
        """Calculate win rate."""
        if self.total_trades == 0:
            return Decimal(0)
        return Decimal(self.winning_trades) / Decimal(self.total_trades)
    
    @property
    def session_duration(self) -> float:
        """Get session duration in seconds."""
        if self.end_time is None:
            return 0.0
        return self.end_time - self.start_time
    
    @property
    def net_pnl(self) -> Decimal:
        """Calculate net P&L after fees."""
        return self.session_realized_pnl - self.total_fees_paid


# ==================== Update Models ====================

class BalanceUpdate(BaseModel):
    """Balance update event data."""
    exchange_id: str
    asset: str
    total_quantity: Decimal
    available_quantity: Decimal
    timestamp: datetime
    change_amount: Decimal | None = None
    update_reason: str | None = None


class PositionUpdate(BaseModel):
    """Position update event data."""
    exchange_id: str
    symbol: Symbol
    size: Decimal
    entry_price: Decimal | None
    mark_price: Decimal | None
    unrealized_pnl: Decimal | None
    timestamp: datetime
    change_size: Decimal | None = None
    update_reason: str | None = None


class OrderUpdate(BaseModel):
    """Order update event data."""
    exchange_id: str
    order_id: str
    symbol: Symbol
    side: str
    price: Decimal
    quantity: Decimal
    filled_quantity: Decimal = Decimal(0)
    status: str
    timestamp: datetime
    update_reason: str | None = None


class TradeUpdate(BaseModel):
    """Trade execution event data."""
    exchange_id: str
    trade_id: str
    order_id: str
    symbol: Symbol
    side: str
    price: Decimal
    quantity: Decimal
    fee: Decimal
    timestamp: datetime


class PortfolioUpdate(BaseModel):
    """Portfolio state update."""
    model_config = ConfigDict(frozen=True)
    
    update_id: UUID = Field(default_factory=uuid4)
    timestamp: float = Field(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    
    # What changed
    balance_updates: list[BalanceUpdate] = Field(default_factory=list)
    position_updates: list[PositionUpdate] = Field(default_factory=list)
    order_updates: list[OrderUpdate] = Field(default_factory=list)
    trade_updates: list[TradeUpdate] = Field(default_factory=list)
    
    # New metrics
    new_total_value: Decimal | None = None
    new_exposure: Decimal | None = None
    new_realized_pnl: Decimal | None = None
    new_unrealized_pnl: Decimal | None = None
    
    # Update metadata
    update_source: str | None = None
    correlation_id: str | None = None
    processing_time_ms: float | None = None


# ==================== Risk Models ====================

class RiskParameters(BaseModel):
    """Risk management parameters."""
    model_config = ConfigDict(frozen=True)
    
    # Position limits
    max_position_size: Decimal | None = None
    max_positions_per_symbol: int | None = None
    max_total_positions: int | None = None
    
    # Leverage limits
    max_leverage: Decimal = Decimal(10)
    max_portfolio_leverage: Decimal = Decimal(5)
    
    # Exposure limits
    max_gross_exposure: Decimal | None = None
    max_net_exposure: Decimal | None = None
    max_concentration_pct: Decimal = Decimal("0.3")  # 30% max in single asset
    
    # Loss limits
    max_daily_loss: Decimal | None = None
    max_drawdown: Decimal | None = None
    stop_loss_pct: Decimal | None = None
    
    # Margin requirements
    initial_margin_pct: Decimal = Decimal("0.1")  # 10%
    maintenance_margin_pct: Decimal = Decimal("0.05")  # 5%
    
    # Risk metrics thresholds
    min_sharpe_ratio: Decimal | None = None
    max_var_95: Decimal | None = None
    
    # Trading restrictions
    restricted_symbols: list[str] = Field(default_factory=list)
    allowed_exchanges: list[str] = Field(default_factory=list)
    
    @field_validator("max_leverage", "max_portfolio_leverage", mode="before")
    @classmethod
    def validate_leverage(cls, v: Decimal) -> Decimal:
        """Validate leverage is positive."""
        if v <= 0:
            raise ValueError("Leverage must be positive")
        return v


# ==================== Snapshot Models ====================

class PortfolioSnapshot(BaseModel):
    """Point-in-time portfolio snapshot."""
    model_config = ConfigDict(frozen=True)
    
    snapshot_id: UUID = Field(default_factory=uuid4)
    portfolio_id: str
    timestamp: float
    
    # Account values
    total_value: Decimal
    cash_balance: Decimal
    positions_value: Decimal
    
    # P&L snapshot
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    fees_paid: Decimal
    
    # Exposure snapshot
    gross_exposure: Decimal
    net_exposure: Decimal
    leverage: Decimal
    
    # Position details
    position_count: int
    positions_by_symbol: dict[str, Decimal] = Field(default_factory=dict)
    
    # Balance details
    balances_by_asset: dict[str, Decimal] = Field(default_factory=dict)
    
    # Risk metrics
    portfolio_var: Decimal | None = None
    sharpe_ratio: Decimal | None = None
    max_drawdown: Decimal | None = None
    
    # Performance
    return_pct: Decimal | None = None
    win_rate: Decimal | None = None
    
    # Health status
    is_healthy: bool = True
    health_issues: list[str] = Field(default_factory=list)
    
    # Metadata
    created_by: str | None = None
    snapshot_reason: str | None = None
    checksum: str | None = None


# ==================== Performance Models ====================

class PerformanceMetrics(BaseModel):
    """Portfolio performance metrics."""
    model_config = ConfigDict(frozen=True)
    
    # Returns
    total_return: Decimal
    daily_return: Decimal
    monthly_return: Decimal
    annual_return: Decimal
    
    # Risk-adjusted returns
    sharpe_ratio: Decimal | None = None
    sortino_ratio: Decimal | None = None
    calmar_ratio: Decimal | None = None
    
    # Risk metrics
    volatility: Decimal
    downside_deviation: Decimal | None = None
    max_drawdown: Decimal
    var_95: Decimal | None = None
    cvar_95: Decimal | None = None
    
    # Trading metrics
    win_rate: Decimal
    profit_factor: Decimal | None = None
    average_win: Decimal
    average_loss: Decimal
    largest_win: Decimal
    largest_loss: Decimal
    
    # Activity metrics
    total_trades: int
    winning_trades: int
    losing_trades: int
    avg_trade_duration: float | None = None
    
    # Costs
    total_fees: Decimal
    total_slippage: Decimal | None = None
    
    # Period
    start_date: datetime
    end_date: datetime
    trading_days: int


# ==================== Metadata Models ====================

class MetricsMetadata(BaseModel):
    """Typed metadata for metrics data."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    
    source: str | None = None
    aggregation_method: str | None = None
    sample_size: int | None = None
    confidence_level: float | None = None
    calculation_method: str | None = None
    data_quality_score: float | None = None
    alert_thresholds: dict[str, float] = Field(default_factory=dict)


class ValidationMetadata(BaseModel):
    """Typed metadata for validation context."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    
    validator_name: str | None = None
    rule_set: str | None = None
    severity_level: str | None = None
    error_category: str | None = None
    suggestion: str | None = None
    documentation_link: str | None = None
    related_fields: list[str] = Field(default_factory=list)


class OperationMetadata(BaseModel):
    """Typed metadata for operation context."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    
    user_id: str | None = None
    session_id: str | None = None
    request_id: str | None = None
    correlation_id: str | None = None
    environment: str | None = None
    service_version: str | None = None
    execution_context: dict[str, str] = Field(default_factory=dict)


class ConfigurationMetadata(BaseModel):
    """Typed metadata for configuration data."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    
    source: str | None = None
    last_modified: float | None = None
    modified_by: str | None = None
    environment: str | None = None
    validation_rules: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    migration_notes: str | None = None


# ==================== Domain Models ====================

class MetricsData(BaseModel):
    """Model for metrics data."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    
    metric_name: str
    value: float | int
    timestamp: float
    tags: dict[str, str] = Field(default_factory=dict)
    metadata: MetricsMetadata = Field(default_factory=MetricsMetadata)


class ErrorContext(BaseModel):
    """Model for error context information."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    
    error_code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
    traceback: str | None = None
    timestamp: float
    component: str | None = None
    operation: str | None = None
    retry_count: int = 0
    is_retryable: bool = False


class ValidationContext(BaseModel):
    """Model for validation context data."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    
    field_name: str | None = None
    validation_type: str
    expected_value: Any = None
    actual_value: Any = None
    constraints: dict[str, str | int | float | bool] = Field(default_factory=dict)
    metadata: ValidationMetadata = Field(default_factory=ValidationMetadata)
    timestamp: float


class OperationContext(BaseModel):
    """Context information for operations."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    
    operation_id: str
    operation_type: str
    start_time: float
    end_time: float | None = None
    duration_ms: float | None = None
    metadata: OperationMetadata = Field(default_factory=OperationMetadata)
    
    @property
    def duration_ms_computed(self) -> float | None:
        """Calculate duration if end_time is set."""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time) * 1000
        return self.duration_ms


class ConfigurationData(BaseModel):
    """Model for configuration data."""
    model_config = ConfigDict(extra="allow")  # Allow extra fields for flexibility
    
    name: str
    value: Any
    type: str | None = None
    description: str | None = None
    is_sensitive: bool = False
    metadata: ConfigurationMetadata = Field(default_factory=ConfigurationMetadata)


# ==================== Portfolio Data Models ====================

class PortfolioMetrics(BaseModel):
    """Aggregated portfolio metrics."""
    total_value: Decimal
    total_pnl: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    
    # Exposure metrics
    total_exposure: Decimal
    long_exposure: Decimal
    short_exposure: Decimal
    net_exposure: Decimal
    
    # Risk metrics
    leverage: Decimal
    margin_usage: Decimal
    var_95: Decimal | None = None
    max_drawdown: Decimal | None = None
    
    # Performance
    sharpe_ratio: Decimal | None = None
    win_rate: Decimal | None = None
    profit_factor: Decimal | None = None


class ExchangeBalances(BaseModel):
    """Exchange-specific balance information."""
    exchange_id: str
    spot_balances: dict[str, SpotBalance]
    margin_balances: dict[str, Decimal] = Field(default_factory=dict)
    total_value_usd: Decimal


class ExchangePositions(BaseModel):
    """Exchange-specific position information."""
    exchange_id: str
    positions: list[Position]
    total_exposure: Decimal
    unrealized_pnl: Decimal


class ExchangeOrders(BaseModel):
    """Exchange-specific order information."""
    exchange_id: str
    open_orders: list[Order]
    pending_value: Decimal
    order_count: int


# ==================== Summary Models ====================

class CapitalSummary(BaseModel):
    """Capital allocation summary."""
    total_capital: Decimal
    free_capital: Decimal
    used_capital: Decimal
    reserved_capital: Decimal
    
    by_exchange: dict[str, Decimal] = Field(default_factory=dict)
    by_asset: dict[str, Decimal] = Field(default_factory=dict)


class PnLSummary(BaseModel):
    """P&L summary breakdown."""
    total_pnl: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    fees_paid: Decimal
    net_pnl: Decimal
    
    by_exchange: dict[str, Decimal] = Field(default_factory=dict)
    by_symbol: dict[str, Decimal] = Field(default_factory=dict)
    by_date: dict[str, Decimal] = Field(default_factory=dict)


class PortfolioSummary(BaseModel):
    """High-level portfolio summary."""
    portfolio_id: str
    timestamp: datetime
    
    # Capital
    total_value: Decimal
    cash_balance: Decimal
    positions_value: Decimal
    
    # P&L
    total_pnl: Decimal
    daily_pnl: Decimal
    
    # Risk
    leverage: Decimal
    exposure: Decimal
    var_95: Decimal | None = None
    
    # Activity
    active_positions: int
    open_orders: int
    today_trades: int
    
    # Health
    is_healthy: bool
    warnings: list[str] = Field(default_factory=list)


# ==================== Manager Statistics ====================

class ManagerStats(BaseModel):
    """Manager performance statistics."""
    manager_name: str
    operations_count: int
    success_count: int
    error_count: int
    avg_response_time_ms: float
    last_operation_time: float | None = None
    uptime_seconds: float


# ==================== Statistics Models ====================

class ValidationStatistics(BaseModel):
    """Validation statistics data model."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    
    # Counters
    total_validations: int = Field(default=0)
    successful_validations: int = Field(default=0)
    failed_validations: int = Field(default=0)
    
    # Error tracking
    validation_errors: dict[str, int] = Field(default_factory=dict)
    error_count: int = Field(default=0)
    
    # Performance
    average_validation_time_ms: float = Field(default=0.0)
    max_validation_time_ms: float = Field(default=0.0)


class CacheStatistics(BaseModel):
    """Cache statistics data model."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    
    # Basic stats
    hits: int = Field(default=0)
    misses: int = Field(default=0)
    evictions: int = Field(default=0)
    expirations: int = Field(default=0)
    sets: int = Field(default=0)
    deletes: int = Field(default=0)
    
    # Derived metrics
    cache_size: int = Field(default=0)
    max_size: int = Field(default=0)
    hit_rate: float = Field(default=0.0)
    fill_ratio: float = Field(default=0.0)
    memory_usage_estimate: int = Field(default=0)


# ==================== Update Request Models ====================

class BalanceUpdateRequest(BaseModel):
    """Request to update balance."""
    exchange_id: str
    asset: str
    total_quantity: Decimal
    available_quantity: Decimal
    update_source: str | None = None


class PositionUpdateRequest(BaseModel):
    """Request to update position."""
    exchange_id: str
    symbol: Symbol
    size: Decimal
    entry_price: Decimal | None = None
    mark_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    update_source: str | None = None


class OrderUpdateRequest(BaseModel):
    """Request to update order."""
    exchange_id: str
    order: Order
    update_source: str | None = None