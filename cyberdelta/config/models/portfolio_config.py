"""Portfolio management configuration models.

This module contains Pydantic models for portfolio settings,
including cache, state management, validation, and calculation settings.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.models.config_types import ConfigDecimal, NonEmptyConfigString


class PerformanceMetricsConfig(BaseModel):
    """Performance metrics configuration."""
    
    model_config = ConfigDict(extra="forbid", frozen=True)
    
    enabled_metrics: dict[str, bool] = Field(
        default_factory=lambda: {
            "sharpe_ratio": True,
            "max_drawdown": True,
            "total_return": True,
            "win_rate": True,
            "profit_factor": True,
            "volatility": True,
        },
        description="Enable/disable specific performance metrics calculations"
    )
    calculation_period_days: int = Field(default=30, gt=0, le=365, description="Performance calculation period in days")
    risk_free_rate: float = Field(default=0.02, ge=0.0, le=1.0, description="Risk-free rate for Sharpe ratio calculation")
    include_fees_in_metrics: bool = Field(default=True, description="Include trading fees in performance metrics")
    sharpe_calculation_method: str = Field(default="daily", description="Sharpe ratio calculation method")
    drawdown_calculation_method: str = Field(default="peak_to_trough", description="Drawdown calculation method")


class PortfolioCacheSettings(BaseModel):
    """Portfolio cache configuration settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    max_size: int = Field(default=10000, gt=0, le=100000)
    default_ttl: float = Field(default=300.0, gt=0, le=3600)
    stale_while_revalidate: float = Field(default=60.0, gt=0, le=600)
    cleanup_interval: float = Field(default=600.0, gt=0, le=3600)
    enable_memory_optimization: bool = True
    cache_statistics_enabled: bool = True


class PortfolioStateSettings(BaseModel):
    """Portfolio state management settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # State persistence
    persist_interval: float = Field(default=60.0, gt=0, le=600)
    backup_count: int = Field(default=5, gt=0, le=20)
    backup_directory: NonEmptyConfigString = "data/portfolio_backups"

    # State update settings
    atomic_updates: bool = True
    update_timeout: float = Field(default=5.0, gt=0, le=30)
    max_concurrent_updates: int = Field(default=1, gt=0, le=10)

    # State validation
    validate_on_load: bool = True
    validate_on_update: bool = True
    strict_validation: bool = False

    # Reconciliation settings
    reconciliation_enabled: bool = True

    # Trading service integration
    update_on_execution: bool = True
    persist_on_trade: bool = True

    # State history
    max_state_history_size: int = Field(default=100, gt=0, le=1000)
    max_trade_history_size: int = Field(default=1000, gt=0, le=10000)


class PortfolioValidationSettings(BaseModel):
    """Portfolio validation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Balance validation
    enable_balance_validation: bool = True
    balance_tolerance: ConfigDecimal = Field(default=Decimal("0.0001"), gt=Decimal(0))
    require_non_negative_balances: bool = True

    # Position validation
    enable_position_validation: bool = True
    position_size_tolerance: ConfigDecimal = Field(default=Decimal("0.0001"), gt=Decimal(0))
    position_closure_threshold: ConfigDecimal = Field(
        default=Decimal("0.0000001"),
        gt=Decimal(0),
        description="Threshold below which a position is considered closed",
    )
    max_position_age_seconds: int = Field(default=300, gt=0)

    # Trade validation
    enable_trade_validation: bool = True
    max_trade_age_seconds: int = Field(default=86400, gt=0)  # 24 hours
    require_valid_timestamps: bool = True

    # Cross-validation
    enable_cross_validation: bool = True
    validation_timeout: float = Field(default=10.0, gt=0, le=60)

    # Price and quantity validation
    min_price: ConfigDecimal = Field(default=Decimal("0.0000001"), gt=Decimal(0))
    max_price: ConfigDecimal = Field(default=Decimal("1000000.0"), gt=Decimal(0))
    min_quantity: ConfigDecimal = Field(default=Decimal("0.00000001"), gt=Decimal(0))
    max_quantity: ConfigDecimal = Field(default=Decimal("1000000.0"), gt=Decimal(0))
    min_trade_value: ConfigDecimal = Field(default=Decimal("0.01"), gt=Decimal(0))
    max_trade_value: ConfigDecimal = Field(default=Decimal("10000000.0"), gt=Decimal(0))

    # Balance thresholds
    min_balance_threshold: ConfigDecimal = Field(default=Decimal("0.00001"), ge=Decimal(0))
    allow_negative_balances: bool = False

    # Position limits
    max_position_size: ConfigDecimal = Field(default=Decimal("100000.0"), gt=Decimal(0))
    max_leverage: ConfigDecimal = Field(default=Decimal("10.0"), gt=Decimal(0))
    max_position_value: ConfigDecimal = Field(default=Decimal("1000000.0"), gt=Decimal(0))

    # Other settings
    max_recent_issues: int = Field(default=100, gt=0)
    enabled: bool = True
    strict_mode: bool = False


class PortfolioCalculationSettings(BaseModel):
    """Portfolio calculation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # PnL calculation
    pnl_calculation_method: Literal["fifo", "lifo", "weighted_average"] = "weighted_average"
    include_fees_in_pnl: bool = True
    include_funding_in_pnl: bool = True
    base_currency: NonEmptyConfigString = "USD"

    # Exposure calculation
    max_exposure_calculation_depth: int = Field(default=100, gt=0, le=1000)
    group_by_base_asset: bool = True
    exposure_update_interval: float = Field(default=5.0, gt=0, le=60)

    # Performance metrics
    calculate_sharpe_ratio: bool = True
    sharpe_lookback_days: int = Field(default=30, gt=0, le=365)
    performance_period_days: int = Field(default=30, gt=0, le=365)
    calculate_max_drawdown: bool = True
    performance_update_interval: float = Field(default=300.0, gt=0, le=3600)
    
    # Performance metrics configuration
    performance_metrics: PerformanceMetricsConfig = Field(
        default_factory=PerformanceMetricsConfig,
        description="Performance metrics configuration"
    )
    risk_free_rate: ConfigDecimal = Field(default=Decimal("0.02"), ge=Decimal(0), le=Decimal("0.1"))

    # Exposure calculation settings
    default_volatility: ConfigDecimal = Field(
        default=Decimal("0.2"), gt=Decimal(0), le=Decimal("5.0")
    )
    stress_scenario_move: ConfigDecimal = Field(
        default=Decimal("0.1"), gt=Decimal(0), le=Decimal("1.0")
    )
    var_confidence_level: ConfigDecimal = Field(
        default=Decimal("0.95"), gt=Decimal("0.5"), lt=Decimal("1.0")
    )
    leverage_warning_threshold: ConfigDecimal = Field(
        default=Decimal("3.0"), gt=Decimal(0), le=Decimal("50.0")
    )

    # PnL calculation
    realized_pnl_method: Literal["fifo", "lifo", "weighted_average"] = "weighted_average"

    # Price service settings
    price_cache_ttl: int = Field(default=60, gt=0, le=3600)
    batch_size_limit: int = Field(default=100, gt=0, le=1000)
    price_staleness_threshold: int = Field(default=300, gt=0, le=3600)
