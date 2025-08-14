"""Financial calculation configuration models.

This module contains Pydantic models for financial calculations including
PnL calculations, fee calculations, position sizing, performance metrics,
currency operations, and risk analytics.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.models.config_types import ConfigDecimal, NonEmptyConfigString


class PnLCalculationConfig(BaseModel):
    """PnL calculation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # PnL calculation method
    calculation_method: Literal["mark_to_market", "fifo", "lifo", "weighted_average"] = (
        "mark_to_market"
    )
    include_fees_in_pnl: bool = Field(
        default=True, description="Include trading fees in PnL calculations"
    )
    include_funding_in_pnl: bool = Field(
        default=True, description="Include funding payments in PnL calculations"
    )

    # Realized PnL calculation method
    realized_pnl_method: Literal["fifo", "lifo", "weighted_average"] = "weighted_average"

    # Base currency for reporting
    base_currency: NonEmptyConfigString = Field(
        default="USD", description="Base currency for PnL reporting"
    )


class FeeCalculationConfig(BaseModel):
    """Fee calculation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Fee calculation method
    calculation_method: Literal["exchange_reported", "estimated", "hybrid"] = "exchange_reported"

    # Default fee rates for estimation (when exchange doesn't report fees)
    default_maker_fee_rate: ConfigDecimal = Field(
        default=Decimal("0.0010"), ge=Decimal(0), description="Default maker fee rate"
    )
    default_taker_fee_rate: ConfigDecimal = Field(
        default=Decimal("0.0015"), ge=Decimal(0), description="Default taker fee rate"
    )

    # Fee estimation fallback behavior
    enable_fee_estimation: bool = Field(
        default=True, description="Enable fee estimation when exchange doesn't report fees"
    )


class CurrencyConfig(BaseModel):
    """Currency handling configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Supported currencies
    supported_currencies: list[str] = Field(
        default_factory=lambda: ["USD", "USDC", "BTC", "ETH", "SOL"],
        description="List of supported currencies for calculations",
    )

    # Base currency for all calculations
    base_currency: NonEmptyConfigString = Field(default="USD", description="Primary base currency")

    # Currency conversion settings
    enable_currency_conversion: bool = Field(
        default=True, description="Enable cross-currency calculations and conversions"
    )

    # Exchange rate settings
    exchange_rate_cache_ttl: int = Field(
        default=300, gt=0, le=3600, description="Exchange rate cache TTL in seconds"
    )
    exchange_rate_tolerance: ConfigDecimal = Field(
        default=Decimal("0.05"),
        gt=Decimal(0),
        le=Decimal("0.5"),
        description="Maximum acceptable exchange rate age in decimal hours",
    )

    # Stablecoin handling
    treat_stablecoins_as_usd: bool = Field(
        default=False, description="Whether to treat USDC, USDT, etc. as equivalent to USD"
    )
    stablecoin_list: list[str] = Field(
        default_factory=lambda: ["USDC", "USDT", "BUSD", "DAI"],
        description="List of currencies considered stablecoins",
    )


class PrecisionConfig(BaseModel):
    """Precision and rounding configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Calculation precision (internal)
    calculation_precision: int = Field(
        default=18, ge=8, le=28, description="Decimal precision for internal calculations"
    )

    # Display precision per currency type
    display_precision: dict[str, int] = Field(
        default_factory=lambda: {
            "USD": 2,
            "USDC": 6,
            "BTC": 8,
            "ETH": 6,
            "SOL": 4,
        },
        description="Display precision per currency",
    )

    # Rounding modes for different operations
    pnl_rounding_mode: Literal["ROUND_HALF_UP", "ROUND_HALF_DOWN", "ROUND_DOWN"] = "ROUND_HALF_UP"
    fee_rounding_mode: Literal["ROUND_HALF_UP", "ROUND_HALF_DOWN", "ROUND_UP"] = "ROUND_UP"
    quantity_rounding_mode: Literal["ROUND_HALF_UP", "ROUND_HALF_DOWN", "ROUND_DOWN"] = "ROUND_DOWN"


class PerformanceMetricsConfig(BaseModel):
    """Performance metrics calculation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Enabled metrics
    enabled_metrics: dict[str, bool] = Field(
        default_factory=lambda: {
            "total_pnl": True,
            "realized_pnl": True,
            "unrealized_pnl": True,
            "total_return": True,
            "sharpe_ratio": True,
            "sortino_ratio": True,
            "max_drawdown": True,
            "win_rate": True,
            "profit_factor": True,
            "volatility": True,
            "daily_return": False,
            "weekly_return": False,
            "monthly_return": True,
            "yearly_return": True,
            "beta": False,
            "alpha": False,
        },
        description="Enable/disable specific performance metrics calculations",
    )

    # Calculation periods
    calculation_period_days: int = Field(
        default=30, gt=0, le=365, description="Default performance calculation period in days"
    )

    # Risk-free rate for Sharpe/Sortino calculations
    risk_free_rate: ConfigDecimal = Field(
        default=Decimal("0.02"),
        ge=Decimal(0),
        le=Decimal("0.20"),
        description="Risk-free rate for risk-adjusted returns",
    )

    # Calculation methods
    sharpe_calculation_method: Literal["daily", "annualized"] = Field(
        default="daily", description="Sharpe ratio calculation method"
    )
    drawdown_calculation_method: Literal["peak_to_trough", "underwater"] = Field(
        default="peak_to_trough", description="Drawdown calculation method"
    )

    # Fee inclusion in metrics
    include_fees_in_metrics: bool = Field(
        default=True, description="Include trading fees in performance metrics"
    )

    # Update intervals
    performance_update_interval: int = Field(
        default=300, gt=0, le=3600, description="Performance metrics update interval in seconds"
    )

    # Historical data retention
    max_equity_curve_days: int = Field(
        default=365, gt=0, le=1000, description="Maximum days to retain equity curve data"
    )


class RiskAnalyticsConfig(BaseModel):
    """Risk analytics configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # VaR (Value at Risk) configuration
    var_confidence_level: ConfigDecimal = Field(
        default=Decimal("0.95"),
        gt=Decimal("0.5"),
        lt=Decimal("1.0"),
        description="VaR confidence level",
    )
    var_calculation_method: Literal["historical", "parametric", "monte_carlo"] = "historical"
    var_lookback_days: int = Field(
        default=30, gt=0, le=365, description="VaR calculation lookback period in days"
    )

    # Exposure calculation
    enable_exposure_analytics: bool = Field(
        default=True, description="Enable portfolio exposure analytics"
    )
    max_correlation_threshold: ConfigDecimal = Field(
        default=Decimal("0.8"),
        gt=Decimal(0),
        le=Decimal("1.0"),
        description="Maximum allowed correlation between positions",
    )

    # Stress testing
    enable_stress_testing: bool = Field(default=True, description="Enable stress testing scenarios")
    stress_scenario_moves: dict[str, ConfigDecimal] = Field(
        default_factory=lambda: {
            "mild": Decimal("0.05"),  # 5% move
            "moderate": Decimal("0.10"),  # 10% move
            "severe": Decimal("0.20"),  # 20% move
            "extreme": Decimal("0.50"),  # 50% move
        },
        description="Stress test scenario price moves",
    )

    # Default volatility for new instruments
    default_volatility: ConfigDecimal = Field(
        default=Decimal("0.20"), gt=Decimal(0), le=Decimal("5.0"), description="Default volatility"
    )


class FinancialCalculationConfig(BaseModel):
    """Comprehensive financial calculation configuration.

    This configuration consolidates all financial calculation settings that were
    previously scattered across portfolio, risk, and monitoring configurations.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Core calculation configurations
    pnl: PnLCalculationConfig = Field(
        default_factory=lambda: PnLCalculationConfig(),
        description="PnL calculation settings",  # noqa: PLW0108
    )

    fees: FeeCalculationConfig = Field(
        default_factory=lambda: FeeCalculationConfig(),
        description="Fee calculation settings",  # noqa: PLW0108
    )

    currency: CurrencyConfig = Field(
        default_factory=lambda: CurrencyConfig(),
        description="Currency handling settings",  # noqa: PLW0108
    )

    precision: PrecisionConfig = Field(
        default_factory=lambda: PrecisionConfig(),
        description="Precision and rounding settings",  # noqa: PLW0108
    )

    performance: PerformanceMetricsConfig = Field(
        default_factory=lambda: PerformanceMetricsConfig(),  # noqa: PLW0108
        description="Performance metrics settings",
    )

    risk_analytics: RiskAnalyticsConfig = Field(
        default_factory=lambda: RiskAnalyticsConfig(),
        description="Risk analytics settings",  # noqa: PLW0108
    )

    # Global financial settings
    enable_financial_logging: bool = Field(
        default=True, description="Enable detailed logging of financial calculations"
    )

    validation_enabled: bool = Field(
        default=True, description="Enable validation of financial calculation inputs and outputs"
    )

    # Calculation timeouts
    calculation_timeout_seconds: int = Field(
        default=30, gt=0, le=300, description="Timeout for complex financial calculations"
    )

    # Caching settings
    enable_calculation_caching: bool = Field(
        default=True, description="Enable caching of financial calculation results"
    )
    calculation_cache_ttl: int = Field(
        default=60, gt=0, le=3600, description="Financial calculation cache TTL in seconds"
    )
