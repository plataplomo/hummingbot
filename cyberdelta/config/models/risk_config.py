"""Risk management configuration models.

This module contains Pydantic models for risk management settings,
including position sizing, risk limits, and validation thresholds.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.config.models.config_types import ConfigDecimal
from cyberdelta.utils.parsing import validate_enum_field


if TYPE_CHECKING:
    pass


class GlobalRiskSettings(BaseModel):
    """Global risk management settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_position_usd: ConfigDecimal = Field(..., gt=Decimal(0))
    max_total_exposure_usd: ConfigDecimal = Field(..., gt=Decimal(0))

    # Drawdown monitoring settings
    max_drawdown_pct: ConfigDecimal = Field(default=Decimal("20.0"), gt=Decimal(0), le=Decimal(100))
    drawdown_lookback_days: int = Field(default=30, gt=0, le=365)
    drawdown_warning_pct: ConfigDecimal = Field(
        default=Decimal("15.0"), gt=Decimal(0), le=Decimal(100)
    )
    drawdown_check_interval_sec: float = Field(default=300.0, gt=0, le=3600)


class CheckerThresholds(BaseModel):
    """Complete strongly typed checker thresholds covering all risk module needs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Profitability thresholds
    min_profitability: ConfigDecimal = Field(default=Decimal("0.001"), gt=Decimal(0))

    # Price sanity thresholds
    max_price_deviation: ConfigDecimal = Field(default=Decimal("0.1"), gt=Decimal(0), le=Decimal(1))
    max_price_spread: ConfigDecimal = Field(default=Decimal("0.05"), gt=Decimal(0), le=Decimal(1))
    min_price: ConfigDecimal = Field(default=Decimal("0.0000001"), gt=Decimal(0))
    max_price: ConfigDecimal = Field(default=Decimal(1000000), gt=Decimal(0))
    outlier_z_score_threshold: float = Field(default=3.0, gt=0, le=10)

    # Signal confidence thresholds
    min_signal_confidence: ConfigDecimal = Field(
        default=Decimal("0.7"), ge=Decimal(0), le=Decimal(1)
    )

    # Funding rate thresholds
    min_funding_rate: ConfigDecimal = Field(
        default=Decimal("-0.01"), ge=Decimal(-1), le=Decimal(0)
    )  # -1%
    max_funding_rate: ConfigDecimal = Field(default=Decimal("0.01"), gt=Decimal(0), le=Decimal(1))
    max_funding_rate_spread: ConfigDecimal = Field(default=Decimal("0.005"), gt=Decimal(0))
    max_funding_rate_volatility: ConfigDecimal = Field(
        default=Decimal("0.002"), gt=Decimal(0), le=Decimal(1)
    )  # 0.2%
    min_funding_rate_confidence: ConfigDecimal = Field(
        default=Decimal("0.7"), ge=Decimal(0), le=Decimal(1)
    )  # 70%

    # Volatility thresholds
    max_volatility: ConfigDecimal = Field(default=Decimal("0.2"), gt=Decimal(0), le=Decimal(2))
    min_volatility: ConfigDecimal = Field(default=Decimal("0.001"), gt=Decimal(0))

    # Balance thresholds
    min_balance_ratio: ConfigDecimal = Field(default=Decimal("0.1"), gt=Decimal(0), le=Decimal(1))

    @model_validator(mode="after")
    def validate_threshold_relationships(self) -> Self:
        """Validate logical relationships between thresholds.

        Returns:
            Self instance after validation

        Raises:
            ValueError: If threshold relationships are invalid
        """
        if self.min_profitability >= self.max_price_spread:
            msg = "min_profitability must be less than max_price_spread"
            raise ValueError(msg)
        if self.min_price >= self.max_price:
            msg = "min_price must be less than max_price"
            raise ValueError(msg)
        return self


class CheckerSettings(BaseModel):
    """Enhanced checker configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Enable/disable flags
    enable_required_fields: bool = True
    enable_symbol_validation: bool = True
    enable_confidence_check: bool = True
    enable_profitability: bool = True
    enable_circuit_breaker: bool = True
    enable_price_sanity: bool = True
    enable_funding_rate: bool = True
    enable_volatility: bool = True
    enable_balance: bool = True

    # Thresholds
    thresholds: CheckerThresholds = Field(default_factory=CheckerThresholds)

    # Pipeline configuration
    fail_fast: bool = True
    max_concurrent_checks: int = Field(default=5, gt=0, le=20)
    check_timeout_seconds: float = Field(default=5.0, gt=0, le=60)

    # Lookback periods
    funding_rate_lookback_hours: int = Field(default=24, gt=0, le=168)
    volatility_lookback_hours: int = Field(default=24, gt=0, le=168)

    # Feature flags
    include_fees_in_profitability: bool = True
    enable_outlier_detection: bool = True
    check_both_exchanges: bool = True

    # Funding rate specific settings
    enable_funding_rate_stability_check: bool = True
    require_primary_funding_source: bool = True

    # Extensibility (preserve from current CheckConfig)
    extra_config: dict[str, Any] | None = None


class RiskLimitsSettings(BaseModel):
    """Risk limits configuration for position and concentration management."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Position count limits
    max_positions_per_symbol: int = Field(default=5, gt=0, le=100)
    max_positions_total: int = Field(default=20, gt=0, le=1000)
    max_positions_per_exchange: int = Field(default=10, gt=0, le=100)

    # Concentration limits (as percentages)
    max_concentration_per_symbol: ConfigDecimal = Field(
        default=Decimal("25.0"), gt=Decimal(0), le=Decimal(100)
    )
    max_concentration_per_asset_class: ConfigDecimal = Field(
        default=Decimal("50.0"), gt=Decimal(0), le=Decimal(100)
    )


class SizingSettings(BaseModel):
    """Enhanced sizing configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Method selection
    method: Literal["kelly", "simple"] = "simple"

    # Kelly criterion parameters
    kelly_multiplier: ConfigDecimal = Field(default=Decimal("0.25"), gt=Decimal(0), le=Decimal(1))
    kelly_max_allocation: ConfigDecimal = Field(
        default=Decimal("0.1"), gt=Decimal(0), le=Decimal(1)
    )
    kelly_min_allocation: ConfigDecimal = Field(
        default=Decimal("0.01"), gt=Decimal(0), le=Decimal(1)
    )
    kelly_risk_free_rate: float = Field(default=0.02, ge=0, le=1)  # Annual rate

    # Simple sizing parameters
    simple_method: Literal["fixed_usd", "fixed_fraction"] = "fixed_fraction"
    simple_fixed_usd: ConfigDecimal = Field(default=Decimal(1000), gt=Decimal(0))
    simple_fixed_fraction: ConfigDecimal = Field(
        default=Decimal("0.02"), gt=Decimal(0), le=Decimal(1)
    )

    # Position limits
    min_position_size: ConfigDecimal = Field(default=Decimal(100), gt=Decimal(0))
    max_position_size: ConfigDecimal = Field(default=Decimal(10000), gt=Decimal(0))
    max_leverage: ConfigDecimal = Field(default=Decimal("5.0"), gt=Decimal(1))

    # Portfolio limits
    max_portfolio_allocation: ConfigDecimal = Field(
        default=Decimal("0.5"), gt=Decimal(0), le=Decimal(1)
    )
    total_capital: ConfigDecimal | None = None

    # Volatility bounds and adjustment factors
    min_volatility: ConfigDecimal = Field(default=Decimal("0.001"), gt=Decimal(0))
    max_volatility_bound: ConfigDecimal = Field(default=Decimal("1.0"), gt=Decimal(0))
    volatility_lookback_hours: int = Field(default=24, gt=0, le=168)

    # Validation factors
    enable_validation_factors: bool = True
    enable_volatility_adjustment: bool = True
    enable_spread_adjustment: bool = True
    base_validation_factor: ConfigDecimal = Field(
        default=Decimal("0.8"), gt=Decimal(0), le=Decimal(1)
    )

    # Timing configuration
    sizing_timeout_seconds: float = Field(default=10.0, gt=0)

    @field_validator("method", mode="before")
    @classmethod
    def _validate_method(cls, v: str | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"kelly", "simple"},
            field_name=info.field_name or "method",
        )

    @field_validator("simple_method", mode="before")
    @classmethod
    def _validate_simple_method(cls, v: str | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"fixed_usd", "fixed_fraction"},
            field_name=info.field_name or "simple_method",
        )

    @model_validator(mode="after")
    def validate_allocation_ranges(self) -> Self:
        """Validate allocation ranges are logical.

        Returns:
            Self instance after validation

        Raises:
            ValueError: If allocation ranges are invalid
        """
        if self.kelly_min_allocation >= self.kelly_max_allocation:
            msg = "kelly_min_allocation must be less than kelly_max_allocation"
            raise ValueError(msg)
        if self.min_position_size >= self.max_position_size:
            msg = "min_position_size must be less than max_position_size"
            raise ValueError(msg)
        return self


class EnhancedRiskSettings(BaseModel):
    """Complete risk management configuration preserving existing GlobalRiskSettings."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True)

    # CRITICAL: Preserve existing GlobalRiskSettings integration
    global_risk: GlobalRiskSettings = Field(..., alias="global")

    # Enhanced checker and sizing configuration
    checkers: CheckerSettings = Field(default_factory=CheckerSettings)
    sizing: SizingSettings = Field(default_factory=SizingSettings)
    limits: RiskLimitsSettings = Field(default_factory=RiskLimitsSettings)

    # System configuration
    enabled: bool = True
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_all_checks: bool = False
    log_performance_metrics: bool = True

    # Concurrency limits
    max_concurrent_checks: int = Field(default=10, gt=0, le=50)
    max_concurrent_sizing: int = Field(default=5, gt=0, le=20)

    @field_validator("log_level", mode="before")
    @classmethod
    def _validate_log_level(cls, v: str | float | bool, info: ValidationInfo) -> str:
        return validate_enum_field(
            v,
            allowed={"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"},
            field_name=info.field_name or "log_level",
        )

    @model_validator(mode="after")
    def validate_cross_settings(self) -> Self:
        """Validate relationships between different settings and GlobalRiskSettings.

        Returns:
            Self instance after validation

        Raises:
            ValueError: If cross-setting relationships are invalid
        """
        # Ensure system-level concurrency is higher than component level
        if self.max_concurrent_checks < self.checkers.max_concurrent_checks:
            msg = "System max_concurrent_checks must be >= checkers.max_concurrent_checks"
            raise ValueError(msg)

        # Validate sizing limits don't exceed global risk limits
        if (
            hasattr(self.global_risk, "max_position_usd")
            and self.sizing.max_position_size > self.global_risk.max_position_usd
        ):
            msg = "Sizing max_position_size cannot exceed global_risk.max_position_usd"
            raise ValueError(msg)

        return self


# Alias for backward compatibility during migration
RiskSettings = EnhancedRiskSettings
