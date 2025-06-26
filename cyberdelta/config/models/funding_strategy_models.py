"""Funding Strategy Configuration Models for CyberDeltaEngine.

This module contains Pydantic models specifically for funding rate arbitrage
strategy configuration validation and management.
"""

from decimal import Decimal
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.config.models.config_types import ConfigDecimal, NonEmptyConfigString


class StrategyParamsHLPerpBPSpot(BaseModel):
    """Parameters for HyperLiquid Perpetual vs Backpack Spot strategy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    funding_threshold: ConfigDecimal = Field(..., gt=Decimal(0))
    max_price_spread_pct: ConfigDecimal = Field(..., gt=Decimal(0), lt=Decimal(1))
    min_profit_usd: ConfigDecimal = Field(..., gt=Decimal(0))
    min_funding_differential: ConfigDecimal = Field(..., gt=Decimal(0))
    check_interval: int = Field(..., gt=0)
    risk_aversion: ConfigDecimal = Field(..., gt=Decimal(0))
    rebalance_threshold: ConfigDecimal = Field(..., gt=Decimal(0), lt=Decimal(1))
    perp_exchange: NonEmptyConfigString
    spot_exchange: NonEmptyConfigString

    @field_validator("funding_threshold")
    @classmethod
    def validate_funding_threshold(cls, v: Decimal) -> Decimal:
        """Validate funding threshold is within reasonable bounds."""
        if v > Decimal("0.1"):  # 10% seems unreasonably high
            raise ValueError(f"funding_threshold {v} is too high (max 0.1 for 10%)")
        return v

    @field_validator("max_price_spread_pct")
    @classmethod
    def validate_max_price_spread_pct(cls, v: Decimal) -> Decimal:
        """Validate price spread percentage is reasonable."""
        if v > Decimal("0.05"):  # 5% spread seems high for arbitrage
            raise ValueError(f"max_price_spread_pct {v} is too high (max 0.05 for 5%)")
        return v

    @field_validator("check_interval")
    @classmethod
    def validate_check_interval(cls, v: int) -> int:
        """Validate check interval is within reasonable bounds."""
        if v < 1:
            raise ValueError("check_interval must be at least 1 second")
        if v > 3600:  # 1 hour
            raise ValueError(f"check_interval {v} is too long (max 3600 seconds)")
        return v

    @field_validator("perp_exchange", "spot_exchange")
    @classmethod
    def validate_exchange_names(cls, v: str, info: ValidationInfo) -> str:
        """Validate exchange names are supported."""
        supported_exchanges = {"hyperliquid", "backpack"}
        if v.lower() not in supported_exchanges:
            raise ValueError(f"Unsupported exchange '{v}'. Supported: {supported_exchanges}")
        return v.lower()

    @model_validator(mode="after")
    def validate_exchange_combination(self) -> Self:
        """Validate that exchange combination makes sense for this strategy."""
        if self.perp_exchange == self.spot_exchange:
            raise ValueError("perp_exchange and spot_exchange must be different for arbitrage")

        # For HL Perp BP Spot strategy, validate specific combination
        expected_perp = "hyperliquid"
        expected_spot = "backpack"

        if self.perp_exchange != expected_perp:
            raise ValueError(
                f"For HL Perp BP Spot strategy, perp_exchange must be '{expected_perp}', "
                f"got '{self.perp_exchange}'",
            )

        if self.spot_exchange != expected_spot:
            raise ValueError(
                f"For HL Perp BP Spot strategy, spot_exchange must be '{expected_spot}', "
                f"got '{self.spot_exchange}'",
            )

        return self


class StrategyConfigHLPerpBPSpot(BaseModel):
    """Configuration for HyperLiquid Perpetual vs Backpack Spot strategy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    long_exchange: NonEmptyConfigString
    short_exchange: NonEmptyConfigString
    symbol_long: NonEmptyConfigString
    symbol_short: NonEmptyConfigString
    params: StrategyParamsHLPerpBPSpot


class StrategiesSettings(BaseModel):
    """Strategies configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hl_perp_bp_spot: StrategyConfigHLPerpBPSpot
