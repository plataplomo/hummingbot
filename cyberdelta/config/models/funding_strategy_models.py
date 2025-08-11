"""Funding Strategy Configuration Models for CyberDeltaEngine.

This module contains Pydantic models specifically for funding rate arbitrage
strategy configuration validation and management.
"""

from decimal import Decimal
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.config.models.config_types import ConfigDecimal, NonEmptyConfigString
from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.field_validation import EnumFieldError, OrderLogicError, RangeFieldError


# Strategy timing constraints
MAX_CHECK_INTERVAL_SECONDS = 3600  # Maximum allowed check interval (1 hour)

# Funding strategy validation thresholds
MAX_FUNDING_THRESHOLD = Decimal("0.1")  # 10% maximum funding threshold
MAX_PRICE_SPREAD_PCT = Decimal("0.05")  # 5% maximum price spread for arbitrage
MIN_CHECK_INTERVAL_SECONDS = 1  # Minimum check interval

# Supported exchanges for strategy validation
SUPPORTED_EXCHANGES = {ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK}

# Expected exchange configuration for HL Perp BP Spot strategy
EXPECTED_PERP_EXCHANGE = ExchangeName.HYPERLIQUID
EXPECTED_SPOT_EXCHANGE = ExchangeName.BACKPACK


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
        """Validate funding threshold is within reasonable bounds.

        Args:
            v: The funding threshold value to validate.

        Returns:
            The validated funding threshold.

        Raises:
            RangeFieldError: If funding threshold exceeds maximum allowed value.
        """
        if v > MAX_FUNDING_THRESHOLD:
            raise RangeFieldError(
                field_name="funding_threshold",
                value=v,
                min_value=None,
                max_value=float(MAX_FUNDING_THRESHOLD),
                constraint=(
                    f"funding_threshold {v} is too high (max {MAX_FUNDING_THRESHOLD} for 10%)"
                ),
            )
        return v

    @field_validator("max_price_spread_pct")
    @classmethod
    def validate_max_price_spread_pct(cls, v: Decimal) -> Decimal:
        """Validate price spread percentage is reasonable.

        Args:
            v: The price spread percentage to validate.

        Returns:
            The validated price spread percentage.

        Raises:
            RangeFieldError: If price spread percentage exceeds maximum allowed value.
        """
        if v > MAX_PRICE_SPREAD_PCT:
            raise RangeFieldError(
                field_name="max_price_spread_pct",
                value=v,
                min_value=None,
                max_value=float(MAX_PRICE_SPREAD_PCT),
                constraint=(
                    f"max_price_spread_pct {v} is too high (max {MAX_PRICE_SPREAD_PCT} for 5%)"
                ),
            )
        return v

    @field_validator("check_interval")
    @classmethod
    def validate_check_interval(cls, v: int) -> int:
        """Validate check interval is within reasonable bounds.

        Args:
            v: The check interval in seconds to validate.

        Returns:
            The validated check interval.

        Raises:
            RangeFieldError: If check interval is outside allowed bounds.
        """
        if v < MIN_CHECK_INTERVAL_SECONDS:
            raise RangeFieldError(
                field_name="check_interval",
                value=v,
                min_value=float(MIN_CHECK_INTERVAL_SECONDS),
                max_value=None,
                constraint=f"check_interval must be at least {MIN_CHECK_INTERVAL_SECONDS} second",
            )
        if v > MAX_CHECK_INTERVAL_SECONDS:
            raise RangeFieldError(
                field_name="check_interval",
                value=v,
                min_value=None,
                max_value=float(MAX_CHECK_INTERVAL_SECONDS),
                constraint=(
                    f"check_interval {v} is too long (max {MAX_CHECK_INTERVAL_SECONDS} seconds)"
                ),
            )
        return v

    @field_validator("perp_exchange", "spot_exchange")
    @classmethod
    def validate_exchange_names(cls, v: str, info: ValidationInfo) -> str:
        """Validate exchange names are supported.

        Args:
            v: The exchange name to validate.
            info: Validation context information.

        Returns:
            The validated exchange name in lowercase.

        Raises:
            EnumFieldError: If exchange name is not in the supported exchanges list.
        """
        if v.lower() not in SUPPORTED_EXCHANGES:
            raise EnumFieldError(
                field_name=info.field_name if info and info.field_name else "exchange",
                value=v,
                valid_values=list(SUPPORTED_EXCHANGES),
                enum_name="SupportedExchange",
            )
        return v.lower()

    @model_validator(mode="after")
    def validate_exchange_combination(self) -> Self:
        """Validate that exchange combination makes sense for this strategy.

        Ensures that:
        - Perp and spot exchanges are different
        - Perp exchange is HyperLiquid
        - Spot exchange is Backpack

        Returns:
            The validated model instance.

        Raises:
            OrderLogicError: If exchange combination is invalid for the strategy.
        """
        if self.perp_exchange == self.spot_exchange:
            msg = "perp_exchange and spot_exchange must be different for arbitrage"
            raise OrderLogicError(
                validation_type="exchange_combination",
                message=msg,
                fields={"perp_exchange": self.perp_exchange, "spot_exchange": self.spot_exchange},
            )

        # For HL Perp BP Spot strategy, validate specific combination
        if self.perp_exchange != EXPECTED_PERP_EXCHANGE:
            msg = (
                f"For HL Perp BP Spot strategy, perp_exchange must be '{EXPECTED_PERP_EXCHANGE}', "
                f"got '{self.perp_exchange}'"
            )
            raise OrderLogicError(
                validation_type="perp_exchange_validation",
                message=msg,
                fields={"perp_exchange": self.perp_exchange, "expected": EXPECTED_PERP_EXCHANGE},
            )

        if self.spot_exchange != EXPECTED_SPOT_EXCHANGE:
            msg = (
                f"For HL Perp BP Spot strategy, spot_exchange must be '{EXPECTED_SPOT_EXCHANGE}', "
                f"got '{self.spot_exchange}'"
            )
            raise OrderLogicError(
                validation_type="spot_exchange_validation",
                message=msg,
                fields={"spot_exchange": self.spot_exchange, "expected": EXPECTED_SPOT_EXCHANGE},
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


class MomentumStrategyConfig(BaseModel):
    """Configuration for momentum-based trading strategy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    price_change_threshold: ConfigDecimal = Field(
        ..., gt=Decimal(0), description="Minimum price change % to trigger buy"
    )
    negative_threshold: ConfigDecimal = Field(
        ..., lt=Decimal(0), description="Maximum negative change % to trigger sell"
    )
    signal_confidence: ConfigDecimal = Field(
        ..., ge=Decimal(0), le=Decimal(1), description="Signal confidence level"
    )
    target_symbol: NonEmptyConfigString = Field(
        ..., description="Symbol to trade (e.g., 'BTC_USD')"
    )
    target_exchange: NonEmptyConfigString = Field(
        ..., description="Exchange to trade on (e.g., 'hyperliquid')"
    )
    lookback_hours: int = Field(..., gt=0, description="Hours to look back for price change")
    min_position_hold_hours: int = Field(..., ge=0, description="Minimum hours to hold position")


class StrategiesSettings(BaseModel):
    """Strategies configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    hl_perp_bp_spot: StrategyConfigHLPerpBPSpot

    # Strategy execution settings
    enabled_strategies: list[str] = Field(
        default_factory=list, description="List of enabled strategy names"
    )
    execution_interval_seconds: float = Field(
        default=10.0, gt=0, description="Strategy execution loop interval"
    )

    # Momentum strategy configuration (optional)
    momentum: MomentumStrategyConfig | None = None
