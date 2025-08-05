"""Safety systems configuration models.

This module contains Pydantic models for safety system settings,
including circuit breakers, position reconciliation, and balance monitoring.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.config.models.config_types import ConfigDecimal
from cyberdelta.exceptions.field_validation import RangeFieldError
from cyberdelta.utils.parsing import validate_str_field


class CircuitBreakerSettings(BaseModel):
    """Circuit breaker configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    global_consecutive_failures: int = Field(default=5, gt=0)
    global_reset_timeout_sec: int = Field(default=300, gt=0)
    exchange_consecutive_failures: int = Field(default=3, gt=0)
    exchange_reset_timeout_sec: int = Field(default=180, gt=0)

    # Additional configuration fields required by circuit_breaker.py
    cooldown_period_seconds: int = Field(default=300, gt=0)
    half_open_max_calls: int = Field(default=5, gt=0)
    recovery_threshold: ConfigDecimal = Field(default=Decimal("0.6"), gt=Decimal(0), le=Decimal(1))
    failure_history_limit: int = Field(default=100, gt=0)
    per_service_enabled: bool = Field(default=True)


class PositionReconciliationSettings(BaseModel):
    """Position reconciliation configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    check_interval_sec: int = Field(default=600, gt=0)
    max_discrepancy_pct: ConfigDecimal = Field(
        default=Decimal("0.01"), ge=Decimal(0), lt=Decimal(1)
    )


class BalanceMonitoringSettings(BaseModel):
    """Balance monitoring configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    check_interval_sec: int = Field(default=300, gt=0)
    min_balance_thresholds_usd: dict[str, ConfigDecimal]

    @field_validator("min_balance_thresholds_usd", mode="before")
    @classmethod
    def _validate_balance_thresholds_keys(
        cls,
        v: dict[str, str | int | float | Decimal] | list[str] | str | float | bool,
        info: ValidationInfo,
    ) -> dict[str, str | int | float | Decimal]:
        """Validate dictionary structure and keys before ConfigDecimal processes values.

        Args:
            v: The value to validate (dict, list, str, float, bool, or Decimal)
            info: Validation context containing field information

        Returns:
            Validated dictionary with string keys and numeric values

        Raises:
            TypeError: If value is not a dict
        """
        if not isinstance(v, dict):
            field_name = info.field_name or "min_balance_thresholds_usd"
            msg = f"{field_name}: Expected dict, got {type(v).__name__}"
            raise TypeError(msg)

        validated_thresholds: dict[str, str | int | float | Decimal] = {}
        for raw_key, raw_value in v.items():
            validated_key = validate_str_field(
                raw_key,
                field_name=f"{info.field_name or 'min_balance_thresholds_usd'}.key",
                allow_empty=False,
            )
            validated_thresholds[validated_key] = raw_value

        return validated_thresholds

    @field_validator("min_balance_thresholds_usd", mode="after")
    @classmethod
    def _validate_balance_thresholds_values(
        cls,
        v: dict[str, Decimal],
        info: ValidationInfo,
    ) -> dict[str, Decimal]:
        """Validate that all Decimal values are positive after ConfigDecimal parsing.

        Args:
            v: Dictionary with validated Decimal values
            info: Validation context containing field information

        Returns:
            Validated dictionary with positive Decimal values

        Raises:
            RangeFieldError: If any Decimal value is not positive
        """
        for key, value in v.items():
            if value <= Decimal(0):
                raise RangeFieldError(
                    field_name=f"{info.field_name or 'min_balance_thresholds_usd'}.{key}",
                    value=value,
                    min_value=0.0,
                    constraint="Balance threshold must be positive",
                )
        return v


class SafetySystemsSettings(BaseModel):
    """Safety systems configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    circuit_breakers: CircuitBreakerSettings
    position_reconciliation: PositionReconciliationSettings
    balance_monitoring: BalanceMonitoringSettings

    def should_halt_trading(self) -> bool:
        """Check if trading should be halted based on safety systems.
        
        Returns:
            True if any safety system indicates trading should halt
        """
        # For now, return False - actual implementation would check circuit breaker states
        return False
