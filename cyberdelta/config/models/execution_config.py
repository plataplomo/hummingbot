"""Execution configuration models.

This module contains Pydantic models for order execution settings,
including compensation strategies, retry logic, and settlement delays.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.models.config_types import ConfigDecimal


class ExecutionCompensationSettings(BaseModel):
    """Execution compensation settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    use_limit_orders: bool = True
    limit_price_offset_pct: ConfigDecimal = Field(default=Decimal("0.05"), ge=Decimal(0))


class ExecutionSettings(BaseModel):
    """Execution configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_slippage_pct: ConfigDecimal = Field(..., gt=Decimal(0), lt=Decimal(1))
    max_retries: int = Field(default=3, gt=0)
    retry_delay_base_sec: ConfigDecimal = Field(default=Decimal("1.0"), gt=Decimal(0))
    retry_backoff_multiplier: float = Field(
        default=2.0, gt=1.0, description="Exponential backoff multiplier for retries"
    )
    settlement_delay: ConfigDecimal = Field(default=Decimal("2.0"), ge=Decimal(0))
    compensation: ExecutionCompensationSettings

    # Additional fields for trading service compatibility
    timeout_seconds: float = Field(default=30.0, gt=0, description="Execution timeout in seconds")
    retry_enabled: bool = Field(default=True, description="Whether retries are enabled")
    max_retry_attempts: int = Field(default=3, gt=0, description="Maximum retry attempts")

    # Minimal quantity fallback for when price is not available
    minimal_quantity_fallback: ConfigDecimal = Field(
        default=Decimal("0.001"),
        gt=Decimal(0),
        description="Minimal quantity to use when price is not available for sizing",
    )

    # Shutdown behavior
    cancel_on_shutdown: bool = Field(
        default=True,
        description="Whether to cancel all open orders on shutdown",
    )
