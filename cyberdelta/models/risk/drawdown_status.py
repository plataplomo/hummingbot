"""Typed models for drawdown monitoring status and configuration.

This module provides typed Pydantic models to replace dict[str, object] patterns
in drawdown monitoring, ensuring type safety for risk management reporting.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class DrawdownConfiguration(BaseModel):
    """Type-safe drawdown configuration settings.

    This model provides structured configuration data for drawdown monitoring,
    replacing nested dict patterns.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Type-safe configuration data
    - Uses Decimal for percentage values
    """

    max_drawdown_pct: Decimal = Field(description="Maximum allowed drawdown percentage")

    lookback_days: int = Field(description="Number of days to look back for drawdown calculation")

    check_interval_sec: Decimal = Field(description="Check interval in seconds")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True


class DrawdownStatus(BaseModel):
    """Type-safe drawdown monitoring status.

    This model replaces dict[str, object] returns in drawdown monitoring
    to ensure type safety for risk management status reporting.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO default values for critical fields
    - All monetary values use Decimal type
    - Uses proper datetime objects
    """

    current_drawdown_pct: Decimal = Field(description="Current drawdown percentage")

    max_allowed_pct: Decimal = Field(description="Maximum allowed drawdown percentage")

    drawdown_violated: bool = Field(description="Whether drawdown limits are currently violated")

    violation_timestamp: datetime | None = Field(
        default=None, description="Timestamp when violation occurred"
    )

    peak_value: Decimal = Field(description="Peak portfolio value")

    peak_timestamp: datetime | None = Field(default=None, description="Timestamp of peak value")

    trough_value: Decimal = Field(description="Trough portfolio value")

    trough_timestamp: datetime | None = Field(default=None, description="Timestamp of trough value")

    history_entries: int = Field(description="Number of entries in value history")

    lookback_days: int = Field(description="Lookback period in days")

    configuration: DrawdownConfiguration = Field(description="Drawdown monitoring configuration")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable for thread safety
        validate_assignment = True
