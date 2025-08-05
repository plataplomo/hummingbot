"""Monitoring and alerting configuration models.

This module contains Pydantic models for monitoring settings,
including notification preferences and alert methods.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.config.models.market_data_config import MarketDataSettings
from cyberdelta.config.models.portfolio_config import PortfolioCacheSettings
from cyberdelta.utils.parsing import validate_enum_field


def _default_alert_methods() -> list[Literal["log", "telegram"]]:
    """Create default factory for alert_methods field.

    Returns the default list of alert methods for monitoring configuration.
    Used as a factory function to avoid mutable default arguments.

    Returns:
        List containing default alert methods (currently just "log").

    """
    return ["log"]


class MonitoringSettings(BaseModel):
    """Monitoring and notifications configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    notifications_enabled: bool = True
    alert_methods: list[Literal["log", "telegram"]] = Field(default_factory=_default_alert_methods)
    health_check_interval_seconds: float = Field(default=300.0, gt=0, le=3600)
    cache: PortfolioCacheSettings = Field(default_factory=PortfolioCacheSettings)
    market_data: MarketDataSettings = Field(default_factory=MarketDataSettings)

    @field_validator("alert_methods", mode="before")
    @classmethod
    def _validate_alert_methods(
        cls,
        v: list[str | int | float | bool] | str | float | bool,
        info: ValidationInfo,
    ) -> list[str]:
        if not isinstance(v, list):
            field_name = info.field_name or "alert_methods"
            msg = f"{field_name}: Expected list, got {type(v).__name__}"
            raise TypeError(msg)

        validated_methods: list[str] = []
        for i, raw_method in enumerate(v):
            validated_method = validate_enum_field(
                raw_method,
                allowed={"log", "telegram"},
                field_name=f"{info.field_name or 'alert_methods'}[{i}]",
            )
            validated_methods.append(validated_method)

        return validated_methods
