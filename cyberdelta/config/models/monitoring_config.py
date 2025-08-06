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


class HealthCheckThresholds(BaseModel):
    """Health check thresholds configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    critical_service_threshold: float = Field(
        default=0.5, ge=0.0, le=1.0, description="Critical service threshold ratio"
    )
    unhealthy_service_threshold: float = Field(
        default=0.3, ge=0.0, le=1.0, description="Unhealthy service threshold ratio"
    )
    degraded_service_threshold: float = Field(
        default=0.2, ge=0.0, le=1.0, description="Degraded service threshold ratio"
    )


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

    # Missing fields referenced in monitoring modules
    metrics_enabled: bool = Field(default=True, description="Enable metrics collection")
    metrics_collection_interval: float = Field(
        default=60.0, gt=0, le=3600, description="Metrics collection interval in seconds"
    )
    metrics_retention_days: int = Field(
        default=30, gt=0, le=365, description="Metrics retention period in days"
    )
    max_metrics_per_snapshot: int = Field(
        default=1000, gt=0, le=10000, description="Maximum metrics per snapshot"
    )

    # Alert configuration
    alert_suppression_seconds: float = Field(
        default=300.0, gt=0, le=3600, description="Alert suppression duration in seconds"
    )
    escalation_enabled: bool = Field(default=False, description="Enable alert escalation")
    escalation_delay_seconds: float = Field(
        default=900.0, gt=0, le=7200, description="Alert escalation delay in seconds"
    )
    max_alerts_per_minute: int = Field(
        default=10, gt=0, le=100, description="Maximum alerts per minute"
    )

    # Health check configuration
    stale_data_threshold_seconds: float = Field(
        default=300.0, gt=0, le=3600, description="Stale data threshold in seconds"
    )
    response_time_threshold_ms: float = Field(
        default=5000.0, gt=0, le=60000, description="Response time threshold in milliseconds"
    )
    error_rate_threshold: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Error rate threshold as decimal (e.g., 0.05 = 5%)",
    )
    memory_threshold_mb: float = Field(
        default=1024.0, gt=0, le=32768, description="Memory threshold in MB"
    )
    cpu_threshold_percent: float = Field(
        default=80.0, gt=0, le=100, description="CPU threshold as percentage"
    )
    health_check_timeout_seconds: float = Field(
        default=30.0, gt=0, le=300, description="Health check timeout in seconds"
    )

    # Health check thresholds with proper structure
    health_check_thresholds: HealthCheckThresholds = Field(
        default_factory=HealthCheckThresholds, description="Health check thresholds configuration"
    )

    # Audit logging configuration
    audit_log_file: str = Field(default="logs/audit.log", description="Audit log file path")
    audit_retention_days: int = Field(
        default=90, gt=0, le=3650, description="Audit log retention in days"
    )
    audit_log_format: str = Field(default="json", description="Audit log format (json or text)")
    audit_buffer_size: int = Field(default=100, gt=0, le=10000, description="Audit log buffer size")
    audit_flush_interval_seconds: float = Field(
        default=60.0, gt=0, le=3600, description="Audit log flush interval in seconds"
    )

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
