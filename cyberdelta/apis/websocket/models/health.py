"""Health monitoring models for WebSocket operations.

This module contains models and dataclasses for WebSocket health monitoring
and system status reporting.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Any

from pydantic import BaseModel, Field

from cyberdelta.apis.enums.websocket import HealthStatus


class ComponentStatus(BaseModel):
    """Status of a single component."""

    name: str = Field(..., description="Component name")
    status: HealthStatus = Field(..., description="Component health status")
    message: str = Field(..., description="Status message")
    last_check: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


class PerformanceHealth(BaseModel):
    """Performance health metrics."""

    error_creation_us: float = Field(..., description="Error creation time in microseconds")
    context_creation_us: float = Field(..., description="Context creation time in microseconds")
    handler_overhead_percent: float = Field(..., description="Handler overhead percentage")
    memory_usage_mb: float = Field(..., description="Memory usage in MB")
    status: HealthStatus = Field(..., description="Performance health status")


class SystemHealth(BaseModel):
    """Overall system health."""

    overall_status: HealthStatus = Field(..., description="Overall health status")
    components: Annotated[list[ComponentStatus], Field(default_factory=list)]
    performance: PerformanceHealth | None = Field(default=None)
    error_rate: float = Field(default=0.0, description="Current error rate")
    recovery_success_rate: float = Field(default=0.0, description="Recovery success rate")
    last_error: str | None = Field(default=None)
    check_timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


@dataclass
class HealthCheckConfig:
    """Configuration for health checks."""

    # Performance thresholds
    max_error_creation_us: float = 1000  # 1ms
    max_context_creation_us: float = 500  # 0.5ms
    max_handler_overhead_percent: float = 20  # 20%
    max_memory_usage_mb: float = 100  # 100MB

    # Error rate thresholds
    max_error_rate: float = 0.05  # 5% error rate
    min_recovery_success_rate: float = 0.8  # 80% recovery success

    # Check intervals
    check_interval_seconds: int = 60  # 1 minute
    metrics_window_minutes: int = 5  # 5 minute window

    # Component checks
    check_error_handler: bool = True
    check_metrics_collector: bool = True
    check_recovery_system: bool = True
    check_performance: bool = True
