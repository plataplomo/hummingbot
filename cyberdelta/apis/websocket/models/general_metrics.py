"""General metrics models for WebSocket operations.

This module contains general-purpose metric models for WebSocket message processing
and monitoring.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.websocket.enums import MetricUnit, WSMetricType


class MetricPoint(BaseModel):
    """A single metric data point."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metric_type: WSMetricType
    metric_name: str
    value: float
    unit: MetricUnit
    labels: dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True)


class MetricSummary(BaseModel):
    """Summary statistics for a metric."""

    metric_name: str
    count: int = 0
    total: float = 0
    min: float | None = None
    max: float | None = None
    average: float = Field(default=0, init=False)
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None
    unit: MetricUnit

    def model_post_init(self, __context: object, /) -> None:
        """Calculate average after initialization."""
        if self.count > 0:
            self.average = self.total / self.count
        else:
            self.average = 0
