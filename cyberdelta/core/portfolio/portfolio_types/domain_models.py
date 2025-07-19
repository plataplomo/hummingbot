"""Domain models for portfolio data structures."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class MetricsData(BaseModel):
    """Model for metrics data."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    metric_name: str
    value: float | int
    timestamp: float
    tags: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ErrorContext(BaseModel):
    """Model for error context information."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    error_code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
    traceback: str | None = None
    timestamp: float
    component: str | None = None
    operation: str | None = None
    retry_count: int = 0
    is_retryable: bool = False


class ValidationContext(BaseModel):
    """Model for validation context data."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    field_name: str | None = None
    validation_type: str
    expected_value: Any = None
    actual_value: Any = None
    constraints: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: float


class OperationContext(BaseModel):
    """Context information for operations."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation_id: str
    operation_type: str
    start_time: float
    end_time: float | None = None
    duration_ms: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def duration_ms_computed(self) -> float | None:
        """Calculate duration if end_time is set."""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time) * 1000
        return self.duration_ms


class ConfigurationData(BaseModel):
    """Model for configuration data."""

    model_config = ConfigDict(extra="allow")  # Allow extra fields for flexibility

    name: str
    value: Any
    type: str | None = None
    description: str | None = None
    is_sensitive: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)
