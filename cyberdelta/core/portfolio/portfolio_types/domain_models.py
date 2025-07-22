"""Domain models for portfolio data structures."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class MetricsMetadata(BaseModel):
    """Typed metadata for metrics data."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str | None = None
    aggregation_method: str | None = None
    sample_size: int | None = None
    confidence_level: float | None = None
    calculation_method: str | None = None
    data_quality_score: float | None = None
    alert_thresholds: dict[str, float] = Field(default_factory=dict)


class ValidationMetadata(BaseModel):
    """Typed metadata for validation context."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    validator_name: str | None = None
    rule_set: str | None = None
    severity_level: str | None = None
    error_category: str | None = None
    suggestion: str | None = None
    documentation_link: str | None = None
    related_fields: list[str] = Field(default_factory=list)


class OperationMetadata(BaseModel):
    """Typed metadata for operation context."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    user_id: str | None = None
    session_id: str | None = None
    request_id: str | None = None
    correlation_id: str | None = None
    environment: str | None = None
    service_version: str | None = None
    execution_context: dict[str, str] = Field(default_factory=dict)


class ConfigurationMetadata(BaseModel):
    """Typed metadata for configuration data."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str | None = None
    last_modified: float | None = None
    modified_by: str | None = None
    environment: str | None = None
    validation_rules: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    migration_notes: str | None = None


class MetricsData(BaseModel):
    """Model for metrics data."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    metric_name: str
    value: float | int
    timestamp: float
    tags: dict[str, str] = Field(default_factory=dict)
    metadata: MetricsMetadata = Field(default_factory=MetricsMetadata)


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
    constraints: dict[str, str | int | float | bool] = Field(default_factory=dict)
    metadata: ValidationMetadata = Field(default_factory=ValidationMetadata)
    timestamp: float


class OperationContext(BaseModel):
    """Context information for operations."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation_id: str
    operation_type: str
    start_time: float
    end_time: float | None = None
    duration_ms: float | None = None
    metadata: OperationMetadata = Field(default_factory=OperationMetadata)

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
    metadata: ConfigurationMetadata = Field(default_factory=ConfigurationMetadata)
