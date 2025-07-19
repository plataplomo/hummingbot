"""Type-safe event models with discriminated unions for portfolio system.

This module provides strongly-typed event models using Pydantic's discriminated
unions for complete type safety in event handling.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from .base import BaseStateModel


class EventMetadata(BaseModel):
    """Metadata for events."""
    correlation_id: str | None = Field(
        default=None, description="Correlation ID for event tracking"
    )
    source_component: str = Field(
        default="unknown", description="Component that generated the event"
    )
    trace_id: str | None = Field(default=None, description="Distributed tracing ID")
    user_id: str | None = Field(default=None, description="User associated with the event")
    session_id: str | None = Field(default=None, description="Session ID")
    custom_tags: dict[str, str | int | float | bool] = Field(
        default_factory=dict,
        description="Custom tags for event categorization"
    )


class StateChangeEvent(BaseStateModel):
    """Event representing a state change in the portfolio system.
    
    This event is fired whenever any significant state change occurs,
    providing full type safety for state transition tracking.
    """
    
    # Discriminator for union types
    event_type: Literal["state_change"] = Field(
        default="state_change",
        description="Event type discriminator"
    )
    
    # Entity information
    entity_type: str = Field(..., description="Type of entity that changed")
    entity_id: str = Field(..., description="Unique identifier of the entity")
    
    # State change details
    change_type: Literal["created", "updated", "deleted", "restored"] = Field(
        ..., description="Type of state change"
    )
    
    # State data (optional for privacy/size reasons)
    old_state: BaseStateModel | None = Field(
        default=None,
        description="Previous state (optional)"
    )
    new_state: BaseStateModel | None = Field(
        default=None,
        description="New state (optional)"
    )
    
    # Change details
    changed_fields: list[str] = Field(
        default_factory=list,
        description="List of fields that changed"
    )
    change_reason: str | None = Field(
        default=None,
        description="Reason for the state change"
    )
    
    # Event metadata
    metadata: EventMetadata = Field(
        default_factory=EventMetadata,
        description="Event metadata"
    )
    
    # Timing
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When the state change occurred"
    )
    
    def get_summary(self) -> str:
        """Get a human-readable summary of the state change.
        
        Returns:
            Summary string describing the change
        """
        return (
            f"{self.entity_type} {self.entity_id} was {self.change_type} "
            f"at {self.occurred_at.isoformat()}"
        )
    
    def has_field_changed(self, field_name: str) -> bool:
        """Check if a specific field was changed.
        
        Args:
            field_name: Name of the field to check
            
        Returns:
            True if the field was changed
        """
        return field_name in self.changed_fields


class ErrorEvent(BaseStateModel):
    """Event representing an error or exception in the portfolio system.
    
    This event provides structured error reporting with full type safety
    for debugging and monitoring purposes.
    """
    
    # Discriminator for union types
    event_type: Literal["error"] = Field(
        default="error",
        description="Event type discriminator"
    )
    
    # Error classification
    error_code: str = Field(..., description="Unique error code")
    error_category: Literal[
        "validation", 
        "persistence", 
        "concurrency", 
        "external_api", 
        "business_logic", 
        "system",
        "unknown"
    ] = Field(
        default="unknown",
        description="Category of error"
    )
    
    severity: Literal["low", "medium", "high", "critical"] = Field(
        default="medium",
        description="Error severity level"
    )
    
    # Error details
    message: str = Field(..., description="Human-readable error message")
    technical_details: str | None = Field(
        default=None,
        description="Technical details for debugging"
    )
    
    # Context information
    entity_type: str | None = Field(
        default=None,
        description="Type of entity related to the error"
    )
    entity_id: str | None = Field(
        default=None,
        description="ID of entity related to the error"
    )
    operation: str | None = Field(
        default=None,
        description="Operation that caused the error"
    )
    
    # Exception details
    exception_type: str | None = Field(
        default=None,
        description="Type of exception that occurred"
    )
    stack_trace: str | None = Field(
        default=None,
        description="Stack trace (optional for security)"
    )
    
    # Recovery information
    is_recoverable: bool = Field(
        default=True,
        description="Whether the error is recoverable"
    )
    recovery_suggestion: str | None = Field(
        default=None,
        description="Suggested recovery action"
    )
    
    # Event metadata
    metadata: EventMetadata = Field(
        default_factory=EventMetadata,
        description="Event metadata"
    )
    
    # Timing
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When the error occurred"
    )
    
    def get_summary(self) -> str:
        """Get a human-readable summary of the error.
        
        Returns:
            Summary string describing the error
        """
        context = ""
        if self.entity_type and self.entity_id:
            context = f" in {self.entity_type} {self.entity_id}"
        elif self.operation:
            context = f" during {self.operation}"
        
        return f"{self.severity.upper()} {self.error_category} error{context}: {self.message}"
    
    def is_critical(self) -> bool:
        """Check if this is a critical error.
        
        Returns:
            True if the error is critical
        """
        return self.severity == "critical"


class ValidationEvent(BaseStateModel):
    """Event representing validation results in the portfolio system.
    
    This event is fired when validation occurs, providing detailed
    information about validation success or failure.
    """
    
    # Discriminator for union types
    event_type: Literal["validation"] = Field(
        default="validation",
        description="Event type discriminator"
    )
    
    # Validation details
    validation_type: str = Field(..., description="Type of validation performed")
    entity_type: str = Field(..., description="Type of entity validated")
    entity_id: str = Field(..., description="ID of entity validated")
    
    # Results
    is_valid: bool = Field(..., description="Whether validation passed")
    error_count: int = Field(default=0, description="Number of validation errors")
    warning_count: int = Field(default=0, description="Number of validation warnings")
    
    # Details
    errors: list[str] = Field(
        default_factory=list,
        description="List of validation errors"
    )
    warnings: list[str] = Field(
        default_factory=list,
        description="List of validation warnings"
    )
    
    # Performance
    validation_duration_ms: float | None = Field(
        default=None,
        description="Validation duration in milliseconds"
    )
    
    # Event metadata
    metadata: EventMetadata = Field(
        default_factory=EventMetadata,
        description="Event metadata"
    )
    
    # Timing
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When validation occurred"
    )
    
    def get_summary(self) -> str:
        """Get a human-readable summary of the validation.
        
        Returns:
            Summary string describing the validation results
        """
        status = "PASSED" if self.is_valid else "FAILED"
        details = f"{self.error_count} errors, {self.warning_count} warnings"
        return f"Validation {status} for {self.entity_type} {self.entity_id} ({details})"


class MetricsEvent(BaseStateModel):
    """Event representing metrics data in the portfolio system.
    
    This event carries performance and business metrics for monitoring
    and analytics purposes.
    """
    
    # Discriminator for union types
    event_type: Literal["metrics"] = Field(
        default="metrics",
        description="Event type discriminator"
    )
    
    # Metrics classification
    metric_category: Literal[
        "performance", 
        "business", 
        "technical", 
        "usage",
        "financial"
    ] = Field(
        ..., description="Category of metrics"
    )
    
    # Metrics data
    metrics: dict[str, float | int] = Field(
        ..., description="Metrics data with numeric values"
    )
    
    # Context
    component: str = Field(..., description="Component that generated the metrics")
    entity_type: str | None = Field(
        default=None,
        description="Type of entity the metrics relate to"
    )
    entity_id: str | None = Field(
        default=None,
        description="ID of entity the metrics relate to"
    )
    
    # Aggregation info
    aggregation_window: str | None = Field(
        default=None,
        description="Time window for aggregated metrics (e.g., '1h', '1d')"
    )
    sample_count: int | None = Field(
        default=None,
        description="Number of samples in aggregated metrics"
    )
    
    # Event metadata
    metadata: EventMetadata = Field(
        default_factory=EventMetadata,
        description="Event metadata"
    )
    
    # Timing
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When metrics were collected"
    )
    
    def get_metric_value(self, metric_name: str) -> float | int | None:
        """Get a specific metric value.
        
        Args:
            metric_name: Name of the metric
            
        Returns:
            Metric value or None if not found
        """
        return self.metrics.get(metric_name)
    
    def get_summary(self) -> str:
        """Get a human-readable summary of the metrics.
        
        Returns:
            Summary string describing the metrics
        """
        metric_count = len(self.metrics)
        return (
            f"{self.metric_category.title()} metrics from {self.component} "
            f"({metric_count} metrics)"
        )


# Discriminated union of all portfolio events
PortfolioEvent = (
    StateChangeEvent |
    ErrorEvent |
    ValidationEvent |
    MetricsEvent
)

# Type alias for annotated discriminated union


AnnotatedPortfolioEvent = Annotated[
    PortfolioEvent,
    Field(discriminator="event_type")
]


# Factory functions for creating events

def create_state_change_event(
    entity_type: str,
    entity_id: str,
    change_type: Literal["created", "updated", "deleted", "restored"],
    source_component: str,
    state_id: str | None = None,
    changed_fields: list[str] | None = None,
    change_reason: str | None = None,
    old_state: BaseStateModel | None = None,
    new_state: BaseStateModel | None = None,
    **metadata_kwargs: str | float | bool
) -> StateChangeEvent:
    """Create a state change event with proper typing.
    
    Args:
        entity_type: Type of entity that changed
        entity_id: Unique identifier of the entity
        change_type: Type of state change
        source_component: Component that generated the event
        state_id: Optional state ID for the event
        source_component: Component that generated the event
        changed_fields: Optional list of changed fields
        change_reason: Optional reason for the change
        old_state: Optional previous state
        new_state: Optional new state
        **metadata_kwargs: Additional metadata
        
    Returns:
        Properly typed StateChangeEvent
    """
    metadata = EventMetadata(
        source_component=source_component,
        custom_tags=metadata_kwargs
    )
    
    return StateChangeEvent(
        state_id=(
            state_id or 
            f"event_{entity_type}_{entity_id}_{int(datetime.now(UTC).timestamp())}"
        ),
        entity_type=entity_type,
        entity_id=entity_id,
        change_type=change_type,
        changed_fields=changed_fields or [],
        change_reason=change_reason,
        old_state=old_state,
        new_state=new_state,
        metadata=metadata
    )


def create_error_event(
    error_code: str,
    message: str,
    source_component: str,
    error_category: Literal[
        "validation", 
        "persistence", 
        "concurrency", 
        "external_api", 
        "business_logic", 
        "system",
        "unknown"
    ] = "unknown",
    severity: Literal["low", "medium", "high", "critical"] = "medium",
    state_id: str | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    operation: str | None = None,
    exception: Exception | None = None,
    **metadata_kwargs: str | float | bool
) -> ErrorEvent:
    """Create an error event with proper typing.
    
    Args:
        error_code: Unique error code
        message: Human-readable error message
        source_component: Component that generated the event
        error_category: Category of error
        severity: Error severity level
        state_id: Optional state identifier for the error event
        entity_type: Optional type of related entity
        entity_id: Optional ID of related entity
        operation: Optional operation that caused the error
        exception: Optional exception object
        **metadata_kwargs: Additional metadata
        
    Returns:
        Properly typed ErrorEvent
    """
    metadata = EventMetadata(
        source_component=source_component,
        custom_tags=metadata_kwargs
    )
    
    # Extract exception details if provided
    exception_type = None
    technical_details = None
    if exception:
        exception_type = type(exception).__name__
        technical_details = str(exception)
    
    return ErrorEvent(
        state_id=state_id or f"error_{error_code}_{int(datetime.now(UTC).timestamp())}",
        error_code=error_code,
        error_category=error_category,
        severity=severity,
        message=message,
        technical_details=technical_details,
        entity_type=entity_type,
        entity_id=entity_id,
        operation=operation,
        exception_type=exception_type,
        metadata=metadata
    )


def create_validation_event(
    validation_type: str,
    entity_type: str,
    entity_id: str,
    source_component: str,
    is_valid: bool,
    state_id: str | None = None,
    errors: list[str] | None = None,
    warnings: list[str] | None = None,
    validation_duration_ms: float | None = None,
    **metadata_kwargs: str | float | bool
) -> ValidationEvent:
    """Create a validation event with proper typing.
    
    Args:
        validation_type: Type of validation performed
        entity_type: Type of entity validated
        entity_id: ID of entity validated
        source_component: Component that performed validation
        is_valid: Whether validation passed
        state_id: Optional state identifier for the validation event
        errors: Optional list of validation errors
        warnings: Optional list of validation warnings
        validation_duration_ms: Optional validation duration
        **metadata_kwargs: Additional metadata
        
    Returns:
        Properly typed ValidationEvent
    """
    metadata = EventMetadata(
        source_component=source_component,
        custom_tags=metadata_kwargs
    )
    
    error_list = errors or []
    warning_list = warnings or []
    
    return ValidationEvent(
        state_id=(
            state_id or 
            f"validation_{validation_type}_{entity_id}_{int(datetime.now(UTC).timestamp())}"
        ),
        validation_type=validation_type,
        entity_type=entity_type,
        entity_id=entity_id,
        is_valid=is_valid,
        error_count=len(error_list),
        warning_count=len(warning_list),
        errors=error_list,
        warnings=warning_list,
        validation_duration_ms=validation_duration_ms,
        metadata=metadata
    )


def create_metrics_event(
    metric_category: Literal[
        "performance", 
        "business", 
        "technical", 
        "usage",
        "financial"
    ],
    metrics: dict[str, float | int],
    component: str,
    state_id: str | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    aggregation_window: str | None = None,
    sample_count: int | None = None,
    **metadata_kwargs: str | float | bool
) -> MetricsEvent:
    """Create a metrics event with proper typing.
    
    Args:
        metric_category: Category of metrics
        metrics: Metrics data with numeric values
        component: Component that generated the metrics
        state_id: Optional state identifier for the metrics event
        entity_type: Optional type of related entity
        entity_id: Optional ID of related entity
        aggregation_window: Optional aggregation window
        sample_count: Optional sample count for aggregated metrics
        **metadata_kwargs: Additional metadata
        
    Returns:
        Properly typed MetricsEvent
    """
    metadata = EventMetadata(
        source_component=component,
        custom_tags=metadata_kwargs
    )
    
    return MetricsEvent(
        state_id=state_id or (
            f"metrics_{metric_category}_{component}_{int(datetime.now(UTC).timestamp())}"
        ),
        metric_category=metric_category,
        metrics=metrics,
        component=component,
        entity_type=entity_type,
        entity_id=entity_id,
        aggregation_window=aggregation_window,
        sample_count=sample_count,
        metadata=metadata
    )