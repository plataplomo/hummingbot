"""WebSocket error event models.

Type-safe event models for WebSocket error tracking, recovery attempts,
and health monitoring. These models are used by the event publishing system.
"""

from __future__ import annotations

import time
from uuid import uuid4

from pydantic import BaseModel, Field

from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket import WebSocketErrorCode


# ============================================================================
# Event Data Models
# ============================================================================


class ErrorEventMetadata(BaseModel):
    """Metadata for error events."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    event_version: str = Field(default="1.0")
    source_system: str = Field(default="websocket_error_handler")
    correlation_id: str | None = Field(default=None)
    trace_id: str | None = Field(default=None)


class WebSocketErrorEvent(BaseModel):
    """Type-safe error event for WebSocket errors."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    # Event metadata
    metadata: ErrorEventMetadata = Field(default_factory=ErrorEventMetadata)

    # Error classification
    event_type: str = Field(default="websocket_error")
    error_code: WebSocketErrorCode = Field(...)
    error_category: str = Field(...)  # From error code category
    severity: ErrorSeverity = Field(...)

    # Error details
    error_message: str = Field(...)
    exchange: str = Field(...)
    connection_id: str = Field(...)
    channel: str | None = Field(default=None)
    topic: str | None = Field(default=None)

    # Context information
    user_id: str | None = Field(default=None)
    session_id: str | None = Field(default=None)
    sequence_number: int | None = Field(default=None)

    # Recovery information
    recovery_strategy: WebSocketRecoveryStrategy = Field(...)
    recovery_attempted: bool = Field(default=False)
    recovery_successful: bool | None = Field(default=None)
    recovery_duration_ms: int | None = Field(default=None)

    # Additional context
    raw_message_size: int | None = Field(default=None)
    connection_duration_ms: int | None = Field(default=None)
    error_count_in_window: int = Field(default=1)

    # Technical details
    stack_trace: str | None = Field(default=None)
    additional_context: dict[str, str] = Field(default_factory=dict)


class RecoveryAttemptEvent(BaseModel):
    """Event for recovery attempts."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    # Event metadata
    metadata: ErrorEventMetadata = Field(default_factory=ErrorEventMetadata)

    # Event classification
    event_type: str = Field(default="recovery_attempt")

    # Recovery details
    exchange: str = Field(...)
    connection_id: str = Field(...)
    strategy: WebSocketRecoveryStrategy = Field(...)
    attempt_number: int = Field(..., ge=1)
    successful: bool = Field(...)
    duration_ms: int = Field(..., ge=0)

    # Context
    original_error_code: WebSocketErrorCode = Field(...)
    error_count_before: int = Field(..., ge=0)
    channel: str | None = Field(default=None)

    # Result details
    failure_reason: str | None = Field(default=None)
    next_strategy: WebSocketRecoveryStrategy | None = Field(default=None)


class ConnectionHealthEvent(BaseModel):
    """Event for connection health status changes."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    # Event metadata
    metadata: ErrorEventMetadata = Field(default_factory=ErrorEventMetadata)

    # Event classification
    event_type: str = Field(default="connection_health")

    # Connection details
    exchange: str = Field(...)
    connection_id: str = Field(...)
    health_status: str = Field(...)  # "healthy", "degraded", "unhealthy", "failed"

    # Health metrics
    error_rate: float = Field(..., ge=0.0, le=1.0)
    recovery_success_rate: float = Field(..., ge=0.0, le=1.0)
    connection_uptime_ms: int = Field(..., ge=0)
    total_errors: int = Field(..., ge=0)
    total_recoveries: int = Field(..., ge=0)
    successful_recoveries: int = Field(..., ge=0)

    # Recent activity
    recent_error_codes: list[str] = Field(default_factory=list)
    last_successful_message_ms: int | None = Field(default=None)

    # Threshold information
    health_threshold_breached: str | None = Field(default=None)
    recommended_action: str | None = Field(default=None)


class SystemHealthEvent(BaseModel):
    """Event for overall system health status."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    # Event metadata
    metadata: ErrorEventMetadata = Field(default_factory=ErrorEventMetadata)

    # Event classification
    event_type: str = Field(default="system_health")

    # Overall health
    overall_health: str = Field(...)  # "healthy", "degraded", "critical"
    active_connections: int = Field(..., ge=0)
    total_connections: int = Field(..., ge=0)

    # Aggregate metrics
    system_error_rate: float = Field(..., ge=0.0, le=1.0)
    system_recovery_rate: float = Field(..., ge=0.0, le=1.0)
    average_connection_uptime_ms: float = Field(..., ge=0.0)

    # Exchange breakdown
    exchange_health: dict[str, str] = Field(default_factory=dict)
    problematic_exchanges: list[str] = Field(default_factory=list)

    # Alert information
    active_alerts: int = Field(..., ge=0)
    critical_issues: list[str] = Field(default_factory=list)
