"""Error metrics models for WebSocket operations.

This module contains typed models for WebSocket error metrics collection,
providing comprehensive metrics tracking for error events and recovery attempts.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.enums import WebSocketErrorCode


class ErrorOccurrence(BaseModel):
    """Represents a single error occurrence for metrics."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    timestamp_ms: int = Field(..., description="Error timestamp in milliseconds")
    exchange: str = Field(..., description="Exchange name")
    error_code: WebSocketErrorCode = Field(..., description="Error code")
    severity: ErrorSeverity = Field(..., description="Error severity")
    connection_id: str = Field(..., description="Connection identifier")
    channel: str | None = Field(default=None, description="Channel or stream name")
    recovery_strategy: WebSocketRecoveryStrategy = Field(..., description="Recovery strategy used")
    recovery_successful: bool = Field(..., description="Whether recovery was successful")
    recovery_duration_ms: int | None = Field(default=None, description="Recovery duration")


class RecoveryAttempt(BaseModel):
    """Represents a recovery attempt for metrics."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    timestamp_ms: int = Field(..., description="Recovery attempt timestamp")
    exchange: str = Field(..., description="Exchange name")
    connection_id: str = Field(..., description="Connection identifier")
    strategy: WebSocketRecoveryStrategy = Field(..., description="Recovery strategy")
    attempt_number: int = Field(..., ge=1, description="Attempt number (1-based)")
    successful: bool = Field(..., description="Whether recovery was successful")
    duration_ms: int = Field(..., ge=0, description="Recovery duration")
    error_count_before: int = Field(..., ge=0, description="Error count before recovery")


class ConnectionMetrics(BaseModel):
    """Metrics for a WebSocket connection."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    connection_id: str = Field(..., description="Connection identifier")
    exchange: str = Field(..., description="Exchange name")
    connection_start_ms: int = Field(..., description="Connection start timestamp")
    connection_end_ms: int | None = Field(default=None, description="Connection end timestamp")
    total_messages: int = Field(default=0, ge=0, description="Total messages received")
    total_errors: int = Field(default=0, ge=0, description="Total errors encountered")
    total_recoveries: int = Field(default=0, ge=0, description="Total recovery attempts")
    successful_recoveries: int = Field(default=0, ge=0, description="Successful recoveries")


class ErrorRateMetrics(BaseModel):
    """Error rate metrics over a time window."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    window_start_ms: int = Field(..., description="Window start timestamp")
    window_end_ms: int = Field(..., description="Window end timestamp")
    window_duration_ms: int = Field(..., ge=1, description="Window duration")
    total_events: int = Field(default=0, ge=0, description="Total events in window")
    error_events: int = Field(default=0, ge=0, description="Error events in window")
    error_rate: float = Field(default=0.0, ge=0.0, le=1.0, description="Error rate (0.0-1.0)")


class CollectorStatistics(BaseModel):
    """Statistics for the error metrics collector."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    uptime_seconds: int = Field(..., ge=0, description="Collector uptime in seconds")
    total_errors_recorded: int = Field(..., ge=0, description="Total errors recorded")
    total_recoveries_recorded: int = Field(..., ge=0, description="Total recoveries recorded")
    current_error_buffer_size: int = Field(..., ge=0, description="Current error buffer size")
    current_recovery_buffer_size: int = Field(..., ge=0, description="Current recovery buffer size")
    active_connections: int = Field(..., ge=0, description="Number of active connections")
    total_connections_tracked: int = Field(..., ge=0, description="Total connections tracked")
    error_chains_tracked: int = Field(..., ge=0, description="Number of error chains tracked")
    error_rate_buckets: int = Field(..., ge=0, description="Number of error rate buckets")
    collection_enabled: bool = Field(..., description="Whether collection is enabled")


class AggregatedMetrics(BaseModel):
    """Aggregated metrics for reporting."""

    model_config = {
        "frozen": False,
        "extra": "forbid",
    }

    # Time period
    start_timestamp_ms: int = Field(..., description="Metrics period start")
    end_timestamp_ms: int = Field(..., description="Metrics period end")
    duration_ms: int = Field(..., ge=1, description="Metrics period duration")

    # Error counts by code
    error_counts_by_code: dict[str, int] = Field(
        default_factory=dict, description="Error counts by error code name"
    )

    # Error counts by exchange
    error_counts_by_exchange: dict[str, int] = Field(
        default_factory=dict, description="Error counts by exchange"
    )

    # Recovery metrics
    recovery_attempts_by_strategy: dict[str, int] = Field(
        default_factory=dict, description="Recovery attempts by strategy name"
    )

    successful_recoveries_by_strategy: dict[str, int] = Field(
        default_factory=dict, description="Successful recoveries by strategy name"
    )

    # Timing metrics
    average_recovery_duration_ms: float = Field(
        default=0.0, ge=0.0, description="Average recovery duration"
    )

    median_recovery_duration_ms: float = Field(
        default=0.0, ge=0.0, description="Median recovery duration"
    )

    # Connection metrics
    total_connections: int = Field(default=0, ge=0, description="Total connections tracked")
    active_connections: int = Field(default=0, ge=0, description="Currently active connections")
    average_connection_duration_ms: float = Field(
        default=0.0, ge=0.0, description="Average connection duration"
    )

    # Overall health
    overall_error_rate: float = Field(default=0.0, ge=0.0, le=1.0, description="Overall error rate")

    recovery_success_rate: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Recovery success rate"
    )

    @property
    def total_errors(self) -> int:
        """Total number of errors across all codes.

        Returns:
            Sum of all error counts
        """
        return sum(self.error_counts_by_code.values())
