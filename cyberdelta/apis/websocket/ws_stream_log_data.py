"""Type-safe log data model for WebSocket errors.

Provides structured logging data for WebSocket stream errors
with full type safety and no dict conversions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext


class WebSocketStreamLogData(BaseModel):
    """Type-safe log data for WebSocket stream errors."""

    # ========================================================================
    # Error Identification
    # ========================================================================
    error_domain: str = Field(default="websocket_stream", description="Error domain identifier")
    error_code: WebSocketErrorCode = Field(description="WebSocket error code")
    error_code_name: str = Field(description="Human-readable error code name")
    error_category: str = Field(description="Error category from error code")

    # ========================================================================
    # Error Details
    # ========================================================================
    message: str = Field(description="Error message")
    severity: ErrorSeverity = Field(description="Error severity level")
    is_critical: bool = Field(description="Whether error is critical")
    is_retryable: bool = Field(description="Whether error is retryable")

    # ========================================================================
    # Recovery Information
    # ========================================================================
    recovery_strategy: WebSocketRecoveryStrategy = Field(
        description="Recommended recovery strategy"
    )
    suggested_action: str = Field(description="Suggested action for this error")
    retry_after_ms: int | None = Field(
        default=None, description="Suggested retry delay in milliseconds"
    )

    # ========================================================================
    # Context Information
    # ========================================================================
    connection_id: str = Field(description="WebSocket connection identifier")
    exchange: str = Field(description="Exchange name")
    channel: str | None = Field(default=None, description="Channel name if applicable")
    topic: str | None = Field(default=None, description="Topic within channel if applicable")

    # ========================================================================
    # Sequence Information
    # ========================================================================
    sequence_number: int | None = Field(default=None, description="Message sequence number")
    sequence_gap: int | None = Field(default=None, description="Size of sequence gap if detected")

    # ========================================================================
    # Timing Information
    # ========================================================================
    timestamp: datetime = Field(description="Error timestamp")
    timestamp_ms: int = Field(description="Error timestamp in milliseconds")
    connection_duration_ms: int | None = Field(
        default=None, description="Connection duration in milliseconds"
    )
    time_since_last_message_ms: int | None = Field(
        default=None, description="Time since last message in milliseconds"
    )

    # ========================================================================
    # Error Chain
    # ========================================================================
    error_chain_length: int = Field(default=0, description="Number of errors in chain")
    root_cause: str | None = Field(default=None, description="Root cause error if available")

    # ========================================================================
    # Additional Metadata
    # ========================================================================
    correlation_id: str | None = Field(default=None, description="Correlation ID for tracing")
    session_id: str | None = Field(default=None, description="Session identifier")
    user_id: str | None = Field(default=None, description="User identifier if authenticated")
    client_version: str | None = Field(default=None, description="Client library version")

    # ========================================================================
    # Performance Metrics
    # ========================================================================
    active_subscriptions: int = Field(default=0, description="Number of active subscriptions")
    pending_messages: int = Field(default=0, description="Number of pending messages")
    reconnect_count: int = Field(default=0, description="Number of reconnection attempts")

    # ========================================================================
    # Exception Information
    # ========================================================================
    exception_type: str | None = Field(default=None, description="Exception class name")
    exception_message: str | None = Field(default=None, description="Exception message")
    stack_trace: str | None = Field(default=None, description="Stack trace if available")

    # ========================================================================
    # Class Methods
    # ========================================================================

    @classmethod
    def from_stream_error(
        cls,
        error_code: WebSocketErrorCode,
        message: str,
        context: StreamErrorContext,
        severity: ErrorSeverity,
        recovery_strategy: WebSocketRecoveryStrategy,
        exception: Exception | None = None,
        stack_trace: str | None = None,
    ) -> WebSocketStreamLogData:
        """Create log data from stream error components.

        Args:
            error_code: WebSocket error code
            message: Error message
            context: Stream error context
            severity: Error severity
            recovery_strategy: Recovery strategy
            exception: Optional exception
            stack_trace: Optional stack trace

        Returns:
            WebSocketStreamLogData instance
        """
        # Extract error chain information
        error_chain_length = len(context.error_chain)
        root_cause = None
        if context.error_chain:
            root_cause = (
                f"{context.error_chain[0].error_class}: {context.error_chain[0].error_message}"
            )

        # Extract exception information
        exception_type = None
        exception_message = None
        if exception:
            exception_type = exception.__class__.__name__
            exception_message = str(exception)

        return cls(
            # Error identification
            error_code=error_code,
            error_code_name=error_code.name,
            error_category=error_code.get_category(),
            # Error details
            message=message,
            severity=severity,
            is_critical=error_code.is_critical(),
            is_retryable=error_code.is_retryable(),
            # Recovery information
            recovery_strategy=recovery_strategy,
            suggested_action=error_code.get_suggested_action(),
            retry_after_ms=context.metadata.backoff_ms,
            # Context information
            connection_id=context.connection_id,
            exchange=context.exchange,
            channel=context.channel,
            topic=context.topic,
            # Sequence information
            sequence_number=context.sequence_number,
            sequence_gap=context.get_sequence_gap_size(),
            # Timing information
            timestamp=datetime.fromtimestamp(context.error_timestamp_ms / 1000, tz=UTC),
            timestamp_ms=context.error_timestamp_ms,
            connection_duration_ms=context.get_connection_duration_ms(),
            time_since_last_message_ms=context.get_time_since_last_message_ms(),
            # Error chain
            error_chain_length=error_chain_length,
            root_cause=root_cause,
            # Additional metadata
            correlation_id=context.metadata.correlation_id,
            session_id=context.session_id,
            user_id=context.user_id,
            client_version=context.client_version,
            # Performance metrics
            active_subscriptions=context.active_subscriptions,
            pending_messages=context.pending_messages,
            reconnect_count=context.reconnect_count,
            # Exception information
            exception_type=exception_type,
            exception_message=exception_message,
            stack_trace=stack_trace,
        )

    def to_log_dict(self) -> dict[str, Any]:
        """Convert to dictionary for structured logging.

        Returns:
            Dictionary suitable for logging
        """
        return self.model_dump(
            exclude_none=True,
            mode="json",
            exclude={"stack_trace"},  # Exclude stack trace from standard logs
        )

    def to_metrics_dict(self) -> dict[str, Any]:
        """Convert to dictionary for metrics collection.

        Returns:
            Dictionary with metrics-relevant fields
        """
        return {
            "error_code": self.error_code.value,
            "error_category": self.error_category,
            "severity": self.severity.value,
            "is_critical": self.is_critical,
            "is_retryable": self.is_retryable,
            "recovery_strategy": self.recovery_strategy.value,
            "exchange": self.exchange,
            "channel": self.channel,
            "reconnect_count": self.reconnect_count,
            "sequence_gap": self.sequence_gap,
        }

    def get_log_level(self) -> str:
        """Get appropriate log level based on severity.

        Returns:
            Log level string (debug, info, warning, error, critical)
        """
        if self.severity <= ErrorSeverity.DEBUG:
            return "debug"
        if self.severity <= ErrorSeverity.INFO:
            return "info"
        if self.severity <= ErrorSeverity.WARNING:
            return "warning"
        if self.severity <= ErrorSeverity.ERROR:
            return "error"
        return "critical"

    def format_log_message(self) -> str:
        """Format a human-readable log message.

        Returns:
            Formatted log message
        """
        parts = [
            f"[{self.error_code_name}]",
            self.message,
            f"(Exchange: {self.exchange}",
        ]

        if self.channel:
            parts.append(f"Channel: {self.channel}")
        if self.topic:
            parts.append(f"Topic: {self.topic}")
        if self.sequence_gap:
            parts.append(f"Gap: {self.sequence_gap}")

        parts.append(f"Reconnects: {self.reconnect_count})")

        if self.suggested_action:
            parts.append(f"Action: {self.suggested_action}")

        return " ".join(parts)

    model_config = {
        "frozen": False,
        "validate_assignment": True,
        "use_enum_values": False,
    }
