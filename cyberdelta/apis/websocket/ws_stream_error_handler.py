"""Type-safe WebSocket stream error handler interface.

Provides a fully typed error handler for WebSocket streams,
eliminating all dict[str, Any] conversions in error handling.
"""

from __future__ import annotations

import asyncio
import logging
from logging import Logger
from typing import TYPE_CHECKING, Protocol

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    TypedLogger,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_error_metrics import AggregatedMetrics, WebSocketErrorMetrics
from cyberdelta.apis.websocket.ws_exceptions import (
    WebSocketConnectionError,
    WebSocketMessageFormatError,
    WebSocketSequenceError,
    WebSocketStreamInterruptedError,
    WebSocketValidationError,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_log_data import WebSocketStreamLogData
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.config.structlog_config import get_logger


# Constants for error handling
RAW_MESSAGE_TRUNCATE_LENGTH = 200  # Max length for raw message in error context

if TYPE_CHECKING:
    pass


# ============================================================================
# Protocols
# ============================================================================


# Use the unified WebSocketContextProtocol from ws_protocols instead of duplicate


class RecoveryHandlerProtocol(Protocol):
    """Protocol for recovery handlers."""

    async def handle_recovery(
        self,
        error: WebSocketStreamError,
        strategy: WebSocketRecoveryStrategy,
    ) -> bool:
        """Handle recovery for an error.

        Args:
            error: The WebSocket error
            strategy: Recovery strategy to apply

        Returns:
            True if recovery succeeded
        """
        ...


# ============================================================================
# Helper Classes
# ============================================================================


class EmptyMetrics:
    """Empty metrics object returned when metrics collection is disabled."""

    def __init__(self) -> None:
        """Initialize empty metrics."""
        self.total_errors = 0
        self.errors_by_code: dict[str, int] = {}
        self.errors_by_severity: dict[str, int] = {}
        self.errors_by_exchange: dict[str, int] = {}


# ============================================================================
# Error Handler Implementation
# ============================================================================


class WebSocketStreamErrorHandler(TypedLogger[WebSocketStreamLogData]):
    """Type-safe error handler for WebSocket streams.

    This handler processes WebSocket errors with full type safety,
    no dict conversions, and rich recovery strategies.
    """

    def __init__(
        self,
        config: WebSocketErrorConfig,
        logger: TypedLogger[WebSocketStreamLogData] | None = None,
        metrics_collector: WebSocketErrorMetrics | None = None,
        recovery_handler: RecoveryHandlerProtocol | None = None,
    ) -> None:
        """Initialize error handler.

        Args:
            config: Error handling configuration
            logger: Logger instance
            metrics_collector: Optional metrics collector
            recovery_handler: Optional recovery handler
        """
        self.config = config
        if logger is not None:
            self._logger = logger
            self._python_logger = None  # Not needed when external logger provided
        else:
            # Use self as the logger since this class implements TypedLogger
            self._logger = self
            # Store the actual Python logger for basic logging operations
            self._python_logger = logging.getLogger(__name__)
        self._structlog_logger = get_logger("ws_stream_error_handler")

        # Initialize metrics if enabled and not provided
        self._metrics: WebSocketErrorMetrics | None
        if metrics_collector is not None:
            self._metrics = metrics_collector
        elif config.metrics.enable_metrics_collection:
            self._metrics = WebSocketErrorMetrics(
                config=config.metrics, logger=self._logger.get_logger()
            )
        else:
            self._metrics = None

        self._recovery_handler = recovery_handler

        # Track error counts for circuit breaking
        self._error_counts: dict[str, int] = {}
        self._last_error_times: dict[str, float] = {}

    @property
    def recovery_handler(self) -> RecoveryHandlerProtocol | None:
        """Get the recovery handler."""
        return self._recovery_handler

    @property
    def metrics_collector(self) -> WebSocketErrorMetrics | None:
        """Get the metrics collector."""
        return self._metrics

    def get_metrics(self) -> object:
        """Get the current error metrics.

        Returns:
            Current metrics snapshot with total_errors and errors_by_code properties
        """
        if self._metrics is None:
            return EmptyMetrics()

        # Get aggregated metrics from the collector
        aggregated = self._metrics.get_aggregated_metrics()

        # Create an object with the expected properties
        class MetricsSnapshot:
            def __init__(self, agg: AggregatedMetrics) -> None:
                self.total_errors = sum(agg.error_counts_by_code.values())
                self.errors_by_code = agg.error_counts_by_code
                self.errors_by_severity: dict[str, int] = {}
                self.errors_by_exchange = agg.error_counts_by_exchange

        return MetricsSnapshot(aggregated)

    @property
    def logger(self) -> TypedLogger[WebSocketStreamLogData]:
        """Get the logger."""
        return self._logger

    # ========================================================================
    # TypedLogger Protocol Implementation
    # ========================================================================

    def log_error(
        self,
        severity: ErrorSeverity,
        log_data: WebSocketStreamLogData,
        exc_info: Exception | None = None,
        stream_error: WebSocketStreamError | None = None,
    ) -> None:
        """Log error with typed data.

        Args:
            severity: Error severity
            log_data: Structured log data
            exc_info: Optional exception info
            stream_error: Optional WebSocket stream error for metrics recording
        """
        log_level = log_data.get_log_level()
        log_message = log_data.format_log_message()

        if self.config.logging.structured_logging:
            # Structured JSON logging
            log_dict = log_data.to_log_dict()
            self._logger.get_logger().log(
                getattr(logging, log_level.upper()),
                log_message,
                extra={"structured_data": log_dict},
                exc_info=exc_info,
            )
        else:
            # Standard text logging
            self._logger.get_logger().log(
                getattr(logging, log_level.upper()),
                log_message,
                exc_info=exc_info,
            )

        # Record metrics if stream_error is provided and metrics are enabled
        if stream_error is not None and self._metrics is not None:
            self._metrics.record_error(stream_error)

    def get_logger(self) -> Logger:
        """Get underlying logger instance.

        Returns:
            Logger instance

        Raises:
            RuntimeError: If python logger is not initialized
        """
        if self._logger is self:
            if self._python_logger is None:
                raise RuntimeError
            return self._python_logger
        return self._logger.get_logger()

    # ========================================================================
    # Validation Error Handling
    # ========================================================================

    async def handle_validation_error(
        self,
        error: ValidationError,
        context: WebSocketContextProtocol,
        payload: BaseModel,
    ) -> None:
        """Handle Pydantic validation error with full type safety.

        Args:
            error: Pydantic validation error
            context: WebSocket context (TYPED!)
            payload: The payload that failed validation (TYPED!)
        
        Raises:
            TypeError: If context does not provide StreamErrorContext
        """
        # Create typed error context
        error_context_obj = context.create_error_context()
        # Cast to StreamErrorContext since we know the actual type
        if not isinstance(error_context_obj, StreamErrorContext):
            raise TypeError(type(error_context_obj).__name__)
        error_context = error_context_obj

        # Extract validation details
        field_errors = error.errors()

        # Extract field name and value safely
        field_name: str | None = None
        field_value: object | None = None

        if field_errors:
            first_error = field_errors[0]
            # Extract field location
            if first_error.get("loc"):
                loc = first_error["loc"]
                if len(loc) > 0:
                    # Convert location element to string safely
                    loc_element = loc[0]
                    field_name = str(loc_element)

            # Extract field value
            if "input" in first_error:
                field_value = first_error["input"]

        # Create typed WebSocket error
        ws_error = WebSocketValidationError(
            message=f"Validation failed: {error}",
            context=error_context,
            field=field_name,
            value=field_value,
            cause=error,
        )

        # Handle the error
        await self.handle_stream_error(ws_error)

    # ========================================================================
    # Message Format Error Handling
    # ========================================================================

    async def handle_message_format_error(
        self,
        context: WebSocketContextProtocol,
        expected_format: str,
        actual_data: dict[str, object]
        | list[object]
        | BaseModel
        | str
        | bytes
        | float
        | bool
        | None,
    ) -> None:
        """Handle message format error.

        Args:
            context: WebSocket context
            expected_format: Expected message format
            actual_data: Actual data received
        
        Raises:
            TypeError: If context does not provide StreamErrorContext
        """
        error_context_obj = context.create_error_context()
        if not isinstance(error_context_obj, StreamErrorContext):
            raise TypeError(type(error_context_obj).__name__)
        error_context = error_context_obj

        # Determine actual format
        actual_format = type(actual_data).__name__
        if isinstance(actual_data, dict):
            actual_format = "dict"
        elif isinstance(actual_data, list):
            actual_format = "list"
        elif isinstance(actual_data, str):
            actual_format = "string"

        # Convert actual_data to string for raw_message
        raw_message: str | None = None
        if actual_data is not None:
            data_str = str(actual_data)
            raw_message = (
                data_str[:RAW_MESSAGE_TRUNCATE_LENGTH]
                if len(data_str) > RAW_MESSAGE_TRUNCATE_LENGTH
                else data_str
            )

        ws_error = WebSocketMessageFormatError(
            context=error_context,
            expected_format=expected_format,
            actual_format=actual_format,
            raw_message=raw_message,
        )

        await self.handle_stream_error(ws_error)

    # ========================================================================
    # Sequence Error Handling
    # ========================================================================

    async def handle_sequence_error(
        self,
        context: WebSocketContextProtocol,
        expected_seq: int,
        actual_seq: int,
    ) -> None:
        """Handle sequence error.

        Args:
            context: WebSocket context
            expected_seq: Expected sequence number
            actual_seq: Actual sequence number received
        
        Raises:
            TypeError: If context does not provide StreamErrorContext
        """
        error_context_obj = context.create_error_context()
        if not isinstance(error_context_obj, StreamErrorContext):
            raise TypeError(type(error_context_obj).__name__)
        error_context = error_context_obj
        error_context.expected_sequence = expected_seq
        error_context.sequence_number = actual_seq

        gap_size = abs(actual_seq - expected_seq) if actual_seq > expected_seq else 0

        ws_error = WebSocketSequenceError(
            context=error_context,
            expected_seq=expected_seq,
            actual_seq=actual_seq,
            gap_size=gap_size if gap_size > 0 else None,
        )

        await self.handle_stream_error(ws_error)

    # ========================================================================
    # Connection Error Handling
    # ========================================================================

    async def handle_connection_error(
        self,
        context: WebSocketContextProtocol,
        error: Exception,
        message: str | None = None,
    ) -> None:
        """Handle connection error.

        Args:
            context: WebSocket context
            error: Original exception
            message: Optional error message
        
        Raises:
            TypeError: If context does not provide StreamErrorContext
        """
        error_context_obj = context.create_error_context()
        if not isinstance(error_context_obj, StreamErrorContext):
            raise TypeError(type(error_context_obj).__name__)
        error_context = error_context_obj

        # Determine error code based on exception type
        code = WebSocketErrorCode.CONNECTION_LOST
        if "timeout" in str(error).lower():
            code = WebSocketErrorCode.CONNECTION_TIMEOUT
        elif "refused" in str(error).lower():
            code = WebSocketErrorCode.CONNECTION_REFUSED
        elif "reset" in str(error).lower():
            code = WebSocketErrorCode.CONNECTION_RESET

        ws_error = WebSocketConnectionError(
            message=message or f"Connection error: {error}",
            context=error_context,
            code=code,
            cause=error,
        )

        await self.handle_stream_error(ws_error)

    # ========================================================================
    # Stream Interruption Handling
    # ========================================================================

    async def handle_stream_interruption(
        self,
        context: WebSocketContextProtocol,
        reason: str | None = None,
    ) -> None:
        """Handle stream interruption.

        Args:
            context: WebSocket context
            reason: Reason for interruption
        
        Raises:
            TypeError: If context does not provide StreamErrorContext
        """
        error_context_obj = context.create_error_context()
        if not isinstance(error_context_obj, StreamErrorContext):
            raise TypeError(type(error_context_obj).__name__)
        error_context = error_context_obj

        ws_error = WebSocketStreamInterruptedError(
            context=error_context,
            reason=reason,
        )

        await self.handle_stream_error(ws_error)

    # ========================================================================
    # Core Error Handling
    # ========================================================================

    async def handle_stream_error(self, error: WebSocketStreamError) -> None:
        """Handle WebSocket stream error.

        Args:
            error: WebSocket stream error to handle
        """
        # Log the error and record metrics
        log_data = error.to_log_data()
        self.log_error(error.severity, log_data, error.cause, error)

        # Track error for circuit breaking
        self._track_error(error)

        # Check circuit breaker
        if self._should_circuit_break(error):
            await self._trigger_circuit_breaker(error)
            return

        # Attempt recovery if configured
        if self.config.recovery.max_recovery_attempts > 0:
            await self._attempt_recovery(error)

        # Send alerts if needed
        if self.config.alerting.enable_alerting:
            await self._check_and_send_alerts(error)

    # ========================================================================
    # Error Tracking & Circuit Breaking
    # ========================================================================

    def _track_error(self, error: WebSocketStreamError) -> None:
        """Track error for circuit breaking.

        Args:
            error: Error to track
        """
        key = f"{error.context.exchange}:{error.context.channel or 'global'}"

        # Initialize if needed
        if key not in self._error_counts:
            self._error_counts[key] = 0
            self._last_error_times[key] = 0

        # Update counts
        self._error_counts[key] += 1

        # Reset count if enough time has passed
        now = error.timestamp_ms / 1000.0
        timeout_seconds = self.config.recovery.circuit_breaker_timeout_ms / 1000.0
        if now - self._last_error_times[key] > timeout_seconds:
            self._error_counts[key] = 1

        self._last_error_times[key] = now

    def _should_circuit_break(self, error: WebSocketStreamError) -> bool:
        """Check if circuit breaker should trigger.

        Args:
            error: Error to check

        Returns:
            True if circuit breaker should trigger
        """
        if not self.config.recovery.circuit_breaker_enabled:
            return False

        # Critical errors always trigger
        if error.is_critical:
            return True

        # Check error count threshold
        key = f"{error.context.exchange}:{error.context.channel or 'global'}"
        error_count = self._error_counts.get(key, 0)

        return error_count >= self.config.recovery.circuit_breaker_threshold

    async def _trigger_circuit_breaker(self, error: WebSocketStreamError) -> None:
        """Trigger circuit breaker.

        Args:
            error: Error that triggered circuit breaker
        """
        error_key = f"{error.context.exchange}:{error.context.channel or 'global'}"
        error_count = self._error_counts.get(error_key, 0)
        self._logger.get_logger().critical(
            "Circuit breaker triggered for %s:%s after %d errors",
            error.context.exchange,
            error.context.channel or "global",
            error_count,
        )

        # Reset error count
        key = f"{error.context.exchange}:{error.context.channel or 'global'}"
        self._error_counts[key] = 0

        # Trigger circuit breaker recovery
        if self._recovery_handler:
            await self._recovery_handler.handle_recovery(
                error,
                WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
            )

    # ========================================================================
    # Recovery
    # ========================================================================

    async def _attempt_recovery(self, error: WebSocketStreamError) -> None:
        """Attempt recovery for an error.

        Args:
            error: Error to recover from
        """
        if not self._recovery_handler:
            return

        strategy = error.get_recovery_strategy()
        if strategy == WebSocketRecoveryStrategy.NONE:
            return

        # Check retry count
        if error.context.metadata.retry_count >= self.config.recovery.max_recovery_attempts:
            self._logger.get_logger().error(
                "Max recovery attempts (%d) exceeded for %s",
                self.config.recovery.max_recovery_attempts,
                error.code.name,
            )
            return

        # Calculate delay
        delay_ms = error.get_retry_delay_ms()
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

        # Attempt recovery
        success = await self._recovery_handler.handle_recovery(error, strategy)

        if success:
            self._logger.get_logger().info("Recovery successful for %s", error.code.name)
        else:
            error.context.metadata.retry_count += 1
            self._logger.get_logger().warning(
                "Recovery failed for %s, attempt %d/%d",
                error.code.name,
                error.context.metadata.retry_count,
                self.config.recovery.max_recovery_attempts,
            )

    # ========================================================================
    # Alerting
    # ========================================================================

    async def _check_and_send_alerts(self, error: WebSocketStreamError) -> None:
        """Check if alerts should be sent and send them.

        Args:
            error: Error to check for alerting
        """
        # Check critical error threshold
        if error.is_critical:
            await self._send_alert(
                level="critical",
                message=f"Critical WebSocket error: {error}",
                error=error,
            )

        # Check error rate (simplified - real implementation would track rates)
        key = f"{error.context.exchange}:{error.context.channel or 'global'}"
        error_count = self._error_counts.get(key, 0)

        if error_count >= self.config.alerting.connection_failure_alert_threshold:
            await self._send_alert(
                level="warning",
                message=f"High error rate detected: {error_count} errors for {key}",
                error=error,
            )

    async def _send_alert(
        self,
        level: str,
        message: str,
        error: WebSocketStreamError,
    ) -> None:
        """Send an alert.

        Args:
            level: Alert level
            message: Alert message
            error: Related error
        """
        if self.config.alerting.log_alerts:
            self._logger.get_logger().log(
                getattr(logging, level.upper()),
                "ALERT: %s",
                message,
                extra={"error_data": {"ws_error_code": error.code.name, "message": error.message}},
            )

        if self.config.alerting.console_alerts:
            # Console alerting via structlog with console output
            console_logger = get_logger("websocket_console_alerts")

            # Use appropriate structlog log level method
            log_method = getattr(console_logger, level.lower(), console_logger.warning)
            log_method(
                f"WEBSOCKET ALERT: {message}",
                alert_level=level.upper(),
                exchange=error.context.exchange,
                channel=error.context.channel or "global",
                error_code=error.code.value,
                error_severity=error.severity.name,
                timestamp_ms=error.timestamp_ms,
                alert_type="websocket_error",
            )

        # Webhook alerts would go here
        if self.config.alerting.webhook_alerts and self.config.alerting.webhook_url:
            # Implementation would send to webhook
            pass

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def get_error_stats(self) -> dict[str, int]:
        """Get error statistics.

        Returns:
            Dictionary of error counts by key
        """
        return self._error_counts.copy()

    def get_statistics(self) -> dict[str, object]:
        """Get handler statistics.

        Returns:
            Dictionary with handler statistics
        """
        total_errors = sum(self._error_counts.values())

        return {
            "total_errors_handled": total_errors,
            "error_counts_by_channel": self._error_counts.copy(),
            "has_recovery_handler": self._recovery_handler is not None,
            "has_metrics_collector": self._metrics is not None,
            "circuit_breaker_enabled": self.config.recovery.circuit_breaker_enabled,
            "max_recovery_attempts": self.config.recovery.max_recovery_attempts,
        }

    def reset_error_stats(self) -> None:
        """Reset error statistics."""
        self._error_counts.clear()
        self._last_error_times.clear()

    async def shutdown(self) -> None:
        """Shutdown error handler gracefully."""
        # Flush any pending metrics asynchronously
        if self._metrics:
            try:
                # Give metrics collector a chance to flush pending data
                await asyncio.wait_for(
                    self._flush_metrics(),
                    timeout=self.config.metrics.metrics_flush_interval_ms / 1000.0,
                )
                self._structlog_logger.info(
                    "Metrics flushed successfully during shutdown",
                    metrics_flush_timeout_ms=self.config.metrics.metrics_flush_interval_ms,
                )
            except TimeoutError:
                self._structlog_logger.warning(
                    "Metrics flush timed out during shutdown, some data may be lost",
                    timeout_ms=self.config.metrics.metrics_flush_interval_ms,
                )
            except Exception as e:
                self._structlog_logger.exception(
                    "Error flushing metrics during shutdown",
                    error=str(e),
                    error_type=type(e).__name__,
                )
            finally:
                # Clear metrics after attempting to flush
                self._metrics.clear_metrics()

        # Log shutdown completion
        self._structlog_logger.info("WebSocket error handler shutdown complete")

    async def _flush_metrics(self) -> None:
        """Flush pending metrics data."""
        if not self._metrics:
            return

        # Log final metrics statistics
        stats = self._metrics.get_statistics()
        self._structlog_logger.info(
            "Final metrics before shutdown",
            total_errors_recorded=stats.total_errors_recorded,
            active_connections=stats.active_connections,
            total_recoveries_recorded=stats.total_recoveries_recorded,
        )
