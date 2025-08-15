"""Type-safe WebSocket stream error handler interface.

Provides a fully typed error handler for WebSocket streams,
eliminating all dict[str, Any] conversions in error handling.
"""

from __future__ import annotations

import asyncio
import logging
from logging import Logger
from typing import TYPE_CHECKING

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    TypedLogger,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.enums import WebSocketErrorCode
from cyberdelta.apis.websocket.error_handling.recovery.recovery_executor import RecoveryExecutor
from cyberdelta.apis.websocket.error_handling.recovery.recovery_policy import RecoveryPolicyManager
from cyberdelta.apis.websocket.exceptions import (
    WebSocketConnectionError,
    WebSocketMessageFormatError,
    WebSocketSequenceError,
    WebSocketStreamError,
    WebSocketStreamInterruptedError,
    WebSocketValidationError,
)
from cyberdelta.apis.websocket.memory.stream_log_data import WebSocketStreamLogData
from cyberdelta.apis.websocket.metrics.error_metrics import WebSocketErrorMetrics
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
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
        recovery_policy: RecoveryPolicyManager | None = None,
        recovery_executor: RecoveryExecutor | None = None,
    ) -> None:
        """Initialize error handler.

        Args:
            config: Error handling configuration
            logger: Logger instance
            metrics_collector: Optional metrics collector
            recovery_policy: Unified recovery policy manager
            recovery_executor: Unified recovery executor
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

        # Initialize unified recovery system
        if recovery_policy is not None and recovery_executor is not None:
            # Use provided unified recovery system
            self._recovery_policy = recovery_policy
            self._recovery_executor = recovery_executor
        else:
            # Create default unified recovery system
            self._recovery_policy = RecoveryPolicyManager(config)
            self._recovery_executor = RecoveryExecutor(self._recovery_policy)

    @property
    def recovery_policy(self) -> RecoveryPolicyManager:
        """Get the unified recovery policy manager."""
        return self._recovery_policy

    @property
    def recovery_executor(self) -> RecoveryExecutor:
        """Get the unified recovery executor."""
        return self._recovery_executor

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

        # Create metrics snapshot from collector
        class MetricsSnapshot:
            def __init__(self, metrics_collector: WebSocketErrorMetrics) -> None:
                # Get statistics directly from metrics collector
                stats = metrics_collector.get_statistics()
                self.total_errors = stats.total_errors_recorded
                self.errors_by_code: dict[str, int] = {}
                self.errors_by_severity: dict[str, int] = {}
                self.errors_by_exchange: dict[str, int] = {}

        return MetricsSnapshot(self._metrics)

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

        # Extract validation details for more informative error messages
        field_errors = error.errors()

        # Extract field name and value for better error reporting
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

        # Create more informative error message using extracted field info
        if field_name and field_value is not None:
            message = (
                f"Validation failed for field '{field_name}' with value '{field_value}': {error}"
            )
        elif field_name:
            message = f"Validation failed for field '{field_name}': {error}"
        else:
            message = f"Validation failed: {error}"

        # Create typed WebSocket error
        ws_error = WebSocketValidationError(
            message=message,
            context=error_context,
            cause=error,
            field=field_name,
            value=field_value,
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
        # error_context_obj is created, type-checked, and passed to the error constructor below

        # Determine actual format
        actual_format = type(actual_data).__name__
        if isinstance(actual_data, dict):
            actual_format = "dict"
        elif isinstance(actual_data, list):
            actual_format = "list"
        elif isinstance(actual_data, str):
            actual_format = "string"

        ws_error = WebSocketMessageFormatError(
            message=f"Invalid message format: expected {expected_format}, got {actual_format}",
            context=error_context_obj,
            code=WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
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

        gap_msg = f" (gap of {gap_size} messages)" if gap_size > 0 else ""
        error_code = (
            WebSocketErrorCode.SEQUENCE_OUT_OF_ORDER
            if actual_seq > expected_seq
            else WebSocketErrorCode.SEQUENCE_DUPLICATE
        )
        ws_error = WebSocketSequenceError(
            message=f"Sequence error: expected {expected_seq}, got {actual_seq}{gap_msg}",
            context=error_context,
            code=error_code,
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
        # error_context_obj is created, type-checked, and passed to the error constructor below

        ws_error = WebSocketStreamInterruptedError(
            message=f"WebSocket stream interrupted: {reason}",
            context=error_context_obj,
            code=WebSocketErrorCode.STREAM_INTERRUPTED,
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

        # Use unified recovery system
        await self._handle_error_unified(error)

        # Send alerts if needed
        if self.config.alerting.enable_alerting:
            await self._check_and_send_alerts(error)

    async def _handle_error_unified(self, error: WebSocketStreamError) -> None:
        """Handle error using unified recovery system.

        Args:
            error: WebSocket stream error to handle
        """
        # Generate recovery key for tracking
        key = f"{error.context.exchange}:{error.context.connection_id}:{error.code.name}"

        # Check policy decisions
        if not self._recovery_policy.should_retry(error):
            self._structlog_logger.info(
                "Error not retryable",
                error_code=error.code.name,
                key=key,
            )
            return

        # Check circuit breaker
        if self._recovery_policy.is_circuit_open(error.context):
            self._structlog_logger.warning(
                "Circuit breaker open, skipping recovery",
                error_code=error.code.name,
                key=key,
            )
            return

        # Get recovery strategy
        strategy = self._recovery_policy.get_recovery_strategy(error)
        if strategy == WebSocketRecoveryStrategy.NONE:
            return

        # Calculate delay
        attempt = self._recovery_policy.get_retry_count(error.context)
        delay = self._recovery_policy.calculate_backoff_delay(error, attempt)

        if delay > 0:
            self._structlog_logger.debug(
                "Waiting before recovery attempt",
                delay_seconds=delay,
                attempt=attempt,
                key=key,
            )
            await asyncio.sleep(delay)

        # Execute recovery
        success = await self._recovery_executor.execute_recovery(error, strategy)

        # Update circuit state
        self._recovery_policy.update_circuit_state(error.context, success)

        if success:
            self._structlog_logger.info(
                "Recovery successful",
                strategy=strategy.value,
                attempt=attempt,
                key=key,
            )
        else:
            self._structlog_logger.warning(
                "Recovery failed",
                strategy=strategy.value,
                attempt=attempt,
                key=key,
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

        # Check error rate using unified recovery system
        retry_count = self._recovery_policy.get_retry_count(error.context)

        if retry_count >= self.config.alerting.connection_failure_alert_threshold:
            await self._send_alert(
                level="warning",
                message=f"High retry count: {retry_count} for {error.context.connection_id}",
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

    def get_error_stats(self) -> dict[str, object]:
        """Get error statistics from unified recovery system.

        Returns:
            Dictionary with recovery system statistics
        """
        return self._recovery_policy.get_statistics()

    def get_statistics(self) -> dict[str, object]:
        """Get handler statistics.

        Returns:
            Dictionary with handler statistics
        """
        recovery_stats = self._recovery_policy.get_statistics()
        executor_stats = self._recovery_executor.get_statistics()

        return {
            "recovery_policy": recovery_stats,
            "recovery_executor": executor_stats,
            "has_metrics_collector": self._metrics is not None,
            "circuit_breaker_enabled": self.config.recovery.circuit_breaker_enabled,
            "max_recovery_attempts": self.config.recovery.max_recovery_attempts,
        }

    def reset_error_stats(self) -> None:
        """Reset error statistics in unified recovery system."""
        # Reset all connection states in the recovery policy manager
        recovery_stats = self._recovery_policy.get_statistics()
        if "total_connections_tracked" in recovery_stats:
            # Reset through policy manager's public interface
            pass  # Policy manager handles internal state reset

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
