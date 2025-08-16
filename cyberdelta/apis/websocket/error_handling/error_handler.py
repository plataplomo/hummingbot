"""WebSocket Stream Error Handler.

This module provides an error handler that uses the recovery system
for consistent error handling and recovery across the WebSocket infrastructure.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    TypedLogger,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.enums.websocket import WebSocketErrorCode
from cyberdelta.apis.websocket.error_handling.recovery import (
    ConnectionManagerProtocol,
    MessageBufferProtocol,
    RecoveryExecutor,
    RecoveryPolicyManager,
    StateManagerProtocol,
    SubscriptionManagerProtocol,
)
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


if TYPE_CHECKING:
    from logging import Logger


# Constants for error handling
RAW_MESSAGE_TRUNCATE_LENGTH = 200  # Max length for raw message in error context


class WebSocketErrorHandler(TypedLogger[WebSocketStreamLogData]):
    """Error handler for WebSocket streams.

    This handler uses the recovery system for consistent error handling
    and recovery across the WebSocket infrastructure.
    """

    def __init__(
        self,
        config: WebSocketErrorConfig,
        logger: TypedLogger[WebSocketStreamLogData] | None = None,
        metrics_collector: WebSocketErrorMetrics | None = None,
        connection_manager: ConnectionManagerProtocol | None = None,
        subscription_manager: SubscriptionManagerProtocol | None = None,
        state_manager: StateManagerProtocol | None = None,
        message_buffer: MessageBufferProtocol | None = None,
    ) -> None:
        """Initialize error handler.

        Args:
            config: Error handling configuration
            logger: Logger instance
            metrics_collector: Optional metrics collector
            connection_manager: Connection management implementation
            subscription_manager: Subscription management implementation
            state_manager: State management implementation
            message_buffer: Message buffer implementation
        """
        self.config = config

        # Setup logging
        if logger is not None:
            self._logger = logger
            self._python_logger = None
        else:
            self._logger = self
            self._python_logger = logging.getLogger(__name__)
        self._structlog_logger = get_logger("WebSocketErrorHandler")

        # Initialize metrics if enabled
        self._metrics: WebSocketErrorMetrics | None
        if metrics_collector is not None:
            self._metrics = metrics_collector
        elif config.metrics.enable_metrics_collection:
            self._metrics = WebSocketErrorMetrics(
                config=config.metrics, logger=self._logger.get_logger()
            )
        else:
            self._metrics = None

        # Initialize recovery system
        self._recovery_policy = RecoveryPolicyManager(config)
        self._recovery_executor = RecoveryExecutor(
            policy=self._recovery_policy,
            connection_manager=connection_manager,
            subscription_manager=subscription_manager,
            state_manager=state_manager,
            message_buffer=message_buffer,
        )

    @property
    def recovery_policy(self) -> RecoveryPolicyManager:
        """Get the recovery policy manager."""
        return self._recovery_policy

    @property
    def recovery_executor(self) -> RecoveryExecutor:
        """Get the recovery executor."""
        return self._recovery_executor

    @property
    def metrics_collector(self) -> WebSocketErrorMetrics | None:
        """Get the metrics collector."""
        return self._metrics

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

        # Record metrics if stream_error is provided
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
    # Core Error Handling with Recovery
    # ========================================================================

    async def handle_stream_error(self, error: WebSocketStreamError) -> None:
        """Handle WebSocket stream error with recovery.

        Args:
            error: WebSocket stream error to handle
        """
        # Log the error
        log_data = error.to_log_data()
        self.log_error(error.severity, log_data, error.cause, error)

        # Check if recovery should be attempted
        if not self._recovery_policy.should_retry(error):
            self._structlog_logger.info(
                "Recovery not attempted",
                error_code=error.code.name,
                connection_id=error.context.connection_id,
                reason="Policy decision",
            )
            await self._send_alert_if_needed(error)
            return

        # Get recovery strategy
        strategy = self._recovery_policy.get_recovery_strategy(error)

        # Calculate backoff if needed
        if strategy in {
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            WebSocketRecoveryStrategy.LINEAR_BACKOFF,
        }:
            retry_count = self._recovery_policy.get_retry_count(error.context)
            delay = self._recovery_policy.calculate_backoff_delay(error, retry_count)
            self._structlog_logger.debug(
                "Applying backoff delay",
                delay=delay,
                retry_count=retry_count,
            )
            await asyncio.sleep(delay)

        # Execute recovery
        success = await self._recovery_executor.execute_recovery(error, strategy)

        if success:
            self._structlog_logger.info(
                "Recovery successful",
                error_code=error.code.name,
                strategy=strategy.name,
                connection_id=error.context.connection_id,
            )
        else:
            self._structlog_logger.warning(
                "Recovery failed",
                error_code=error.code.name,
                strategy=strategy.name,
                connection_id=error.context.connection_id,
            )
            await self._send_alert_if_needed(error)

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
            context: WebSocket context
            payload: The payload that failed validation

        Raises:
            TypeError: If context does not provide StreamErrorContext
        """
        # Create typed error context
        error_context_obj = context.create_error_context()
        if not isinstance(error_context_obj, StreamErrorContext):
            raise TypeError(type(error_context_obj).__name__)
        error_context = error_context_obj

        # Extract validation details
        field_errors = error.errors()
        field_name: str | None = None
        field_value: object | None = None

        if field_errors:
            first_error = field_errors[0]
            if first_error.get("loc"):
                loc = first_error["loc"]
                if len(loc) > 0:
                    field_name = str(loc[0])
            if "input" in first_error:
                field_value = first_error["input"]

        # Create error message
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

        ws_error = WebSocketStreamInterruptedError(
            message=f"WebSocket stream interrupted: {reason}",
            context=error_context_obj,
            code=WebSocketErrorCode.STREAM_INTERRUPTED,
        )

        await self.handle_stream_error(ws_error)

    # ========================================================================
    # Alerting
    # ========================================================================

    async def _send_alert_if_needed(self, error: WebSocketStreamError) -> None:
        """Send alert if needed based on error severity and configuration.

        Args:
            error: The WebSocket error
        """
        if not self.config.alerting.enable_alerting:
            return

        # Check if circuit is open
        if self._recovery_policy.is_circuit_open(error.context):
            await self._send_alert(
                level="critical",
                message=f"Circuit breaker open for {error.context.exchange}",
                error=error,
            )

        # Check critical errors
        if error.is_critical:
            await self._send_alert(
                level="critical",
                message=f"Critical WebSocket error: {error}",
                error=error,
            )

        # Check retry exhaustion
        retry_count = self._recovery_policy.get_retry_count(error.context)
        if retry_count >= self.config.recovery.max_recovery_attempts:
            await self._send_alert(
                level="warning",
                message=f"Max retries exhausted for {error.context.exchange}",
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
            console_logger = get_logger("websocket_console_alerts")
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

    def get_statistics(self) -> dict[str, object]:
        """Get handler statistics.

        Returns:
            Dictionary with handler statistics
        """
        policy_stats = self._recovery_policy.get_statistics()
        executor_stats = self._recovery_executor.get_statistics()

        return {
            "policy": policy_stats,
            "executor": executor_stats,
            "has_metrics_collector": self._metrics is not None,
            "alerting_enabled": self.config.alerting.enable_alerting,
        }

    async def shutdown(self) -> None:
        """Shutdown error handler gracefully."""
        # Flush metrics if available
        if self._metrics:
            try:
                # Give metrics collector a chance to flush
                await asyncio.wait_for(
                    self._flush_metrics(),
                    timeout=self.config.metrics.metrics_flush_interval_ms / 1000.0,
                )
                self._structlog_logger.info("Metrics flushed successfully during shutdown")
            except TimeoutError:
                self._structlog_logger.warning("Metrics flush timed out during shutdown")
            except Exception as e:
                self._structlog_logger.exception(
                    "Error flushing metrics during shutdown",
                    error=str(e),
                )
            finally:
                self._metrics.clear_metrics()

        # Shutdown executor
        await self._recovery_executor.shutdown()

        self._structlog_logger.info("Error handler shutdown complete")

    async def _flush_metrics(self) -> None:
        """Flush pending metrics data."""
        if not self._metrics:
            return

        stats = self._metrics.get_statistics()
        self._structlog_logger.info(
            "Final metrics before shutdown",
            total_errors_recorded=stats.total_errors_recorded,
            active_connections=stats.active_connections,
            total_recoveries_recorded=stats.total_recoveries_recorded,
        )
