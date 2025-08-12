"""WebSocket Connection Manager Error Bridge.

This module bridges the connection manager with the typed error system,
implementing Step 48: Update Connection Manager Error Handling.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_recovery_strategy_router import (
    RecoveryAction,
    RecoveryResult,
    RecoveryStrategyRouter,
)
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.connectivity.ws_manager import WebSocketManager
    from cyberdelta.apis.websocket.ws_error_recovery import WebSocketErrorRecovery


# Connection error handling constants
ERROR_SEVERITY_FAILURE_THRESHOLD = 3
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
LINEAR_BACKOFF_FAILURE_THRESHOLD = 2


class ConnectionErrorContext(BaseModel):
    """Context for connection manager errors."""

    exchange_name: str
    ws_url: str
    is_connected: bool
    failure_count: int = Field(default=0, ge=0)
    circuit_open: bool = False
    reconnect_attempts: int = Field(default=0, ge=0)
    max_reconnect_attempts: int = Field(default=10, ge=0)
    last_error: str | None = None


class ConnectionErrorBridge:
    """Bridges connection manager errors with typed error system."""

    def __init__(
        self,
        exchange_name: str,
        connection_id: str | None = None,
    ) -> None:
        """Initialize connection error bridge.

        Args:
            exchange_name: Name of the exchange
            connection_id: Optional connection identifier
        """
        self.exchange_name = exchange_name
        self.connection_id = connection_id or f"{exchange_name}-conn"
        self.logger = get_logger(f"ConnectionErrorBridge.{exchange_name}")

        # Initialize recovery router
        self.recovery_router = RecoveryStrategyRouter()

        # Track error history
        self.error_history: list[WebSocketStreamError] = []
        self.max_history = 100

    def create_connection_error(
        self,
        error: Exception,
        context: ConnectionErrorContext,
        error_type: str = "connection",
    ) -> WebSocketStreamError:
        """Create typed error from connection manager exception.

        Args:
            error: The original exception
            context: Connection error context
            error_type: Type of error (connection, listener, ping, etc.)

        Returns:
            WebSocketStreamError with appropriate context
        """
        # Map error types to WebSocket error codes
        error_code = self._map_error_code(error, error_type)

        # Determine severity based on error and context
        severity = self._determine_severity(error_code, context)

        # Determine recovery strategy
        recovery_strategy = self._determine_recovery_strategy(
            error_code,
            context,
            severity,
        )

        # Create stream error context
        stream_context = StreamErrorContext(
            connection_id=self.connection_id,
            exchange=self.exchange_name,
            reconnect_count=context.reconnect_attempts,
        )

        # Add extra context data
        stream_context.extra_context = {
            "failure_count": context.failure_count,
            "circuit_open": context.circuit_open,
            "ws_url": context.ws_url,
        }

        # Create typed error
        typed_error = WebSocketStreamError(
            message=f"{error_type.capitalize()} error: {error!s}",
            code=error_code,
            context=stream_context,
            severity=severity,
            recovery_strategy=recovery_strategy,
            cause=error,
        )

        # Track error history
        self._track_error(typed_error)

        return typed_error

    def _map_error_code(self, error: Exception, error_type: str) -> WebSocketErrorCode:
        """Map exception to WebSocket error code.

        Args:
            error: The exception to map
            error_type: Type of error context

        Returns:
            Appropriate WebSocketErrorCode
        """
        error_class = type(error).__name__
        error_msg = str(error).lower()

        # Connection errors
        if isinstance(error, ConnectionError):
            if "reset" in error_msg:
                return WebSocketErrorCode.CONNECTION_RESET
            if "refused" in error_msg:
                return WebSocketErrorCode.CONNECTION_REFUSED
            return WebSocketErrorCode.CONNECTION_FAILED

        # Timeout errors
        if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
            if error_type == "connection":
                return WebSocketErrorCode.CONNECTION_TIMEOUT
            if error_type == "ping":
                return WebSocketErrorCode.PING_TIMEOUT
            return WebSocketErrorCode.CONNECTION_TIMEOUT

        # OS errors
        if isinstance(error, OSError):
            if "broken pipe" in error_msg:
                return WebSocketErrorCode.CONNECTION_RESET
            if "network" in error_msg:
                return WebSocketErrorCode.CONNECTION_LOST
            return WebSocketErrorCode.CONNECTION_LOST

        # SSL/TLS errors
        if "ssl" in error_class.lower() or "tls" in error_msg:
            return WebSocketErrorCode.SSL_ERROR

        # Default mappings by error type
        if error_type == "listener":
            return WebSocketErrorCode.STREAM_INTERRUPTED
        if error_type == "ping":
            return WebSocketErrorCode.HEARTBEAT_TIMEOUT
        if error_type == "reconnect":
            return WebSocketErrorCode.CONNECTION_FAILED
        return WebSocketErrorCode.CONNECTION_FAILED

    def _determine_severity(
        self,
        error_code: WebSocketErrorCode,
        context: ConnectionErrorContext,
    ) -> ErrorSeverity:
        """Determine error severity based on code and context.

        Args:
            error_code: The WebSocket error code
            context: Connection error context

        Returns:
            Appropriate ErrorSeverity
        """
        # Critical if circuit breaker is open
        if context.circuit_open:
            return ErrorSeverity.CRITICAL

        # Critical for auth/SSL errors
        if error_code in {
            WebSocketErrorCode.AUTH_FAILED,
            WebSocketErrorCode.SSL_ERROR,
        }:
            return ErrorSeverity.CRITICAL

        # Error for repeated failures
        if context.failure_count >= ERROR_SEVERITY_FAILURE_THRESHOLD:
            return ErrorSeverity.ERROR

        # Error for certain connection issues
        if error_code in {
            WebSocketErrorCode.CONNECTION_REFUSED,
            WebSocketErrorCode.PROTOCOL_ERROR,
            WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
        }:
            return ErrorSeverity.ERROR

        # Warning for transient issues
        if error_code in {
            WebSocketErrorCode.CONNECTION_TIMEOUT,
            WebSocketErrorCode.MESSAGE_TIMEOUT,
            WebSocketErrorCode.HEARTBEAT_TIMEOUT,
            WebSocketErrorCode.CONNECTION_RESET,
        }:
            return ErrorSeverity.WARNING

        # Default to warning
        return ErrorSeverity.WARNING

    def _determine_recovery_strategy(
        self,
        error_code: WebSocketErrorCode,
        context: ConnectionErrorContext,
        severity: ErrorSeverity,
    ) -> WebSocketRecoveryStrategy:
        """Determine recovery strategy based on error and context.

        Args:
            error_code: The WebSocket error code
            context: Connection error context
            severity: Error severity

        Returns:
            Appropriate WebSocketRecoveryStrategy
        """
        # No recovery for critical non-retryable errors
        if error_code in {
            WebSocketErrorCode.AUTH_FAILED,
            WebSocketErrorCode.INVALID_API_KEY,
            WebSocketErrorCode.PERMISSION_DENIED,
        }:
            return WebSocketRecoveryStrategy.NONE

        # Circuit breaker if too many failures
        if context.circuit_open or context.failure_count >= CIRCUIT_BREAKER_FAILURE_THRESHOLD:
            return WebSocketRecoveryStrategy.CIRCUIT_BREAKER

        # Full reconnect for corrupted state
        if error_code in {
            WebSocketErrorCode.STREAM_CORRUPTED,
            WebSocketErrorCode.PROTOCOL_ERROR,
        }:
            return WebSocketRecoveryStrategy.FULL_RECONNECT

        # Different endpoint for connection refused
        if error_code == WebSocketErrorCode.CONNECTION_REFUSED:
            return WebSocketRecoveryStrategy.RECONNECT_DIFFERENT

        # Exponential backoff for rate limits
        if error_code == WebSocketErrorCode.RATE_LIMITED:
            return WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        # Linear backoff for repeated failures
        if context.failure_count >= LINEAR_BACKOFF_FAILURE_THRESHOLD:
            return WebSocketRecoveryStrategy.LINEAR_BACKOFF

        # Immediate retry for first transient error
        if context.failure_count == 0 and severity == ErrorSeverity.WARNING:
            return WebSocketRecoveryStrategy.IMMEDIATE_RETRY

        # Default to reconnect same endpoint
        return WebSocketRecoveryStrategy.RECONNECT_SAME

    def _track_error(self, error: WebSocketStreamError) -> None:
        """Track error in history.

        Args:
            error: The error to track
        """
        self.error_history.append(error)

        # Trim history if too long
        if len(self.error_history) > self.max_history:
            self.error_history = self.error_history[-self.max_history :]

    async def handle_connection_error(
        self,
        manager: WebSocketManager,
        error: Exception,
        recovery: WebSocketErrorRecovery | None = None,
    ) -> RecoveryResult:
        """Handle connection error with typed error system.

        Args:
            manager: The WebSocket manager instance
            error: The connection error
            recovery: Optional recovery system

        Returns:
            Result of recovery attempt
        """
        # Create connection context
        context = ConnectionErrorContext(
            exchange_name=manager.exchange_name,
            ws_url=manager.ws_url,
            is_connected=manager.is_connected,
            failure_count=manager.failure_count,
            circuit_open=manager.circuit_open,
            reconnect_attempts=getattr(manager, "_reconnect_attempts", 0),
            max_reconnect_attempts=manager.max_reconnect_attempts,
            last_error=str(error),
        )

        # Create typed error
        typed_error = self.create_connection_error(
            error,
            context,
            error_type="connection",
        )

        # Log the typed error
        self.logger.warning(
            "connection_error_typed",
            connection_id=self.connection_id,
            error_code=typed_error.code.name,
            severity=typed_error.severity.name,
            recovery_strategy=typed_error.get_recovery_strategy().name,
            message=typed_error.message,
        )

        # Route to recovery strategy
        result = await self.recovery_router.route_recovery(typed_error, recovery)

        # Update manager state based on result
        if result.success:
            if result.should_continue:
                # Continue with reconnection
                manager.should_reconnect = True
            else:
                # Stop reconnection attempts
                manager.should_reconnect = False
        else:
            # Recovery failed
            if result.strategy_used == WebSocketRecoveryStrategy.CIRCUIT_BREAKER:
                manager.circuit_open = True
            manager.should_reconnect = False

        return result

    async def handle_listener_error(
        self,
        manager: WebSocketManager,
        error: Exception,
        recovery: WebSocketErrorRecovery | None = None,
    ) -> RecoveryResult:
        """Handle listener error with typed error system.

        Args:
            manager: The WebSocket manager instance
            error: The listener error
            recovery: Optional recovery system

        Returns:
            Result of recovery attempt
        """
        # Create connection context
        context = ConnectionErrorContext(
            exchange_name=manager.exchange_name,
            ws_url=manager.ws_url,
            is_connected=manager.is_connected,
            failure_count=manager.failure_count,
            circuit_open=manager.circuit_open,
            last_error=str(error),
        )

        # Create typed error
        typed_error = self.create_connection_error(
            error,
            context,
            error_type="listener",
        )

        # Route to recovery strategy
        return await self.recovery_router.route_recovery(typed_error, recovery)

    def get_recovery_action(
        self,
        manager: WebSocketManager,
        error: Exception,
    ) -> RecoveryAction:
        """Get recovery action for an error.

        Args:
            manager: The WebSocket manager instance
            error: The error to handle

        Returns:
            Recovery action to take
        """
        # Create connection context
        context = ConnectionErrorContext(
            exchange_name=manager.exchange_name,
            ws_url=manager.ws_url,
            is_connected=manager.is_connected,
            failure_count=manager.failure_count,
            circuit_open=manager.circuit_open,
        )

        # Create typed error
        typed_error = self.create_connection_error(error, context)

        # Get recovery action from router
        return self.recovery_router.get_recovery_action(typed_error)

    def get_error_stats(self) -> dict[str, object]:
        """Get error statistics.

        Returns:
            Dictionary of error statistics
        """
        error_counts: dict[str, int] = {}
        severity_counts: dict[str, int] = {}
        strategy_counts: dict[str, int] = {}

        for error in self.error_history:
            # Count by error code
            code_name = error.code.name
            error_counts[code_name] = error_counts.get(code_name, 0) + 1

            # Count by severity
            severity_name = error.severity.name
            severity_counts[severity_name] = severity_counts.get(severity_name, 0) + 1

            # Count by recovery strategy
            strategy_name = error.get_recovery_strategy().name
            strategy_counts[strategy_name] = strategy_counts.get(strategy_name, 0) + 1

        return {
            "total_errors": len(self.error_history),
            "error_codes": error_counts,
            "severities": severity_counts,
            "recovery_strategies": strategy_counts,
            "recovery_stats": self.recovery_router.get_stats(),
        }
