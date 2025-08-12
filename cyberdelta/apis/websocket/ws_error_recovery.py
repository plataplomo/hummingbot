"""WebSocket error recovery and resilience system.

This module provides comprehensive error recovery strategies including
automatic reconnection with backoff, message replay, and state synchronization.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from collections import deque
from contextlib import suppress
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel, Field

from cyberdelta.apis.base.infrastructure_config_domain import ReconnectionResult


if TYPE_CHECKING:
    from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName


class ConnectionState(StrEnum):
    """WebSocket connection states."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"
    CIRCUIT_OPEN = "circuit_open"


class BackoffConfig(BaseModel):
    """Configuration for backoff strategies."""

    initial_delay: float = Field(default=1.0, gt=0, description="Initial delay in seconds")
    max_delay: float = Field(default=300.0, gt=0, description="Maximum delay in seconds")
    multiplier: float = Field(default=2.0, gt=1, description="Backoff multiplier")
    jitter: bool = Field(default=True, description="Add random jitter to delays")
    max_retries: int = Field(default=10, gt=0, description="Maximum retry attempts")


class CircuitBreakerConfig(BaseModel):
    """Configuration for circuit breaker pattern."""

    failure_threshold: int = Field(default=5, gt=0, description="Failures before opening circuit")
    success_threshold: int = Field(default=3, gt=0, description="Successes to close circuit")
    timeout_seconds: float = Field(default=60.0, gt=0, description="Circuit open timeout")
    half_open_max_calls: int = Field(default=1, gt=0, description="Max calls in half-open state")


class MessageReplayConfig(BaseModel):
    """Configuration for message replay functionality."""

    enabled: bool = True
    buffer_size: int = Field(default=1000, gt=0, description="Maximum messages to buffer")
    replay_timeout_seconds: float = Field(default=30.0, gt=0, description="Timeout for replay")
    persist_to_disk: bool = Field(default=False, description="Persist buffer to disk")
    replay_on_reconnect: bool = Field(default=True, description="Auto-replay on reconnection")


class ErrorRecoveryConfig(BaseModel):
    """Main configuration for error recovery system."""

    strategy: WebSocketRecoveryStrategy = WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF
    backoff: BackoffConfig = Field(default_factory=BackoffConfig)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
    message_replay: MessageReplayConfig = Field(default_factory=MessageReplayConfig)
    health_check_interval: float = Field(default=30.0, gt=0, description="Health check interval")
    state_sync_enabled: bool = Field(default=True, description="Enable state synchronization")


class RecoveryEvent(BaseModel):
    """Event data for recovery operations."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    event_type: str
    connection_id: str
    error_details: str | None = None
    recovery_action: str | None = None
    success: bool
    attempt_number: int = 0
    delay_seconds: float = 0.0


class ConnectionHealth(BaseModel):
    """Connection health status."""

    connection_id: str
    state: ConnectionState
    last_seen: datetime = Field(default_factory=lambda: datetime.now(UTC))
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    total_reconnections: int = 0
    uptime_seconds: float = 0.0
    error_rate: float = 0.0


class MessageBuffer:
    """Buffer for message replay functionality."""

    def __init__(self, config: MessageReplayConfig) -> None:
        """Initialize message buffer.

        Args:
            config: Message replay configuration
        """
        self.config = config
        self.buffer: deque[dict[str, Any]] = deque(maxlen=config.buffer_size)
        self.sent_messages: deque[dict[str, Any]] = deque(maxlen=config.buffer_size)
        self.logger = get_logger("MessageBuffer")

    def add_outgoing_message(self, message: dict[str, Any]) -> None:
        """Add outgoing message to buffer for potential replay.

        Args:
            message: Message to buffer
        """
        if not self.config.enabled:
            return

        timestamped_message = {
            **message,
            "_timestamp": datetime.now(UTC).isoformat(),
            "_buffer_id": len(self.sent_messages),
        }
        self.sent_messages.append(timestamped_message)

    def add_failed_message(self, message: dict[str, Any]) -> None:
        """Add failed message for retry.

        Args:
            message: Failed message to retry
        """
        if not self.config.enabled:
            return

        self.buffer.append(message)

    def get_replay_messages(self) -> list[dict[str, Any]]:
        """Get messages for replay.

        Returns:
            List of messages to replay
        """
        if not self.config.replay_on_reconnect:
            return []

        messages = list(self.buffer)
        self.buffer.clear()
        return messages

    def clear(self) -> None:
        """Clear all buffered messages."""
        self.buffer.clear()
        self.sent_messages.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get buffer statistics.

        Returns:
            Buffer statistics
        """
        return {
            "pending_replay": len(self.buffer),
            "sent_messages": len(self.sent_messages),
            "max_capacity": self.config.buffer_size,
            "replay_enabled": self.config.enabled,
        }


class StateSnapshot(BaseModel):
    """Snapshot of connection state for synchronization."""

    connection_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    subscriptions: list[str] = Field(default_factory=list)
    user_data: dict[str, Any] = Field(default_factory=dict)
    sequence_numbers: dict[str, int] = Field(default_factory=dict)
    last_message_ids: dict[str, str] = Field(default_factory=dict)


class StateManager:
    """Manages connection state for synchronization."""

    def __init__(self) -> None:
        """Initialize state manager."""
        self.snapshots: dict[str, StateSnapshot] = {}
        self.logger = get_logger("StateManager")

    def create_snapshot(
        self,
        connection_id: str,
        subscriptions: list[str],
        user_data: dict[str, Any] | None = None,
        sequence_numbers: dict[str, int] | None = None,
    ) -> StateSnapshot:
        """Create state snapshot.

        Args:
            connection_id: Connection identifier
            subscriptions: Active subscriptions
            user_data: User-specific data
            sequence_numbers: Message sequence numbers

        Returns:
            Created state snapshot
        """
        snapshot = StateSnapshot(
            connection_id=connection_id,
            subscriptions=subscriptions,
            user_data=user_data or {},
            sequence_numbers=sequence_numbers or {},
        )
        self.snapshots[connection_id] = snapshot
        return snapshot

    def get_snapshot(self, connection_id: str) -> StateSnapshot | None:
        """Get state snapshot for connection.

        Args:
            connection_id: Connection identifier

        Returns:
            State snapshot or None if not found
        """
        return self.snapshots.get(connection_id)

    def restore_state(self, connection_id: str) -> StateSnapshot | None:
        """Restore state for connection.

        Args:
            connection_id: Connection identifier

        Returns:
            Restored state snapshot or None
        """
        snapshot = self.snapshots.get(connection_id)
        if snapshot:
            self.logger.info(
                "state_restored",
                connection_id=connection_id,
                subscriptions=len(snapshot.subscriptions),
                timestamp=snapshot.timestamp,
            )
        return snapshot

    def remove_snapshot(self, connection_id: str) -> None:
        """Remove state snapshot.

        Args:
            connection_id: Connection identifier
        """
        self.snapshots.pop(connection_id, None)


class ConnectionRecovery(Protocol):
    """Protocol for connection recovery implementations."""

    async def connect(self) -> bool:
        """Establish connection.

        Returns:
            True if connection successful
        """
        ...

    async def disconnect(self) -> None:
        """Close connection."""
        ...

    async def is_healthy(self) -> bool:
        """Check if connection is healthy.

        Returns:
            True if connection is healthy
        """
        ...

    async def send_message(self, message: dict[str, Any]) -> bool:
        """Send message through connection.

        Args:
            message: Message to send

        Returns:
            True if message sent successfully
        """
        ...


class WebSocketErrorRecovery:
    """Comprehensive WebSocket error recovery system."""

    def __init__(self, connection_id: str, config: ErrorRecoveryConfig) -> None:
        """Initialize error recovery system.

        Args:
            connection_id: Connection identifier
            config: Recovery configuration
        """
        self.connection_id = connection_id
        self.config = config
        self.logger = get_logger(f"ErrorRecovery.{connection_id}")

        # State management
        self.state = ConnectionState.DISCONNECTED
        self.health = ConnectionHealth(connection_id=connection_id, state=self.state)

        # Recovery components
        self.message_buffer = MessageBuffer(config.message_replay)
        self.state_manager = StateManager()

        # Backoff tracking
        self.current_delay = config.backoff.initial_delay
        self.retry_count = 0
        self.last_failure_time = 0.0

        # Circuit breaker state
        self.circuit_failures = 0
        self.circuit_successes = 0
        self.circuit_opened_at = 0.0

        # Recovery events
        self.recovery_events: deque[RecoveryEvent] = deque(maxlen=100)

        # Tasks
        self.health_check_task: asyncio.Task[None] | None = None
        self.recovery_task: asyncio.Task[None] | None = None

    async def start_recovery(self, connection: ConnectionRecovery) -> None:
        """Start error recovery system.

        Args:
            connection: Connection implementation
        """
        self.connection = connection

        # Start health monitoring
        if self.health_check_task is None or self.health_check_task.done():
            self.health_check_task = asyncio.create_task(self._health_check_loop())

        self.logger.info(
            "recovery_started",
            connection_id=self.connection_id,
            strategy=self.config.strategy,
        )

    async def stop_recovery(self) -> None:
        """Stop error recovery system."""
        if self.health_check_task:
            self.health_check_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.health_check_task

        if self.recovery_task:
            self.recovery_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.recovery_task

        self.logger.info("recovery_stopped", connection_id=self.connection_id)

    async def handle_connection_error(self, error: Exception) -> None:
        """Handle connection error and initiate recovery.

        Args:
            error: Connection error (can be WebSocketStreamError, APIError or generic Exception)
        """
        self.state = ConnectionState.FAILED
        self.health.state = self.state
        self.health.consecutive_failures += 1
        self.health.consecutive_successes = 0

        # Extract structured error information if available
        error_details = str(error)
        error_code: str | int | None = None
        retry_after = None
        is_retryable = True
        recovery_strategy = WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        # Handle new WebSocketStreamError first
        if isinstance(error, WebSocketStreamError):
            error_details = error.message
            error_code = str(error.code.name)  # Convert to string for consistency
            is_retryable = error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE
            recovery_strategy = error.get_recovery_strategy()
            retry_after = error.get_retry_delay_ms() / 1000.0  # Convert ms to seconds

            # Log structured error information
            self.logger.warning(
                "websocket_stream_error_details",
                connection_id=self.connection_id,
                error_code=error_code,
                category=error.category,
                severity=error.severity.name,
                retryable=is_retryable,
                recovery_strategy=recovery_strategy.name,
                retry_delay_ms=error.get_retry_delay_ms(),
            )
        # Fallback to legacy APIError handling
        elif isinstance(error, APIError):
            error_details = error.message
            error_code = error.code
            retry_after = error.retry_after
            # Check if the error is retryable based on the code
            is_retryable = error.is_retryable

            # Log structured error information
            self.logger.warning(
                "api_error_details",
                connection_id=self.connection_id,
                error_code=error_code,
                exchange_code=error.exchange_code,
                http_status=error.http_status,
                retryable=is_retryable,
                retry_after=retry_after,
            )

        # Record recovery event with structured information
        event = RecoveryEvent(
            event_type="connection_error",
            connection_id=self.connection_id,
            error_details=error_details,
            success=False,
        )
        self.recovery_events.append(event)

        # Handle WebSocketStreamError recovery strategies
        if isinstance(error, WebSocketStreamError):
            if recovery_strategy == WebSocketRecoveryStrategy.NONE:
                self.logger.warning(
                    "non_retryable_websocket_error",
                    connection_id=self.connection_id,
                    error_code=error_code,
                    error_message=error_details,
                )
                self.state = ConnectionState.FAILED
                return
            if recovery_strategy == WebSocketRecoveryStrategy.CIRCUIT_BREAKER:
                self.state = ConnectionState.CIRCUIT_OPEN
                self.circuit_opened_at = time.time()
                self.logger.warning(
                    "circuit_breaker_opened_by_strategy",
                    connection_id=self.connection_id,
                    failures=self.circuit_failures,
                )
                return
            # Apply recovery strategy to config
            self.config.strategy = recovery_strategy
        # Don't retry if APIError indicates it's not retryable
        elif isinstance(error, APIError) and not error.is_retryable:
            self.logger.warning(
                "non_retryable_error",
                connection_id=self.connection_id,
                error_code=error_code,
                error_message=error_details,
            )
            self.state = ConnectionState.FAILED
            return

        # Update circuit breaker
        self.circuit_failures += 1

        # Check circuit breaker
        if self._should_open_circuit():
            self.state = ConnectionState.CIRCUIT_OPEN
            self.circuit_opened_at = time.time()
            self.logger.warning(
                "circuit_breaker_opened",
                connection_id=self.connection_id,
                failures=self.circuit_failures,
            )
            return

        # Use retry_after from error if available
        if retry_after:
            self.current_delay = max(self.current_delay, retry_after)
            self.logger.info(
                "using_error_retry_after",
                connection_id=self.connection_id,
                retry_after=retry_after,
            )

        # Start recovery if not already running
        if self.recovery_task is None or self.recovery_task.done():
            self.recovery_task = asyncio.create_task(self._recovery_loop())

    async def handle_message_failure(self, message: dict[str, Any], error: Exception) -> None:
        """Handle message sending failure.

        Args:
            message: Failed message
            error: Failure reason (can be WebSocketStreamError, APIError or generic Exception)
        """
        # Extract structured error information if available
        error_details = str(error)
        should_retry = True
        recovery_strategy = WebSocketRecoveryStrategy.LINEAR_BACKOFF

        # Handle new WebSocketStreamError first
        if isinstance(error, WebSocketStreamError):
            error_details = error.message
            should_retry = error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE
            recovery_strategy = error.get_recovery_strategy()

            # Log structured error information
            self.logger.warning(
                "websocket_stream_message_failure",
                connection_id=self.connection_id,
                error_code=error.code.name,
                category=error.category,
                severity=error.severity.name,
                retryable=should_retry,
                recovery_strategy=recovery_strategy.name,
                retry_delay_ms=error.get_retry_delay_ms(),
            )

            # Handle special recovery strategies for message failures
            if recovery_strategy == WebSocketRecoveryStrategy.FULL_RECONNECT:
                # Message failure requires full reconnection
                await self.handle_connection_error(error)
                return
        # Fallback to legacy APIError handling
        elif isinstance(error, APIError):
            error_details = error.message
            should_retry = error.is_retryable

            # Log structured error information
            self.logger.warning(
                "api_message_failure",
                connection_id=self.connection_id,
                error_code=error.code,
                exchange_code=error.exchange_code,
                http_status=error.http_status,
                retryable=should_retry,
                retry_after=error.retry_after,
            )

        # Only buffer for retry if the error is retryable
        if should_retry:
            self.message_buffer.add_failed_message(message)
        else:
            self.logger.warning(
                "non_retryable_message_failure",
                connection_id=self.connection_id,
                error_details=error_details,
                message_dropped=True,
            )

        event = RecoveryEvent(
            event_type="message_failure",
            connection_id=self.connection_id,
            error_details=error_details,
            success=False,
        )
        self.recovery_events.append(event)

        self.logger.warning(
            "message_send_failed",
            connection_id=self.connection_id,
            error=error_details,
            retryable=should_retry,
        )

    async def handle_successful_operation(self) -> None:
        """Handle successful operation."""
        self.health.consecutive_successes += 1
        self.health.consecutive_failures = 0
        self.circuit_successes += 1

        # Reset backoff on success
        self.current_delay = self.config.backoff.initial_delay
        self.retry_count = 0

        # Check if circuit should close
        if (
            self.state == ConnectionState.CIRCUIT_OPEN
            and self.circuit_successes >= self.config.circuit_breaker.success_threshold
        ):
            self.state = ConnectionState.CONNECTED
            self.circuit_failures = 0
            self.circuit_successes = 0
            self.logger.info("circuit_breaker_closed", connection_id=self.connection_id)

    async def _health_check_loop(self) -> None:
        """Continuous health checking loop."""
        while True:
            try:
                await asyncio.sleep(self.config.health_check_interval)

                connection = getattr(self, "connection", None)
                if connection is not None:
                    is_healthy = await connection.is_healthy()

                    if not is_healthy and self.state == ConnectionState.CONNECTED:
                        await self.handle_connection_error(Exception("Health check failed"))
                    elif is_healthy and self.state != ConnectionState.CONNECTED:
                        self.state = ConnectionState.CONNECTED
                        self.health.state = self.state

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.exception(
                    "health_check_error",
                    connection_id=self.connection_id,
                    error=str(e),
                )

    async def _recovery_loop(self) -> None:
        """Recovery loop with backoff strategy."""
        while self.retry_count < self.config.backoff.max_retries:
            try:
                # Check circuit breaker
                if self.state == ConnectionState.CIRCUIT_OPEN:
                    if self._should_close_circuit():
                        self.state = ConnectionState.DISCONNECTED
                    else:
                        await asyncio.sleep(self.config.health_check_interval)
                        continue

                # Apply backoff delay
                if self.retry_count > 0:
                    delay = self._calculate_backoff_delay()
                    self.logger.info(
                        "recovery_backoff",
                        connection_id=self.connection_id,
                        delay=delay,
                        attempt=self.retry_count,
                    )
                    await asyncio.sleep(delay)

                self.state = ConnectionState.RECONNECTING
                self.retry_count += 1

                # Attempt reconnection
                if await self._attempt_reconnection():
                    await self._replay_messages()
                    await self._synchronize_state()
                    break

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.exception(
                    "recovery_loop_error",
                    connection_id=self.connection_id,
                    error=str(e),
                )

        # Max retries exceeded
        if self.retry_count >= self.config.backoff.max_retries:
            self.state = ConnectionState.FAILED
            self.logger.error(
                "recovery_failed_max_retries",
                connection_id=self.connection_id,
                max_retries=self.config.backoff.max_retries,
            )

    async def _attempt_reconnection(self) -> bool:
        """Attempt to reconnect.

        Returns:
            True if reconnection successful
        """
        try:
            success = await self.connection.connect()
            result = ReconnectionResult.SUCCESS if success else ReconnectionResult.FAILED
            return await self._handle_reconnection_result(result)

        except (ConnectionError, TimeoutError, OSError) as e:
            self.logger.warning(
                "reconnection_attempt_failed",
                connection_id=self.connection_id,
                attempt=self.retry_count,
                error=str(e),
            )
            return False

    async def _handle_reconnection_result(self, result: ReconnectionResult) -> bool:
        """Handle the result of a reconnection attempt.

        Args:
            result: Result of the reconnection attempt

        Returns:
            True if successful, False otherwise
        """
        if not result.is_successful:
            event = RecoveryEvent(
                event_type="reconnection_failed",
                connection_id=self.connection_id,
                success=False,
                attempt_number=self.retry_count,
            )
            self.recovery_events.append(event)
            return False

        self.state = ConnectionState.CONNECTED
        self.health.state = self.state
        self.health.total_reconnections += 1
        await self.handle_successful_operation()

        event = RecoveryEvent(
            event_type="reconnection_success",
            connection_id=self.connection_id,
            success=True,
            attempt_number=self.retry_count,
        )
        self.recovery_events.append(event)

        self.logger.info(
            "reconnection_successful",
            connection_id=self.connection_id,
            attempt=self.retry_count,
        )
        return True

    async def _replay_messages(self) -> None:
        """Replay buffered messages."""
        if not self.config.message_replay.enabled:
            return

        messages = self.message_buffer.get_replay_messages()
        if not messages:
            return

        self.logger.info(
            "replaying_messages",
            connection_id=self.connection_id,
            count=len(messages),
        )

        for message in messages:
            try:
                success = await self.connection.send_message(message)
                if not success:
                    self.message_buffer.add_failed_message(message)
            except (ConnectionError, TimeoutError, OSError) as e:
                self.logger.warning(
                    "message_replay_failed",
                    connection_id=self.connection_id,
                    error=str(e),
                )
                self.message_buffer.add_failed_message(message)

    async def _synchronize_state(self) -> None:
        """Synchronize connection state."""
        if not self.config.state_sync_enabled:
            return

        snapshot = self.state_manager.restore_state(self.connection_id)
        if not snapshot:
            return

        self.logger.info(
            "synchronizing_state",
            connection_id=self.connection_id,
            subscriptions=len(snapshot.subscriptions),
        )

        # Restore subscriptions (implementation specific)
        # This would typically involve re-subscribing to channels

    def _calculate_backoff_delay(self) -> float:
        """Calculate backoff delay.

        Returns:
            Delay in seconds
        """
        # Handle immediate retry
        if self.config.strategy == WebSocketRecoveryStrategy.IMMEDIATE_RETRY:
            return 0.0

        # Linear backoff
        if self.config.strategy == WebSocketRecoveryStrategy.LINEAR_BACKOFF:
            delay = self.config.backoff.initial_delay * self.retry_count
        # Exponential backoff (default for most strategies)
        elif self.config.strategy in {
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            WebSocketRecoveryStrategy.RECONNECT_SAME,
            WebSocketRecoveryStrategy.RECONNECT_DIFFERENT,
            WebSocketRecoveryStrategy.FULL_RECONNECT,
        }:
            delay = self.config.backoff.initial_delay * (
                self.config.backoff.multiplier ** (self.retry_count - 1)
            )
        else:
            # Default delay for other strategies
            delay = self.config.backoff.initial_delay

        delay = min(delay, self.config.backoff.max_delay)

        # Add jitter
        if self.config.backoff.jitter:
            delay *= 0.5 + secrets.SystemRandom().random() * 0.5

        return delay

    def _should_open_circuit(self) -> bool:
        """Check if circuit breaker should open.

        Returns:
            True if circuit should open
        """
        return self.circuit_failures >= self.config.circuit_breaker.failure_threshold

    def _should_close_circuit(self) -> bool:
        """Check if circuit breaker should close.

        Returns:
            True if circuit should close
        """
        return time.time() - self.circuit_opened_at >= self.config.circuit_breaker.timeout_seconds

    def get_health_status(self) -> ConnectionHealth:
        """Get current health status.

        Returns:
            Connection health status
        """
        self.health.last_seen = datetime.now(UTC)
        return self.health

    def get_recovery_stats(self) -> dict[str, Any]:
        """Get recovery statistics.

        Returns:
            Recovery statistics
        """
        return {
            "connection_id": self.connection_id,
            "current_state": self.state,
            "retry_count": self.retry_count,
            "circuit_failures": self.circuit_failures,
            "circuit_successes": self.circuit_successes,
            "total_reconnections": self.health.total_reconnections,
            "message_buffer": self.message_buffer.get_stats(),
            "recent_events": [event.model_dump() for event in list(self.recovery_events)[-10:]],
        }

    @staticmethod
    def create_websocket_stream_error(
        message: str,
        error_code: WebSocketErrorCode,
        connection_id: str,
        exchange: ExchangeName,
        original_exception: Exception | None = None,
        channel: str | None = None,
        sequence_number: int | None = None,
        reconnect_count: int = 0,
    ) -> WebSocketStreamError:
        """Create a WebSocketStreamError with proper context.

        Args:
            message: Human-readable error description
            error_code: WebSocket-specific error code
            connection_id: Connection identifier
            exchange: Exchange enum value
            original_exception: The underlying exception
            channel: WebSocket channel name (optional)
            sequence_number: Message sequence number (optional)
            reconnect_count: Number of reconnection attempts

        Returns:
            WebSocketStreamError instance with typed context
        """
        # Create typed error context
        context = StreamErrorContext(
            connection_id=connection_id,
            exchange=exchange.value,
            channel=channel,
            sequence_number=sequence_number,
            reconnect_count=reconnect_count,
        )

        return WebSocketStreamError(
            message=message,
            code=error_code,
            context=context,
            cause=original_exception,
        )
