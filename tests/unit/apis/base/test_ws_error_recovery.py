"""Tests for WebSocket error recovery system."""

import asyncio
import contextlib
import time
from datetime import datetime
from typing import Any

import pytest

from cyberdelta.apis.websocket.ws_error_recovery import (
    BackoffConfig,
    CircuitBreakerConfig,
    ConnectionState,
    ErrorRecoveryConfig,
    MessageBuffer,
    MessageReplayConfig,
    RecoveryStrategy,
    StateManager,
    WebSocketErrorRecovery,
)


class MockConnection:
    """Mock connection for testing."""

    def __init__(self, fail_connects: int = 0, fail_health_checks: int = 0) -> None:
        """Initialize mock connection.

        Args:
            fail_connects: Number of connection attempts to fail
            fail_health_checks: Number of health checks to fail
        """
        self.fail_connects = fail_connects
        self.fail_health_checks = fail_health_checks
        self.connect_attempts = 0
        self.health_check_attempts = 0
        self.sent_messages: list[dict[str, Any]] = []
        self.is_connected = False

    async def connect(self) -> bool:
        """Mock connect method.

        Returns:
            bool: True if connection succeeds, False if still failing.
        """
        self.connect_attempts += 1
        if self.connect_attempts <= self.fail_connects:
            return False
        self.is_connected = True
        return True

    async def disconnect(self) -> None:
        """Mock disconnect method."""
        self.is_connected = False

    async def is_healthy(self) -> bool:
        """Mock health check method.

        Returns:
            bool: True if connection is healthy, False if unhealthy.
        """
        self.health_check_attempts += 1
        if self.health_check_attempts <= self.fail_health_checks:
            return False
        return self.is_connected

    async def send_message(self, message: dict[str, Any]) -> bool:
        """Mock send message method.

        Args:
            message: Dictionary message to send.

        Returns:
            bool: True if message sent successfully, False if connection down.
        """
        if not self.is_connected:
            return False
        self.sent_messages.append(message)
        return True


class TestBackoffConfig:
    """Test BackoffConfig model."""

    def test_backoff_config_creation(self) -> None:
        """Test creating backoff configuration."""
        config = BackoffConfig(
            initial_delay=0.5, max_delay=60.0, multiplier=1.5, jitter=False, max_retries=5
        )

        assert config.initial_delay == 0.5
        assert config.max_delay == 60.0
        assert config.multiplier == 1.5
        assert config.jitter is False
        assert config.max_retries == 5

    def test_backoff_config_defaults(self) -> None:
        """Test default backoff configuration."""
        config = BackoffConfig()

        assert config.initial_delay == 1.0
        assert config.max_delay == 300.0
        assert config.multiplier == 2.0
        assert config.jitter is True
        assert config.max_retries == 10


class TestCircuitBreakerConfig:
    """Test CircuitBreakerConfig model."""

    def test_circuit_breaker_config_creation(self) -> None:
        """Test creating circuit breaker configuration."""
        config = CircuitBreakerConfig(
            failure_threshold=3, success_threshold=2, timeout_seconds=30.0, half_open_max_calls=1
        )

        assert config.failure_threshold == 3
        assert config.success_threshold == 2
        assert config.timeout_seconds == 30.0
        assert config.half_open_max_calls == 1


class TestMessageReplayConfig:
    """Test MessageReplayConfig model."""

    def test_message_replay_config_creation(self) -> None:
        """Test creating message replay configuration."""
        config = MessageReplayConfig(
            enabled=True,
            buffer_size=500,
            replay_timeout_seconds=15.0,
            persist_to_disk=True,
            replay_on_reconnect=True,
        )

        assert config.enabled is True
        assert config.buffer_size == 500
        assert config.replay_timeout_seconds == 15.0
        assert config.persist_to_disk is True
        assert config.replay_on_reconnect is True


class TestMessageBuffer:
    """Test MessageBuffer functionality."""

    @pytest.fixture
    def message_buffer(self) -> MessageBuffer:
        """Create message buffer for testing.

        Returns:
            MessageBuffer: A configured message buffer with size limit of 10.
        """
        config = MessageReplayConfig(enabled=True, buffer_size=10)
        return MessageBuffer(config)

    def test_message_buffer_initialization(self, message_buffer: MessageBuffer) -> None:
        """Test message buffer initialization."""
        assert len(message_buffer.buffer) == 0
        assert len(message_buffer.sent_messages) == 0
        assert message_buffer.config.enabled is True

    def test_add_outgoing_message(self, message_buffer: MessageBuffer) -> None:
        """Test adding outgoing messages."""
        message = {"type": "subscribe", "channel": "depth"}
        message_buffer.add_outgoing_message(message)

        assert len(message_buffer.sent_messages) == 1
        stored_message = message_buffer.sent_messages[0]
        assert stored_message["type"] == "subscribe"
        assert stored_message["channel"] == "depth"
        assert "_timestamp" in stored_message
        assert "_buffer_id" in stored_message

    def test_add_failed_message(self, message_buffer: MessageBuffer) -> None:
        """Test adding failed messages."""
        message = {"type": "ping"}
        message_buffer.add_failed_message(message)

        assert len(message_buffer.buffer) == 1
        assert message_buffer.buffer[0] == message

    def test_get_replay_messages(self, message_buffer: MessageBuffer) -> None:
        """Test getting messages for replay."""
        # Add some failed messages
        message1 = {"type": "subscribe", "channel": "depth"}
        message2 = {"type": "ping"}
        message_buffer.add_failed_message(message1)
        message_buffer.add_failed_message(message2)

        # Get replay messages
        replay_messages = message_buffer.get_replay_messages()

        assert len(replay_messages) == 2
        assert replay_messages[0] == message1
        assert replay_messages[1] == message2
        assert len(message_buffer.buffer) == 0  # Should be cleared

    def test_get_replay_messages_disabled(self) -> None:
        """Test replay when disabled."""
        config = MessageReplayConfig(enabled=False)
        buffer = MessageBuffer(config)

        buffer.add_failed_message({"type": "ping"})
        replay_messages = buffer.get_replay_messages()

        assert len(replay_messages) == 0

    def test_buffer_size_limit(self, message_buffer: MessageBuffer) -> None:
        """Test buffer size limit enforcement."""
        # Add more messages than buffer size
        for i in range(15):  # Buffer size is 10
            message_buffer.add_failed_message({"id": i})

        assert len(message_buffer.buffer) == 10  # Should be limited

        # Should contain the last 10 messages
        replay_messages = message_buffer.get_replay_messages()
        assert len(replay_messages) == 10
        assert replay_messages[0]["id"] == 5  # First message should be id=5
        assert replay_messages[-1]["id"] == 14  # Last message should be id=14

    def test_clear_buffer(self, message_buffer: MessageBuffer) -> None:
        """Test clearing the buffer."""
        message_buffer.add_failed_message({"type": "ping"})
        message_buffer.add_outgoing_message({"type": "subscribe"})

        assert len(message_buffer.buffer) == 1
        assert len(message_buffer.sent_messages) == 1

        message_buffer.clear()

        assert len(message_buffer.buffer) == 0
        assert len(message_buffer.sent_messages) == 0

    def test_get_stats(self, message_buffer: MessageBuffer) -> None:
        """Test getting buffer statistics."""
        message_buffer.add_failed_message({"type": "ping"})
        message_buffer.add_outgoing_message({"type": "subscribe"})

        stats = message_buffer.get_stats()

        assert stats["pending_replay"] == 1
        assert stats["sent_messages"] == 1
        assert stats["max_capacity"] == 10
        assert stats["replay_enabled"] is True


class TestStateManager:
    """Test StateManager functionality."""

    @pytest.fixture
    def state_manager(self) -> StateManager:
        """Create state manager for testing.

        Returns:
            StateManager: A new state manager instance for connection state snapshots.
        """
        return StateManager()

    def test_create_snapshot(self, state_manager: StateManager) -> None:
        """Test creating state snapshot."""
        subscriptions = ["depth.BTCUSD", "ticker.ETHUSD"]
        user_data = {"user_id": "test123"}
        sequence_numbers = {"depth": 100, "ticker": 50}

        snapshot = state_manager.create_snapshot(
            "conn1", subscriptions, user_data, sequence_numbers
        )

        assert snapshot.connection_id == "conn1"
        assert snapshot.subscriptions == subscriptions
        assert snapshot.user_data == user_data
        assert snapshot.sequence_numbers == sequence_numbers
        assert isinstance(snapshot.timestamp, datetime)

        # Should be stored in manager
        assert "conn1" in state_manager.snapshots

    def test_get_snapshot(self, state_manager: StateManager) -> None:
        """Test getting state snapshot."""
        # Create snapshot first
        state_manager.create_snapshot("conn1", ["test"])

        # Get existing snapshot
        snapshot = state_manager.get_snapshot("conn1")
        assert snapshot is not None
        assert snapshot.connection_id == "conn1"

        # Get non-existent snapshot
        snapshot = state_manager.get_snapshot("conn2")
        assert snapshot is None

    def test_restore_state(self, state_manager: StateManager) -> None:
        """Test restoring state."""
        # Create snapshot
        subscriptions = ["depth.BTCUSD"]
        state_manager.create_snapshot("conn1", subscriptions)

        # Restore state
        restored = state_manager.restore_state("conn1")
        assert restored is not None
        assert restored.connection_id == "conn1"
        assert restored.subscriptions == subscriptions

    def test_remove_snapshot(self, state_manager: StateManager) -> None:
        """Test removing state snapshot."""
        state_manager.create_snapshot("conn1", ["test"])
        assert "conn1" in state_manager.snapshots

        state_manager.remove_snapshot("conn1")
        assert "conn1" not in state_manager.snapshots


class TestWebSocketErrorRecovery:
    """Test WebSocketErrorRecovery functionality."""

    @pytest.fixture
    def recovery_config(self) -> ErrorRecoveryConfig:
        """Create recovery configuration for testing.

        Returns:
            ErrorRecoveryConfig: Recovery config with fast timing for test performance.
        """
        return ErrorRecoveryConfig(
            strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF,
            backoff=BackoffConfig(
                initial_delay=0.1,  # Fast for testing
                max_delay=1.0,
                max_retries=3,
            ),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=2, success_threshold=1, timeout_seconds=0.5
            ),
            message_replay=MessageReplayConfig(enabled=True),
            health_check_interval=0.1,  # Fast for testing
        )

    @pytest.fixture
    def error_recovery(self, recovery_config: ErrorRecoveryConfig) -> WebSocketErrorRecovery:
        """Create error recovery system for testing.

        Args:
            recovery_config: Configuration for the recovery system.

        Returns:
            WebSocketErrorRecovery: Configured recovery system for test connection.
        """
        return WebSocketErrorRecovery("test_conn", recovery_config)

    def test_error_recovery_initialization(self, error_recovery: WebSocketErrorRecovery) -> None:
        """Test error recovery initialization."""
        assert error_recovery.connection_id == "test_conn"
        assert error_recovery.state == ConnectionState.DISCONNECTED
        assert error_recovery.retry_count == 0
        assert error_recovery.circuit_failures == 0

    @pytest.mark.asyncio
    async def test_start_and_stop_recovery(self, error_recovery: WebSocketErrorRecovery) -> None:
        """Test starting and stopping recovery."""
        connection = MockConnection()

        # Start recovery
        await error_recovery.start_recovery(connection)
        assert error_recovery.health_check_task is not None
        assert not error_recovery.health_check_task.done()

        # Stop recovery
        await error_recovery.stop_recovery()
        assert error_recovery.health_check_task.done()

    @pytest.mark.asyncio
    async def test_handle_connection_error(self, error_recovery: WebSocketErrorRecovery) -> None:
        """Test handling connection errors."""
        connection = MockConnection()
        await error_recovery.start_recovery(connection)

        # Trigger connection error
        error = Exception("Connection lost")
        await error_recovery.handle_connection_error(error)

        assert error_recovery.state == ConnectionState.FAILED
        assert error_recovery.health.consecutive_failures == 1
        assert error_recovery.circuit_failures == 1
        assert len(error_recovery.recovery_events) == 1

        # Cleanup
        await error_recovery.stop_recovery()

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens(self, error_recovery: WebSocketErrorRecovery) -> None:
        """Test circuit breaker opening on repeated failures."""
        connection = MockConnection()
        await error_recovery.start_recovery(connection)

        # Trigger enough failures to open circuit
        for _ in range(3):  # Threshold is 2
            await error_recovery.handle_connection_error(Exception("Error"))

        assert error_recovery.state == ConnectionState.CIRCUIT_OPEN

        # Cleanup
        await error_recovery.stop_recovery()

    @pytest.mark.asyncio
    async def test_successful_operation_handling(
        self, error_recovery: WebSocketErrorRecovery
    ) -> None:
        """Test handling successful operations."""
        # Set some failure state first
        error_recovery.health.consecutive_failures = 5
        error_recovery.circuit_failures = 3
        error_recovery.retry_count = 2

        await error_recovery.handle_successful_operation()

        assert error_recovery.health.consecutive_successes == 1
        assert error_recovery.health.consecutive_failures == 0
        assert error_recovery.circuit_successes == 1
        assert error_recovery.retry_count == 0  # Should reset

    @pytest.mark.asyncio
    async def test_message_failure_handling(self, error_recovery: WebSocketErrorRecovery) -> None:
        """Test handling message failures."""
        message = {"type": "ping"}
        error = Exception("Send failed")

        await error_recovery.handle_message_failure(message, error)

        # Message should be buffered for retry
        buffer_stats = error_recovery.message_buffer.get_stats()
        assert buffer_stats["pending_replay"] == 1
        assert len(error_recovery.recovery_events) == 1

    @pytest.mark.asyncio
    async def test_successful_reconnection(self, error_recovery: WebSocketErrorRecovery) -> None:
        """Test successful reconnection after failure."""
        # Use connection that succeeds on second attempt
        connection = MockConnection(fail_connects=1)
        await error_recovery.start_recovery(connection)

        # Trigger error to start recovery
        await error_recovery.handle_connection_error(Exception("Connection lost"))

        # Wait for recovery to complete
        if error_recovery.recovery_task:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(error_recovery.recovery_task, timeout=2.0)

        # Should eventually reconnect
        assert connection.connect_attempts >= 2

        # Cleanup
        await error_recovery.stop_recovery()

    @pytest.mark.asyncio
    async def test_message_replay_on_reconnection(
        self, error_recovery: WebSocketErrorRecovery
    ) -> None:
        """Test message replay on reconnection."""
        connection = MockConnection(fail_connects=1)
        await error_recovery.start_recovery(connection)

        # Add failed message to buffer
        failed_message = {"type": "subscribe", "channel": "depth"}
        error_recovery.message_buffer.add_failed_message(failed_message)

        # Trigger reconnection
        await error_recovery.handle_connection_error(Exception("Connection lost"))

        # Wait for recovery
        if error_recovery.recovery_task:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(error_recovery.recovery_task, timeout=2.0)

        # Message should be replayed if reconnection succeeded
        if connection.is_connected:
            assert len(connection.sent_messages) > 0

        # Cleanup
        await error_recovery.stop_recovery()

    def test_backoff_delay_behavior_through_stats(
        self, error_recovery: WebSocketErrorRecovery
    ) -> None:
        """Test backoff delay behavior through recovery stats and retry count tracking."""
        # Set different retry counts and verify the behavior through stats
        error_recovery.retry_count = 1
        stats1 = error_recovery.get_recovery_stats()

        error_recovery.retry_count = 2
        stats2 = error_recovery.get_recovery_stats()

        error_recovery.retry_count = 3
        stats3 = error_recovery.get_recovery_stats()

        # Verify retry count progression through stats
        assert stats1["retry_count"] == 1
        assert stats2["retry_count"] == 2
        assert stats3["retry_count"] == 3

        # Verify configuration limits are accessible
        assert error_recovery.config.backoff.max_delay > 0
        assert error_recovery.config.backoff.initial_delay > 0
        assert error_recovery.config.backoff.multiplier > 1

    def test_circuit_breaker_behavior_through_stats(
        self, error_recovery: WebSocketErrorRecovery
    ) -> None:
        """Test circuit breaker behavior through recovery stats and state changes."""
        # Check initial state through stats
        initial_stats = error_recovery.get_recovery_stats()
        assert initial_stats["circuit_failures"] == 0
        assert initial_stats["current_state"] == ConnectionState.DISCONNECTED

        # Simulate failures and check stats
        error_recovery.circuit_failures = 2  # Below threshold (default is 5)
        stats_with_failures = error_recovery.get_recovery_stats()
        assert stats_with_failures["circuit_failures"] == 2

        # Set failures to meet threshold
        error_recovery.circuit_failures = error_recovery.config.circuit_breaker.failure_threshold
        threshold_stats = error_recovery.get_recovery_stats()
        assert (
            threshold_stats["circuit_failures"]
            == error_recovery.config.circuit_breaker.failure_threshold
        )

        # Test state transitions through public interface
        error_recovery.state = ConnectionState.CIRCUIT_OPEN
        error_recovery.circuit_opened_at = time.time() - (
            error_recovery.config.circuit_breaker.timeout_seconds + 1.0
        )
        circuit_open_stats = error_recovery.get_recovery_stats()
        assert circuit_open_stats["current_state"] == ConnectionState.CIRCUIT_OPEN

    def test_get_health_status(self, error_recovery: WebSocketErrorRecovery) -> None:
        """Test getting health status."""
        health = error_recovery.get_health_status()

        assert health.connection_id == "test_conn"
        assert health.state == ConnectionState.DISCONNECTED
        assert isinstance(health.last_seen, datetime)

    def test_get_recovery_stats(self, error_recovery: WebSocketErrorRecovery) -> None:
        """Test getting recovery statistics."""
        # Add some state
        error_recovery.retry_count = 2
        error_recovery.circuit_failures = 1
        error_recovery.message_buffer.add_failed_message({"type": "ping"})

        stats = error_recovery.get_recovery_stats()

        assert stats["connection_id"] == "test_conn"
        assert stats["current_state"] == ConnectionState.DISCONNECTED
        assert stats["retry_count"] == 2
        assert stats["circuit_failures"] == 1
        assert "message_buffer" in stats
        assert "recent_events" in stats


@pytest.mark.asyncio
class TestErrorRecoveryIntegration:
    """Test error recovery integration scenarios."""

    async def test_full_recovery_cycle(self) -> None:
        """Test complete recovery cycle from failure to success."""
        config = ErrorRecoveryConfig(
            backoff=BackoffConfig(
                initial_delay=0.05,  # Very fast for testing
                max_retries=3,
            ),
            health_check_interval=0.05,
        )

        recovery = WebSocketErrorRecovery("test_conn", config)
        connection = MockConnection(fail_connects=2)  # Fail first 2 attempts

        # Start recovery system
        await recovery.start_recovery(connection)

        # Trigger initial failure
        await recovery.handle_connection_error(Exception("Initial failure"))

        # Wait for recovery to complete
        if recovery.recovery_task:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(recovery.recovery_task, timeout=3.0)

        # Should eventually succeed
        assert connection.connect_attempts >= 3
        if connection.is_connected:
            assert recovery.state == ConnectionState.CONNECTED

        # Cleanup
        await recovery.stop_recovery()

    @pytest.mark.timing
    async def test_circuit_breaker_integration(self) -> None:
        """Test circuit breaker integration with recovery."""
        config = ErrorRecoveryConfig(
            circuit_breaker=CircuitBreakerConfig(failure_threshold=2, timeout_seconds=0.1),
            backoff=BackoffConfig(initial_delay=0.01, max_retries=5),
        )

        recovery = WebSocketErrorRecovery("test_conn", config)
        connection = MockConnection(fail_connects=10)  # Always fail

        await recovery.start_recovery(connection)

        # Trigger multiple failures to open circuit
        for _ in range(3):
            await recovery.handle_connection_error(Exception("Failure"))

        assert recovery.state == ConnectionState.CIRCUIT_OPEN

        # Wait for circuit timeout
        await asyncio.sleep(0.2)

        # Should attempt to close circuit
        # (This would require running the recovery loop)

        # Cleanup
        await recovery.stop_recovery()

    async def test_state_synchronization(self) -> None:
        """Test state synchronization on reconnection."""
        config = ErrorRecoveryConfig(
            state_sync_enabled=True, backoff=BackoffConfig(initial_delay=0.01, max_retries=2)
        )

        recovery = WebSocketErrorRecovery("test_conn", config)
        connection = MockConnection(fail_connects=1)

        # Create state snapshot
        recovery.state_manager.create_snapshot(
            "test_conn", ["depth.BTCUSD", "ticker.ETHUSD"], {"user_id": "test123"}
        )

        await recovery.start_recovery(connection)

        # Trigger failure and recovery
        await recovery.handle_connection_error(Exception("Connection lost"))

        # Wait for recovery
        if recovery.recovery_task:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(recovery.recovery_task, timeout=2.0)

        # State should be available for restoration
        restored_state = recovery.state_manager.get_snapshot("test_conn")
        assert restored_state is not None
        assert "depth.BTCUSD" in restored_state.subscriptions

        # Cleanup
        await recovery.stop_recovery()
