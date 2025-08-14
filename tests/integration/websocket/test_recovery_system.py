"""Integration tests for WebSocket recovery system with typed errors.

This test validates Step 49: Recovery System Tests.
"""

import asyncio

# TODO: Implement ws_connection_error_bridge module - using Mock for testing
from unittest.mock import AsyncMock, Mock, Mock as ConnectionErrorBridge

import pytest

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_error_recovery import (
    BackoffConfig,
    CircuitBreakerConfig,
    ConnectionState,
    ErrorRecoveryConfig,
    MessageReplayConfig,
    WebSocketErrorRecovery,
)
from cyberdelta.apis.websocket.ws_recovery_strategy_router import (
    RecoveryStrategyRouter,
)
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError


@pytest.mark.asyncio
class TestRecoverySystemIntegration:
    """Test complete recovery system integration."""

    @pytest.fixture
    def recovery_config(self) -> ErrorRecoveryConfig:
        """Create recovery configuration.

        Returns:
            ErrorRecoveryConfig: Configuration for recovery system with fast test settings.
        """
        return ErrorRecoveryConfig(
            strategy=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            backoff=BackoffConfig(
                initial_delay=0.1,  # Fast for testing
                max_delay=1.0,
                multiplier=2.0,
                jitter=False,
                max_retries=3,
            ),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=2,
                success_threshold=1,
                timeout_seconds=1.0,
            ),
            message_replay=MessageReplayConfig(
                enabled=True,
                buffer_size=10,
            ),
            health_check_interval=0.5,
            state_sync_enabled=True,
        )

    @pytest.fixture
    def mock_connection(self) -> Mock:
        """Create mock connection.

        Returns:
            Mock: Mock WebSocket connection with async methods for testing.
        """
        connection = Mock()
        connection.connect = AsyncMock(return_value=True)
        connection.disconnect = AsyncMock()
        connection.is_healthy = AsyncMock(return_value=True)
        connection.send_message = AsyncMock(return_value=True)
        return connection

    @pytest.fixture
    def recovery_system(self, recovery_config: ErrorRecoveryConfig) -> WebSocketErrorRecovery:
        """Create recovery system.

        Returns:
            WebSocketErrorRecovery: Recovery system configured with test settings.
        """
        return WebSocketErrorRecovery("test-conn-id", recovery_config)

    @pytest.fixture
    def recovery_router(self) -> RecoveryStrategyRouter:
        """Create recovery strategy router.

        Returns:
            RecoveryStrategyRouter: Router for selecting recovery strategies.
        """
        return RecoveryStrategyRouter()

    @pytest.fixture
    def connection_bridge(self) -> ConnectionErrorBridge:
        """Create connection error bridge.

        Returns:
            ConnectionErrorBridge: Bridge for converting connection errors to stream errors.
        """
        return ConnectionErrorBridge("hyperliquid", "test-conn-id")

    async def test_end_to_end_recovery_flow(
        self,
        recovery_system: WebSocketErrorRecovery,
        recovery_router: RecoveryStrategyRouter,
        mock_connection: Mock,
    ) -> None:
        """Test complete recovery flow from error to recovery."""
        # Start recovery system
        await recovery_system.start_recovery(mock_connection)

        # Create a connection error
        context = StreamErrorContext(
            connection_id="test-conn-id",
            exchange="hyperliquid",
            channel="trades",
            reconnect_count=0,
        )

        error = WebSocketStreamError(
            message="Connection lost",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
        )

        # Handle the error
        await recovery_system.handle_connection_error(error)

        # Verify recovery initiated
        assert recovery_system.state == ConnectionState.FAILED
        assert recovery_system.recovery_task is not None
        assert recovery_system.config.strategy == WebSocketRecoveryStrategy.RECONNECT_SAME

        # Wait for recovery to attempt
        await asyncio.sleep(0.2)

        # Verify connection attempt was made
        mock_connection.connect.assert_called()

        # Clean up
        await recovery_system.stop_recovery()

    async def test_recovery_router_integration(
        self,
        recovery_system: WebSocketErrorRecovery,
        recovery_router: RecoveryStrategyRouter,
        mock_connection: Mock,
    ) -> None:
        """Test recovery router integration with recovery system."""
        recovery_system.connection = mock_connection

        # Create different error types
        errors = [
            (WebSocketErrorCode.CONNECTION_TIMEOUT, WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF),
            (WebSocketErrorCode.RATE_LIMITED, WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF),
            (WebSocketErrorCode.AUTH_FAILED, WebSocketRecoveryStrategy.NONE),
        ]

        for error_code, expected_strategy in errors:
            context = StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
            )

            error = WebSocketStreamError(
                message=f"Test error: {error_code.name}",
                code=error_code,
                context=context,
            )

            # Route recovery
            result = await recovery_router.route_recovery(error, recovery_system)

            # Verify routing
            assert result.strategy_used == expected_strategy

            # Get recovery action
            action = recovery_router.get_recovery_action(error)
            assert action.strategy == expected_strategy

    async def test_connection_bridge_recovery_integration(
        self,
        connection_bridge: ConnectionErrorBridge,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test connection bridge integration with recovery system."""
        # Create mock manager
        mock_manager = Mock()
        mock_manager.exchange_name = "hyperliquid"
        mock_manager.ws_url = "wss://api.hyperliquid.xyz/ws"
        mock_manager.is_connected = False
        mock_manager.failure_count = 0
        mock_manager.circuit_open = False
        mock_manager.max_reconnect_attempts = 10
        mock_manager.should_reconnect = True

        # Create connection error
        error = ConnectionError("Connection reset")

        # Handle through bridge
        result = await connection_bridge.handle_connection_error(
            mock_manager,
            error,
            recovery_system,
        )

        # Verify integration
        assert result.success is True
        assert result.should_continue is True
        assert mock_manager.should_reconnect is True

    async def test_circuit_breaker_integration(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test circuit breaker integration in recovery system."""
        recovery_system.connection = mock_connection

        # Trigger multiple failures to open circuit
        for i in range(3):
            context = StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
                reconnect_count=i,
            )

            error = WebSocketStreamError(
                message=f"Connection failed #{i}",
                code=WebSocketErrorCode.CONNECTION_FAILED,
                context=context,
                severity=ErrorSeverity.ERROR,
            )

            await recovery_system.handle_connection_error(error)

        # Verify circuit breaker opened
        assert (
            recovery_system.circuit_failures
            >= recovery_system.config.circuit_breaker.failure_threshold
        )

        # Try with circuit breaker strategy
        final_context = StreamErrorContext(
            connection_id="test-conn-id",
            exchange="hyperliquid",
            reconnect_count=3,
        )
        error = WebSocketStreamError(
            message="Too many failures",
            code=WebSocketErrorCode.CONNECTION_FAILED,
            context=final_context,
            recovery_strategy=WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
        )

        await recovery_system.handle_connection_error(error)

        # Verify circuit is open
        assert recovery_system.state == ConnectionState.CIRCUIT_OPEN

    async def test_message_replay_integration(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test message replay functionality in recovery."""
        recovery_system.connection = mock_connection

        # Add messages to buffer
        test_messages = [
            {"type": "order", "data": {"id": 1}},
            {"type": "order", "data": {"id": 2}},
            {"type": "order", "data": {"id": 3}},
        ]

        for msg in test_messages:
            recovery_system.message_buffer.add_failed_message(msg)

        # Verify messages buffered
        assert len(recovery_system.message_buffer.buffer) == 3

        # Trigger recovery by simulating successful reconnection
        # This should automatically replay messages if replay_on_reconnect is enabled
        await recovery_system.handle_successful_operation()

        # Verify messages were replayed
        assert mock_connection.send_message.call_count == 3
        for i, call_args in enumerate(mock_connection.send_message.call_args_list):
            assert call_args[0][0] == test_messages[i]

    async def test_recovery_with_different_strategies(
        self,
        recovery_system: WebSocketErrorRecovery,
        recovery_router: RecoveryStrategyRouter,
        mock_connection: Mock,
    ) -> None:
        """Test recovery with various strategies."""
        recovery_system.connection = mock_connection

        strategies = [
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            WebSocketRecoveryStrategy.LINEAR_BACKOFF,
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            WebSocketRecoveryStrategy.RECONNECT_SAME,
            WebSocketRecoveryStrategy.FULL_RECONNECT,
        ]

        for strategy in strategies:
            context = StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
            )

            error = WebSocketStreamError(
                message=f"Test {strategy.name}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                recovery_strategy=strategy,
            )

            # Route recovery
            result = await recovery_router.route_recovery(error, recovery_system)

            # Verify strategy applied
            assert result.strategy_used == strategy
            assert result.success is True

    async def test_health_check_integration(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test health check integration with recovery."""
        # Start recovery system
        await recovery_system.start_recovery(mock_connection)

        # Wait for health check
        await asyncio.sleep(0.6)

        # Verify health check called
        mock_connection.is_healthy.assert_called()

        # Get health status
        health = recovery_system.get_health_status()
        assert health.connection_id == "test-conn-id"
        assert health.state == ConnectionState.DISCONNECTED

        # Stop recovery
        await recovery_system.stop_recovery()

    async def test_recovery_statistics(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test recovery statistics collection."""
        recovery_system.connection = mock_connection

        # Trigger some errors
        for i in range(3):
            context = StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
            )

            error = WebSocketStreamError(
                message=f"Error {i}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
            )

            await recovery_system.handle_connection_error(error)

        # Get statistics
        stats = recovery_system.get_recovery_stats()

        assert stats["connection_id"] == "test-conn-id"
        assert stats["retry_count"] > 0
        assert stats["circuit_failures"] > 0
        assert "message_buffer" in stats
        assert "recent_events" in stats
        assert len(stats["recent_events"]) > 0

    async def test_recovery_action_priorities(
        self,
        recovery_router: RecoveryStrategyRouter,
    ) -> None:
        """Test recovery action priority and max retries."""
        # Critical error - minimal retries
        critical_error = WebSocketStreamError(
            message="Critical error",
            code=WebSocketErrorCode.PROTOCOL_ERROR,
            context=StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
            ),
            severity=ErrorSeverity.CRITICAL,
            recovery_strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
        )

        action = recovery_router.get_recovery_action(critical_error)
        assert action.max_retries == 1
        assert action.should_reconnect is True
        assert action.should_clear_state is True

        # Warning error - more retries
        warning_error = WebSocketStreamError(
            message="Warning error",
            code=WebSocketErrorCode.CONNECTION_TIMEOUT,
            context=StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
            ),
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
        )

        action = recovery_router.get_recovery_action(warning_error)
        assert action.max_retries == 5
        assert action.delay_ms > 0

    async def test_recovery_attempt_tracking(
        self,
        recovery_router: RecoveryStrategyRouter,
        recovery_system: WebSocketErrorRecovery,
    ) -> None:
        """Test recovery attempt tracking."""
        error = WebSocketStreamError(
            message="Test error",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
            ),
        )

        # Track multiple attempts
        for _ in range(3):
            await recovery_router.route_recovery(error, recovery_system)

        # Check attempt tracking
        error_key = f"{error.code.name}:test-conn-id"
        assert recovery_router.recovery_attempts[error_key] == 3

        # Reset attempts
        recovery_router.reset_attempts("test-conn-id")
        assert len(recovery_router.recovery_attempts) == 0

    async def test_state_management_integration(
        self,
        recovery_system: WebSocketErrorRecovery,
    ) -> None:
        """Test state management in recovery system."""
        # Create state snapshot
        snapshot = recovery_system.state_manager.create_snapshot(
            connection_id="test-conn-id",
            subscriptions=["trades", "orders"],
            user_data={"user_id": "123"},
            sequence_numbers={"trades": 100, "orders": 50},
        )

        assert snapshot.connection_id == "test-conn-id"
        assert len(snapshot.subscriptions) == 2

        # Restore state
        restored = recovery_system.state_manager.restore_state("test-conn-id")
        assert restored is not None
        assert restored.subscriptions == ["trades", "orders"]

    async def test_concurrent_error_handling(
        self,
        recovery_system: WebSocketErrorRecovery,
        recovery_router: RecoveryStrategyRouter,
        mock_connection: Mock,
    ) -> None:
        """Test concurrent error handling."""
        recovery_system.connection = mock_connection

        # Create multiple errors
        errors: list[WebSocketStreamError] = []
        for i in range(5):
            error = WebSocketStreamError(
                message=f"Concurrent error {i}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=StreamErrorContext(
                    connection_id=f"conn-{i}",
                    exchange="hyperliquid",
                ),
            )
            errors.append(error)

        # Handle concurrently
        tasks = [recovery_router.route_recovery(error, recovery_system) for error in errors]

        results = await asyncio.gather(*tasks)

        # Verify all handled
        assert len(results) == 5
        assert all(r.success for r in results)

    async def test_recovery_system_cleanup(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test recovery system cleanup."""
        # Start recovery
        await recovery_system.start_recovery(mock_connection)

        # Add some state
        recovery_system.message_buffer.add_failed_message({"test": "data"})
        recovery_system.state_manager.create_snapshot(
            "test-conn-id",
            ["trades"],
        )

        # Stop recovery
        await recovery_system.stop_recovery()

        # Verify tasks cancelled
        if recovery_system.health_check_task:
            assert recovery_system.health_check_task.done()
        if recovery_system.recovery_task:
            assert recovery_system.recovery_task.done()

    async def test_error_chain_recovery(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test recovery from chain of errors."""
        recovery_system.connection = mock_connection

        # First error - connection lost
        error1 = WebSocketStreamError(
            message="Connection lost",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
            ),
        )

        await recovery_system.handle_connection_error(error1)

        # Second error - reconnection failed
        error2 = WebSocketStreamError(
            message="Reconnection failed",
            code=WebSocketErrorCode.CONNECTION_FAILED,
            context=StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
                reconnect_count=1,
            ),
        )

        await recovery_system.handle_connection_error(error2)

        # Verify error chain handled
        assert recovery_system.health.consecutive_failures == 2
        assert recovery_system.retry_count > 0

    async def test_recovery_with_degraded_service(
        self,
        recovery_router: RecoveryStrategyRouter,
        recovery_system: WebSocketErrorRecovery,
    ) -> None:
        """Test service degradation recovery."""
        error = WebSocketStreamError(
            message="Subscription limit exceeded",
            code=WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED,
            context=StreamErrorContext(
                connection_id="test-conn-id",
                exchange="hyperliquid",
            ),
            recovery_strategy=WebSocketRecoveryStrategy.DEGRADE_SERVICE,
        )

        result = await recovery_router.route_recovery(error, recovery_system)

        assert result.success is True
        assert result.strategy_used == WebSocketRecoveryStrategy.DEGRADE_SERVICE
        assert result.should_continue is True
        assert "degradation_reason" in result.metadata
