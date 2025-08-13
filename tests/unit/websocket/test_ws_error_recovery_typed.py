"""Test WebSocket error recovery with typed error system.

This test validates Step 46: Update WebSocket Error Recovery.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode
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
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.enums import ExchangeName


@pytest.mark.asyncio
class TestWebSocketErrorRecoveryTyped:
    """Test WebSocket error recovery with typed error system."""

    @pytest.fixture
    def recovery_config(self) -> ErrorRecoveryConfig:
        """Create recovery configuration.

        Returns:
            ErrorRecoveryConfig: Configuration for error recovery with exponential backoff.
        """
        return ErrorRecoveryConfig(
            strategy=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            backoff=BackoffConfig(
                initial_delay=1.0,
                max_delay=60.0,
                multiplier=2.0,
                jitter=False,
                max_retries=5,
            ),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=3,
                success_threshold=2,
                timeout_seconds=30.0,
            ),
            message_replay=MessageReplayConfig(
                enabled=True,
                buffer_size=100,
            ),
            health_check_interval=10.0,
            state_sync_enabled=True,
        )

    @pytest.fixture
    def mock_connection(self) -> Mock:
        """Create mock connection.

        Returns:
            Mock: Mock WebSocket connection with async methods for recovery testing.
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
            WebSocketErrorRecovery: Error recovery system configured with test settings.
        """
        return WebSocketErrorRecovery("test-conn-id", recovery_config)

    async def test_handle_websocket_stream_error_retryable(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test handling retryable WebSocketStreamError."""
        # Create typed error with retryable strategy
        context = StreamErrorContext(
            connection_id="test-conn-id",
            exchange=ExchangeName.HYPERLIQUID,
            channel="trades",
            sequence_number=123,
            reconnect_count=1,
        )

        error = WebSocketStreamError(
            message="Connection lost",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
        )

        recovery_system.connection = mock_connection

        # Handle the error
        await recovery_system.handle_connection_error(error)

        # Verify state updated
        assert recovery_system.state == ConnectionState.FAILED
        assert recovery_system.health.consecutive_failures == 1
        assert recovery_system.health.consecutive_successes == 0

        # Verify recovery strategy applied
        assert recovery_system.config.strategy == WebSocketRecoveryStrategy.RECONNECT_SAME

        # Verify recovery task started
        assert recovery_system.recovery_task is not None

        # Clean up
        await recovery_system.stop_recovery()

    async def test_handle_websocket_stream_error_non_retryable(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test handling non-retryable WebSocketStreamError."""
        # Create typed error with no recovery strategy
        context = StreamErrorContext(
            connection_id="test-conn-id",
            exchange=ExchangeName.HYPERLIQUID,
            channel="auth",
        )

        error = WebSocketStreamError(
            message="Authentication failed",
            code=WebSocketErrorCode.AUTH_FAILED,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.NONE,
        )

        recovery_system.connection = mock_connection

        # Handle the error
        await recovery_system.handle_connection_error(error)

        # Verify state is failed with no recovery
        assert recovery_system.state == ConnectionState.FAILED
        assert recovery_system.health.consecutive_failures == 1
        assert recovery_system.recovery_task is None

    async def test_handle_websocket_stream_error_circuit_breaker(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test handling WebSocketStreamError with circuit breaker strategy."""
        # Create typed error with circuit breaker strategy
        context = StreamErrorContext(
            connection_id="test-conn-id",
            exchange=ExchangeName.HYPERLIQUID,
            reconnect_count=10,  # High reconnect count
        )

        error = WebSocketStreamError(
            message="Too many failures",
            code=WebSocketErrorCode.CONNECTION_FAILED,
            context=context,
            recovery_strategy=WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
        )

        recovery_system.connection = mock_connection

        # Handle the error
        await recovery_system.handle_connection_error(error)

        # Verify circuit breaker opened
        assert recovery_system.state == ConnectionState.CIRCUIT_OPEN
        assert recovery_system.circuit_opened_at > 0

    async def test_handle_message_failure_with_websocket_stream_error(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test handling message failure with WebSocketStreamError."""
        # Create typed error for message failure
        context = StreamErrorContext(
            connection_id="test-conn-id",
            exchange=ExchangeName.HYPERLIQUID,
            channel="orders",
            sequence_number=456,
        )

        error = WebSocketStreamError(
            message="Failed to send order",
            code=WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
            context=context,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        recovery_system.connection = mock_connection

        test_message = {"type": "order", "data": {"symbol": "BTC-USD"}}

        # Handle the message failure
        await recovery_system.handle_message_failure(test_message, error)

        # Verify message buffered for retry
        assert len(recovery_system.message_buffer.buffer) == 1
        assert recovery_system.message_buffer.buffer[0] == test_message

        # Verify event recorded
        assert len(recovery_system.recovery_events) == 1
        assert recovery_system.recovery_events[0].event_type == "message_failure"

    async def test_handle_full_reconnect_strategy_on_message_failure(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test message failure requiring full reconnection."""
        # Create typed error requiring full reconnect
        context = StreamErrorContext(
            connection_id="test-conn-id",
            exchange=ExchangeName.HYPERLIQUID,
            channel="critical",
        )

        error = WebSocketStreamError(
            message="Stream corrupted",
            code=WebSocketErrorCode.STREAM_CORRUPTED,
            context=context,
            recovery_strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
        )

        recovery_system.connection = mock_connection

        test_message = {"type": "data", "data": {"corrupted": True}}

        # Handle the message failure
        await recovery_system.handle_message_failure(test_message, error)

        # Verify connection error handling triggered
        assert recovery_system.state == ConnectionState.FAILED
        assert recovery_system.config.strategy == WebSocketRecoveryStrategy.FULL_RECONNECT
        assert recovery_system.recovery_task is not None

        # Clean up
        await recovery_system.stop_recovery()

    async def test_backoff_calculation_with_immediate_retry(
        self,
        recovery_system: WebSocketErrorRecovery,
    ) -> None:
        """Test backoff calculation for immediate retry strategy."""
        recovery_system.config.strategy = WebSocketRecoveryStrategy.IMMEDIATE_RETRY

        delay = recovery_system._calculate_backoff_delay()

        # Immediate retry should have zero delay
        assert delay == 0.0

    async def test_backoff_calculation_with_linear_strategy(
        self,
        recovery_system: WebSocketErrorRecovery,
    ) -> None:
        """Test backoff calculation for linear backoff strategy."""
        recovery_system.config.strategy = WebSocketRecoveryStrategy.LINEAR_BACKOFF
        recovery_system.retry_count = 3

        delay = recovery_system._calculate_backoff_delay()

        # Linear backoff: initial_delay * retry_count
        expected = recovery_system.config.backoff.initial_delay * 3
        assert delay == expected

    async def test_backoff_calculation_with_exponential_strategy(
        self,
        recovery_system: WebSocketErrorRecovery,
    ) -> None:
        """Test backoff calculation for exponential backoff strategy."""
        recovery_system.config.strategy = WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF
        recovery_system.retry_count = 3

        delay = recovery_system._calculate_backoff_delay()

        # Exponential backoff: initial_delay * (multiplier ^ (retry_count - 1))
        expected = recovery_system.config.backoff.initial_delay * (
            recovery_system.config.backoff.multiplier**2
        )
        assert delay == expected

    async def test_create_websocket_stream_error_helper(
        self,
        recovery_system: WebSocketErrorRecovery,
    ) -> None:
        """Test helper method for creating WebSocketStreamError."""
        # Create error using helper
        error = WebSocketErrorRecovery.create_websocket_stream_error(
            message="Test error",
            error_code=WebSocketErrorCode.CONNECTION_TIMEOUT,
            connection_id="test-conn-id",
            exchange=ExchangeName.HYPERLIQUID,
            channel="trades",
            sequence_number=789,
            reconnect_count=2,
        )

        # Verify error created correctly
        assert isinstance(error, WebSocketStreamError)
        assert error.message == "Test error"
        assert error.code == WebSocketErrorCode.CONNECTION_TIMEOUT
        assert error.context.connection_id == "test-conn-id"
        assert error.context.exchange == "hyperliquid"
        assert error.context.channel == "trades"
        assert error.context.sequence_number == 789
        assert error.context.reconnect_count == 2

    async def test_fallback_to_api_error_handling(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test fallback to legacy APIError handling."""
        # Create legacy APIError
        api_error = APIError(
            message="Legacy error",
            code=APIErrorCode.CONNECTION_ERROR.value,
            http_status=500,
            retry_after=5.0,
        )

        recovery_system.connection = mock_connection

        # Handle the error
        await recovery_system.handle_connection_error(api_error)

        # Verify error handled with legacy system
        assert recovery_system.state == ConnectionState.FAILED
        assert recovery_system.health.consecutive_failures == 1
        assert recovery_system.current_delay >= 5.0  # Uses retry_after
        assert recovery_system.recovery_task is not None

        # Clean up
        await recovery_system.stop_recovery()

    async def test_mixed_error_handling(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test handling both typed and legacy errors in sequence."""
        recovery_system.connection = mock_connection

        # First handle a typed error
        context = StreamErrorContext(
            connection_id="test-conn-id",
            exchange=ExchangeName.HYPERLIQUID,
        )

        typed_error = WebSocketStreamError(
            message="Typed error",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=context,
            recovery_strategy=WebSocketRecoveryStrategy.LINEAR_BACKOFF,
        )

        await recovery_system.handle_connection_error(typed_error)

        assert recovery_system.config.strategy == WebSocketRecoveryStrategy.LINEAR_BACKOFF
        assert recovery_system.health.consecutive_failures == 1

        # Then handle a legacy error
        api_error = APIError(
            message="Legacy error",
            code=APIErrorCode.RATE_LIMITED.value,
            retry_after=10.0,
        )

        await recovery_system.handle_connection_error(api_error)

        # Verify both errors handled correctly
        assert recovery_system.health.consecutive_failures == 2
        assert recovery_system.current_delay >= 10.0

        # Clean up
        await recovery_system.stop_recovery()

    async def test_recovery_with_different_strategies(
        self,
        recovery_system: WebSocketErrorRecovery,
        mock_connection: Mock,
    ) -> None:
        """Test recovery with different WebSocket recovery strategies."""
        recovery_system.connection = mock_connection

        strategies_to_test = [
            (WebSocketRecoveryStrategy.IMMEDIATE_RETRY, 0.0),
            (WebSocketRecoveryStrategy.LINEAR_BACKOFF, 1.0),  # initial_delay * 1
            (WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF, 1.0),  # initial_delay * (2^0)
            (WebSocketRecoveryStrategy.RECONNECT_SAME, 1.0),
            (WebSocketRecoveryStrategy.RECONNECT_DIFFERENT, 1.0),
            (WebSocketRecoveryStrategy.FULL_RECONNECT, 1.0),
        ]

        for strategy, expected_min_delay in strategies_to_test:
            recovery_system.config.strategy = strategy
            recovery_system.retry_count = 1

            delay = recovery_system._calculate_backoff_delay()

            # Verify delay is at least the expected minimum
            assert delay >= expected_min_delay, (
                f"Strategy {strategy.name} delay {delay} < {expected_min_delay}"
            )
