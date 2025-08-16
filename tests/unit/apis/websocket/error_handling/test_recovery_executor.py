"""Unit tests for the unified recovery executor.

Tests the execution of recovery strategies with mocked dependencies.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.enums.error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.error_handling.recovery import (
    ConnectionManagerProtocol,
    MessageBufferProtocol,
    RecoveryExecutor,
    RecoveryPolicyManager,
    StateManagerProtocol,
    SubscriptionManagerProtocol,
    WebSocketErrorConfig,
)
from cyberdelta.apis.websocket.exceptions import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext


class TestRecoveryExecutor:
    """Test suite for RecoveryExecutor."""

    @pytest.fixture
    def policy_manager(self) -> RecoveryPolicyManager:
        """Create mock policy manager."""
        config = WebSocketErrorConfig(
            max_retry_attempts=3,
            initial_backoff_delay=1.0,
        )
        return RecoveryPolicyManager(config)

    @pytest.fixture
    def connection_manager(self) -> AsyncMock:
        """Create mock connection manager."""
        mock = AsyncMock(spec=ConnectionManagerProtocol)
        mock.reconnect.return_value = True
        mock.reset_connection.return_value = True
        mock.get_connection_state.return_value = "connected"
        return mock

    @pytest.fixture
    def subscription_manager(self) -> AsyncMock:
        """Create mock subscription manager."""
        mock = AsyncMock(spec=SubscriptionManagerProtocol)
        mock.resubscribe.return_value = True
        mock.resubscribe_all.return_value = True
        mock.get_active_subscriptions.return_value = [
            ("trades", "BTC-USD"),
            ("orderbook", "BTC-USD"),
            ("userEvents", None),
        ]
        return mock

    @pytest.fixture
    def state_manager(self) -> AsyncMock:
        """Create mock state manager."""
        mock = AsyncMock(spec=StateManagerProtocol)
        mock.save_state.return_value = True
        mock.restore_state.return_value = True
        mock.clear_state.return_value = True
        return mock

    @pytest.fixture
    def message_buffer(self) -> AsyncMock:
        """Create mock message buffer."""
        mock = AsyncMock(spec=MessageBufferProtocol)
        mock.replay_messages.return_value = 10  # Number of messages replayed
        mock.clear_buffer.return_value = None
        return mock

    @pytest.fixture
    def executor(
        self,
        policy_manager: RecoveryPolicyManager,
        connection_manager: AsyncMock,
        subscription_manager: AsyncMock,
        state_manager: AsyncMock,
        message_buffer: AsyncMock,
    ) -> RecoveryExecutor:
        """Create executor with mocked dependencies."""
        return RecoveryExecutor(
            policy=policy_manager,
            connection_manager=connection_manager,
            subscription_manager=subscription_manager,
            state_manager=state_manager,
            message_buffer=message_buffer,
        )

    @pytest.fixture
    def error_context(self) -> StreamErrorContext:
        """Create test error context."""
        return StreamErrorContext(
            connection_id="test-conn-1",
            exchange="hyperliquid",
            channel="trades",
            last_received_sequence=100,
        )

    @pytest.fixture
    def stream_error(self, error_context: StreamErrorContext) -> WebSocketStreamError:
        """Create test stream error."""
        return WebSocketStreamError(
            message="Test connection error",
            context=error_context,
            code=WebSocketErrorCode.CONNECTION_TIMEOUT,
        )

    # ========================================================================
    # Basic Strategy Tests
    # ========================================================================

    @pytest.mark.asyncio
    async def test_handle_none_strategy(
        self,
        executor: RecoveryExecutor,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test NONE strategy returns False."""
        result = await executor._handle_none(stream_error)
        assert result is False

    @pytest.mark.asyncio
    async def test_handle_immediate_retry(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test immediate retry strategy."""
        result = await executor._handle_immediate_retry(stream_error)

        assert result is True
        connection_manager.reconnect.assert_called_once_with(
            "test-conn-1",
            "hyperliquid",
        )

    @pytest.mark.asyncio
    async def test_handle_immediate_retry_no_manager(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test immediate retry with no connection manager."""
        executor = RecoveryExecutor(policy=policy_manager)
        result = await executor._handle_immediate_retry(stream_error)
        assert result is False

    # ========================================================================
    # Backoff Strategy Tests
    # ========================================================================

    @pytest.mark.asyncio
    async def test_handle_exponential_backoff(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test exponential backoff strategy."""
        with patch.object(executor.policy, "get_retry_count", return_value=1):
            with patch.object(executor.policy, "calculate_backoff_delay", return_value=0.01):
                with patch("asyncio.sleep") as mock_sleep:
                    result = await executor._handle_exponential_backoff(stream_error)

                    assert result is True
                    mock_sleep.assert_called_once_with(0.01)
                    connection_manager.reconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_linear_backoff(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test linear backoff strategy."""
        with patch.object(executor.policy, "get_retry_count", return_value=2):
            with patch("asyncio.sleep") as mock_sleep:
                result = await executor._handle_linear_backoff(stream_error)

                assert result is True
                # Linear: attempt * 2.0 = 2 * 2.0 = 4.0
                mock_sleep.assert_called_once_with(4.0)
                connection_manager.reconnect.assert_called_once()

    # ========================================================================
    # Reconnection Strategy Tests
    # ========================================================================

    @pytest.mark.asyncio
    async def test_handle_reconnect_same(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test reconnection to same endpoint."""
        result = await executor._handle_reconnect_same(stream_error)

        assert result is True
        connection_manager.reconnect.assert_called_once_with(
            "test-conn-1",
            "hyperliquid",
            force=False,
        )

    @pytest.mark.asyncio
    async def test_handle_reconnect_different(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test reconnection to different endpoint."""
        result = await executor._handle_reconnect_different(stream_error)

        assert result is True
        connection_manager.reset_connection.assert_called_once_with(
            "test-conn-1",
            "hyperliquid",
        )
        connection_manager.reconnect.assert_called_once_with(
            "test-conn-1",
            "hyperliquid",
            force=True,
        )

    @pytest.mark.asyncio
    async def test_handle_full_reconnect(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        subscription_manager: AsyncMock,
        state_manager: AsyncMock,
        message_buffer: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test full reconnection with state restoration."""
        result = await executor._handle_full_reconnect(stream_error)

        assert result is True

        # Verify call sequence
        state_manager.save_state.assert_called_once()
        connection_manager.reset_connection.assert_called_once()
        connection_manager.reconnect.assert_called_once_with(
            "test-conn-1",
            "hyperliquid",
            force=True,
        )
        state_manager.restore_state.assert_called_once()
        subscription_manager.resubscribe_all.assert_called_once()
        message_buffer.replay_messages.assert_called_once_with(
            "test-conn-1",
            "hyperliquid",
            since_sequence=100,
        )

    @pytest.mark.asyncio
    async def test_handle_full_reconnect_failure(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        state_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test full reconnection failure doesn't restore state."""
        connection_manager.reconnect.return_value = False

        result = await executor._handle_full_reconnect(stream_error)

        assert result is False
        state_manager.save_state.assert_called_once()
        state_manager.restore_state.assert_not_called()

    # ========================================================================
    # Subscription Strategy Tests
    # ========================================================================

    @pytest.mark.asyncio
    async def test_handle_resubscribe_single(
        self,
        executor: RecoveryExecutor,
        subscription_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test single channel resubscription."""
        result = await executor._handle_resubscribe_single(stream_error)

        assert result is True
        subscription_manager.resubscribe.assert_called_once_with(
            "test-conn-1",
            "hyperliquid",
            channel="trades",
            topic=None,
        )

    @pytest.mark.asyncio
    async def test_handle_resubscribe_all(
        self,
        executor: RecoveryExecutor,
        subscription_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test all channels resubscription."""
        result = await executor._handle_resubscribe_all(stream_error)

        assert result is True
        subscription_manager.resubscribe_all.assert_called_once_with(
            "test-conn-1",
            "hyperliquid",
        )

    @pytest.mark.asyncio
    async def test_handle_resubscribe_selective(
        self,
        executor: RecoveryExecutor,
        subscription_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test selective channel resubscription."""
        result = await executor._handle_resubscribe_selective(stream_error)

        assert result is True

        # Should only resubscribe to critical channels
        subscription_manager.get_active_subscriptions.assert_called_once()
        # Should resubscribe to orderbook, trades, and userEvents
        assert subscription_manager.resubscribe.call_count == 3

    # ========================================================================
    # Circuit Breaker Strategy Tests
    # ========================================================================

    @pytest.mark.asyncio
    async def test_handle_circuit_breaker(
        self,
        executor: RecoveryExecutor,
        state_manager: AsyncMock,
        message_buffer: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test circuit breaker strategy."""
        result = await executor._handle_circuit_breaker(stream_error)

        assert result is False
        state_manager.clear_state.assert_called_once()
        message_buffer.clear_buffer.assert_called_once()

    # ========================================================================
    # Other Strategy Tests
    # ========================================================================

    @pytest.mark.asyncio
    async def test_handle_fallback_exchange(
        self,
        executor: RecoveryExecutor,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test fallback exchange strategy (not supported)."""
        result = await executor._handle_fallback_exchange(stream_error)
        assert result is False

    @pytest.mark.asyncio
    async def test_handle_degrade_service(
        self,
        executor: RecoveryExecutor,
        subscription_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test service degradation strategy."""
        result = await executor._handle_degrade_service(stream_error)

        assert result is True
        subscription_manager.get_active_subscriptions.assert_called_once()

    # ========================================================================
    # Execute Recovery Tests
    # ========================================================================

    @pytest.mark.asyncio
    async def test_execute_recovery_success(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test successful recovery execution."""
        result = await executor.execute_recovery(
            stream_error,
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        assert result is True
        connection_manager.reconnect.assert_called_once()

        # Verify state updates
        with patch.object(executor.policy, "update_circuit_state") as mock_circuit:
            with patch.object(executor.policy, "update_retry_state") as mock_retry:
                await executor.execute_recovery(
                    stream_error,
                    WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
                )
                mock_circuit.assert_called_with(stream_error.context, True)
                mock_retry.assert_called_with(stream_error.context, True)

    @pytest.mark.asyncio
    async def test_execute_recovery_failure(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test failed recovery execution."""
        connection_manager.reconnect.return_value = False

        result = await executor.execute_recovery(
            stream_error,
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_execute_recovery_exception(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test recovery execution with exception."""
        connection_manager.reconnect.side_effect = Exception("Connection failed")

        result = await executor.execute_recovery(
            stream_error,
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_execute_recovery_unknown_strategy(
        self,
        executor: RecoveryExecutor,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test recovery with unknown strategy."""
        # Remove a handler to simulate unknown strategy
        executor._strategy_handlers.pop(WebSocketRecoveryStrategy.IMMEDIATE_RETRY)

        result = await executor.execute_recovery(
            stream_error,
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_execute_recovery_uses_policy_strategy(
        self,
        executor: RecoveryExecutor,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test recovery uses policy strategy when not provided."""
        with patch.object(
            executor.policy,
            "get_recovery_strategy",
            return_value=WebSocketRecoveryStrategy.NONE,
        ):
            result = await executor.execute_recovery(stream_error)
            assert result is False

    # ========================================================================
    # Utility Method Tests
    # ========================================================================

    @pytest.mark.asyncio
    async def test_shutdown(
        self,
        executor: RecoveryExecutor,
    ) -> None:
        """Test executor shutdown."""
        await executor.shutdown()
        # Should complete without error

    def test_get_statistics(
        self,
        executor: RecoveryExecutor,
    ) -> None:
        """Test statistics gathering."""
        stats = executor.get_statistics()

        assert stats["has_connection_manager"] is True
        assert stats["has_subscription_manager"] is True
        assert stats["has_state_manager"] is True
        assert stats["has_message_buffer"] is True
        assert stats["available_strategies"] == 13  # Number of strategies

    def test_get_statistics_no_managers(
        self,
        policy_manager: RecoveryPolicyManager,
    ) -> None:
        """Test statistics with no managers."""
        executor = RecoveryExecutor(policy=policy_manager)
        stats = executor.get_statistics()

        assert stats["has_connection_manager"] is False
        assert stats["has_subscription_manager"] is False
        assert stats["has_state_manager"] is False
        assert stats["has_message_buffer"] is False
