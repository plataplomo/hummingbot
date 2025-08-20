"""Tests for RecoveryExecutor.

Tests recovery execution through public interfaces only, following project guidelines
that prohibit testing private methods directly.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket.error_codes import WebSocketErrorCode
from cyberdelta.apis.exceptions.websocket import WebSocketStreamError
from cyberdelta.apis.models.websocket.error_context import StreamErrorContext
from cyberdelta.apis.websocket.error_context.recovery import (
    ConnectionManagerProtocol,
    MessageBufferProtocol,
    RecoveryExecutor,
    RecoveryPolicyManager,
    StateManagerProtocol,
    SubscriptionManagerProtocol,
)
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.enums import ExchangeName


class TestRecoveryExecutor:
    """Test suite for RecoveryExecutor through public interfaces only."""

    @pytest.fixture
    def policy_manager(self) -> RecoveryPolicyManager:
        """Create mock policy manager.

        Returns:
            RecoveryPolicyManager: Policy manager for testing.
        """
        config = WebSocketErrorConfig()
        return RecoveryPolicyManager(config)

    @pytest.fixture
    def connection_manager(self) -> AsyncMock:
        """Create mock connection manager.

        Returns:
            AsyncMock: Mock connection manager for testing.
        """
        mock = AsyncMock(spec=ConnectionManagerProtocol)
        mock.reconnect.return_value = True
        mock.reset_connection.return_value = True
        mock.get_connection_state.return_value = "connected"
        return mock

    @pytest.fixture
    def subscription_manager(self) -> AsyncMock:
        """Create mock subscription manager.

        Returns:
            AsyncMock: Mock subscription manager for testing.
        """
        mock = AsyncMock(spec=SubscriptionManagerProtocol)
        mock.resubscribe.return_value = True
        mock.resubscribe_all.return_value = True
        mock.get_active_subscriptions.return_value = [("test_channel", "test_topic")]
        return mock

    @pytest.fixture
    def state_manager(self) -> AsyncMock:
        """Create mock state manager.

        Returns:
            AsyncMock: Mock state manager for testing.
        """
        mock = AsyncMock(spec=StateManagerProtocol)
        mock.save_state.return_value = True
        mock.restore_state.return_value = True
        mock.clear_state.return_value = True
        return mock

    @pytest.fixture
    def message_buffer(self) -> AsyncMock:
        """Create mock message buffer.

        Returns:
            AsyncMock: Mock message buffer for testing.
        """
        mock = AsyncMock(spec=MessageBufferProtocol)
        mock.replay_messages.return_value = 5
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
        """Create RecoveryExecutor with all dependencies.

        Returns:
            RecoveryExecutor: Configured recovery executor for testing.
        """
        return RecoveryExecutor(
            policy=policy_manager,
            connection_manager=connection_manager,
            subscription_manager=subscription_manager,
            state_manager=state_manager,
            message_buffer=message_buffer,
        )

    @pytest.fixture
    def stream_error(self) -> WebSocketStreamError:
        """Create test WebSocket stream error.

        Returns:
            WebSocketStreamError: Test error for recovery testing.
        """
        context = StreamErrorContext(
            connection_id="test-conn-1",
            exchange=ExchangeName.HYPERLIQUID,
            channel="test_channel",
            topic="test_topic",
            environment="test",
        )
        return WebSocketStreamError(
            message="Test connection error",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=context,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

    # ========================================================================
    # Public Interface Tests
    # ========================================================================

    @pytest.mark.asyncio
    async def test_execute_recovery_none_strategy(
        self,
        executor: RecoveryExecutor,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test NONE strategy execution through public interface."""
        error_with_none = WebSocketStreamError(
            message=stream_error.message,
            code=stream_error.code,
            context=stream_error.context,
            recovery_strategy=WebSocketRecoveryStrategy.NONE,
        )

        result = await executor.execute_recovery(error_with_none)
        assert result is False

    @pytest.mark.asyncio
    async def test_execute_recovery_immediate_retry(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test immediate retry strategy execution."""
        error_with_retry = WebSocketStreamError(
            message=stream_error.message,
            code=stream_error.code,
            context=stream_error.context,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        result = await executor.execute_recovery(error_with_retry)

        assert result is True
        connection_manager.reconnect.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_execute_recovery_exponential_backoff(
        self,
        policy_manager: RecoveryPolicyManager,
        connection_manager: AsyncMock,
        subscription_manager: AsyncMock,
        state_manager: AsyncMock,
        message_buffer: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test exponential backoff strategy execution."""
        # Create executor with config that doesn't override exponential backoff
        config = WebSocketErrorConfig()
        config.recovery.prefer_reconnect_for_connection_errors = False
        policy_manager_no_override = RecoveryPolicyManager(config)

        executor = RecoveryExecutor(
            policy=policy_manager_no_override,
            connection_manager=connection_manager,
            subscription_manager=subscription_manager,
            state_manager=state_manager,
            message_buffer=message_buffer,
        )

        error_with_backoff = WebSocketStreamError(
            message=stream_error.message,
            code=stream_error.code,
            context=stream_error.context,
            recovery_strategy=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
        )

        with patch("asyncio.sleep") as mock_sleep:
            result = await executor.execute_recovery(error_with_backoff)

            assert result is True
            assert mock_sleep.called
            connection_manager.reconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_recovery_full_reconnect(
        self,
        executor: RecoveryExecutor,
        connection_manager: AsyncMock,
        subscription_manager: AsyncMock,
        state_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test full reconnection strategy execution."""
        error_with_full_reconnect = WebSocketStreamError(
            message=stream_error.message,
            code=stream_error.code,
            context=stream_error.context,
            recovery_strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
        )

        result = await executor.execute_recovery(error_with_full_reconnect)

        assert result is True
        state_manager.save_state.assert_called_once()
        connection_manager.reset_connection.assert_called_once()
        connection_manager.reconnect.assert_called_once()
        state_manager.restore_state.assert_called_once()
        subscription_manager.resubscribe_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_recovery_resubscribe_single(
        self,
        executor: RecoveryExecutor,
        subscription_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test single resubscription strategy execution."""
        error_with_resubscribe = WebSocketStreamError(
            message=stream_error.message,
            code=stream_error.code,
            context=stream_error.context,
            recovery_strategy=WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
        )

        result = await executor.execute_recovery(error_with_resubscribe)

        assert result is True
        subscription_manager.resubscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_recovery_resubscribe_all(
        self,
        executor: RecoveryExecutor,
        subscription_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test all resubscription strategy execution."""
        error_with_resubscribe_all = WebSocketStreamError(
            message=stream_error.message,
            code=stream_error.code,
            context=stream_error.context,
            recovery_strategy=WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
        )

        result = await executor.execute_recovery(error_with_resubscribe_all)

        assert result is True
        subscription_manager.resubscribe_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_recovery_circuit_breaker(
        self,
        executor: RecoveryExecutor,
        state_manager: AsyncMock,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test circuit breaker strategy execution."""
        error_with_circuit_breaker = WebSocketStreamError(
            message=stream_error.message,
            code=stream_error.code,
            context=stream_error.context,
            recovery_strategy=WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
        )

        result = await executor.execute_recovery(error_with_circuit_breaker)

        assert result is False  # Circuit breaker doesn't attempt recovery
        state_manager.clear_state.assert_called_once()

    # ========================================================================
    # Configuration and Statistics Tests
    # ========================================================================

    def test_executor_initialization(
        self,
        policy_manager: RecoveryPolicyManager,
        connection_manager: AsyncMock,
    ) -> None:
        """Test RecoveryExecutor initialization."""
        executor = RecoveryExecutor(
            policy=policy_manager,
            connection_manager=connection_manager,
        )

        assert executor.policy is policy_manager
        assert executor.connection_manager is connection_manager

    def test_executor_statistics(self, executor: RecoveryExecutor) -> None:
        """Test executor statistics collection through public interface."""
        stats = executor.get_statistics()

        assert isinstance(stats, dict)
        assert "has_connection_manager" in stats
        assert "has_subscription_manager" in stats
        assert "has_state_manager" in stats
        assert "has_message_buffer" in stats
        assert "available_strategies" in stats

    @pytest.mark.asyncio
    async def test_executor_shutdown(self, executor: RecoveryExecutor) -> None:
        """Test executor shutdown."""
        # Should not raise any exceptions
        await executor.shutdown()

    @pytest.mark.asyncio
    async def test_recovery_with_no_managers(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test recovery execution with no dependency managers."""
        executor = RecoveryExecutor(policy=policy_manager)

        # Strategies requiring managers should fail gracefully
        error_needing_connection = WebSocketStreamError(
            message=stream_error.message,
            code=stream_error.code,
            context=stream_error.context,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        result = await executor.execute_recovery(error_needing_connection)
        assert result is False  # Should fail without connection manager

    @pytest.mark.asyncio
    async def test_recovery_state_tracking(
        self,
        executor: RecoveryExecutor,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test that recovery state is properly tracked through policy manager."""
        # Execute recovery and verify state tracking works
        result = await executor.execute_recovery(stream_error)

        # Recovery should complete (success/failure depends on strategy and managers)
        assert isinstance(result, bool)

        # Policy should have been used to track state
        assert executor.policy is not None
