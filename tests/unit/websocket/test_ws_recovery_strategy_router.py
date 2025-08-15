"""Test WebSocket recovery strategy router.

This test validates Step 47: Create Recovery Strategy Router.
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_recovery_strategy_router import (
    RecoveryStrategyRouter,
)
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError


@pytest.mark.asyncio
class TestRecoveryStrategyRouter:
    """Test recovery strategy router functionality."""

    @pytest.fixture
    def router(self) -> RecoveryStrategyRouter:
        """Create recovery strategy router.

        Returns:
            RecoveryStrategyRouter: Router for determining recovery strategies.
        """
        return RecoveryStrategyRouter()

    @pytest.fixture
    def mock_recovery(self) -> Mock:
        """Create mock recovery system.

        Returns:
            Mock: Mock recovery system with async error handling methods.
        """
        recovery = Mock()
        recovery.handle_connection_error = AsyncMock()
        recovery.handle_message_failure = AsyncMock()
        return recovery

    def create_test_error(
        self,
        code: WebSocketErrorCode,
        strategy: WebSocketRecoveryStrategy | None = None,
        severity: ErrorSeverity | None = None,
    ) -> WebSocketStreamError:
        """Create a test error with specified parameters.

        Returns:
            WebSocketStreamError: Test error configured with provided parameters.
        """
        context = StreamErrorContext(
            connection_id="test-conn-id",
            exchange="hyperliquid",
            channel="test-channel",
            reconnect_count=2,
        )

        return WebSocketStreamError(
            message="Test error",
            code=code,
            context=context,
            severity=severity,
            recovery_strategy=strategy,
        )

    async def test_route_immediate_retry_recovery(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test routing immediate retry recovery."""
        error = self.create_test_error(
            WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        result = await router.route_recovery(error, mock_recovery)

        assert result.success is True
        assert result.strategy_used == WebSocketRecoveryStrategy.IMMEDIATE_RETRY
        assert result.time_elapsed_ms == 0
        assert result.should_continue is True
        assert "Immediate retry" in result.action_taken

    async def test_route_exponential_backoff_recovery(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test routing exponential backoff recovery."""
        error = self.create_test_error(
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
        )

        # Mock sleep to avoid actual delay
        with pytest.MonkeyPatch.context() as mp:
            mock_sleep = AsyncMock()
            mp.setattr(asyncio, "sleep", mock_sleep)

            result = await router.route_recovery(error, mock_recovery)

        assert result.success is True
        assert result.strategy_used == WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF
        assert result.should_continue is True
        assert "exponential backoff" in result.action_taken.lower()
        mock_sleep.assert_called_once()

    async def test_route_reconnection_strategies(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test routing reconnection strategies."""
        strategies = [
            (WebSocketRecoveryStrategy.RECONNECT_SAME, "same endpoint"),
            (WebSocketRecoveryStrategy.RECONNECT_DIFFERENT, "different endpoint"),
            (WebSocketRecoveryStrategy.FULL_RECONNECT, "full reconnection"),
        ]

        for strategy, expected_text in strategies:
            error = self.create_test_error(
                WebSocketErrorCode.CONNECTION_LOST,
                strategy,
            )

            result = await router.route_recovery(error, mock_recovery)

            assert result.success is True
            assert result.strategy_used == strategy
            assert result.should_continue is True
            assert expected_text in result.action_taken.lower()
            assert "reconnect_count" in result.metadata

    async def test_route_resubscription_strategies(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test routing resubscription strategies."""
        # Test single resubscription
        error = self.create_test_error(
            WebSocketErrorCode.SUBSCRIPTION_FAILED,
            WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
        )

        result = await router.route_recovery(error, mock_recovery)

        assert result.success is True
        assert result.strategy_used == WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE
        assert "Resubscribing to channel" in result.action_taken
        assert result.metadata["channel"] == "test-channel"

        # Test all resubscription
        error = self.create_test_error(
            WebSocketErrorCode.SUBSCRIPTION_FAILED,
            WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
        )

        result = await router.route_recovery(error, mock_recovery)

        assert result.success is True
        assert result.strategy_used == WebSocketRecoveryStrategy.RESUBSCRIBE_ALL
        assert "all channels" in result.action_taken

    async def test_route_circuit_breaker_recovery(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test routing circuit breaker recovery."""
        error = self.create_test_error(
            WebSocketErrorCode.CONNECTION_FAILED,
            WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
        )

        result = await router.route_recovery(error, mock_recovery)

        assert result.success is False
        assert result.strategy_used == WebSocketRecoveryStrategy.CIRCUIT_BREAKER
        assert result.should_continue is False
        assert "Circuit breaker activated" in result.action_taken
        assert result.error_message is not None
        assert "Too many failures" in result.error_message

    async def test_route_degrade_service_recovery(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test routing service degradation recovery."""
        error = self.create_test_error(
            WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED,
            WebSocketRecoveryStrategy.DEGRADE_SERVICE,
        )

        result = await router.route_recovery(error, mock_recovery)

        assert result.success is True
        assert result.strategy_used == WebSocketRecoveryStrategy.DEGRADE_SERVICE
        assert result.should_continue is True
        assert "Service degraded" in result.action_taken
        assert "degradation_reason" in result.metadata

    async def test_route_no_recovery(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test routing no recovery strategy."""
        error = self.create_test_error(
            WebSocketErrorCode.AUTH_FAILED,
            WebSocketRecoveryStrategy.NONE,
        )

        result = await router.route_recovery(error, mock_recovery)

        assert result.success is False
        assert result.strategy_used == WebSocketRecoveryStrategy.NONE
        assert result.should_continue is False
        assert "No recovery action" in result.action_taken
        assert "non-retryable" in result.action_taken

    async def test_recovery_attempt_tracking(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test tracking of recovery attempts."""
        error = self.create_test_error(
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketRecoveryStrategy.RECONNECT_SAME,
        )

        # First attempt
        await router.route_recovery(error, mock_recovery)
        assert router.recovery_attempts[f"{error.code.name}:test-conn-id"] == 1

        # Second attempt
        await router.route_recovery(error, mock_recovery)
        assert router.recovery_attempts[f"{error.code.name}:test-conn-id"] == 2

        # Reset attempts for connection
        router.reset_attempts("test-conn-id")
        assert len(router.recovery_attempts) == 0

    async def test_get_recovery_action(
        self,
        router: RecoveryStrategyRouter,
    ) -> None:
        """Test getting recovery action for an error."""
        # Test reconnection action
        error = self.create_test_error(
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketRecoveryStrategy.FULL_RECONNECT,
        )

        action = router.get_recovery_action(error)

        assert action.strategy == WebSocketRecoveryStrategy.FULL_RECONNECT
        assert action.should_reconnect is True
        assert action.should_clear_state is True
        assert action.should_resubscribe is False
        assert action.delay_ms > 0

        # Test resubscription action
        error = self.create_test_error(
            WebSocketErrorCode.SUBSCRIPTION_FAILED,
            WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
        )

        action = router.get_recovery_action(error)

        assert action.strategy == WebSocketRecoveryStrategy.RESUBSCRIBE_ALL
        assert action.should_resubscribe is True
        assert action.should_reconnect is False

        # Test service degradation action
        error = self.create_test_error(
            WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED,
            WebSocketRecoveryStrategy.DEGRADE_SERVICE,
        )

        action = router.get_recovery_action(error)

        assert action.strategy == WebSocketRecoveryStrategy.DEGRADE_SERVICE
        assert action.should_degrade_service is True

    async def test_recovery_action_max_retries(
        self,
        router: RecoveryStrategyRouter,
    ) -> None:
        """Test max retries based on error severity."""
        # Critical error - 1 retry
        error = self.create_test_error(
            WebSocketErrorCode.PROTOCOL_ERROR,
            WebSocketRecoveryStrategy.FULL_RECONNECT,
            ErrorSeverity.CRITICAL,
        )

        action = router.get_recovery_action(error)
        assert action.max_retries == 1

        # Error severity - 3 retries
        error = self.create_test_error(
            WebSocketErrorCode.CONNECTION_TIMEOUT,
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            ErrorSeverity.ERROR,
        )

        action = router.get_recovery_action(error)
        assert action.max_retries == 3

        # Warning severity - 5 retries
        error = self.create_test_error(
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketRecoveryStrategy.RECONNECT_SAME,
            ErrorSeverity.WARNING,
        )

        action = router.get_recovery_action(error)
        assert action.max_retries == 5

    async def test_handler_exception_handling(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test handling of exceptions in recovery handlers."""
        # Create error with immediate retry
        error = self.create_test_error(
            WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
        )

        # Mock handler to raise exception
        for handler in router.handlers:
            if (
                hasattr(handler, "__class__")
                and handler.__class__.__name__ == "ImmediateRetryHandler"
            ):
                # Mock the handle method to simulate failure
                handler.handle = AsyncMock(side_effect=RuntimeError("Handler failed"))  # type: ignore[method-assign] # Mock assignment for testing
                break

        result = await router.route_recovery(error, mock_recovery)

        assert result.success is False
        assert result.should_continue is False
        assert "Recovery handler failed" in result.action_taken
        assert result.error_message is not None
        assert "Handler failed" in result.error_message

    async def test_get_stats(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test getting recovery statistics."""
        # Process some errors to populate stats
        for _ in range(3):
            error = self.create_test_error(
                WebSocketErrorCode.CONNECTION_LOST,
                WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            await router.route_recovery(error, mock_recovery)

        stats = router.get_stats()

        assert stats["total_handlers"] == 7  # Number of registered handlers
        assert stats["active_recovery_attempts"] == 1
        assert stats["total_attempts"] == 3

    async def test_reset_all_attempts(
        self,
        router: RecoveryStrategyRouter,
        mock_recovery: Mock,
    ) -> None:
        """Test resetting all recovery attempts."""
        # Create multiple errors to track
        errors = [
            self.create_test_error(
                WebSocketErrorCode.CONNECTION_LOST,
                WebSocketRecoveryStrategy.RECONNECT_SAME,
            ),
            self.create_test_error(
                WebSocketErrorCode.RATE_LIMITED,
                WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            ),
        ]

        for error in errors:
            await router.route_recovery(error, mock_recovery)

        assert len(router.recovery_attempts) == 2

        # Reset all attempts
        router.reset_attempts()

        assert len(router.recovery_attempts) == 0
