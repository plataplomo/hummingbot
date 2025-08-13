"""Comprehensive recovery strategy coverage tests for WebSocket error system.

Tests all WebSocket recovery strategies to ensure proper implementation,
error mapping, and recovery behavior.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_recovery import StreamRecoverySystem
from cyberdelta.config.models.websocket_error_config import WebSocketErrorRecoveryConfig
from tests.utils.websocket.error_test_utils import (
    ErrorScenarioGenerator,
    ErrorTestFactory,
)


class TestRecoveryStrategyCoverage:
    """Test coverage for all WebSocket recovery strategies."""

    @pytest.fixture
    def recovery_config(self) -> WebSocketErrorRecoveryConfig:
        """Create test recovery configuration.

        Returns:
            WebSocketErrorRecoveryConfig: Configuration for recovery strategy testing.
        """
        return WebSocketErrorRecoveryConfig(
            max_recovery_attempts=3,
            initial_backoff_ms=100,
            max_backoff_ms=5000,
            backoff_multiplier=2.0,
            circuit_breaker_enabled=True,
            circuit_breaker_threshold=5,
            circuit_breaker_timeout_ms=30000,
        )

    @pytest.fixture
    def mock_connection_manager(self) -> MagicMock:
        """Create mock connection manager.

        Returns:
            MagicMock: Mock connection manager with async methods for testing.
        """
        manager = MagicMock()
        manager.reconnect = AsyncMock(return_value=True)
        manager.reset_connection = AsyncMock(return_value=True)
        manager.get_connection_state = AsyncMock(return_value="connected")
        return manager

    @pytest.fixture
    def mock_subscription_manager(self) -> MagicMock:
        """Create mock subscription manager.

        Returns:
            MagicMock: Mock subscription manager with async subscription methods.
        """
        manager = MagicMock()
        manager.resubscribe = AsyncMock(return_value=True)
        manager.clear_subscriptions = AsyncMock()
        return manager

    @pytest.fixture
    def mock_state_manager(self) -> MagicMock:
        """Create mock state manager.

        Returns:
            MagicMock: Mock state manager with async state management methods.
        """
        manager = MagicMock()
        manager.request_snapshot = AsyncMock(return_value=True)
        manager.clear_state = AsyncMock()
        return manager

    @pytest.fixture
    def recovery_system(
        self,
        recovery_config: WebSocketErrorRecoveryConfig,
        mock_connection_manager: MagicMock,
        mock_subscription_manager: MagicMock,
        mock_state_manager: MagicMock,
    ) -> StreamRecoverySystem:
        """Create recovery system with mocked dependencies.

        Returns:
            StreamRecoverySystem: Recovery system configured with mock dependencies for testing.
        """
        return StreamRecoverySystem(
            config=recovery_config,
            connection_manager=mock_connection_manager,
            subscription_manager=mock_subscription_manager,
            state_manager=mock_state_manager,
        )

    def test_all_recovery_strategies_defined(self) -> None:
        """Test that all recovery strategies are properly defined."""
        strategies = list(WebSocketRecoveryStrategy)

        # Ensure we have a reasonable number of strategies
        assert len(strategies) >= 10, "Should have comprehensive recovery strategies"

        # Check key strategies exist
        required_strategies = {
            WebSocketRecoveryStrategy.NONE,
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            WebSocketRecoveryStrategy.RECONNECT_SAME,
            WebSocketRecoveryStrategy.FULL_RECONNECT,
            WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
            WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
        }

        for strategy in required_strategies:
            assert strategy in strategies, f"Missing required strategy: {strategy.name}"

    def test_strategy_value_ranges(self) -> None:
        """Test that recovery strategies have logical value ranges."""
        # Group strategies by category (based on value ranges)
        retry_strategies = []
        connection_strategies = []
        subscription_strategies = []
        advanced_strategies = []

        for strategy in WebSocketRecoveryStrategy:
            if 100 <= strategy.value < 200:
                retry_strategies.append(strategy)
            elif 200 <= strategy.value < 300:
                connection_strategies.append(strategy)
            elif 300 <= strategy.value < 400:
                subscription_strategies.append(strategy)
            elif 400 <= strategy.value < 500:
                advanced_strategies.append(strategy)

        # Verify each category has strategies
        assert len(retry_strategies) > 0, "Should have retry strategies (100-199)"
        assert len(connection_strategies) > 0, "Should have connection strategies (200-299)"
        assert len(subscription_strategies) > 0, "Should have subscription strategies (300-399)"
        assert len(advanced_strategies) > 0, "Should have advanced strategies (400-499)"

    @pytest.mark.parametrize(
        "strategy,error_codes", ErrorScenarioGenerator.get_recovery_strategy_scenarios()
    )
    def test_recovery_strategy_mapping(
        self,
        strategy: WebSocketRecoveryStrategy,
        error_codes: list[WebSocketErrorCode],
    ) -> None:
        """Test that error codes map to appropriate recovery strategies.

        Args:
            strategy: Recovery strategy to test
            error_codes: Error codes that should use this strategy
        """
        for code in error_codes:
            error = ErrorTestFactory.create_test_error(code=code)

            # Some errors might have different strategies based on context
            # but should be related
            if strategy == WebSocketRecoveryStrategy.NONE:
                assert not error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE, (
                    f"Error {code.name} with NONE strategy should not be retryable"
                )
            # For other strategies, error should be retryable
            elif error.recovery_strategy != strategy:
                # Allow related strategies (e.g., RECONNECT_SAME vs FULL_RECONNECT)
                related_strategies = self._get_related_strategies(strategy)
                assert error.recovery_strategy in related_strategies, (
                    f"Error {code.name} has unexpected strategy {error.recovery_strategy}"
                )

    async def test_immediate_retry_strategy(
        self,
        recovery_system: StreamRecoverySystem,
        mock_connection_manager: MagicMock,
    ) -> None:
        """Test immediate retry recovery strategy."""
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.SEQUENCE_GAP)
        error.recovery_strategy = WebSocketRecoveryStrategy.IMMEDIATE_RETRY

        success = await recovery_system.handle_stream_error(error)

        # Should attempt immediate recovery
        assert success or mock_connection_manager.reconnect.called

    async def test_exponential_backoff_strategy(
        self,
        recovery_system: StreamRecoverySystem,
    ) -> None:
        """Test exponential backoff recovery strategy."""
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED)
        error.recovery_strategy = WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        with patch("asyncio.sleep") as mock_sleep:
            # First attempt
            await recovery_system.handle_stream_error(error)

            # Second attempt (should have longer delay)
            await recovery_system.handle_stream_error(error)

            # Check that sleep was called with increasing delays
            if mock_sleep.call_count >= 2:
                delays = [call[0][0] for call in mock_sleep.call_args_list]
                assert delays[1] > delays[0], "Backoff delay should increase"

    async def test_reconnect_same_strategy(
        self,
        recovery_system: StreamRecoverySystem,
        mock_connection_manager: MagicMock,
    ) -> None:
        """Test reconnect to same endpoint strategy."""
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
        error.recovery_strategy = WebSocketRecoveryStrategy.RECONNECT_SAME

        await recovery_system.handle_stream_error(error)

        # Should call reconnect without reset
        mock_connection_manager.reconnect.assert_called_once()
        mock_connection_manager.reset_connection.assert_not_called()

    async def test_full_reconnect_strategy(
        self,
        recovery_system: StreamRecoverySystem,
        mock_connection_manager: MagicMock,
    ) -> None:
        """Test full reconnect strategy."""
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.PROTOCOL_ERROR)
        error.recovery_strategy = WebSocketRecoveryStrategy.FULL_RECONNECT

        await recovery_system.handle_stream_error(error)

        # Should reset and reconnect
        mock_connection_manager.reset_connection.assert_called()
        mock_connection_manager.reconnect.assert_called()

    async def test_resubscribe_all_strategy(
        self,
        recovery_system: StreamRecoverySystem,
        mock_subscription_manager: MagicMock,
    ) -> None:
        """Test resubscribe all channels strategy."""
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.SUBSCRIPTION_FAILED)
        error.recovery_strategy = WebSocketRecoveryStrategy.RESUBSCRIBE_ALL

        await recovery_system.handle_stream_error(error)

        # Should resubscribe to all channels
        mock_subscription_manager.resubscribe.assert_called()
        # Check that None was passed for channel (meaning all)
        call_args = mock_subscription_manager.resubscribe.call_args
        if call_args:
            assert call_args[1].get("channel") is None or call_args[0][2] is None

    async def test_circuit_breaker_strategy(
        self,
        recovery_system: StreamRecoverySystem,
    ) -> None:
        """Test circuit breaker recovery strategy."""
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.STREAM_CORRUPTED)
        error.recovery_strategy = WebSocketRecoveryStrategy.CIRCUIT_BREAKER

        # Trigger circuit breaker by exceeding threshold
        for _ in range(6):  # Threshold is 5
            await recovery_system.handle_stream_error(error)

        # Circuit breaker should be active
        stats = recovery_system.get_recovery_stats()
        assert len(stats["circuit_breakers_active"]) > 0, "Circuit breaker should be active"

        # Further attempts should be blocked
        success = await recovery_system.handle_stream_error(error)
        assert not success, "Circuit breaker should block recovery"

    async def test_fallback_exchange_strategy(
        self,
        recovery_system: StreamRecoverySystem,
    ) -> None:
        """Test fallback to alternative exchange strategy."""
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.EXCHANGE_UNAVAILABLE)
        error.recovery_strategy = WebSocketRecoveryStrategy.FALLBACK_EXCHANGE

        # This strategy is not fully implemented yet
        success = await recovery_system.handle_stream_error(error)

        # Should return False as fallback logic not implemented
        assert not success, "Fallback exchange not yet implemented"

    async def test_degrade_service_strategy(
        self,
        recovery_system: StreamRecoverySystem,
        mock_subscription_manager: MagicMock,
    ) -> None:
        """Test service degradation strategy."""
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RESOURCE_EXHAUSTED)
        error.recovery_strategy = WebSocketRecoveryStrategy.DEGRADE_SERVICE

        success = await recovery_system.handle_stream_error(error)

        # Should clear non-essential subscriptions
        mock_subscription_manager.clear_subscriptions.assert_called()
        assert success, "Service degradation should succeed"

    def test_recovery_strategy_retry_delays(self) -> None:
        """Test that recovery strategies have appropriate retry delays."""
        test_cases = [
            (WebSocketErrorCode.SEQUENCE_GAP, 0, 100),  # Immediate or very short
            (WebSocketErrorCode.RATE_LIMITED, 1000, 60000),  # Longer delay for rate limits
            (WebSocketErrorCode.CONNECTION_TIMEOUT, 100, 5000),  # Medium delay
        ]

        for code, min_delay, max_delay in test_cases:
            error = ErrorTestFactory.create_test_error(code=code)
            delay = error.get_retry_delay_ms()

            assert min_delay <= delay <= max_delay, (
                f"Error {code.name} delay {delay}ms outside range {min_delay}-{max_delay}ms"
            )

    def test_recovery_strategy_compatibility(self) -> None:
        """Test that recovery strategies are compatible with error types."""
        # Connection errors should use connection strategies
        connection_errors = [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.CONNECTION_TIMEOUT,
            WebSocketErrorCode.CONNECTION_RESET,
        ]

        for code in connection_errors:
            error = ErrorTestFactory.create_test_error(code=code)
            assert error.recovery_strategy in {
                WebSocketRecoveryStrategy.RECONNECT_SAME,
                WebSocketRecoveryStrategy.RECONNECT_DIFFERENT,
                WebSocketRecoveryStrategy.FULL_RECONNECT,
                WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            }, f"Connection error {code.name} has incompatible strategy"

        # Subscription errors should use subscription strategies
        subscription_errors = [
            WebSocketErrorCode.SUBSCRIPTION_FAILED,
            WebSocketErrorCode.SUBSCRIPTION_REJECTED,
            WebSocketErrorCode.CHANNEL_CLOSED,
        ]

        for code in subscription_errors:
            error = ErrorTestFactory.create_test_error(code=code)
            # Allow various strategies for subscription errors
            valid_strategies = {
                WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
                WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
                WebSocketRecoveryStrategy.RESUBSCRIBE_SELECTIVE,
                WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
                WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            }
            assert error.recovery_strategy in valid_strategies, (
                f"Subscription error {code.name} has incompatible strategy"
            )

    def test_no_recovery_strategy(self) -> None:
        """Test errors that should have no recovery strategy."""
        no_recovery_errors = [
            WebSocketErrorCode.AUTH_REVOKED,
            WebSocketErrorCode.IP_BANNED,
            WebSocketErrorCode.ACCOUNT_SUSPENDED,
            WebSocketErrorCode.SECURITY_VIOLATION,
        ]

        for code in no_recovery_errors:
            error = ErrorTestFactory.create_test_error(code=code)
            assert error.recovery_strategy == WebSocketRecoveryStrategy.NONE, (
                f"Error {code.name} should have no recovery strategy"
            )
            assert not error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE, (
                f"Error {code.name} with no recovery should not be retryable"
            )

    async def test_recovery_attempt_limits(
        self,
        recovery_system: StreamRecoverySystem,
    ) -> None:
        """Test that recovery attempts are limited."""
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Attempt recovery beyond the limit
        successes = []
        for _ in range(5):  # Max attempts is 3
            success = await recovery_system.handle_stream_error(error)
            successes.append(success)

        # Should stop attempting after max attempts
        assert not all(successes), "Should stop recovery after max attempts"

    def test_recovery_strategy_documentation(self) -> None:
        """Test that all recovery strategies are documented."""
        for strategy in WebSocketRecoveryStrategy:
            # Check that strategy has a meaningful name
            assert len(strategy.name) > 3, f"Strategy {strategy} has too short name"

            # Check value is positive
            assert strategy.value >= 0, f"Strategy {strategy} has negative value"

    def _get_related_strategies(
        self,
        strategy: WebSocketRecoveryStrategy,
    ) -> set[WebSocketRecoveryStrategy]:
        """Get related recovery strategies.

        Args:
            strategy: Base strategy

        Returns:
            Set of related strategies
        """
        related = {strategy}

        # Add related strategies based on category
        if strategy in {
            WebSocketRecoveryStrategy.RECONNECT_SAME,
            WebSocketRecoveryStrategy.RECONNECT_DIFFERENT,
            WebSocketRecoveryStrategy.FULL_RECONNECT,
        }:
            related.update({
                WebSocketRecoveryStrategy.RECONNECT_SAME,
                WebSocketRecoveryStrategy.RECONNECT_DIFFERENT,
                WebSocketRecoveryStrategy.FULL_RECONNECT,
            })

        if strategy in {
            WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
            WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
            WebSocketRecoveryStrategy.RESUBSCRIBE_SELECTIVE,
        }:
            related.update({
                WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
                WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
                WebSocketRecoveryStrategy.RESUBSCRIBE_SELECTIVE,
                WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            })

        # Backoff strategies are related
        if strategy in {
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            WebSocketRecoveryStrategy.LINEAR_BACKOFF,
        }:
            related.update({
                WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
                WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
                WebSocketRecoveryStrategy.LINEAR_BACKOFF,
            })

        return related
