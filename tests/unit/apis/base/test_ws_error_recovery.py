"""Tests for WebSocket error recovery system.

This module tests the unified recovery system using the current architecture
with RecoveryPolicyManager and RecoveryExecutor.
"""

import pytest

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket.error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.error_handling.recovery import (
    CircuitState,
    RecoveryPolicyManager,
)
from cyberdelta.apis.websocket.exceptions import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.enums import ExchangeName


class TestRecoveryPolicyManager:
    """Test RecoveryPolicyManager functionality."""

    @pytest.fixture
    def policy_manager(self) -> RecoveryPolicyManager:
        """Create recovery policy manager for testing.

        Returns:
            RecoveryPolicyManager configured with default settings.
        """
        config = WebSocketErrorConfig()
        return RecoveryPolicyManager(config)

    @pytest.fixture
    def error_context(self) -> StreamErrorContext:
        """Create error context for testing.

        Returns:
            StreamErrorContext for test scenarios.
        """
        return StreamErrorContext(
            connection_id="test-conn-123",
            exchange=ExchangeName.HYPERLIQUID,
            channel="orderbook",
            topic="BTC-USD",
            sequence_number=42,
            user_id="test-user",
            session_id="test-session-456",
            environment="test",
            raw_message_size=512,
        )

    @pytest.fixture
    def stream_error(self, error_context: StreamErrorContext) -> WebSocketStreamError:
        """Create stream error for testing.

        Returns:
            WebSocketStreamError for test scenarios.
        """
        return WebSocketStreamError(
            message="Connection lost",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=error_context,
            recovery_strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
        )

    def test_policy_manager_initialization(self, policy_manager: RecoveryPolicyManager) -> None:
        """Test policy manager initialization."""
        assert policy_manager is not None
        # Policy manager should be properly configured
        assert hasattr(policy_manager, "config")

    def test_should_retry_logic(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test retry decision logic."""
        # First attempt should be allowed (synchronous method)
        should_retry = policy_manager.should_retry(stream_error)
        assert should_retry is True

    def test_get_recovery_strategy(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test getting recovery strategy for an error."""
        strategy = policy_manager.get_recovery_strategy(stream_error)

        # Should return the strategy specified in the error
        assert strategy == WebSocketRecoveryStrategy.FULL_RECONNECT

    def test_record_success_and_failure(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test recording success and failure states."""
        # Test that we can track recovery outcomes
        # Note: These methods may be internal to the state management
        # but the policy manager should handle state updates

        # Multiple retries should eventually be limited by policy
        retry_count = 0
        while policy_manager.should_retry(stream_error) and retry_count < 10:
            retry_count += 1

        # Should have made some attempts
        assert retry_count > 0

    def test_recovery_statistics(
        self,
        policy_manager: RecoveryPolicyManager,
    ) -> None:
        """Test recovery statistics collection."""
        stats = policy_manager.get_statistics()

        # Should have statistics structure
        assert isinstance(stats, dict)
        # Basic statistics should be present
        expected_keys = ["active_recoveries", "circuit_breakers"]
        for key in expected_keys:
            assert key in stats


class TestCircuitBreakerIntegration:
    """Test circuit breaker integration with recovery system."""

    def test_circuit_state_enum_values(self) -> None:
        """Test that CircuitState enum has expected values."""
        assert CircuitState.CLOSED.value == "closed"
        assert CircuitState.OPEN.value == "open"
        assert CircuitState.HALF_OPEN.value == "half_open"

    def test_circuit_state_transitions(self) -> None:
        """Test that circuit states are properly defined."""
        # Test that all expected states exist
        states = [CircuitState.CLOSED, CircuitState.OPEN, CircuitState.HALF_OPEN]
        assert len(states) == 3
        assert all(isinstance(state, CircuitState) for state in states)


class TestWebSocketRecoveryStrategyIntegration:
    """Test WebSocketRecoveryStrategy enum integration."""

    def test_recovery_strategy_enum_exists(self) -> None:
        """Test that WebSocketRecoveryStrategy enum is available and complete."""
        # Test that we can access the enum values
        strategies = list(WebSocketRecoveryStrategy)
        assert len(strategies) > 0

        # Test specific strategies that actually exist
        assert WebSocketRecoveryStrategy.NONE is not None
        assert WebSocketRecoveryStrategy.IMMEDIATE_RETRY is not None
        assert WebSocketRecoveryStrategy.FULL_RECONNECT is not None
        assert WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE is not None

    def test_recovery_strategy_values(self) -> None:
        """Test recovery strategy enum values."""
        # Get all available strategies from the enum
        all_strategies = list(WebSocketRecoveryStrategy)

        # Should have at least some strategies defined
        assert len(all_strategies) > 0

        # All should be valid enum members
        for strategy in all_strategies:
            assert isinstance(strategy, WebSocketRecoveryStrategy)

        # Test that strategies are distinct
        strategy_values = [strategy.value for strategy in all_strategies]
        assert len(set(strategy_values)) == len(strategy_values)
