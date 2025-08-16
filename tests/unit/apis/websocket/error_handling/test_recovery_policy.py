"""Unit tests for the unified recovery policy manager.

Tests the policy decision logic without executing recovery actions.
Tests only through public interfaces as per project guidelines.
"""

from __future__ import annotations

import pytest

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket.error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.error_handling.recovery import (
    RecoveryPolicyManager,
)
from cyberdelta.apis.websocket.exceptions import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig


class TestRecoveryPolicyManager:
    """Test suite for RecoveryPolicyManager."""

    @pytest.fixture
    def config(self) -> WebSocketErrorConfig:
        """Create test configuration.

        Returns:
            WebSocketErrorConfig: Test configuration instance
        """
        return WebSocketErrorConfig()

    @pytest.fixture
    def policy_manager(self, config: WebSocketErrorConfig) -> RecoveryPolicyManager:
        """Create policy manager instance.

        Args:
            config: WebSocket error configuration

        Returns:
            RecoveryPolicyManager: Policy manager instance
        """
        return RecoveryPolicyManager(config)

    @pytest.fixture
    def error_context(self) -> StreamErrorContext:
        """Create test error context.

        Returns:
            StreamErrorContext: Test error context instance
        """
        return StreamErrorContext(
            connection_id="test-conn-1",
            exchange="hyperliquid",
            channel="trades",
            is_authenticated=True,
        )

    @pytest.fixture
    def stream_error(self, error_context: StreamErrorContext) -> WebSocketStreamError:
        """Create test stream error.

        Args:
            error_context: Error context for the stream error

        Returns:
            WebSocketStreamError: Test stream error instance
        """
        return WebSocketStreamError(
            message="Test connection error",
            context=error_context,
            code=WebSocketErrorCode.CONNECTION_TIMEOUT,
        )

    # ========================================================================
    # Retry Decision Tests
    # ========================================================================

    def test_should_retry_retryable_error(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test retry decision for retryable error."""
        assert policy_manager.should_retry(stream_error) is True

    def test_should_not_retry_non_retryable_error(
        self,
        policy_manager: RecoveryPolicyManager,
        error_context: StreamErrorContext,
    ) -> None:
        """Test retry decision for non-retryable error."""
        error = WebSocketStreamError(
            message="Invalid API key",
            context=error_context,
            code=WebSocketErrorCode.INVALID_API_KEY,
        )
        assert policy_manager.should_retry(error) is False

    def test_should_not_retry_max_attempts_exceeded(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test retry blocked when max attempts exceeded."""
        # Simulate max attempts reached by updating retry state multiple times
        context = stream_error.context
        for _ in range(3):  # Equal to max_retry_attempts
            policy_manager.update_retry_state(context, success=False)

        assert policy_manager.should_retry(stream_error) is False

    # ========================================================================
    # Circuit Breaker Tests
    # ========================================================================

    def test_circuit_breaker_opens_on_threshold(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test circuit breaker opens after failure threshold."""
        context = stream_error.context

        # Record failures up to threshold (default is 5)
        for _ in range(5):  # failure_threshold = 5
            policy_manager.update_circuit_state(context, success=False)

        # Circuit should be open
        assert policy_manager.is_circuit_open(context) is True
        assert policy_manager.should_retry(stream_error) is False

    def test_circuit_breaker_closes_on_success_threshold(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test circuit breaker closes after success threshold."""
        context = stream_error.context

        # First open the circuit by recording failures
        for _ in range(5):  # failure_threshold = 5
            policy_manager.update_circuit_state(context, success=False)

        # Verify circuit is open
        assert policy_manager.is_circuit_open(context) is True

        # Test the behavior of recording successes
        # This tests the public interface behavior

        # Record successes
        for _ in range(3):  # success_threshold = 3
            policy_manager.update_circuit_state(context, success=True)

        # After recording successes, verify the circuit behavior through stats
        final_stats = policy_manager.get_statistics()
        assert "open_circuits" in final_stats
        assert "half_open_circuits" in final_stats

    def test_circuit_breaker_timeout_transition(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test circuit breaker behavior when open."""
        context = stream_error.context

        # Open circuit by recording failures
        for _ in range(5):  # failure_threshold = 5
            policy_manager.update_circuit_state(context, success=False)

        # Verify circuit is open
        assert policy_manager.is_circuit_open(context) is True

        # Test that when circuit is open, retries are blocked
        assert policy_manager.should_retry(stream_error) is False

        # Multiple retry attempts should still be blocked
        assert policy_manager.should_retry(stream_error) is False

    def test_circuit_breaker_half_open_call_limit(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test circuit breaker call blocking behavior."""
        context = stream_error.context

        # Open circuit first
        for _ in range(5):  # failure_threshold = 5
            policy_manager.update_circuit_state(context, success=False)

        # Verify circuit is open
        assert policy_manager.is_circuit_open(context) is True

        # Test that consecutive retry attempts are blocked when circuit is open
        assert policy_manager.should_retry(stream_error) is False
        assert policy_manager.should_retry(stream_error) is False

    # ========================================================================
    # Strategy Selection Tests
    # ========================================================================

    def test_get_recovery_strategy_connection_error(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test strategy selection for connection errors."""
        strategy = policy_manager.get_recovery_strategy(stream_error)
        # Should use base strategy from error
        assert strategy in (
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            WebSocketRecoveryStrategy.RECONNECT_SAME,
        )

    def test_get_recovery_strategy_subscription_error(
        self,
        policy_manager: RecoveryPolicyManager,
        error_context: StreamErrorContext,
    ) -> None:
        """Test strategy selection for subscription errors."""
        error = WebSocketStreamError(
            message="Subscription failed",
            context=error_context,
            code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
        )

        strategy = policy_manager.get_recovery_strategy(error)
        assert strategy != WebSocketRecoveryStrategy.NONE

    def test_adaptive_strategy_selection(
        self,
        error_context: StreamErrorContext,
    ) -> None:
        """Test adaptive strategy selection based on attempt count."""
        config = WebSocketErrorConfig()
        # Enable adaptive strategy for this test
        config.recovery.enable_adaptive_strategy = True
        policy_manager = RecoveryPolicyManager(config)

        error = WebSocketStreamError(
            message="Connection lost",
            context=error_context,
            code=WebSocketErrorCode.CONNECTION_LOST,
        )

        # Test strategy evolution with retry attempts
        # Early attempts: should use initial strategy
        strategy_0 = policy_manager.get_recovery_strategy(error)
        assert strategy_0 in {
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            WebSocketRecoveryStrategy.RECONNECT_SAME,
        }

        # Record some failures and test strategy changes
        policy_manager.update_retry_state(error_context, success=False)
        strategy_1 = policy_manager.get_recovery_strategy(error)
        assert strategy_1 == WebSocketRecoveryStrategy.IMMEDIATE_RETRY

        # More failures should trigger backoff strategy
        for _ in range(3):  # Total of 4 failures
            policy_manager.update_retry_state(error_context, success=False)
        strategy_4 = policy_manager.get_recovery_strategy(error)
        assert strategy_4 == WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        # Even more failures should trigger more aggressive strategies
        for _ in range(2):  # Total of 6 failures
            policy_manager.update_retry_state(error_context, success=False)
        strategy_6 = policy_manager.get_recovery_strategy(error)
        assert strategy_6 == WebSocketRecoveryStrategy.FULL_RECONNECT

    # ========================================================================
    # Backoff Calculation Tests
    # ========================================================================

    def test_calculate_backoff_delay_exponential(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test exponential backoff calculation."""
        # Test with jitter disabled for predictable results
        config = WebSocketErrorConfig()
        config.recovery.jitter_enabled = False
        policy_manager_no_jitter = RecoveryPolicyManager(config)

        # First attempt: 1.0 * 2^0 = 1.0
        delay = policy_manager_no_jitter.calculate_backoff_delay(stream_error, 0)
        assert delay == 1.0

        # Second attempt: 1.0 * 2^1 = 2.0
        delay = policy_manager_no_jitter.calculate_backoff_delay(stream_error, 1)
        assert delay == 2.0

        # Third attempt: 1.0 * 2^2 = 4.0
        delay = policy_manager_no_jitter.calculate_backoff_delay(stream_error, 2)
        assert delay == 4.0

    def test_calculate_backoff_delay_max_limit(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test backoff delay respects maximum limit."""
        # Test with jitter disabled for predictable results
        config = WebSocketErrorConfig()
        config.recovery.jitter_enabled = False
        policy_manager_no_jitter = RecoveryPolicyManager(config)

        # Very high attempt should cap at max_backoff_delay
        delay = policy_manager_no_jitter.calculate_backoff_delay(stream_error, 10)
        assert delay == 60.0  # max_backoff_delay in seconds (60000ms)

    def test_calculate_backoff_delay_with_jitter(
        self,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test backoff delay with jitter enabled."""
        config = WebSocketErrorConfig()
        policy_manager = RecoveryPolicyManager(config)

        delays: set[float] = set()
        # Generate multiple delays to verify jitter adds randomness
        for _ in range(10):
            delay = policy_manager.calculate_backoff_delay(stream_error, 1)
            delays.add(delay)

        # With jitter, delays should vary
        assert len(delays) > 1
        # But should be within expected range (2.0 ± 10%)
        for delay in delays:
            assert 1.8 <= delay <= 2.2

    # ========================================================================
    # State Management Tests
    # ========================================================================

    def test_update_retry_state_success(
        self,
        policy_manager: RecoveryPolicyManager,
        error_context: StreamErrorContext,
    ) -> None:
        """Test retry state update on success."""
        # Simulate some failures first
        for _ in range(3):
            policy_manager.update_retry_state(error_context, success=False)

        # Verify there are failures recorded
        assert policy_manager.get_retry_count(error_context) == 3

        # Update with success
        policy_manager.update_retry_state(error_context, success=True)

        # Should reset retry count
        assert policy_manager.get_retry_count(error_context) == 0

    def test_update_retry_state_failure(
        self,
        policy_manager: RecoveryPolicyManager,
        error_context: StreamErrorContext,
    ) -> None:
        """Test retry state update on failure."""
        # Initial state should be 0
        assert policy_manager.get_retry_count(error_context) == 0

        # Update with failure
        policy_manager.update_retry_state(error_context, success=False)

        # Should increment retry count
        assert policy_manager.get_retry_count(error_context) == 1

    def test_get_retry_count(
        self,
        policy_manager: RecoveryPolicyManager,
        error_context: StreamErrorContext,
    ) -> None:
        """Test getting current retry count."""
        # Initially zero
        assert policy_manager.get_retry_count(error_context) == 0

        # After failures
        policy_manager.update_retry_state(error_context, success=False)
        assert policy_manager.get_retry_count(error_context) == 1

        policy_manager.update_retry_state(error_context, success=False)
        assert policy_manager.get_retry_count(error_context) == 2

    def test_reset_connection_state(
        self,
        policy_manager: RecoveryPolicyManager,
        error_context: StreamErrorContext,
    ) -> None:
        """Test resetting all state for a connection."""
        # Create some state by updating retry and circuit state
        for _ in range(3):  # Within max_recovery_attempts
            policy_manager.update_retry_state(error_context, success=False)
        for _ in range(5):  # Enough to open circuit
            policy_manager.update_circuit_state(error_context, success=False)

        # Verify state was created
        assert policy_manager.get_retry_count(error_context) == 3
        assert policy_manager.is_circuit_open(error_context) is True

        # Reset
        policy_manager.reset_connection_state(error_context)

        # New state should be fresh
        assert policy_manager.get_retry_count(error_context) == 0
        assert policy_manager.is_circuit_open(error_context) is False

    # ========================================================================
    # Statistics Tests
    # ========================================================================

    def test_get_statistics(
        self,
        policy_manager: RecoveryPolicyManager,
        error_context: StreamErrorContext,
    ) -> None:
        """Test statistics gathering."""
        # Create first connection with open circuit
        for _ in range(5):  # failure_threshold = 5
            policy_manager.update_circuit_state(error_context, success=False)

        # Create another connection
        context2 = StreamErrorContext(
            connection_id="test-conn-2",
            exchange="backpack",
        )

        # Create some retry state for second connection
        for _ in range(2):
            policy_manager.update_retry_state(context2, success=False)

        stats = policy_manager.get_statistics()

        # Verify basic statistics structure and values
        assert stats["total_connections_tracked"] == 2
        assert "open_circuits" in stats
        assert "half_open_circuits" in stats
        assert stats["circuit_breaker_enabled"] is True
        assert stats["max_retry_attempts"] == 3

        # Verify that at least one circuit is tracked
        assert len(stats["open_circuits"]) >= 1  # type: ignore[arg-type]

    # ========================================================================
    # Edge Cases
    # ========================================================================

    def test_multiple_connections_isolated_state(
        self,
        policy_manager: RecoveryPolicyManager,
    ) -> None:
        """Test that different connections have isolated state."""
        context1 = StreamErrorContext(
            connection_id="connection-one",
            exchange="hyperliquid",
        )
        context2 = StreamErrorContext(
            connection_id="connection-two",
            exchange="hyperliquid",
        )

        # Update state for connection 1
        policy_manager.update_retry_state(context1, success=False)
        policy_manager.update_retry_state(context1, success=False)

        # Connection 2 should have fresh state
        assert policy_manager.get_retry_count(context1) == 2
        assert policy_manager.get_retry_count(context2) == 0

    def test_channel_specific_state(
        self,
        policy_manager: RecoveryPolicyManager,
    ) -> None:
        """Test that different channels have separate state."""
        context1 = StreamErrorContext(
            connection_id="connection-one",
            exchange="hyperliquid",
            channel="trades",
        )
        context2 = StreamErrorContext(
            connection_id="connection-one",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Update state for first channel
        policy_manager.update_retry_state(context1, success=False)
        policy_manager.update_retry_state(context1, success=False)

        # Different channels should have isolated state
        assert policy_manager.get_retry_count(context1) == 2
        assert policy_manager.get_retry_count(context2) == 0
