"""Unit tests for the unified recovery policy manager.

Tests the policy decision logic without executing recovery actions.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.enums.error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.error_handling.recovery import (
    CircuitState,
    RecoveryPolicyManager,
)
from cyberdelta.apis.websocket.exceptions import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig


class TestRecoveryPolicyManager:
    """Test suite for RecoveryPolicyManager."""

    @pytest.fixture
    def config(self) -> WebSocketErrorConfig:
        """Create test configuration."""
        return WebSocketErrorConfig()

    @pytest.fixture
    def policy_manager(self, config: WebSocketErrorConfig) -> RecoveryPolicyManager:
        """Create policy manager instance."""
        return RecoveryPolicyManager(config)

    @pytest.fixture
    def error_context(self) -> StreamErrorContext:
        """Create test error context."""
        return StreamErrorContext(
            connection_id="test-conn-1",
            exchange="hyperliquid",
            channel="trades",
            is_authenticated=True,
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
        # Simulate max attempts reached
        state = policy_manager._get_or_create_state(stream_error.context)
        state.retry.attempts = 3  # Equal to max_retry_attempts

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

        # Record failures up to threshold
        for _ in range(3):  # failure_threshold = 3
            policy_manager.update_circuit_state(context, success=False)

        # Circuit should be open
        assert policy_manager.is_circuit_open(context) is True
        assert policy_manager.should_retry(stream_error) is False

    def test_circuit_breaker_closes_on_success_threshold(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test circuit breaker closes after success threshold in half-open state."""
        context = stream_error.context
        state = policy_manager._get_or_create_state(context)

        # Open circuit
        state.circuit_breaker.state = CircuitState.HALF_OPEN

        # Record successes
        for _ in range(2):  # success_threshold = 2
            policy_manager.update_circuit_state(context, success=True)

        # Circuit should be closed
        assert state.circuit_breaker.state == CircuitState.CLOSED
        assert policy_manager.is_circuit_open(context) is False

    def test_circuit_breaker_timeout_transition(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test circuit breaker transitions to half-open after timeout."""
        context = stream_error.context
        state = policy_manager._get_or_create_state(context)

        # Open circuit
        state.circuit_breaker.open_circuit()

        # Mock timeout expiration
        with patch.object(state.circuit_breaker, "is_timeout_expired", return_value=True):
            # Should allow retry in half-open state
            assert policy_manager.should_retry(stream_error) is True
            assert state.circuit_breaker.state == CircuitState.HALF_OPEN

    def test_circuit_breaker_half_open_call_limit(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test half-open state call limit."""
        context = stream_error.context
        state = policy_manager._get_or_create_state(context)

        # Set to half-open
        state.circuit_breaker.state = CircuitState.HALF_OPEN

        # First call allowed
        assert policy_manager.should_retry(stream_error) is True
        assert state.circuit_breaker.half_open_calls == 1

        # Second call blocked (half_open_max_calls = 1)
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
        policy_manager = RecoveryPolicyManager(config)

        error = WebSocketStreamError(
            message="Connection lost",
            context=error_context,
            code=WebSocketErrorCode.CONNECTION_LOST,
        )

        state = policy_manager._get_or_create_state(error_context)

        # Early attempts: immediate retry
        state.retry.attempts = 1
        strategy = policy_manager.get_recovery_strategy(error)
        assert strategy == WebSocketRecoveryStrategy.IMMEDIATE_RETRY

        # Mid attempts: exponential backoff
        state.retry.attempts = 4
        strategy = policy_manager.get_recovery_strategy(error)
        assert strategy == WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        # Late attempts: full reconnect
        state.retry.attempts = 6
        strategy = policy_manager.get_recovery_strategy(error)
        assert strategy == WebSocketRecoveryStrategy.FULL_RECONNECT

        # Final attempts: circuit breaker
        state.retry.attempts = 9
        strategy = policy_manager.get_recovery_strategy(error)
        assert strategy == WebSocketRecoveryStrategy.CIRCUIT_BREAKER

    # ========================================================================
    # Backoff Calculation Tests
    # ========================================================================

    def test_calculate_backoff_delay_exponential(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test exponential backoff calculation."""
        # First attempt: 1.0 * 2^0 = 1.0
        delay = policy_manager.calculate_backoff_delay(stream_error, 0)
        assert delay == 1.0

        # Second attempt: 1.0 * 2^1 = 2.0
        delay = policy_manager.calculate_backoff_delay(stream_error, 1)
        assert delay == 2.0

        # Third attempt: 1.0 * 2^2 = 4.0
        delay = policy_manager.calculate_backoff_delay(stream_error, 2)
        assert delay == 4.0

    def test_calculate_backoff_delay_max_limit(
        self,
        policy_manager: RecoveryPolicyManager,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test backoff delay respects maximum limit."""
        # Very high attempt should cap at max_backoff_delay
        delay = policy_manager.calculate_backoff_delay(stream_error, 10)
        assert delay == 10.0  # max_backoff_delay

    def test_calculate_backoff_delay_with_jitter(
        self,
        stream_error: WebSocketStreamError,
    ) -> None:
        """Test backoff delay with jitter enabled."""
        config = WebSocketErrorConfig()
        policy_manager = RecoveryPolicyManager(config)

        delays = set()
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
        state = policy_manager._get_or_create_state(error_context)

        # Simulate some failures
        state.retry.attempts = 3
        state.retry.consecutive_failures = 3

        # Update with success
        policy_manager.update_retry_state(error_context, success=True)

        # Should reset
        assert state.retry.attempts == 0
        assert state.retry.consecutive_failures == 0
        assert state.retry.last_success is not None

    def test_update_retry_state_failure(
        self,
        policy_manager: RecoveryPolicyManager,
        error_context: StreamErrorContext,
    ) -> None:
        """Test retry state update on failure."""
        state = policy_manager._get_or_create_state(error_context)

        # Update with failure
        policy_manager.update_retry_state(error_context, success=False)

        assert state.retry.attempts == 1
        assert state.retry.consecutive_failures == 1
        assert state.retry.last_attempt is not None

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
        # Create some state
        state = policy_manager._get_or_create_state(error_context)
        state.retry.attempts = 5
        state.circuit_breaker.failure_count = 3

        # Reset
        policy_manager.reset_connection_state(error_context)

        # State should be removed
        key = policy_manager._get_recovery_key(error_context)
        assert key not in policy_manager._states

        # New state should be fresh
        new_state = policy_manager._get_or_create_state(error_context)
        assert new_state.retry.attempts == 0
        assert new_state.circuit_breaker.failure_count == 0

    # ========================================================================
    # Statistics Tests
    # ========================================================================

    def test_get_statistics(
        self,
        policy_manager: RecoveryPolicyManager,
        error_context: StreamErrorContext,
    ) -> None:
        """Test statistics gathering."""
        # Create some state
        state1 = policy_manager._get_or_create_state(error_context)
        state1.circuit_breaker.state = CircuitState.OPEN

        # Create another connection
        context2 = StreamErrorContext(
            connection_id="test-conn-2",
            exchange="backpack",
        )
        state2 = policy_manager._get_or_create_state(context2)
        state2.circuit_breaker.state = CircuitState.HALF_OPEN

        stats = policy_manager.get_statistics()

        assert stats["total_connections_tracked"] == 2
        assert len(stats["open_circuits"]) == 1  # type: ignore[arg-type]
        assert len(stats["half_open_circuits"]) == 1  # type: ignore[arg-type]
        assert stats["circuit_breaker_enabled"] is True
        assert stats["max_retry_attempts"] == 3

    # ========================================================================
    # Edge Cases
    # ========================================================================

    def test_multiple_connections_isolated_state(
        self,
        policy_manager: RecoveryPolicyManager,
    ) -> None:
        """Test that different connections have isolated state."""
        context1 = StreamErrorContext(
            connection_id="conn-1",
            exchange="hyperliquid",
        )
        context2 = StreamErrorContext(
            connection_id="conn-2",
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
            connection_id="conn-1",
            exchange="hyperliquid",
            channel="trades",
        )
        context2 = StreamErrorContext(
            connection_id="conn-1",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Different channels should have different keys
        key1 = policy_manager._get_recovery_key(context1)
        key2 = policy_manager._get_recovery_key(context2)
        assert key1 != key2
