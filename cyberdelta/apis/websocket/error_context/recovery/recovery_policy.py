"""WebSocket Recovery Policy Manager.

This module implements the policy layer of the recovery system,
responsible for making decisions about WHAT recovery actions to take
and WHEN to take them, without executing the actions.
"""

from __future__ import annotations

import secrets
from typing import TYPE_CHECKING

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket import CircuitState, WebSocketErrorCode
from cyberdelta.apis.models.websocket.recovery import (
    RecoveryState,
)
from cyberdelta.apis.exceptions.websocket import WebSocketStreamError
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.models.websocket import StreamErrorContext


# ============================================================================
# Recovery Policy Manager
# ============================================================================


class RecoveryPolicyManager:
    """Manages recovery policies and decisions.

    This class is responsible for making policy decisions about error recovery
    without executing the recovery actions. It maintains the state necessary
    for making consistent decisions across the system.
    """

    def __init__(self, config: WebSocketErrorConfig) -> None:
        """Initialize the policy manager.

        Args:
            config: WebSocket error configuration
        """
        self.config = config
        self.logger = get_logger("RecoveryPolicyManager")

        # Recovery state per connection key
        self._states: dict[str, RecoveryState] = {}

    def _get_recovery_key(self, context: StreamErrorContext) -> str:
        """Generate a unique key for recovery tracking.

        Args:
            context: Error context

        Returns:
            Unique key for the connection/channel combination
        """
        base_key = f"{context.exchange}:{context.connection_id}"
        if context.channel:
            return f"{base_key}:{context.channel}"
        return base_key

    def _get_or_create_state(self, context: StreamErrorContext) -> RecoveryState:
        """Get or create recovery state for a connection.

        Args:
            context: Error context

        Returns:
            Recovery state for the connection
        """
        key = self._get_recovery_key(context)
        if key not in self._states:
            self._states[key] = RecoveryState(
                connection_id=context.connection_id,
                exchange=context.exchange,
            )
        return self._states[key]

    def should_retry(self, error: WebSocketStreamError) -> bool:
        """Determine if an error should be retried.

        Args:
            error: The WebSocket error

        Returns:
            True if the error should be retried
        """
        # Check if error is retryable
        if not error.is_retryable:
            self.logger.debug(
                "Error not retryable",
                error_code=error.code.name,
                connection_id=error.context.connection_id,
            )
            return False

        state = self._get_or_create_state(error.context)

        # Check circuit breaker
        if self.config.recovery.circuit_breaker_enabled:
            if state.circuit_breaker.state == CircuitState.OPEN:
                # Check if timeout expired for transition to half-open
                timeout_sec = self.config.recovery.circuit_breaker_timeout_ms / 1000.0
                if state.circuit_breaker.is_timeout_expired(timeout_sec):
                    state.circuit_breaker.transition_to_half_open()
                    self.logger.info(
                        "Circuit breaker transitioning to half-open",
                        connection_id=error.context.connection_id,
                    )
                else:
                    self.logger.warning(
                        "Circuit breaker is open, rejecting retry",
                        connection_id=error.context.connection_id,
                    )
                    return False
            elif state.circuit_breaker.state == CircuitState.HALF_OPEN:
                # Allow limited calls in half-open state
                half_open_limit = self.config.recovery.circuit_breaker_half_open_requests
                if state.circuit_breaker.half_open_calls >= half_open_limit:
                    self.logger.warning(
                        "Half-open call limit reached",
                        connection_id=error.context.connection_id,
                    )
                    return False
                state.circuit_breaker.half_open_calls += 1

        # Check retry limits
        if state.retry.attempts >= self.config.recovery.max_recovery_attempts:
            self.logger.error(
                "Max retry attempts exceeded",
                attempts=state.retry.attempts,
                max_attempts=self.config.recovery.max_recovery_attempts,
                connection_id=error.context.connection_id,
            )
            # Open circuit breaker on max retries
            if self.config.recovery.circuit_breaker_enabled:
                state.circuit_breaker.open_circuit()
            return False

        return True

    def get_recovery_strategy(self, error: WebSocketStreamError) -> WebSocketRecoveryStrategy:
        """Select appropriate recovery strategy for an error.

        Args:
            error: The WebSocket error

        Returns:
            Recovery strategy to apply
        """
        # Use error's suggested strategy if available
        base_strategy = error.get_recovery_strategy()

        # Apply policy overrides based on error type
        if self.config.recovery.enable_adaptive_strategy:
            state = self._get_or_create_state(error.context)
            return self._select_adaptive_strategy(error, state, base_strategy)

        return self._apply_policy_overrides(error, base_strategy)

    def _select_adaptive_strategy(
        self,
        error: WebSocketStreamError,
        state: RecoveryState,
        base_strategy: WebSocketRecoveryStrategy,
    ) -> WebSocketRecoveryStrategy:
        """Select strategy adaptively based on failure patterns.

        Args:
            error: The WebSocket error
            state: Current recovery state
            base_strategy: Base strategy from error

        Returns:
            Adapted recovery strategy
        """
        # Early attempts: Try simple strategies
        early_attempt_threshold = 3
        if state.retry.attempts < early_attempt_threshold:
            if error.code in {
                WebSocketErrorCode.CONNECTION_TIMEOUT,
                WebSocketErrorCode.CONNECTION_LOST,
            }:
                return WebSocketRecoveryStrategy.IMMEDIATE_RETRY
            return base_strategy

        # Mid attempts: Use backoff strategies
        mid_attempt_threshold = 5
        if state.retry.attempts < mid_attempt_threshold:
            return WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

        # Late attempts: Try more aggressive strategies
        late_attempt_threshold = 8
        if state.retry.attempts < late_attempt_threshold:
            if error.code in {
                WebSocketErrorCode.CONNECTION_LOST,
                WebSocketErrorCode.CONNECTION_TIMEOUT,
                WebSocketErrorCode.CONNECTION_RESET,
                WebSocketErrorCode.CONNECTION_REFUSED,
            }:
                return WebSocketRecoveryStrategy.FULL_RECONNECT
            return WebSocketRecoveryStrategy.RESUBSCRIBE_ALL

        # Final attempts: Circuit breaker
        return WebSocketRecoveryStrategy.CIRCUIT_BREAKER

    def _apply_policy_overrides(
        self,
        error: WebSocketStreamError,
        base_strategy: WebSocketRecoveryStrategy,
    ) -> WebSocketRecoveryStrategy:
        """Apply policy configuration overrides to strategy selection.

        Args:
            error: The WebSocket error
            base_strategy: Base strategy from error

        Returns:
            Policy-adjusted strategy
        """
        # Override for connection errors
        if (
            self.config.recovery.prefer_reconnect_for_connection_errors
            and error.code
            in {
                WebSocketErrorCode.CONNECTION_LOST,
                WebSocketErrorCode.CONNECTION_TIMEOUT,
                WebSocketErrorCode.CONNECTION_RESET,
                WebSocketErrorCode.CONNECTION_REFUSED,
                WebSocketErrorCode.CONNECTION_FAILED,
            }
            and base_strategy
            in {
                WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
                WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            }
        ):
            return WebSocketRecoveryStrategy.RECONNECT_SAME

        # Override for subscription errors
        if (
            self.config.recovery.prefer_resubscribe_for_subscription_errors
            and error.code
            in {
                WebSocketErrorCode.SUBSCRIPTION_FAILED,
                WebSocketErrorCode.SUBSCRIPTION_NOT_FOUND,
                WebSocketErrorCode.SUBSCRIPTION_UNAUTHORIZED,
            }
            and base_strategy == WebSocketRecoveryStrategy.IMMEDIATE_RETRY
        ):
            return WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE

        return base_strategy

    def calculate_backoff_delay(self, error: WebSocketStreamError, attempt: int) -> float:
        """Calculate backoff delay for retry attempt.

        Args:
            error: The WebSocket error
            attempt: Current attempt number

        Returns:
            Delay in seconds before next retry
        """
        state = self._get_or_create_state(error.context)

        # Use exponential backoff
        base_delay = self.config.recovery.initial_backoff_ms / 1000.0
        multiplier = self.config.recovery.backoff_multiplier
        delay = min(
            base_delay * (multiplier**attempt),
            self.config.recovery.max_backoff_ms / 1000.0,
        )

        # Add jitter if configured
        if self.config.recovery.jitter_enabled:
            jitter_range = delay * self.config.recovery.jitter_factor
            jitter = secrets.SystemRandom().uniform(-jitter_range, jitter_range)
            delay = max(0.1, delay + jitter)  # Ensure minimum 100ms delay

        state.retry.update_backoff(delay)
        state.update_timestamp()

        self.logger.debug(
            "Calculated backoff delay",
            attempt=attempt,
            delay=delay,
            connection_id=error.context.connection_id,
        )

        return delay

    def is_circuit_open(self, context: StreamErrorContext) -> bool:
        """Check if circuit breaker is open for a connection.

        Args:
            context: Error context

        Returns:
            True if circuit is open
        """
        if not self.config.recovery.circuit_breaker_enabled:
            return False

        state = self._get_or_create_state(context)
        return state.circuit_breaker.state == CircuitState.OPEN

    def update_circuit_state(self, context: StreamErrorContext, success: bool) -> None:
        """Update circuit breaker state based on operation outcome.

        Args:
            context: Error context
            success: Whether the operation was successful
        """
        if not self.config.recovery.circuit_breaker_enabled:
            return

        state = self._get_or_create_state(context)
        circuit = state.circuit_breaker

        if success:
            circuit.record_success()

            # Check for circuit close conditions
            if circuit.state == CircuitState.HALF_OPEN:
                if circuit.should_close(self.config.recovery.circuit_breaker_success_threshold):
                    circuit.close_circuit()
                    self.logger.info(
                        "Circuit breaker closed",
                        connection_id=context.connection_id,
                    )
            elif circuit.state == CircuitState.CLOSED:
                # Reset failure count on success in closed state
                circuit.failure_count = 0
        else:
            circuit.record_failure()

            # Check for circuit open conditions
            if circuit.state == CircuitState.CLOSED:
                if circuit.should_open(self.config.recovery.circuit_breaker_threshold):
                    circuit.open_circuit()
                    self.logger.warning(
                        "Circuit breaker opened",
                        connection_id=context.connection_id,
                        failures=circuit.failure_count,
                    )
            elif circuit.state == CircuitState.HALF_OPEN:
                # Single failure in half-open returns to open
                circuit.open_circuit()
                self.logger.warning(
                    "Circuit breaker reopened from half-open",
                    connection_id=context.connection_id,
                )

        state.update_timestamp()

    def update_retry_state(self, context: StreamErrorContext, success: bool) -> None:
        """Update retry state based on operation outcome.

        Args:
            context: Error context
            success: Whether the operation was successful
        """
        state = self._get_or_create_state(context)

        if success:
            state.retry.reset()
            self.logger.debug(
                "Retry state reset after success",
                connection_id=context.connection_id,
            )
        else:
            state.retry.increment_attempts()
            state.retry.record_failure()
            self.logger.debug(
                "Retry attempt recorded",
                attempts=state.retry.attempts,
                connection_id=context.connection_id,
            )

        state.update_timestamp()

    def get_retry_count(self, context: StreamErrorContext) -> int:
        """Get current retry count for a connection.

        Args:
            context: Error context

        Returns:
            Number of retry attempts
        """
        state = self._get_or_create_state(context)
        return state.retry.attempts

    def reset_connection_state(self, context: StreamErrorContext) -> None:
        """Reset all recovery state for a connection.

        Args:
            context: Error context
        """
        key = self._get_recovery_key(context)
        if key in self._states:
            self.logger.info(
                "Resetting connection recovery state",
                connection_id=context.connection_id,
            )
            del self._states[key]

    def get_statistics(self) -> dict[str, object]:
        """Get policy manager statistics.

        Returns:
            Dictionary of statistics
        """
        open_circuits = [
            key
            for key, state in self._states.items()
            if state.circuit_breaker.state == CircuitState.OPEN
        ]

        half_open_circuits = [
            key
            for key, state in self._states.items()
            if state.circuit_breaker.state == CircuitState.HALF_OPEN
        ]

        return {
            "total_connections_tracked": len(self._states),
            "open_circuits": open_circuits,
            "half_open_circuits": half_open_circuits,
            "circuit_breaker_enabled": self.config.recovery.circuit_breaker_enabled,
            "max_retry_attempts": self.config.recovery.max_recovery_attempts,
        }
