"""Recovery models for WebSocket error handling.

This module contains Pydantic models for recovery state tracking,
circuit breaker patterns, and recovery actions in the WebSocket system.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket import CircuitState


class CircuitBreakerState(BaseModel):
    """Circuit breaker state for a connection."""

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    opened_at: datetime | None = None
    last_failure: datetime | None = None
    last_success: datetime | None = None
    half_open_calls: int = 0

    def should_open(self, failure_threshold: int) -> bool:
        """Check if circuit should open based on failures.

        Args:
            failure_threshold: Number of failures to trigger open

        Returns:
            True if circuit should open
        """
        return self.failure_count >= failure_threshold

    def should_close(self, success_threshold: int) -> bool:
        """Check if circuit should close based on successes.

        Args:
            success_threshold: Number of successes to close circuit

        Returns:
            True if circuit should close
        """
        return self.state == CircuitState.HALF_OPEN and self.success_count >= success_threshold

    def is_timeout_expired(self, timeout_seconds: float) -> bool:
        """Check if circuit breaker timeout has expired.

        Args:
            timeout_seconds: Timeout duration in seconds

        Returns:
            True if timeout has expired
        """
        if not self.opened_at or self.state != CircuitState.OPEN:
            return False
        elapsed = (datetime.now(UTC) - self.opened_at).total_seconds()
        return elapsed >= timeout_seconds

    def record_failure(self) -> None:
        """Record a failure event."""
        self.failure_count += 1
        self.last_failure = datetime.now(UTC)
        self.success_count = 0  # Reset success count on failure

    def record_success(self) -> None:
        """Record a success event."""
        self.success_count += 1
        self.last_success = datetime.now(UTC)
        self.failure_count = 0  # Reset failure count on success

    def open_circuit(self) -> None:
        """Open the circuit breaker."""
        self.state = CircuitState.OPEN
        self.opened_at = datetime.now(UTC)
        self.half_open_calls = 0

    def close_circuit(self) -> None:
        """Close the circuit breaker."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.half_open_calls = 0

    def transition_to_half_open(self) -> None:
        """Transition to half-open state for testing."""
        self.state = CircuitState.HALF_OPEN
        self.half_open_calls = 0
        self.success_count = 0
        self.failure_count = 0


class RetryState(BaseModel):
    """Retry tracking for a connection."""

    attempts: int = 0
    last_attempt: datetime | None = None
    last_success: datetime | None = None
    current_backoff_delay: float = 1.0
    consecutive_failures: int = 0

    def increment_attempts(self) -> None:
        """Increment retry attempts."""
        self.attempts += 1
        self.last_attempt = datetime.now(UTC)

    def reset(self) -> None:
        """Reset retry state after success."""
        self.attempts = 0
        self.consecutive_failures = 0
        self.last_success = datetime.now(UTC)
        self.current_backoff_delay = 1.0

    def record_failure(self) -> None:
        """Record a retry failure."""
        self.consecutive_failures += 1

    def update_backoff(self, new_delay: float) -> None:
        """Update the current backoff delay.

        Args:
            new_delay: New backoff delay in seconds
        """
        self.current_backoff_delay = new_delay


class RecoveryState(BaseModel):
    """Complete recovery state for a connection."""

    connection_id: str
    exchange: str
    circuit_breaker: CircuitBreakerState = Field(default_factory=CircuitBreakerState)
    retry: RetryState = Field(default_factory=RetryState)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_updated: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def update_timestamp(self) -> None:
        """Update the last modified timestamp."""
        self.last_updated = datetime.now(UTC)


class RecoveryAction(BaseModel):
    """Represents a recovery action to be taken."""

    strategy: WebSocketRecoveryStrategy
    delay_ms: int = Field(default=0, ge=0)
    should_reconnect: bool = False
    should_resubscribe: bool = False
    should_clear_state: bool = False
    should_degrade_service: bool = False
    max_retries: int = Field(default=3, ge=0)
    metadata: dict[str, str] = Field(default_factory=dict)


class RecoveryResult(BaseModel):
    """Result of a recovery attempt."""

    success: bool
    strategy_used: WebSocketRecoveryStrategy
    action_taken: str
    time_elapsed_ms: int = Field(default=0, ge=0)
    error_message: str | None = None
    should_continue: bool = True
    metadata: dict[str, str] = Field(default_factory=dict)
