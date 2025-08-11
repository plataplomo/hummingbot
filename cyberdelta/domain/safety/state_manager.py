"""Circuit breaker state management.

This module handles state transitions for circuit breakers including
cooldown periods and recovery logic.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.safety.circuit_breaker import CircuitBreakerState


logger = get_logger(__name__)


class StateManager:
    """Manages circuit breaker state transitions with config-driven behavior.

    This component handles:
    - State transitions between CLOSED/OPEN/HALF_OPEN
    - Cooldown period enforcement
    - Half-open recovery testing
    - State change logging

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL timing from AppSettings, NO hardcoded values
    - Uses structured logging only
    - Explicit state management
    """

    def __init__(self, breaker_name: str, config: AppSettings) -> None:
        """Initialize state manager with configuration.

        Args:
            breaker_name: Name of the circuit breaker for logging
            config: Application settings containing circuit breaker configuration
        """
        self._breaker_name = breaker_name
        self._cb_config = config.safety_systems.circuit_breakers

        # Extract timing configuration - NO hardcoded defaults
        self._cooldown_period = timedelta(seconds=self._cb_config.cooldown_period_seconds)
        self._half_open_max_calls = self._cb_config.half_open_max_calls
        self._recovery_threshold = self._cb_config.recovery_threshold

        # State tracking
        self._state = CircuitBreakerState.CLOSED
        self._last_failure_time: datetime | None = None
        self._half_open_calls = 0
        self._half_open_successes = 0

        logger.debug(
            "state_manager_initialized",
            breaker_name=self._breaker_name,
            cooldown_period_sec=self._cb_config.cooldown_period_seconds,
            half_open_max_calls=self._half_open_max_calls,
            recovery_threshold=float(self._recovery_threshold),
        )

    async def check_state_transition(self) -> None:
        """Check if circuit breaker should transition states.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured cooldown period
        - State transitions based on configuration
        """
        if self._state == CircuitBreakerState.OPEN:
            # Check if cooldown period has passed
            if (
                self._last_failure_time
                and datetime.now(UTC) - self._last_failure_time >= self._cooldown_period
            ):
                logger.info(
                    "circuit_breaker_transitioning_to_half_open",
                    breaker_name=self._breaker_name,
                    cooldown_elapsed=True,
                    last_failure_time=self._last_failure_time.isoformat(),
                )

                self._state = CircuitBreakerState.HALF_OPEN
                self._half_open_calls = 0
                self._half_open_successes = 0

        elif (
            self._state == CircuitBreakerState.HALF_OPEN
            and self._half_open_calls >= self._half_open_max_calls
        ):
            success_ratio = self._half_open_successes / self._half_open_calls

            if success_ratio >= self._recovery_threshold:
                logger.info(
                    "circuit_breaker_recovered",
                    breaker_name=self._breaker_name,
                    success_ratio=success_ratio,
                    required_ratio=float(self._recovery_threshold),
                    test_calls=self._half_open_calls,
                )

                self._state = CircuitBreakerState.CLOSED
            else:
                logger.warning(
                    "circuit_breaker_recovery_failed",
                    breaker_name=self._breaker_name,
                    success_ratio=success_ratio,
                    required_ratio=float(self._recovery_threshold),
                    test_calls=self._half_open_calls,
                )

                self._state = CircuitBreakerState.OPEN
                self._last_failure_time = datetime.now(UTC)

    def get_state(self) -> CircuitBreakerState:
        """Get current circuit breaker state.

        Returns:
            Current circuit breaker state
        """
        return self._state

    def set_state_open(self, failure_time: datetime) -> None:
        """Transition circuit breaker to OPEN state.

        Args:
            failure_time: Time when the failure occurred

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit state setting with logging
        """
        self._state = CircuitBreakerState.OPEN
        self._last_failure_time = failure_time

        logger.error(
            "circuit_breaker_state_set_to_open",
            breaker_name=self._breaker_name,
            failure_time=failure_time.isoformat(),
        )

    def is_half_open(self) -> bool:
        """Check if circuit breaker is in half-open state.

        Returns:
            True if in half-open state
        """
        return self._state == CircuitBreakerState.HALF_OPEN

    def is_open(self) -> bool:
        """Check if circuit breaker is in open state.

        Returns:
            True if in open state
        """
        return self._state == CircuitBreakerState.OPEN

    def is_closed(self) -> bool:
        """Check if circuit breaker is in closed state.

        Returns:
            True if in closed state
        """
        return self._state == CircuitBreakerState.CLOSED

    def track_half_open_call(self) -> None:
        """Track a call made during half-open state.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit call tracking for recovery testing
        """
        if self._state == CircuitBreakerState.HALF_OPEN:
            self._half_open_calls += 1

    def track_half_open_success(self) -> None:
        """Track a successful call during half-open state.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Success tracking for recovery calculation
        """
        if self._state == CircuitBreakerState.HALF_OPEN:
            self._half_open_successes += 1

    def reset_state(self) -> None:
        """Reset state manager to initial state.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Complete state reset for manual recovery
        """
        logger.info(
            "state_manager_reset", breaker_name=self._breaker_name, previous_state=self._state.value
        )

        self._state = CircuitBreakerState.CLOSED
        self._last_failure_time = None
        self._half_open_calls = 0
        self._half_open_successes = 0

    def get_state_info(self) -> dict[str, object]:
        """Get current state information for monitoring.

        Returns:
            Dictionary with state information

        IMPORTANT: Following CODING_STANDARDS.md:
        - Structured data for monitoring
        - Includes configuration context
        """
        return {
            "state": self._state.value,
            "last_failure_time": self._last_failure_time.isoformat()
            if self._last_failure_time
            else None,
            "half_open_calls": self._half_open_calls,
            "half_open_successes": self._half_open_successes,
            "configuration": {
                "cooldown_period_sec": self._cb_config.cooldown_period_seconds,
                "half_open_max_calls": self._half_open_max_calls,
                "recovery_threshold": float(self._recovery_threshold),
            },
        }
