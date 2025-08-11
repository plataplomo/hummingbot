"""Circuit breaker failure tracking and analytics.

This module handles failure recording, success tracking, and failure
categorization for circuit breaker analytics.
"""

from __future__ import annotations

from datetime import UTC, datetime

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.safety.circuit_breaker import CircuitBreakerState, FailureType


logger = get_logger(__name__)


class FailureTracker:
    """Tracks failures and successes for circuit breaker analytics.

    This component handles:
    - Failure recording and categorization
    - Success tracking and streak management
    - Failure history for analytics
    - Statistics calculation

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL limits from AppSettings, NO hardcoded values
    - Uses structured logging only
    - Explicit failure categorization
    """

    def __init__(self, breaker_name: str, config: AppSettings) -> None:
        """Initialize failure tracker with configuration.

        Args:
            breaker_name: Name of the circuit breaker for logging
            config: Application settings containing circuit breaker configuration
        """
        self._breaker_name = breaker_name
        self._cb_config = config.safety_systems.circuit_breakers

        # Extract configuration - NO hardcoded defaults
        self._failure_threshold = self._cb_config.global_consecutive_failures
        self._failure_history_limit = self._cb_config.failure_history_limit

        # Tracking counters
        self._failure_count = 0
        self._consecutive_failures = 0
        self._success_count = 0
        self._total_calls = 0

        # Failure tracking for analytics
        self._failure_history: list[dict[str, str | int]] = []

        logger.debug(
            "failure_tracker_initialized",
            breaker_name=self._breaker_name,
            failure_threshold=self._failure_threshold,
            failure_history_limit=self._failure_history_limit,
        )

    async def record_success(self, operation_name: str, current_state: CircuitBreakerState) -> bool:
        """Record successful operation.

        Args:
            operation_name: Name of successful operation
            current_state: Current circuit breaker state

        Returns:
            True if consecutive failures were reset

        IMPORTANT: Following CODING_STANDARDS.md:
        - Updates tracking based on configured thresholds
        - Structured logging for monitoring
        """
        self._success_count += 1
        self._total_calls += 1
        consecutive_failures_reset = False

        if current_state == CircuitBreakerState.CLOSED and self._consecutive_failures > 0:
            logger.debug(
                "circuit_breaker_failure_streak_reset",
                breaker_name=self._breaker_name,
                operation=operation_name,
                previous_consecutive_failures=self._consecutive_failures,
            )
            self._consecutive_failures = 0
            consecutive_failures_reset = True

        logger.debug(
            "circuit_breaker_success_recorded",
            breaker_name=self._breaker_name,
            operation=operation_name,
            state=current_state.value,
            total_successes=self._success_count,
            consecutive_failures=self._consecutive_failures,
        )

        return consecutive_failures_reset

    async def record_failure(self, operation_name: str, error: Exception) -> tuple[bool, bool]:
        """Record failed operation.

        Args:
            operation_name: Name of failed operation
            error: Exception that caused the failure

        Returns:
            Tuple of (should_trip_closed, should_trip_half_open)

        IMPORTANT: Following CODING_STANDARDS.md:
        - Failure categorization for analytics
        - Memory management with configured limits
        """
        self._failure_count += 1
        self._consecutive_failures += 1
        self._total_calls += 1
        failure_time = datetime.now(UTC)

        # Categorize failure type
        failure_type = self._categorize_failure(error)

        # Store failure for analytics
        failure_record = {
            "timestamp": failure_time.isoformat(),
            "operation": operation_name,
            "error_type": type(error).__name__,
            "failure_type": failure_type.value,
            "error_message": str(error),
            "consecutive_count": self._consecutive_failures,
        }
        self._failure_history.append(failure_record)

        # Keep only recent failures for memory management
        if len(self._failure_history) > self._failure_history_limit:
            self._failure_history = self._failure_history[-self._failure_history_limit :]

        logger.warning(
            "circuit_breaker_failure_recorded",
            breaker_name=self._breaker_name,
            operation=operation_name,
            error_type=type(error).__name__,
            failure_type=failure_type.value,
            consecutive_failures=self._consecutive_failures,
            failure_threshold=self._failure_threshold,
        )

        # Determine if circuit breaker should trip
        should_trip_closed = self._consecutive_failures >= self._failure_threshold
        should_trip_half_open = True  # Half-open always trips on any failure

        return should_trip_closed, should_trip_half_open

    def _categorize_failure(self, error: Exception) -> FailureType:
        """Categorize failure type for analytics.

        Args:
            error: Exception to categorize

        Returns:
            FailureType enum value

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit failure categorization
        - NO assumptions about error types
        """
        error_name = type(error).__name__.lower()
        error_message = str(error).lower()

        if "timeout" in error_name or "timeout" in error_message:
            return FailureType.TIMEOUT
        if "network" in error_message or "connection" in error_message:
            return FailureType.NETWORK_ERROR
        if "rate" in error_message and "limit" in error_message:
            return FailureType.RATE_LIMIT
        if "auth" in error_message or "credential" in error_message:
            return FailureType.AUTHENTICATION_ERROR
        if "validation" in error_name or "value" in error_name:
            return FailureType.VALIDATION_ERROR
        if "execution" in error_message or "order" in error_message:
            return FailureType.EXECUTION_ERROR
        return FailureType.API_ERROR

    def get_success_rate(self) -> float:
        """Calculate current success rate.

        Returns:
            Success rate as float between 0.0 and 1.0

        IMPORTANT: Following CODING_STANDARDS.md:
        - Safe division handling
        """
        return self._success_count / self._total_calls if self._total_calls > 0 else 0.0

    def get_failure_count(self) -> int:
        """Get total failure count.

        Returns:
            Total number of failures recorded
        """
        return self._failure_count

    def get_consecutive_failures(self) -> int:
        """Get consecutive failure count.

        Returns:
            Number of consecutive failures
        """
        return self._consecutive_failures

    def get_success_count(self) -> int:
        """Get total success count.

        Returns:
            Total number of successes recorded
        """
        return self._success_count

    def get_total_calls(self) -> int:
        """Get total call count.

        Returns:
            Total number of calls tracked
        """
        return self._total_calls

    def get_failure_history_count(self) -> int:
        """Get failure history count.

        Returns:
            Number of failures in history
        """
        return len(self._failure_history)

    def reset_tracking(self) -> None:
        """Reset all tracking counters.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Complete tracking reset for manual recovery
        """
        logger.info(
            "failure_tracker_reset",
            breaker_name=self._breaker_name,
            previous_failure_count=self._failure_count,
            previous_consecutive_failures=self._consecutive_failures,
        )

        self._failure_count = 0
        self._consecutive_failures = 0
        self._success_count = 0
        self._total_calls = 0
        self._failure_history.clear()

    def get_tracking_stats(self) -> dict[str, object]:
        """Get tracking statistics for monitoring.

        Returns:
            Dictionary with tracking statistics

        IMPORTANT: Following CODING_STANDARDS.md:
        - Structured data for monitoring
        - Includes configuration context
        """
        return {
            "total_calls": self._total_calls,
            "success_count": self._success_count,
            "failure_count": self._failure_count,
            "consecutive_failures": self._consecutive_failures,
            "success_rate": self.get_success_rate(),
            "failure_history_count": len(self._failure_history),
            "configuration": {
                "failure_threshold": self._failure_threshold,
                "failure_history_limit": self._failure_history_limit,
            },
        }
