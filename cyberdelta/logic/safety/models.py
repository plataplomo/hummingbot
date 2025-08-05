"""Shared types and constants for circuit breaker system.

This module contains enums, exceptions, and constants used across
the circuit breaker implementation.
"""

from __future__ import annotations

from enum import Enum


# Health ratio thresholds for system status determination
HEALTH_RATIO_DEGRADED_THRESHOLD = 0.8
HEALTH_RATIO_IMPAIRED_THRESHOLD = 0.5


class CircuitBreakerState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Blocking all requests
    HALF_OPEN = "half_open"  # Testing recovery


class FailureType(Enum):
    """Types of failures that can trigger circuit breakers."""

    API_ERROR = "api_error"
    TIMEOUT = "timeout"
    VALIDATION_ERROR = "validation_error"
    EXECUTION_ERROR = "execution_error"
    NETWORK_ERROR = "network_error"
    RATE_LIMIT = "rate_limit"
    AUTHENTICATION_ERROR = "auth_error"


class CircuitBreakerViolationError(Exception):
    """Exception raised when circuit breaker is open."""

    def __init__(self, breaker_name: str, state: CircuitBreakerState, message: str) -> None:
        """Initialize circuit breaker violation error.

        Args:
            breaker_name: Name of the circuit breaker
            state: Current state of the circuit breaker
            message: Error message
        """
        self.breaker_name = breaker_name
        self.state = state
        self.message = message
        super().__init__(f"Circuit breaker '{breaker_name}' is {state.value}: {message}")
