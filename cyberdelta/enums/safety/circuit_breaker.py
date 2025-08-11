"""Circuit breaker enums."""

from enum import Enum


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
