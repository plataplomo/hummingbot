"""Circuit breaker exception model."""

from cyberdelta.enums.safety.circuit_breaker import CircuitBreakerState


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
