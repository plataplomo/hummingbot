"""Type-safe circuit breaker implementation for resilience."""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic import Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import CircuitBreakerThresholdError
from cyberdelta.core.portfolio.portfolio_types.resilience_types import (
    ResilienceError,
    ResilienceErrorType,
    ResilienceMetrics,
    ResilienceResult,
)


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


logger = get_logger(__name__)

T = TypeVar("T", bound=object)


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if recovered


# Type-preserving factory function
def _state_changes_factory() -> list[tuple[float, CircuitState]]:
    """Factory function that preserves list[tuple[float, CircuitState]] type information."""
    return []


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = Field(
        default=5, gt=0, le=100, description="Failures before opening circuit"
    )
    success_threshold: int = Field(
        default=2, gt=0, le=50, description="Successes needed to close circuit"
    )
    timeout: float = Field(default=30.0, ge=0, le=300, description="Operation timeout in seconds")
    recovery_timeout: float = Field(
        default=60.0, gt=0, le=600, description="Recovery timeout in seconds"
    )
    excluded_exceptions: tuple[type[BaseException], ...] = Field(
        default_factory=lambda: (asyncio.CancelledError,),
        description="Exceptions to exclude from failure counting",
    )

    @field_validator("success_threshold", mode="before")
    @classmethod
    def validate_success_threshold(cls, v: int, info: ValidationInfo) -> int:
        """Validate success_threshold is less than failure_threshold."""
        if "failure_threshold" in info.data and v >= info.data["failure_threshold"]:
            raise CircuitBreakerThresholdError(
                threshold_type="success_threshold",
                invalid_relationship="must be less than failure_threshold"
            )
        return v


@dataclass
class CircuitBreakerMetrics:
    """Metrics for circuit breaker."""

    total_calls: int = Field(default=0, ge=0, description="Total number of calls")
    successful_calls: int = Field(default=0, ge=0, description="Number of successful calls")
    failed_calls: int = Field(default=0, ge=0, description="Number of failed calls")
    rejected_calls: int = Field(default=0, ge=0, description="Number of rejected calls")
    timeouts: int = Field(default=0, ge=0, description="Number of timeouts")
    last_failure_time: float | None = Field(
        default=None, gt=0, description="Last failure timestamp"
    )
    last_success_time: float | None = Field(
        default=None, gt=0, description="Last success timestamp"
    )
    state_changes: list[tuple[float, CircuitState]] = Field(
        default_factory=_state_changes_factory, description="State change history"
    )
    consecutive_failures: int = Field(default=0, ge=0, description="Consecutive failure count")
    consecutive_successes: int = Field(default=0, ge=0, description="Consecutive success count")


class CircuitBreaker[T]:
    """Type-safe circuit breaker implementation."""

    def __init__(self, name: str, config: CircuitBreakerConfig | None = None) -> None:
        """Initialize circuit breaker."""
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._metrics = CircuitBreakerMetrics()
        self._next_attempt_time = 0.0
        self._lock = asyncio.Lock()

        logger.info(
            "circuit_breaker_created",
            name=name,
            failure_threshold=self.config.failure_threshold,
            recovery_timeout=self.config.recovery_timeout,
        )

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        return self._state

    @property
    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self._state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self._state == CircuitState.CLOSED

    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args: object,
        **kwargs: object,
    ) -> ResilienceResult[T]:
        """Execute function with circuit breaker protection."""
        async with self._lock:
            self._metrics.total_calls += 1

            # Check if circuit is open
            if self._state == CircuitState.OPEN:
                if time.time() < self._next_attempt_time:
                    self._metrics.rejected_calls += 1
                    retry_after = int(self._next_attempt_time - time.time())

                    error = ResilienceError(
                        error_type=ResilienceErrorType.CIRCUIT_OPEN,
                        message=f"Circuit breaker is open for {self.name}",
                        service_name=self.name,
                        retry_after=retry_after,
                    )

                    metrics = self._create_metrics()
                    return ResilienceResult[T].failed(error, metrics)
                # Transition to half-open
                self._transition_to(CircuitState.HALF_OPEN)

        # Execute the function
        start_time = time.time()
        try:
            if self.config.timeout > 0:
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=self.config.timeout,
                )
            else:
                result = await func(*args, **kwargs)

            # Record success
            await self._on_success()

            duration_ms = (time.time() - start_time) * 1000
            metrics = self._create_metrics(duration_ms=duration_ms)
            return ResilienceResult[T].successful(result, metrics)

        except TimeoutError as e:
            await self._on_failure(is_timeout=True)

            duration_ms = (time.time() - start_time) * 1000
            error = ResilienceError(
                error_type=ResilienceErrorType.TIMEOUT,
                message=f"Operation timed out after {self.config.timeout}s",
                service_name=self.name,
                cause=e,
            )

            metrics = self._create_metrics(duration_ms=duration_ms)
            return ResilienceResult[T].failed(error, metrics)

        except Exception as e:
            # Check if exception should be excluded
            if isinstance(e, self.config.excluded_exceptions):
                raise

            await self._on_failure()

            duration_ms = (time.time() - start_time) * 1000
            error = ResilienceError(
                error_type=ResilienceErrorType.SERVICE_UNAVAILABLE,
                message=f"Service error: {e!s}",
                service_name=self.name,
                cause=e,
            )

            metrics = self._create_metrics(duration_ms=duration_ms)
            return ResilienceResult[T].failed(error, metrics)

    async def _on_success(self) -> None:
        """Handle successful execution."""
        async with self._lock:
            self._metrics.successful_calls += 1
            self._metrics.last_success_time = time.time()
            self._metrics.consecutive_failures = 0
            self._metrics.consecutive_successes += 1

            if self._state == CircuitState.HALF_OPEN:
                if self._metrics.consecutive_successes >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
                    logger.info(
                        "circuit_breaker_recovered",
                        name=self.name,
                        consecutive_successes=self._metrics.consecutive_successes,
                    )
            elif self._state == CircuitState.OPEN:
                # Should not happen, but handle gracefully
                self._transition_to(CircuitState.HALF_OPEN)

    async def _on_failure(self, is_timeout: bool = False) -> None:
        """Handle failed execution."""
        async with self._lock:
            self._metrics.failed_calls += 1
            self._metrics.last_failure_time = time.time()
            self._metrics.consecutive_successes = 0
            self._metrics.consecutive_failures += 1

            if is_timeout:
                self._metrics.timeouts += 1

            # Check if we should open the circuit
            if (
                self._metrics.consecutive_failures >= self.config.failure_threshold
                and self._state != CircuitState.OPEN
            ):
                self._transition_to(CircuitState.OPEN)
                self._next_attempt_time = time.time() + self.config.recovery_timeout

                logger.warning(
                    "circuit_breaker_opened",
                    name=self.name,
                    consecutive_failures=self._metrics.consecutive_failures,
                    next_attempt_time=self._next_attempt_time,
                )

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        if self._state != new_state:
            old_state = self._state
            self._state = new_state
            self._metrics.state_changes.append((time.time(), new_state))

            # Reset consecutive counters on state change
            if new_state == CircuitState.HALF_OPEN:
                self._metrics.consecutive_failures = 0
                self._metrics.consecutive_successes = 0

            logger.info(
                "circuit_breaker_state_changed",
                name=self.name,
                old_state=old_state.value,
                new_state=new_state.value,
            )

    def _create_metrics(self, duration_ms: float = 0.0) -> ResilienceMetrics:
        """Create resilience metrics from circuit breaker metrics."""
        return ResilienceMetrics(
            total_attempts=self._metrics.total_calls,
            successful_attempts=self._metrics.successful_calls,
            failed_attempts=self._metrics.failed_calls,
            circuit_breaker_trips=self._metrics.rejected_calls,
            fallback_executions=0,  # Circuit breaker doesn't use fallbacks
            total_duration_ms=duration_ms,
        )

    def get_metrics(self) -> dict[str, object]:
        """Get circuit breaker metrics."""
        return {
            "name": self.name,
            "state": self._state.value,
            "total_calls": self._metrics.total_calls,
            "successful_calls": self._metrics.successful_calls,
            "failed_calls": self._metrics.failed_calls,
            "rejected_calls": self._metrics.rejected_calls,
            "timeouts": self._metrics.timeouts,
            "consecutive_failures": self._metrics.consecutive_failures,
            "consecutive_successes": self._metrics.consecutive_successes,
            "last_failure_time": self._metrics.last_failure_time,
            "last_success_time": self._metrics.last_success_time,
            "success_rate": (
                self._metrics.successful_calls / self._metrics.total_calls
                if self._metrics.total_calls > 0
                else 0.0
            ),
            "failure_rate": (
                self._metrics.failed_calls / self._metrics.total_calls
                if self._metrics.total_calls > 0
                else 0.0
            ),
        }

    async def reset(self) -> None:
        """Reset circuit breaker to closed state."""
        async with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self._metrics.consecutive_failures = 0
            self._metrics.consecutive_successes = 0
            self._next_attempt_time = 0.0

            logger.info("circuit_breaker_reset", name=self.name)


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""

    def __init__(self) -> None:
        """Initialize registry."""
        self._breakers: dict[str, CircuitBreaker[Any]] = {}
        self._default_config = CircuitBreakerConfig()

    def get_or_create(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
    ) -> CircuitBreaker[Any]:
        """Get existing circuit breaker or create new one."""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(
                name=name,
                config=config or self._default_config,
            )
        return self._breakers[name]

    def get(self, name: str) -> CircuitBreaker[Any] | None:
        """Get circuit breaker by name."""
        return self._breakers.get(name)

    def get_all_metrics(self) -> dict[str, dict[str, object]]:
        """Get metrics for all circuit breakers."""
        return {name: breaker.get_metrics() for name, breaker in self._breakers.items()}

    async def reset_all(self) -> None:
        """Reset all circuit breakers."""
        for breaker in self._breakers.values():
            await breaker.reset()

    def clear(self) -> None:
        """Clear all circuit breakers."""
        self._breakers.clear()


class GlobalRegistryManager:
    """Manages the global circuit breaker registry without using global variables."""

    _instance: CircuitBreakerRegistry | None = None

    @classmethod
    def get_registry(cls) -> CircuitBreakerRegistry:
        """Get the global circuit breaker registry."""
        if cls._instance is None:
            cls._instance = CircuitBreakerRegistry()
        return cls._instance

    @classmethod
    def reset_registry(cls) -> None:
        """Reset the global registry."""
        cls._instance = None


def get_registry() -> CircuitBreakerRegistry:
    """Get the global circuit breaker registry."""
    return GlobalRegistryManager.get_registry()


def reset_registry() -> None:
    """Reset the global registry."""
    GlobalRegistryManager.reset_registry()
