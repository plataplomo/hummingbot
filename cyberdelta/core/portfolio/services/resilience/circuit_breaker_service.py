"""Circuit breaker service for fault tolerance and failure protection."""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Any, Awaitable, Callable, TypeVar

from pydantic import Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import (
    CircuitBreakerThresholdError,
    ServiceTimeoutError,
    ServiceUnavailableError,
)
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

logger = get_logger(__name__)

T = TypeVar("T", bound=object)


class CircuitBreakerOpenError(ServiceUnavailableError):
    """Raised when circuit breaker is open."""

    def __init__(self, service_name: str, retry_after: int = 0) -> None:
        super().__init__(
            "Circuit breaker is OPEN",
            service_name=service_name,
            retry_after=retry_after,
        )


class CircuitBreakerState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = Field(
        default=5, gt=0, le=100, description="Failures before opening circuit"
    )
    recovery_timeout: float = Field(
        default=60.0, gt=0, le=600, description="Recovery timeout in seconds"
    )
    success_threshold: int = Field(
        default=2, gt=0, le=50, description="Successes needed to close circuit"
    )
    timeout: float = Field(default=30.0, ge=0, le=300, description="Operation timeout in seconds")

    @field_validator("success_threshold", mode="before")
    @classmethod
    def validate_success_threshold(cls, v: int, info: ValidationInfo) -> int:
        if "failure_threshold" in info.data and v >= info.data["failure_threshold"]:
            raise CircuitBreakerThresholdError(
                threshold_type="success_threshold",
                invalid_relationship="must be less than failure_threshold",
            )
        return v


class CircuitBreaker[T]:
    """Circuit breaker implementation for fault tolerance."""

    def __init__(self, config: CircuitBreakerConfig, name: str = "CircuitBreaker") -> None:
        self.config = config
        self.name = name
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.next_attempt_time = 0.0

        logger.info(
            "circuit_breaker_initialized",
            name=name,
            failure_threshold=config.failure_threshold,
            recovery_timeout=config.recovery_timeout,
        )

    async def call(self, func: Callable[..., Awaitable[T]], *args: object, **kwargs: object) -> T:
        """Execute function with circuit breaker protection."""
        if self.state == CircuitBreakerState.OPEN:
            if time.time() < self.next_attempt_time:
                raise CircuitBreakerOpenError(
                    self.name,
                    retry_after=int(self.next_attempt_time - time.time()),
                )
            self.state = CircuitBreakerState.HALF_OPEN
            self.success_count = 0
            logger.info("circuit_breaker_state_changed", name=self.name, new_state=self.state.value)

        try:
            if self.config.timeout > 0:
                coro = func(*args, **kwargs)
                result: T = await asyncio.wait_for(coro, timeout=self.config.timeout)
            else:
                coro = func(*args, **kwargs)
                result = await coro

            self._on_success()

        except TimeoutError as e:
            self._on_failure()
            raise ServiceTimeoutError("Timeout") from e
        except Exception:
            self._on_failure()
            raise
        else:
            return result

    def _on_success(self) -> None:
        """Handle successful execution."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                logger.info("circuit_breaker_recovered", name=self.name, new_state=self.state.value)
        else:
            self.failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed execution."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.config.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            self.next_attempt_time = time.time() + self.config.recovery_timeout
            logger.warning(
                "circuit_breaker_opened",
                name=self.name,
                failure_count=self.failure_count,
                next_attempt_time=self.next_attempt_time,
            )

    def get_state(self) -> dict[str, object]:
        """Get current circuit breaker state."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time,
            "next_attempt_time": self.next_attempt_time,
        }


class CircuitBreakerService(BasePortfolioService):
    """Service for managing circuit breakers across portfolio components."""

    def __init__(self, config: dict[str, Any] | None = None):
        super().__init__("circuit_breaker_service", config)
        self._raw_config = config or {}
        self.logger = get_logger(__name__)
        
        # Circuit breaker management
        self.circuit_breakers: dict[str, CircuitBreaker[object]] = {}
        self.default_config = self._init_default_config()

    async def _initialize_service(self) -> None:
        """Initialize circuit breaker service."""
        self.logger.info("Initializing circuit breaker service")

    async def _shutdown_service(self) -> None:
        """Shutdown circuit breaker service."""
        self.logger.info("Shutting down circuit breaker service")

    async def _start_internal(self) -> None:
        """Start internal circuit breaker operations."""
        pass

    async def _stop_internal(self) -> None:
        """Stop internal circuit breaker operations."""
        pass

    def _init_default_config(self) -> CircuitBreakerConfig:
        """Initialize default circuit breaker configuration."""
        failure_threshold = self._raw_config.get("default_failure_threshold", 5)
        recovery_timeout = self._raw_config.get("default_recovery_timeout", 60.0)
        success_threshold = self._raw_config.get("default_success_threshold", 2)
        timeout = self._raw_config.get("default_timeout", 30.0)

        return CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            recovery_timeout=float(recovery_timeout),
            success_threshold=success_threshold,
            timeout=float(timeout),
        )

    def register_circuit_breaker(
        self, service_name: str, config: CircuitBreakerConfig | None = None
    ) -> None:
        """Register a circuit breaker for a service."""
        cb_config = config or self.default_config
        self.circuit_breakers[service_name] = CircuitBreaker(cb_config, service_name)

        self.logger.info(
            "circuit_breaker_registered",
            service_name=service_name,
            failure_threshold=cb_config.failure_threshold,
        )

    def get_circuit_breaker(self, service_name: str) -> CircuitBreaker[object] | None:
        """Get circuit breaker for a service."""
        return self.circuit_breakers.get(service_name)

    async def execute_with_circuit_breaker(
        self,
        service_name: str,
        func: Callable[..., Awaitable[T]],
        *args: object,
        **kwargs: object,
    ) -> T:
        """Execute function with circuit breaker protection."""
        circuit_breaker = self.circuit_breakers.get(service_name)
        if not circuit_breaker:
            self.logger.warning(f"No circuit breaker registered for {service_name}")
            return await func(*args, **kwargs)

        result = await circuit_breaker.call(func, *args, **kwargs)
        return result

    def get_all_states(self) -> dict[str, dict[str, object]]:
        """Get states of all circuit breakers."""
        return {name: cb.get_state() for name, cb in self.circuit_breakers.items()}

    def reset_circuit_breaker(self, service_name: str) -> bool:
        """Reset a circuit breaker to closed state."""
        circuit_breaker = self.circuit_breakers.get(service_name)
        if not circuit_breaker:
            return False

        circuit_breaker.state = CircuitBreakerState.CLOSED
        circuit_breaker.failure_count = 0
        circuit_breaker.success_count = 0
        circuit_breaker.last_failure_time = 0.0
        circuit_breaker.next_attempt_time = 0.0
        
        self.logger.info("circuit_breaker_reset", service_name=service_name)
        return True