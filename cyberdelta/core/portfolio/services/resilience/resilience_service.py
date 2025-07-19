"""Portfolio resilience service for error recovery and fault tolerance."""

from __future__ import annotations

import asyncio
import contextlib
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeVar, cast

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import (
    ServiceTimeoutError,
    ServiceUnavailableError,
)
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService


logger = get_logger(__name__)

T = TypeVar("T", bound=object)


class CircuitBreakerOpenError(ServiceUnavailableError):
    """Raised when circuit breaker is open."""

    def __init__(self, service_name: str, retry_after: int = 0) -> None:
        """Initialize with service name and retry_after."""
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
class RetryConfig:
    """Configuration for retry mechanisms."""

    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    retriable_exceptions: tuple[type[Exception], ...] = field(default_factory=lambda: (Exception,))


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    success_threshold: int = 2
    timeout: float = 30.0


@dataclass
class HealthCheckConfig:
    """Configuration for health checks."""

    check_interval: float = 30.0
    timeout: float = 5.0
    consecutive_failures_threshold: int = 3


class CircuitBreaker[T]:
    """Circuit breaker implementation for fault tolerance."""

    def __init__(self, config: CircuitBreakerConfig, name: str = "CircuitBreaker") -> None:
        """Initialize circuit breaker.

        Args:
            config: Circuit breaker configuration
            name: Circuit breaker name
        """
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
            # Apply timeout if configured
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


class RetryMechanism:
    """Retry mechanism with exponential backoff and jitter."""

    def __init__(self, config: RetryConfig) -> None:
        """Initialize retry mechanism.

        Args:
            config: Retry configuration
        """
        self.config = config

    async def execute(
        self, func: Callable[..., Awaitable[T]], *args: object, **kwargs: object
    ) -> T:
        """Execute function with retry logic."""
        last_exception = None
        delay = self.config.initial_delay

        for attempt in range(self.config.max_attempts):
            try:
                result = func(*args, **kwargs)
                return await result

            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                last_exception = e

                # Check if exception is retriable
                if not any(
                    isinstance(e, exc_type) for exc_type in self.config.retriable_exceptions
                ):
                    logger.warning(
                        "non_retriable_exception",
                        exception_type=type(e).__name__,
                        attempt=attempt + 1,
                        max_attempts=self.config.max_attempts,
                    )
                    raise

                # Don't retry on last attempt
                if attempt == self.config.max_attempts - 1:
                    break

                # Calculate delay with jitter
                actual_delay = delay
                if self.config.jitter:
                    actual_delay *= 0.5 + secrets.SystemRandom().random() * 0.5

                logger.info(
                    "retry_attempt",
                    attempt=attempt + 1,
                    max_attempts=self.config.max_attempts,
                    delay=actual_delay,
                    exception_type=type(e).__name__,
                )

                await asyncio.sleep(actual_delay)

                # Exponential backoff
                delay = min(delay * self.config.backoff_multiplier, self.config.max_delay)

        # All attempts failed
        logger.error(
            "retry_exhausted",
            max_attempts=self.config.max_attempts,
            final_exception=str(last_exception),
        )
        if last_exception:
            raise last_exception
        raise RuntimeError


class GracefulDegradationManager:
    """Manages graceful degradation of portfolio services."""

    def __init__(self) -> None:
        """Initialize graceful degradation manager."""
        self.fallback_handlers: dict[str, Callable[..., Awaitable[Any]]] = {}
        self.degradation_modes: dict[str, bool] = {}
        self.service_priorities: dict[str, int] = {}

    def register_fallback(
        self,
        service_name: str,
        fallback_handler: Callable[..., Awaitable[Any]],
        priority: int = 5,
    ) -> None:
        """Register a fallback handler for a service."""
        self.fallback_handlers[service_name] = fallback_handler
        self.service_priorities[service_name] = priority
        self.degradation_modes[service_name] = False

        logger.info("fallback_registered", service_name=service_name, priority=priority)

    def enable_degradation(self, service_name: str) -> None:
        """Enable degradation mode for a service."""
        if service_name in self.degradation_modes:
            self.degradation_modes[service_name] = True
            logger.warning(
                "degradation_mode_enabled",
                service_name=service_name,
                priority=self.service_priorities.get(service_name, 0),
            )

    def disable_degradation(self, service_name: str) -> None:
        """Disable degradation mode for a service."""
        if service_name in self.degradation_modes:
            self.degradation_modes[service_name] = False
            logger.info("degradation_mode_disabled", service_name=service_name)

    async def execute_with_fallback(
        self,
        service_name: str,
        primary_func: Callable[..., Awaitable[T]],
        *args: object,
        **kwargs: object,
    ) -> T:
        """Execute function with fallback capability."""
        # Check if degradation mode is enabled
        if self.degradation_modes.get(service_name, False):
            fallback_handler = self.fallback_handlers.get(service_name)
            if fallback_handler:
                logger.info("using_fallback_handler", service_name=service_name)
                result = await fallback_handler(*args, **kwargs)
                return cast(T, result)

        # Try primary function
        try:
            return await primary_func(*args, **kwargs)
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            # Enable degradation and try fallback
            self.enable_degradation(service_name)

            fallback_handler = self.fallback_handlers.get(service_name)
            if fallback_handler:
                logger.warning("primary_function_failed_using_fallback", service_name=service_name)
                fallback_result = await fallback_handler(*args, **kwargs)
                return cast(T, fallback_result)
            logger.exception("no_fallback_available", service_name=service_name)
            raise


class PortfolioResilienceService(BasePortfolioService):
    """Comprehensive resilience service for portfolio components."""

    def __init__(
        self, name: str = "PortfolioResilienceService", config: dict[str, object] | None = None
    ) -> None:
        """Initialize the portfolio resilience service.

        Args:
            name: Service name
            config: Configuration dictionary
        """
        super().__init__(name, config)

        # Initialize configurations with proper type validation
        cfg = config or {}

        # Initialize all configurations
        self.retry_config = self._init_retry_config(cfg)
        self.circuit_breaker_config = self._init_circuit_breaker_config(cfg)
        self.health_check_config = self._init_health_check_config(cfg)

        # Initialize components
        self._init_resilience_components()

        logger.info(
            "portfolio_resilience_service_initialized",
            name=name,
            retry_max_attempts=self.retry_config.max_attempts,
            circuit_breaker_failure_threshold=self.circuit_breaker_config.failure_threshold,
            health_check_interval=self.health_check_config.check_interval,
        )

    def _init_retry_config(self, cfg: dict[str, object]) -> RetryConfig:
        """Initialize retry configuration with validation."""
        max_attempts = cfg.get("retry_max_attempts", 3)
        if not isinstance(max_attempts, int) or max_attempts < 1:
            max_attempts = 3

        initial_delay = cfg.get("retry_initial_delay", 1.0)
        if not isinstance(initial_delay, (int, float)) or initial_delay < 0:
            initial_delay = 1.0

        max_delay = cfg.get("retry_max_delay", 60.0)
        if not isinstance(max_delay, (int, float)) or max_delay < initial_delay:
            max_delay = 60.0

        backoff_multiplier = cfg.get("retry_backoff_multiplier", 2.0)
        if not isinstance(backoff_multiplier, (int, float)) or backoff_multiplier < 1.0:
            backoff_multiplier = 2.0

        jitter = cfg.get("retry_jitter", True)
        if not isinstance(jitter, bool):
            jitter = True

        return RetryConfig(
            max_attempts=max_attempts,
            initial_delay=float(initial_delay),
            max_delay=float(max_delay),
            backoff_multiplier=float(backoff_multiplier),
            jitter=jitter,
        )

    def _init_circuit_breaker_config(self, cfg: dict[str, object]) -> CircuitBreakerConfig:
        """Initialize circuit breaker configuration with validation."""
        failure_threshold = cfg.get("circuit_breaker_failure_threshold", 5)
        if not isinstance(failure_threshold, int) or failure_threshold < 1:
            failure_threshold = 5

        recovery_timeout = cfg.get("circuit_breaker_recovery_timeout", 60.0)
        if not isinstance(recovery_timeout, (int, float)) or recovery_timeout < 0:
            recovery_timeout = 60.0

        success_threshold = cfg.get("circuit_breaker_success_threshold", 2)
        if not isinstance(success_threshold, int) or success_threshold < 1:
            success_threshold = 2

        cb_timeout = cfg.get("circuit_breaker_timeout", 30.0)
        if not isinstance(cb_timeout, (int, float)) or cb_timeout < 0:
            cb_timeout = 30.0

        return CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            recovery_timeout=float(recovery_timeout),
            success_threshold=success_threshold,
            timeout=float(cb_timeout),
        )

    def _init_health_check_config(self, cfg: dict[str, object]) -> HealthCheckConfig:
        """Initialize health check configuration with validation."""
        check_interval = cfg.get("health_check_interval", 30.0)
        if not isinstance(check_interval, (int, float)) or check_interval < 0:
            check_interval = 30.0

        hc_timeout = cfg.get("health_check_timeout", 5.0)
        if not isinstance(hc_timeout, (int, float)) or hc_timeout < 0:
            hc_timeout = 5.0

        failures_threshold = cfg.get("health_check_failures_threshold", 3)
        if not isinstance(failures_threshold, int) or failures_threshold < 1:
            failures_threshold = 3

        return HealthCheckConfig(
            check_interval=float(check_interval),
            timeout=float(hc_timeout),
            consecutive_failures_threshold=failures_threshold,
        )

    def _init_resilience_components(self) -> None:
        """Initialize resilience components."""
        self.retry_mechanism = RetryMechanism(self.retry_config)
        self.circuit_breakers: dict[str, CircuitBreaker[object]] = {}
        self.degradation_manager = GracefulDegradationManager()
        self.health_checks: dict[str, Callable[[], bool | Awaitable[bool]]] = {}
        self.service_health: dict[str, bool] = {}

        # Health check task
        self._health_check_task: asyncio.Task[None] | None = None

    async def _start_internal(self) -> None:
        """Internal startup logic."""
        await self._initialize_internal()

    async def _stop_internal(self) -> None:
        """Internal shutdown logic."""
        await self._shutdown_internal()

    async def _initialize_internal(self) -> None:
        """Initialize internal components."""
        # Start health check monitoring
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        logger.info("resilience_service_initialized")

    async def _shutdown_internal(self) -> None:
        """Shutdown internal components."""
        if self._health_check_task:
            self._health_check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_check_task

        logger.info("resilience_service_shutdown")

    def register_circuit_breaker(
        self, service_name: str, config: CircuitBreakerConfig | None = None
    ) -> None:
        """Register a circuit breaker for a service."""
        cb_config = config or self.circuit_breaker_config
        self.circuit_breakers[service_name] = CircuitBreaker(cb_config, service_name)

        logger.info(
            "circuit_breaker_registered",
            service_name=service_name,
            failure_threshold=cb_config.failure_threshold,
        )

    def register_health_check(
        self, service_name: str, health_check_func: Callable[[], bool | Awaitable[bool]]
    ) -> None:
        """Register a health check for a service."""
        self.health_checks[service_name] = health_check_func
        self.service_health[service_name] = True

        logger.info("health_check_registered", service_name=service_name)

    async def execute_with_resilience(
        self,
        service_name: str,
        func: Callable[..., Awaitable[T]],
        *args: object,
        use_circuit_breaker: bool = True,
        use_retry: bool = True,
        use_fallback: bool = True,
        **kwargs: object,
    ) -> T:
        """Execute function with full resilience capabilities."""
        execution_func: Callable[..., Awaitable[T]] = func

        # Apply circuit breaker if enabled
        if use_circuit_breaker and service_name in self.circuit_breakers:
            cb = self.circuit_breakers[service_name]

            async def circuit_breaker_wrapper(*a: object, **k: object) -> T:
                result = await cb.call(func, *a, **k)
                return cast(T, result)

            execution_func = circuit_breaker_wrapper

        # Apply retry mechanism if enabled
        if use_retry:
            retry_func = execution_func

            async def retry_wrapper(*a: object, **k: object) -> T:
                return await self.retry_mechanism.execute(retry_func, *a, **k)

            execution_func = retry_wrapper

        # Apply fallback if enabled
        if use_fallback:
            return await self.degradation_manager.execute_with_fallback(
                service_name, execution_func, *args, **kwargs
            )
        result = execution_func(*args, **kwargs)
        return await result

    async def _health_check_loop(self) -> None:
        """Continuous health check monitoring."""
        consecutive_failures: dict[str, int] = {}

        while True:
            try:
                await self._check_all_services(consecutive_failures)
                await asyncio.sleep(self.health_check_config.check_interval)

            except asyncio.CancelledError:
                break
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception("health_check_loop_error")
                await asyncio.sleep(self.health_check_config.check_interval)

    async def _check_all_services(self, consecutive_failures: dict[str, int]) -> None:
        """Check health of all registered services."""
        for service_name, health_check_func in self.health_checks.items():
            await self._check_single_service(service_name, health_check_func, consecutive_failures)

    async def _check_single_service(
        self,
        service_name: str,
        health_check_func: Callable[[], bool | Awaitable[bool]],
        consecutive_failures: dict[str, int],
    ) -> None:
        """Check health of a single service."""
        try:
            is_healthy = await self._execute_health_check(health_check_func)
            self._process_health_check_result(service_name, is_healthy, consecutive_failures)
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
            consecutive_failures[service_name] = consecutive_failures.get(service_name, 0) + 1
            logger.exception(
                "health_check_exception",
                service_name=service_name,
                consecutive_failures=consecutive_failures[service_name],
            )

    async def _execute_health_check(
        self, health_check_func: Callable[[], bool | Awaitable[bool]]
    ) -> bool:
        """Execute a health check function."""
        result = health_check_func()
        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
            return await asyncio.wait_for(result, timeout=self.health_check_config.timeout)
        return bool(result)

    def _process_health_check_result(
        self, service_name: str, is_healthy: bool, consecutive_failures: dict[str, int]
    ) -> None:
        """Process the result of a health check."""
        if is_healthy:
            self.service_health[service_name] = True
            consecutive_failures[service_name] = 0
            # Re-enable service if it was degraded
            self.degradation_manager.disable_degradation(service_name)
        else:
            consecutive_failures[service_name] = consecutive_failures.get(service_name, 0) + 1

            if (
                consecutive_failures[service_name]
                >= self.health_check_config.consecutive_failures_threshold
            ):
                self.service_health[service_name] = False
                self.degradation_manager.enable_degradation(service_name)

                logger.warning(
                    "service_health_check_failed",
                    service_name=service_name,
                    consecutive_failures=consecutive_failures[service_name],
                )

    def get_resilience_status(self) -> dict[str, object]:
        """Get current resilience status."""
        return {
            "service_health": self.service_health,
            "circuit_breakers": {
                name: cb.get_state() for name, cb in self.circuit_breakers.items()
            },
            "degradation_modes": self.degradation_manager.degradation_modes,
            "retry_config": {
                "max_attempts": self.retry_config.max_attempts,
                "initial_delay": self.retry_config.initial_delay,
                "max_delay": self.retry_config.max_delay,
            },
            "health_checks_count": len(self.health_checks),
            "timestamp": time.time(),
        }
