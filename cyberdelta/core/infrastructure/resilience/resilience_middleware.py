"""Resilience middleware for portfolio components."""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar, cast

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.infrastructure.resilience.circuit_breaker_service import CircuitBreakerConfig, CircuitBreakerService
from cyberdelta.core.infrastructure.resilience.graceful_degradation_service import GracefulDegradationService
from cyberdelta.core.infrastructure.resilience.retry_service import RetryConfig, RetryService

# Use new focused health check services
from cyberdelta.core.monitoring.health import HealthCheckOrchestrator


logger = get_logger(__name__)

P = ParamSpec("P")
T = TypeVar("T", bound=object)


class ResilienceMiddleware:
    """Middleware for adding resilience capabilities to portfolio components."""

    def __init__(
        self,
        circuit_breaker_service: CircuitBreakerService,
        retry_service: RetryService,
        health_check_orchestrator: HealthCheckOrchestrator,
        degradation_service: GracefulDegradationService,
    ) -> None:
        """Initialize middleware with individual resilience services."""
        self.circuit_breaker_service = circuit_breaker_service
        self.retry_service = retry_service
        self.health_check_orchestrator = health_check_orchestrator
        self.degradation_service = degradation_service
        self.registered_services: set[str] = set()

    def resilient(
        self,
        service_name: str,
        use_circuit_breaker: bool = True,
        use_retry: bool = True,
        use_fallback: bool = True,
        circuit_breaker_config: CircuitBreakerConfig | None = None,
        retry_config: RetryConfig | None = None,
        fallback_handler: Callable[..., Any] | None = None,
        health_check: Callable[..., Any] | None = None,
    ) -> Callable[[Callable[..., T]], Callable[..., Awaitable[T]]]:
        """Decorator to add resilience capabilities to a function.

        Returns:
            Decorator function that wraps the target function with resilience features
        """

        def decorator(func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
            # Register service if not already registered
            if service_name not in self.registered_services:
                self._register_service(
                    service_name, circuit_breaker_config, fallback_handler, health_check
                )
                self.registered_services.add(service_name)

            @functools.wraps(func)
            async def wrapper(*args: object, **kwargs: object) -> T:
                async def async_func(*a: object, **kw: object) -> T:
                    result = func(*a, **kw)
                    # If already awaitable, await it and cast result
                    if inspect.iscoroutine(result) or inspect.isawaitable(result):
                        awaited_result = await result
                        return cast(T, awaited_result)
                    # For non-awaitable results, return directly (mypy knows this is T)
                    return result

                # Apply resilience patterns
                execution_func = async_func
                
                # Apply circuit breaker if enabled
                if use_circuit_breaker:
                    execution_func = lambda *a, **k: self.circuit_breaker_service.execute_with_circuit_breaker(
                        service_name, async_func, *a, **k
                    )
                
                # Apply retry if enabled
                if use_retry:
                    retry_func = execution_func
                    execution_func = lambda *a, **k: self.retry_service.execute_with_retry(
                        service_name, retry_func, *a, **k
                    )
                
                # Apply fallback if enabled
                if use_fallback:
                    return await self.degradation_service.execute_with_fallback(
                        service_name, execution_func, *args, **kwargs
                    )
                
                return await execution_func(*args, **kwargs)

            return wrapper

        return decorator

    def _register_service(
        self,
        service_name: str,
        circuit_breaker_config: CircuitBreakerConfig | None,
        fallback_handler: Callable[..., Any] | None,
        health_check: Callable[[], Awaitable[bool]] | None,
    ) -> None:
        """Register a service with resilience components."""
        # Register circuit breaker
        if circuit_breaker_config:
            self.circuit_breaker_service.register_circuit_breaker(service_name, circuit_breaker_config)

        # Register fallback handler
        if fallback_handler:
            self.degradation_service.register_fallback(service_name, fallback_handler)

        # Register health check (Note: HealthCheckOrchestrator uses register_service instead)
        # For compatibility, we skip health check registration here
        # Health checks should be registered directly with the orchestrator using register_service

        logger.info(
            "resilience_middleware_service_registered",
            service_name=service_name,
            has_fallback=fallback_handler is not None,
            has_health_check=health_check is not None,
        )


def create_resilience_mixin(
    circuit_breaker_service: CircuitBreakerService,
    retry_service: RetryService,
    health_check_orchestrator: HealthCheckOrchestrator,
    degradation_service: GracefulDegradationService,
) -> type[Any]:
    """Create a mixin class for adding resilience capabilities.

    Returns:
        ResilienceMixin class with resilience capabilities for portfolio components
    """

    class ResilienceMixin:
        """Mixin for adding resilience capabilities to portfolio components."""

        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, **kwargs)
            self.circuit_breaker_service = circuit_breaker_service
            self.retry_service = retry_service
            self.health_check_orchestrator = health_check_orchestrator
            self.degradation_service = degradation_service
            self.resilience_middleware = ResilienceMiddleware(
                circuit_breaker_service, retry_service, health_check_orchestrator, degradation_service
            )

        def resilient_method(
            self,
            service_name: str | None = None,
            use_circuit_breaker: bool = True,
            use_retry: bool = True,
            use_fallback: bool = True,
            circuit_breaker_config: CircuitBreakerConfig | None = None,
            retry_config: RetryConfig | None = None,
            fallback_handler: Callable[..., Any] | None = None,
            health_check: Callable[..., Any] | None = None,
        ) -> Callable[[Callable[P, T]], Callable[P, Awaitable[T]]]:
            """Decorator for making methods resilient.

            Returns:
                Decorator that wraps methods with resilience capabilities
            """
            # Use class name as service name if not provided
            if service_name is None:
                service_name = self.__class__.__name__

            return self.resilience_middleware.resilient(
                service_name=service_name,
                use_circuit_breaker=use_circuit_breaker,
                use_retry=use_retry,
                use_fallback=use_fallback,
                circuit_breaker_config=circuit_breaker_config,
                retry_config=retry_config,
                fallback_handler=fallback_handler,
                health_check=health_check,
            )

        async def execute_with_resilience(
            self,
            service_name: str,
            func: Callable[..., Awaitable[Any]],
            *args: object,
            **kwargs: object,
        ) -> object:
            """Execute function with resilience capabilities."""
            return await self.degradation_service.execute_with_fallback(
                service_name, func, *args, **kwargs
            )

        async def get_resilience_status(self) -> dict[str, Any]:
            """Get resilience status for this component."""
            return {
                "circuit_breakers": self.circuit_breaker_service.get_all_states(),
                "retry_stats": self.retry_service.get_retry_stats(),
                "health_summary": await self.health_check_orchestrator.get_aggregate_status(),
                "degradation_status": self.degradation_service.get_degradation_status(),
            }

        async def health_check(self) -> bool:
            """Default health check implementation.

            Returns:
                True if component is healthy, False otherwise
            """
            # Check for is_running attribute without getattr
            # The mixin is designed to be used with classes that have is_running
            # Return True as default for classes without this attribute
            attrs = vars(self)
            if "is_running" in attrs:
                return bool(attrs["is_running"])
            return True

    return ResilienceMixin


# Common resilience configurations
PORTFOLIO_RESILIENCE_CONFIGS = {
    "price_service": {
        "circuit_breaker": CircuitBreakerConfig(
            failure_threshold=3, recovery_timeout=30.0, success_threshold=2, timeout=10.0
        ),
        "retry": RetryConfig(
            max_attempts=3, initial_delay=1.0, max_delay=30.0, backoff_multiplier=2.0
        ),
    },
    "balance_manager": {
        "circuit_breaker": CircuitBreakerConfig(
            failure_threshold=5, recovery_timeout=60.0, success_threshold=3, timeout=30.0
        ),
        "retry": RetryConfig(
            max_attempts=2, initial_delay=0.5, max_delay=10.0, backoff_multiplier=2.0
        ),
    },
    "position_manager": {
        "circuit_breaker": CircuitBreakerConfig(
            failure_threshold=5, recovery_timeout=60.0, success_threshold=3, timeout=30.0
        ),
        "retry": RetryConfig(
            max_attempts=2, initial_delay=0.5, max_delay=10.0, backoff_multiplier=2.0
        ),
    },
    "pnl_calculator": {
        "circuit_breaker": CircuitBreakerConfig(
            failure_threshold=3, recovery_timeout=30.0, success_threshold=2, timeout=15.0
        ),
        "retry": RetryConfig(
            max_attempts=3, initial_delay=1.0, max_delay=20.0, backoff_multiplier=1.5
        ),
    },
    "exposure_calculator": {
        "circuit_breaker": CircuitBreakerConfig(
            failure_threshold=3, recovery_timeout=45.0, success_threshold=2, timeout=20.0
        ),
        "retry": RetryConfig(
            max_attempts=2, initial_delay=2.0, max_delay=30.0, backoff_multiplier=2.0
        ),
    },
}


def get_resilience_config(service_type: str) -> dict[str, Any]:
    """Get resilience configuration for a service type.

    Returns:
        Dictionary containing circuit breaker and retry configurations
    """
    return PORTFOLIO_RESILIENCE_CONFIGS.get(
        service_type,
        {
            "circuit_breaker": CircuitBreakerConfig(),
            "retry": RetryConfig(),
        },
    )


# Utility functions for common resilience patterns
async def with_timeout(
    func: Callable[..., Awaitable[Any]], timeout_seconds: float, *args: object, **kwargs: object
) -> object:
    """Execute function with timeout.

    Returns:
        Result of the executed function

    Raises:
        TimeoutError: If function execution exceeds timeout_seconds
    """
    try:
        return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout_seconds)
    except TimeoutError:
        logger.warning("function_timeout", function_name=func.__name__, timeout=timeout_seconds)
        raise


async def with_exponential_backoff(
    func: Callable[[], Awaitable[Any]],
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_multiplier: float = 2.0,
    max_delay: float = 60.0,
) -> object:
    """Execute function with exponential backoff retry.

    Returns:
        Result of the successful function execution

    Raises:
        RuntimeError: If all retry attempts are exhausted without exception
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(max_attempts):
        try:
            return await func()
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            last_exception = e

            if attempt < max_attempts - 1:
                await asyncio.sleep(delay)
                delay = min(delay * backoff_multiplier, max_delay)
            else:
                logger.exception(
                    "retry_exhausted",
                    function_name=func.__name__,
                    max_attempts=max_attempts,
                    final_exception=str(e),
                )

    if last_exception:
        raise last_exception
    raise RuntimeError


def create_fallback_handler(fallback_value: object) -> Callable[..., Any]:
    """Create a simple fallback handler that returns a default value.

    Returns:
        Fallback function that returns the specified fallback_value
    """

    def fallback_handler(*args: object, **kwargs: object) -> object:
        logger.info("fallback_handler_executed", fallback_value=fallback_value)
        return fallback_value

    return fallback_handler


def create_health_check(component: object, check_attr: str = "is_running") -> Callable[[], bool]:
    """Create a health check function for a component.

    Returns:
        Health check function that returns component health status
    """

    def health_check() -> bool:
        try:
            # Access attribute directly
            attr_value = component.__dict__.get(check_attr)
            if attr_value is None:
                # Try to access as property/descriptor
                attr_value = vars(component).get(check_attr, True)
            return bool(attr_value)
        except (AttributeError, TypeError):
            return True

    return health_check
