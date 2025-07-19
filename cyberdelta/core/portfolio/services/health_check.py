"""Health check interfaces and implementations for portfolio services."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast, runtime_checkable

from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine


logger = get_logger(__name__)

T = TypeVar("T", bound=object)


# Type-preserving factory functions
def _str_any_dict_factory() -> dict[str, Any]:
    """Factory function that preserves dict[str, Any] type information."""
    return {}


def _health_status_dict_factory() -> dict[str, HealthStatus]:
    """Factory function that preserves dict[str, HealthStatus] type information."""
    return {}


class HealthStatus(Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""

    status: HealthStatus
    service_name: str
    timestamp: float = field(default_factory=time.time)
    message: str | None = None
    details: dict[str, Any] = field(default_factory=_str_any_dict_factory)
    dependencies: dict[str, HealthStatus] = field(default_factory=_health_status_dict_factory)

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy."""
        return self.status == HealthStatus.HEALTHY

    @property
    def is_degraded(self) -> bool:
        """Check if service is degraded."""
        return self.status == HealthStatus.DEGRADED

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "status": self.status.value,
            "service_name": self.service_name,
            "timestamp": self.timestamp,
            "message": self.message,
            "details": self.details,
            "dependencies": {k: v.value for k, v in self.dependencies.items()},
        }


@runtime_checkable
class HealthCheckable(Protocol):
    """Protocol for services that support health checks."""

    async def check_health(self) -> HealthCheckResult:
        """Check service health."""
        ...


@runtime_checkable
class DependencyHealthCheck(Protocol):
    """Protocol for checking dependency health."""

    async def check_dependency_health(self, dependency_name: str) -> HealthStatus:
        """Check health of a specific dependency."""
        ...


class HealthCheckService:
    """Service for aggregating and monitoring health checks."""

    def __init__(self, check_interval: float = 30.0) -> None:
        """Initialize health check service."""
        self._services: dict[str, HealthCheckable] = {}
        self._check_interval = check_interval
        self._latest_results: dict[str, HealthCheckResult] = {}
        self._health_check_task: asyncio.Task[None] | None = None
        self._callbacks: list[Callable[[HealthCheckResult], Coroutine[Any, Any, None]]] = []

        logger.info("health_check_service_initialized", check_interval=check_interval)

    def register_service(self, service_name: str, service: HealthCheckable) -> None:
        """Register a service for health monitoring."""
        self._services[service_name] = service
        logger.info("health_check_service_registered", service_name=service_name)

    def register_callback(
        self, callback: Callable[[HealthCheckResult], Coroutine[Any, Any, None]]
    ) -> None:
        """Register a callback for health status changes."""
        self._callbacks.append(callback)

    async def start(self) -> None:
        """Start health check monitoring."""
        if self._health_check_task is None:
            self._health_check_task = asyncio.create_task(self._health_check_loop())
            logger.info("health_check_monitoring_started")

    async def stop(self) -> None:
        """Stop health check monitoring."""
        if self._health_check_task:
            self._health_check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_check_task
            self._health_check_task = None
            logger.info("health_check_monitoring_stopped")

    async def check_all(self) -> dict[str, HealthCheckResult]:
        """Check health of all registered services."""
        results: dict[str, HealthCheckResult] = {}

        # Check all services concurrently
        tasks = {
            service_name: asyncio.create_task(self._check_service(service_name, service))
            for service_name, service in self._services.items()
        }

        for service_name, task in tasks.items():
            try:
                result = await task
                results[service_name] = result
                self._latest_results[service_name] = result

                # Notify callbacks on status change
                await self._notify_callbacks(result)

            except (RuntimeError, ValueError, TypeError, OSError, AttributeError) as e:
                logger.exception(
                    "health_check_error",
                    service_name=service_name,
                    error=str(e),
                )
                results[service_name] = HealthCheckResult(
                    status=HealthStatus.UNKNOWN,
                    service_name=service_name,
                    message=f"Health check failed: {e!s}",
                )

        return results

    async def get_aggregate_status(self) -> HealthCheckResult:
        """Get aggregated health status of all services."""
        if not self._latest_results:
            return HealthCheckResult(
                status=HealthStatus.UNKNOWN,
                service_name="aggregate",
                message="No health checks performed yet",
            )

        # Determine aggregate status
        statuses = [result.status for result in self._latest_results.values()]

        if all(s == HealthStatus.HEALTHY for s in statuses):
            aggregate_status = HealthStatus.HEALTHY
            message = "All services healthy"
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            aggregate_status = HealthStatus.UNHEALTHY
            unhealthy_count = sum(1 for s in statuses if s == HealthStatus.UNHEALTHY)
            message = f"{unhealthy_count} service(s) unhealthy"
        elif any(s == HealthStatus.DEGRADED for s in statuses):
            aggregate_status = HealthStatus.DEGRADED
            degraded_count = sum(1 for s in statuses if s == HealthStatus.DEGRADED)
            message = f"{degraded_count} service(s) degraded"
        else:
            aggregate_status = HealthStatus.UNKNOWN
            message = "Unable to determine aggregate status"

        dependencies: dict[str, HealthStatus] = {
            name: result.status for name, result in self._latest_results.items()
        }

        return HealthCheckResult(
            status=aggregate_status,
            service_name="aggregate",
            message=message,
            dependencies=dependencies,
            details={
                "total_services": len(self._services),
                "healthy_count": sum(1 for s in statuses if s == HealthStatus.HEALTHY),
                "degraded_count": sum(1 for s in statuses if s == HealthStatus.DEGRADED),
                "unhealthy_count": sum(1 for s in statuses if s == HealthStatus.UNHEALTHY),
            },
        )

    async def _check_service(
        self, service_name: str, service: HealthCheckable
    ) -> HealthCheckResult:
        """Check health of a single service."""
        try:
            return await asyncio.wait_for(
                service.check_health(),
                timeout=10.0,  # 10 second timeout for health checks
            )
        except TimeoutError:
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=service_name,
                message="Health check timed out",
            )
        except (RuntimeError, ValueError, TypeError, OSError, AttributeError) as e:
            return HealthCheckResult(
                status=HealthStatus.UNHEALTHY,
                service_name=service_name,
                message=f"Health check error: {e!s}",
            )

    async def _health_check_loop(self) -> None:
        """Background health check loop."""
        while True:
            try:
                await self.check_all()
                await asyncio.sleep(self._check_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("health_check_loop_error")
                await asyncio.sleep(self._check_interval)

    async def _notify_callbacks(self, result: HealthCheckResult) -> None:
        """Notify callbacks of health status changes."""
        for callback in self._callbacks:
            try:
                await callback(result)
            except Exception:
                logger.exception(
                    "health_check_callback_error",
                    service_name=result.service_name,
                )


class BaseHealthCheckMixin:
    """Mixin class to add health check capability to services."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize mixin."""
        super().__init__(*args, **kwargs)
        self._health_dependencies: dict[str, HealthCheckable] = {}

    def add_health_dependency(self, name: str, dependency: HealthCheckable) -> None:
        """Add a dependency for health checking."""
        self._health_dependencies[name] = dependency

    async def check_health(self) -> HealthCheckResult:
        """Check service health including dependencies."""
        # Check self health
        self_healthy, self_message = await self._check_self_health()

        # Check dependencies
        dependency_statuses: dict[str, HealthStatus] = {}
        for dep_name, dep_service in self._health_dependencies.items():
            try:
                dep_result = await dep_service.check_health()
                dependency_statuses[dep_name] = dep_result.status
            except (RuntimeError, ValueError, TypeError, OSError, AttributeError) as e:
                logger.warning(
                    "dependency_health_check_failed",
                    dependency=dep_name,
                    error=str(e),
                )
                dependency_statuses[dep_name] = HealthStatus.UNKNOWN

        # Determine overall status
        if not self_healthy:
            status = HealthStatus.UNHEALTHY
        elif any(s == HealthStatus.UNHEALTHY for s in dependency_statuses.values()):
            status = HealthStatus.DEGRADED
            self_message = "Service healthy but dependencies unhealthy"
        elif any(s == HealthStatus.DEGRADED for s in dependency_statuses.values()):
            status = HealthStatus.DEGRADED
            self_message = "Service healthy but dependencies degraded"
        else:
            status = HealthStatus.HEALTHY

        service_name = getattr(self, "service_name", self.__class__.__name__)

        return HealthCheckResult(
            status=status,
            service_name=service_name,
            message=self_message,
            dependencies=dependency_statuses,
            details=await self._get_health_details(),
        )

    async def _check_self_health(self) -> tuple[bool, str | None]:
        """Check internal health of the service.

        Override this method to implement custom health checks.
        """
        # Default implementation checks if service is running
        is_running = getattr(self, "is_running", None)
        if is_running is not None:
            is_healthy = is_running() if callable(is_running) else bool(is_running)
            if is_healthy:
                return True, None
            return False, "Service not running"

        # Assume healthy if no specific check
        return True, None

    async def _get_health_details(self) -> dict[str, Any]:
        """Get additional health details.

        Override this method to provide custom health details.
        """
        details: dict[str, Any] = {}

        # Add metrics if available
        get_metrics_attr = getattr(self, "get_metrics", None)
        if get_metrics_attr is not None and callable(get_metrics_attr):
            with contextlib.suppress(Exception):
                details["metrics"] = get_metrics_attr()

        return details


def create_health_check_decorator() -> Callable[[type[T]], type[T]]:
    """Create a decorator that adds health check capability to a class."""

    def health_check_decorator(cls: type[T]) -> type[T]:
        """Add health check capability to a class."""

        # Create a new class that inherits from both the mixin and original class
        class HealthCheckableClass(BaseHealthCheckMixin, cls):  # type: ignore
            pass

        # Preserve the original class name and module
        HealthCheckableClass.__name__ = cls.__name__
        HealthCheckableClass.__module__ = cls.__module__
        HealthCheckableClass.__qualname__ = cls.__qualname__

        return cast(type[T], HealthCheckableClass)

    return health_check_decorator
