"""Health check interfaces and implementations for portfolio services."""

from __future__ import annotations

import asyncio
import contextlib
import time
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast, runtime_checkable

from pydantic import BaseModel, Field, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.service import HealthCheckValidationError


# Constants
REASONABLE_METRIC_MAX = 1e15  # Maximum reasonable metric value


if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine


logger = get_logger(__name__)

T = TypeVar("T", bound=object)


# Type-preserving factory functions
def _health_status_dict_factory() -> dict[str, HealthStatus]:
    """Factory function that preserves dict[str, HealthStatus] type information.

    Returns:
        Empty dictionary typed as dict[str, HealthStatus]
    """
    return {}


class HealthStatus(Enum):
    """Health status levels."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class HealthCheckDetails(BaseModel):
    """Health check details with validation."""

    total_services: int = Field(default=0, ge=0, description="Total number of services")
    healthy_count: int = Field(default=0, ge=0, description="Number of healthy services")
    degraded_count: int = Field(default=0, ge=0, description="Number of degraded services")
    unhealthy_count: int = Field(default=0, ge=0, description="Number of unhealthy services")
    uptime_seconds: float = Field(default=0.0, ge=0, description="Service uptime in seconds")
    last_check_duration_ms: float = Field(
        default=0.0, ge=0, description="Last check duration in milliseconds"
    )
    memory_usage_mb: float = Field(default=0.0, ge=0, description="Memory usage in MB")
    cpu_usage_percent: float = Field(default=0.0, ge=0, le=100, description="CPU usage percentage")
    error_count: int = Field(default=0, ge=0, description="Total error count")
    warning_count: int = Field(default=0, ge=0, description="Total warning count")

    @field_validator("uptime_seconds", "last_check_duration_ms", "memory_usage_mb", mode="before")
    @classmethod
    def validate_metrics(cls, v: float | str) -> float:
        """Validate metric values are finite and reasonable.

        Args:
            v: Metric value to validate

        Returns:
            Validated float value

        Raises:
            HealthCheckValidationError: If value is negative or exceeds reasonable limits
        """
        value: float = float(v)
        if not (0 <= value < REASONABLE_METRIC_MAX):  # Must be non-negative and reasonable
            raise HealthCheckValidationError(
                metric_type="metric_value",
                requirement="must be non-negative and finite",
                value=str(value),
            )
        return value

    @field_validator(
        "total_services",
        "healthy_count",
        "degraded_count",
        "unhealthy_count",
        "error_count",
        "warning_count",
        mode="before",
    )
    @classmethod
    def validate_counts(cls, v: str | float) -> int:
        """Validate count values are non-negative.

        Args:
            v: Count value to validate

        Returns:
            Validated integer count

        Raises:
            HealthCheckValidationError: If count is negative
        """
        value: int = int(v)
        if value < 0:
            raise HealthCheckValidationError(
                metric_type="count_value", requirement="must be non-negative", value=str(value)
            )
        return value


@dataclass
class HealthCheckResult:
    """Result of a health check."""

    status: HealthStatus
    service_name: str
    timestamp: float = Field(default_factory=time.time)
    message: str | None = None
    details: HealthCheckDetails = Field(default_factory=HealthCheckDetails)
    dependencies: dict[str, HealthStatus] = Field(default_factory=_health_status_dict_factory)

    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy."""
        return self.status == HealthStatus.HEALTHY

    @property
    def is_degraded(self) -> bool:
        """Check if service is degraded."""
        return self.status == HealthStatus.DEGRADED

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary with all health check result fields
        """
        return {
            "status": self.status.value,
            "service_name": self.service_name,
            "timestamp": self.timestamp,
            "message": self.message,
            "details": self.details.model_dump(),
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
        """Check health of all registered services.

        Returns:
            Dictionary mapping service names to their health check results
        """
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
        """Get aggregated health status of all services.

        Returns:
            Aggregate health check result combining all service statuses
        """
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
            details=HealthCheckDetails(
                total_services=len(self._services),
                healthy_count=sum(1 for s in statuses if s == HealthStatus.HEALTHY),
                degraded_count=sum(1 for s in statuses if s == HealthStatus.DEGRADED),
                unhealthy_count=sum(1 for s in statuses if s == HealthStatus.UNHEALTHY),
            ),
        )

    async def _check_service(
        self, service_name: str, service: HealthCheckable
    ) -> HealthCheckResult:
        """Check health of a single service.

        Args:
            service_name: Name of the service to check
            service: The service instance to check

        Returns:
            Health check result for the service
        """
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
        """Check service health including dependencies.

        Returns:
            Health check result with service and dependency statuses
        """
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

        # Get service name with safe fallback
        try:
            service_name_attr = getattr(self, "service_name", None)
            service_name = (
                str(service_name_attr) if service_name_attr is not None else self.__class__.__name__
            )
        except (AttributeError, TypeError):
            service_name = self.__class__.__name__

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

        Returns:
            Tuple of (is_healthy, optional_message)
        """
        # Default implementation checks if service is running
        try:
            is_running_attr = getattr(self, "is_running", None)
            if is_running_attr is not None:
                is_healthy = (
                    is_running_attr() if callable(is_running_attr) else bool(is_running_attr)
                )
                if is_healthy:
                    return True, None
                return False, "Service not running"
        except (AttributeError, TypeError):
            # Assume healthy if no is_running attribute or if it's not callable/boolean
            pass
        return True, None

    async def _get_health_details(self) -> HealthCheckDetails:
        """Get additional health details.

        Override this method to provide custom health details.

        Returns:
            Health check details with metrics and uptime
        """
        details = HealthCheckDetails()

        # Add metrics if available
        self._extract_metrics_to_details(details)

        # Add uptime if available
        self._extract_uptime_to_details(details)

        return details

    def _extract_metrics_to_details(self, details: HealthCheckDetails) -> None:
        """Extract metrics data and populate health details."""
        get_metrics_attr = getattr(self, "get_metrics", None)
        if get_metrics_attr is None or not callable(get_metrics_attr):
            return

        metrics = self._safely_get_metrics(get_metrics_attr)
        if metrics is None:
            return

        if isinstance(metrics, dict):
            # Type narrowing for pyright - metrics is dict[str, object]
            metrics_dict = cast(dict[str, object], metrics)
            self._extract_dict_metrics_to_details(metrics_dict, details)
        else:
            self._extract_object_metrics_to_details(metrics, details)

    def _safely_get_metrics(self, get_metrics_attr: Callable[[], object]) -> object | None:
        """Safely call get_metrics method.

        Args:
            get_metrics_attr: Callable to get metrics

        Returns:
            Metrics object or None if error occurs
        """
        try:
            return get_metrics_attr()
        except (RuntimeError, ValueError, TypeError, OSError, AttributeError):
            return None

    def _extract_dict_metrics_to_details(
        self, metrics: dict[str, object], details: HealthCheckDetails
    ) -> None:
        """Extract metrics from dictionary format."""
        # Extract error count
        error_count_val = metrics.get("error_count", 0)
        details.error_count = self._safe_int_convert(error_count_val)

        # Extract warning count
        warning_count_val = metrics.get("warning_count", 0)
        details.warning_count = self._safe_int_convert(warning_count_val)

    def _extract_object_metrics_to_details(
        self, metrics: object, details: HealthCheckDetails
    ) -> None:
        """Extract metrics from object format."""
        # Try to get error_count attribute
        error_count_val = getattr(metrics, "error_count", 0)
        details.error_count = self._safe_int_convert(error_count_val)

        # Try to get warning_count attribute
        warning_count_val = getattr(metrics, "warning_count", 0)
        details.warning_count = self._safe_int_convert(warning_count_val)

    def _safe_int_convert(self, value: object) -> int:
        """Safely convert value to int with fallback.

        Args:
            value: Value to convert to integer

        Returns:
            Integer value or 0 if conversion fails
        """
        convertible_types = (int, float, str)
        if isinstance(value, convertible_types):
            try:
                return int(value)
            except (ValueError, TypeError):
                return 0
        return 0

    def _extract_uptime_to_details(self, details: HealthCheckDetails) -> None:
        """Extract uptime data and populate health details."""
        try:
            start_time = getattr(self, "_start_time", None)
            if start_time is not None:
                details.uptime_seconds = time.time() - start_time
        except (AttributeError, TypeError):
            # Service doesn't have _start_time attribute
            pass


def create_health_check_decorator() -> Callable[[type[T]], type[T]]:
    """Create a decorator that adds health check capability to a class.

    Returns:
        Decorator function that adds BaseHealthCheckMixin to a class
    """

    def health_check_decorator(cls: type[T]) -> type[T]:
        """Add health check capability to a class.

        Args:
            cls: Class to enhance with health check capability

        Returns:
            New class with health check functionality
        """
        # Create a new type dynamically
        bases = (BaseHealthCheckMixin, cls)
        namespace: dict[str, Any] = {}

        # Create the new class
        health_checkable_class = type(cls.__name__, bases, namespace)

        # Preserve the original class attributes
        health_checkable_class.__module__ = cls.__module__
        health_checkable_class.__qualname__ = cls.__qualname__

        return cast(type[T], health_checkable_class)

    return health_check_decorator
