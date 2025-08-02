"""Health check mixin and utilities for adding health check capability."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, TypeVar, cast

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.monitoring.health.health_check_models import (
    HealthCheckable,
    HealthCheckDetails,
    HealthCheckResult,
    HealthStatus,
)


logger = get_logger(__name__)

T = TypeVar("T", bound=object)


class BaseHealthCheckMixin:
    """Mixin class to add health check capability to services."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize mixin."""
        super().__init__(*args, **kwargs)
        self._health_dependencies: dict[str, HealthCheckable] = {}

    def add_health_dependency(self, name: str, dependency: HealthCheckable) -> None:
        """Add a dependency for health checking.
        
        Args:
            name: Name to identify the dependency.
            dependency: Service instance that implements HealthCheckable.
        """
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
        """Extract metrics data and populate health details.
        
        Args:
            details: Health check details object to populate.
        """
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
        """Extract metrics from dictionary format.
        
        Args:
            metrics: Dictionary containing metric values.
            details: Health check details object to populate.
        """
        # Extract error count
        error_count_val = metrics.get("error_count", 0)
        details.error_count = self._safe_int_convert(error_count_val)

        # Extract warning count
        warning_count_val = metrics.get("warning_count", 0)
        details.warning_count = self._safe_int_convert(warning_count_val)

    def _extract_object_metrics_to_details(
        self, metrics: object, details: HealthCheckDetails
    ) -> None:
        """Extract metrics from object format.
        
        Args:
            metrics: Object containing metric attributes.
            details: Health check details object to populate.
        """
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
        """Extract uptime data and populate health details.
        
        Args:
            details: Health check details object to populate.
        """
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