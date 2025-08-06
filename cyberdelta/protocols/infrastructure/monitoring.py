"""Monitoring and health check protocols."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol


if TYPE_CHECKING:
    from cyberdelta.models.monitoring.system_health_models import ExecutionStatistics


if TYPE_CHECKING:
    from cyberdelta.enums.monitoring import ServiceType


class HealthCheckable(Protocol):
    """Protocol for services that support health checking.

    This protocol defines the interface that all monitorable services
    must implement to participate in the health monitoring system.
    """

    async def check_health(self) -> ExecutionStatistics:
        """Return health status and metrics.

        Returns:
            Typed execution statistics with health status and metrics
        """
        ...

    def get_service_type(self) -> ServiceType:
        """Return the service type for monitoring.

        Returns:
            ServiceType enum value for the service type
        """
        ...


class MetricsProvider(Protocol):
    """Protocol for services that provide metrics data.

    This protocol defines the interface that all services must implement
    to provide metrics to the metrics collection system.
    """

    async def get_metrics(self) -> dict[str, Any]:
        """Return metrics data.

        Returns:
            Dictionary containing metrics data for collection
        """
        ...
