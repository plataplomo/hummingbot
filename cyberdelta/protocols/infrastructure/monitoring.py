"""Monitoring and health check protocols."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol


if TYPE_CHECKING:
    from cyberdelta.logic.monitoring.health_monitor import ServiceType


class HealthCheckable(Protocol):
    """Protocol for services that support health checking.

    This protocol defines the interface that all monitorable services
    must implement to participate in the health monitoring system.
    """

    async def check_health(self) -> dict[str, Any]:
        """Return health status and metrics.

        Returns:
            Dictionary containing health status, metrics, and diagnostic information
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
