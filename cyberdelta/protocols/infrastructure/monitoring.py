"""Monitoring and health check protocols."""

from __future__ import annotations

from typing import Any, Protocol


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

    def get_service_type(self) -> str:
        """Return the service type for monitoring.

        Returns:
            String identifier for the service type (e.g., "portfolio_service")
        """
        ...
