"""Monitoring and performance tracking exceptions for CyberDelta.

Domain-specific exceptions for monitoring, metrics, and performance tracking.
"""

from typing import Any


class MonitoringError(RuntimeError):
    """Base class for monitoring-related errors."""

    def __init__(
        self,
        message: str,
        *,
        component: str | None = None,
        metric: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Initialize monitoring error.

        Args:
            message: Human-readable error description
            component: Component that caused the error
            metric: Metric that failed
            metadata: Additional error context
        """
        super().__init__(message)
        self.component = component
        self.metric = metric
        self.metadata = metadata or {}


class InvalidMetricError(MonitoringError):
    """Raised when a metric value is invalid or out of bounds."""

    def __init__(self, metric_name: str, value: object, reason: str) -> None:
        """Initialize invalid metric error.

        Args:
            metric_name: Name of the invalid metric
            value: The invalid value
            reason: Why the value is invalid
        """
        message = f"Invalid metric {metric_name}: {reason}"
        super().__init__(message, metric=metric_name, metadata={"value": value, "reason": reason})


class MetricCalculationError(MonitoringError):
    """Raised when metric calculation fails."""

    def __init__(self, metric_name: str, calculation: str) -> None:
        """Initialize metric calculation error.

        Args:
            metric_name: Name of the metric
            calculation: What calculation failed
        """
        message = f"Failed to calculate {metric_name}: {calculation}"
        super().__init__(message, metric=metric_name)


class ServiceNotRegisteredError(MonitoringError):
    """Raised when an unregistered service is accessed."""

    def __init__(self, service_name: str, registered_services: list[str]) -> None:
        """Initialize exception with service details.

        Args:
            service_name: Name of the unregistered service
            registered_services: List of registered service names
        """
        self.service_name = service_name
        self.registered_services = registered_services
        message = f"Service '{service_name}' is not registered"
        if registered_services:
            message += f". Registered services: {', '.join(registered_services)}"
        super().__init__(message, component=service_name)


class ServiceAlreadyRegisteredError(MonitoringError):
    """Raised when trying to register a service that already exists."""

    def __init__(self, service_name: str) -> None:
        """Initialize exception with service name.

        Args:
            service_name: Name of the already registered service
        """
        self.service_name = service_name
        super().__init__(f"Service '{service_name}' is already registered", component=service_name)


class HealthMonitorAlreadyRunningError(MonitoringError):
    """Raised when trying to start a health monitor that is already running."""

    def __init__(self) -> None:
        """Initialize exception."""
        super().__init__("Health monitor is already running")
