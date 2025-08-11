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
