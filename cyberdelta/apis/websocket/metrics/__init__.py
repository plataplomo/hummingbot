"""WebSocket metrics collection and monitoring systems.

This module consolidates all metrics-related functionality for WebSocket operations,
including error metrics, performance monitoring, processing metrics, and health checks.

Modules:
- error_metrics: Error metrics collection with type-safe models
- general_metrics: General WebSocket operational metrics
- processing_metrics: Message processing metrics models
- health_check: Health monitoring and system checks
"""

# Core metrics systems
from cyberdelta.apis.models.websocket.error_metrics import (
    AggregatedMetrics,
    CollectorStatistics,
    ConnectionMetrics,
    ErrorOccurrence,
    ErrorRateMetrics,
    RecoveryAttempt,
)
from cyberdelta.apis.models.websocket.general_metrics import (
    MetricPoint,
    MetricSummary,
)
from cyberdelta.apis.models.websocket.health import (
    ComponentStatus,
    HealthCheckConfig,
    SystemHealth,
)
from cyberdelta.apis.models.websocket.processing import (
    ProcessingMetrics,
    ProcessorMetrics,
)

from .error_metrics import (
    MetricsAggregator,
    WebSocketErrorMetrics,
)
from .general_metrics import (
    WebSocketMetricsCollector,
)
from .health_check import (
    WebSocketErrorHealthCheck,
)


__all__ = [
    "AggregatedMetrics",
    "CollectorStatistics",
    "ComponentStatus",
    "ConnectionMetrics",
    "ErrorOccurrence",
    "ErrorRateMetrics",
    "HealthCheckConfig",
    "MetricPoint",
    "MetricSummary",
    "MetricsAggregator",
    "ProcessingMetrics",
    "ProcessorMetrics",
    "RecoveryAttempt",
    "SystemHealth",
    "WebSocketErrorHealthCheck",
    "WebSocketErrorMetrics",
    "WebSocketMetricsCollector",
]
