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
from cyberdelta.apis.websocket.models import (
    AggregatedMetrics,
    CollectorStatistics,
    ComponentStatus,
    ConnectionMetrics,
    ErrorOccurrence,
    ErrorRateMetrics,
    HealthCheckConfig,
    MetricPoint,
    MetricSummary,
    ProcessingMetrics,
    ProcessorMetrics,
    RecoveryAttempt,
    SystemHealth,
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
