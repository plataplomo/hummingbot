"""WebSocket metrics collection and monitoring systems.

This module consolidates all metrics-related functionality for WebSocket operations,
including error metrics, performance monitoring, processing metrics, and health checks.

Modules:
- error_metrics: Error metrics collection with type-safe models
- general_metrics: General WebSocket operational metrics
- processing_metrics: Message processing metrics models
- health_check: Health monitoring and system checks
- performance: Performance optimization and monitoring
- performance_configs: Optimized configurations for performance
- performance_integration: Integrated performance monitoring layer
- performance_monitoring: Pipeline performance monitoring utilities
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
    PerformanceConfig,
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

# Performance monitoring
from .performance_configs import (
    HighFrequencyModelConfig,
    InternalModelConfig,
    RawAPIModelConfig,
)


__all__ = [
    "AggregatedMetrics",
    "CollectorStatistics",
    "ComponentStatus",
    "ConnectionMetrics",
    "ErrorOccurrence",
    "ErrorRateMetrics",
    "HealthCheckConfig",
    "HighFrequencyModelConfig",
    "InternalModelConfig",
    "MetricPoint",
    "MetricSummary",
    "MetricsAggregator",
    "PerformanceConfig",
    "ProcessingMetrics",
    "ProcessorMetrics",
    "RawAPIModelConfig",
    "RecoveryAttempt",
    "SystemHealth",
    "WebSocketErrorHealthCheck",
    "WebSocketErrorMetrics",
    "WebSocketMetricsCollector",
]
