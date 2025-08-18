"""WebSocket models for type-safe data structures.

This package contains all Pydantic models and dataclasses used throughout
the WebSocket system, organized by domain for clear separation of concerns.
"""

from .error_context import StreamErrorContext
from .error_events import (
    ConnectionHealthEvent,
    ErrorEventMetadata,
    RecoveryAttemptEvent,
    SystemHealthEvent,
    WebSocketErrorEvent,
)
from .error_metrics import (
    AggregatedMetrics,
    CollectorStatistics,
    ConnectionMetrics,
    ErrorOccurrence,
    ErrorRateMetrics,
    RecoveryAttempt,
)
from .general_metrics import MetricPoint, MetricSummary
from .health import (
    ComponentStatus,
    HealthCheckConfig,
    PerformanceHealth,
    SystemHealth,
)
from .processing import ProcessingMetrics, ProcessorMetrics


__all__ = [
    "AggregatedMetrics",
    "CollectorStatistics",
    "ComponentStatus",
    "ConnectionHealthEvent",
    "ConnectionMetrics",
    "ErrorEventMetadata",
    "ErrorOccurrence",
    "ErrorRateMetrics",
    "HealthCheckConfig",
    "MetricPoint",
    "MetricSummary",
    "PerformanceHealth",
    "ProcessingMetrics",
    "ProcessorMetrics",
    "RecoveryAttempt",
    "RecoveryAttemptEvent",
    "StreamErrorContext",
    "SystemHealth",
    "SystemHealthEvent",
    "WebSocketErrorEvent",
]
