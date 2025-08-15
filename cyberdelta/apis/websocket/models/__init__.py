"""WebSocket models for type-safe data structures.

This package contains all Pydantic models and dataclasses used throughout
the WebSocket system, organized by domain for clear separation of concerns.
"""

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
from .performance import PerformanceConfig
from .performance_monitoring import OptimizationResult, PerformanceMetrics
from .processing import ProcessingMetrics, ProcessorMetrics


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
    "OptimizationResult",
    "PerformanceConfig",
    "PerformanceHealth",
    "PerformanceMetrics",
    "ProcessingMetrics",
    "ProcessorMetrics",
    "RecoveryAttempt",
    "SystemHealth",
]
