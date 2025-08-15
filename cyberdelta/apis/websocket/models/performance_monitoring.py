"""Performance monitoring models for WebSocket operations.

This module contains dataclasses for performance monitoring and optimization
results tracking.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


__all__ = [
    "OptimizationResult",
    "PerformanceMetrics",
]


@dataclass
class PerformanceMetrics:
    """Container for pipeline performance metrics."""

    # Timing metrics
    validation_time_ms: float = 0.0
    processing_time_ms: float = 0.0
    total_time_ms: float = 0.0

    # Throughput metrics
    messages_per_second: float = 0.0
    validations_per_second: float = 0.0

    # Resource metrics
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0

    # Error metrics
    validation_errors: int = 0
    processing_errors: int = 0
    error_rate_percent: float = 0.0

    # Quality metrics
    type_safety_score: float = 100.0
    validation_coverage: float = 100.0

    # Timestamp
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary format.

        Returns:
            Dictionary representation of performance metrics
        """
        return {
            "validation_time_ms": self.validation_time_ms,
            "processing_time_ms": self.processing_time_ms,
            "total_time_ms": self.total_time_ms,
            "messages_per_second": self.messages_per_second,
            "validations_per_second": self.validations_per_second,
            "memory_usage_mb": self.memory_usage_mb,
            "cpu_usage_percent": self.cpu_usage_percent,
            "validation_errors": self.validation_errors,
            "processing_errors": self.processing_errors,
            "error_rate_percent": self.error_rate_percent,
            "type_safety_score": self.type_safety_score,
            "validation_coverage": self.validation_coverage,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class OptimizationResult:
    """Result of pipeline optimization."""

    original_metrics: PerformanceMetrics
    optimized_metrics: PerformanceMetrics
    optimization_applied: str
    improvement_percent: float
    configuration_changes: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert optimization result to dictionary format.

        Returns:
            Dictionary representation of optimization result
        """
        return {
            "original_metrics": self.original_metrics.to_dict(),
            "optimized_metrics": self.optimized_metrics.to_dict(),
            "optimization_applied": self.optimization_applied,
            "improvement_percent": self.improvement_percent,
            "configuration_changes": self.configuration_changes,
        }
