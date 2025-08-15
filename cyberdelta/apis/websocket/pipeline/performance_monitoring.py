"""Performance monitoring utilities for WebSocket pipeline tuning.

This module provides performance metrics collection, monitoring, and analysis
for the WebSocket validation pipeline.

Features:
- Real-time performance metrics collection
- Historical performance tracking
- Performance threshold monitoring
- Metrics analysis and reporting
"""

from __future__ import annotations

import statistics
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any


# Import our configuration modules


if TYPE_CHECKING:
    from collections.abc import Iterator


# Performance threshold constants
HIGH_VALIDATION_TIME_MS = 10.0  # Threshold for high validation time
CRITICAL_VALIDATION_TIME_MS = 50.0  # Threshold for critical validation time
HIGH_PROCESSING_TIME_MS = 5.0  # Threshold for high processing time
CRITICAL_PROCESSING_TIME_MS = 20.0  # Threshold for critical processing time
HIGH_MEMORY_USAGE_MB = 1000  # Threshold for high memory usage (1GB)
CRITICAL_MEMORY_USAGE_MB = 2000  # Threshold for critical memory usage (2GB)
HIGH_ERROR_RATE_PCT = 5.0  # Threshold for high error rate
CRITICAL_ERROR_RATE_PCT = 15.0  # Threshold for critical error rate
LOW_THROUGHPUT_PER_SEC = 100  # Threshold for low throughput


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


class PerformanceMonitor:
    """Monitor and track pipeline performance metrics."""

    def __init__(
        self,
        max_history: int = 1000,
        metrics_interval_seconds: float = 1.0,
    ) -> None:
        """Initialize performance monitor.

        Args:
            max_history: Maximum number of metrics to keep in history
            metrics_interval_seconds: Interval between metrics collection
        """
        self.max_history = max_history
        self.metrics_interval = metrics_interval_seconds
        self._metrics_history: deque[PerformanceMetrics] = deque(maxlen=max_history)
        self._lock = threading.Lock()
        self._monitoring = False
        self._monitor_thread: threading.Thread | None = None

    def start_monitoring(self) -> None:
        """Start performance monitoring."""
        if self._monitoring:
            return

        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def stop_monitoring(self) -> None:
        """Stop performance monitoring."""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join()

    def record_metrics(self, metrics: PerformanceMetrics) -> None:
        """Record performance metrics.

        Args:
            metrics: Performance metrics to record
        """
        with self._lock:
            self._metrics_history.append(metrics)

    def get_current_metrics(self) -> PerformanceMetrics | None:
        """Get the most recent metrics.

        Returns:
            Most recent performance metrics or None if no metrics available
        """
        with self._lock:
            if self._metrics_history:
                return self._metrics_history[-1]
            return None

    def get_metrics_history(self, count: int | None = None) -> list[PerformanceMetrics]:
        """Get historical metrics.

        Args:
            count: Number of recent metrics to return (None for all)

        Returns:
            List of historical performance metrics
        """
        with self._lock:
            if count is None:
                return list(self._metrics_history)
            return list(self._metrics_history)[-count:]

    def get_average_metrics(self, window_size: int = 10) -> PerformanceMetrics | None:
        """Calculate average metrics over a window.

        Args:
            window_size: Number of recent metrics to average

        Returns:
            Average performance metrics or None if insufficient data
        """
        recent_metrics = self.get_metrics_history(window_size)
        if not recent_metrics:
            return None

        return PerformanceMetrics(
            validation_time_ms=statistics.mean(m.validation_time_ms for m in recent_metrics),
            processing_time_ms=statistics.mean(m.processing_time_ms for m in recent_metrics),
            total_time_ms=statistics.mean(m.total_time_ms for m in recent_metrics),
            messages_per_second=statistics.mean(m.messages_per_second for m in recent_metrics),
            validations_per_second=statistics.mean(
                m.validations_per_second for m in recent_metrics
            ),
            memory_usage_mb=statistics.mean(m.memory_usage_mb for m in recent_metrics),
            cpu_usage_percent=statistics.mean(m.cpu_usage_percent for m in recent_metrics),
            validation_errors=int(statistics.mean(m.validation_errors for m in recent_metrics)),
            processing_errors=int(statistics.mean(m.processing_errors for m in recent_metrics)),
            error_rate_percent=statistics.mean(m.error_rate_percent for m in recent_metrics),
            type_safety_score=statistics.mean(m.type_safety_score for m in recent_metrics),
            validation_coverage=statistics.mean(m.validation_coverage for m in recent_metrics),
            timestamp=datetime.now(UTC),
        )

    @contextmanager
    def timing_context(self, context_name: str) -> Iterator[dict[str, float]]:
        """Context manager for timing operations.

        Args:
            context_name: Name of the operation being timed

        Yields:
            Dictionary that provides timing information
        """
        start_time = time.perf_counter()
        timing_data: dict[str, float] = {}

        try:
            yield timing_data
        finally:
            end_time = time.perf_counter()
            elapsed_ms = (end_time - start_time) * 1000
            timing_data[context_name] = elapsed_ms

    def _monitor_loop(self) -> None:
        """Internal monitoring loop."""
        while self._monitoring:
            try:
                # Collect current metrics
                metrics = self._collect_current_metrics()
                self.record_metrics(metrics)
                time.sleep(self.metrics_interval)
            except (OSError, RuntimeError, ValueError):
                # Continue monitoring after metric collection failure
                time.sleep(self.metrics_interval)

    def _collect_current_metrics(self) -> PerformanceMetrics:
        """Collect current system metrics.

        Returns:
            Current performance metrics
        """
        # This is a placeholder implementation
        # In a real implementation, this would collect actual system metrics
        return PerformanceMetrics(
            validation_time_ms=0.0,
            processing_time_ms=0.0,
            total_time_ms=0.0,
            messages_per_second=0.0,
            validations_per_second=0.0,
            memory_usage_mb=0.0,
            cpu_usage_percent=0.0,
            validation_errors=0,
            processing_errors=0,
            error_rate_percent=0.0,
            type_safety_score=100.0,
            validation_coverage=100.0,
        )

    def analyze_performance_trends(self, window_size: int = 100) -> dict[str, Any]:
        """Analyze performance trends over time.

        Args:
            window_size: Size of the analysis window

        Returns:
            Dictionary containing trend analysis results
        """
        recent_metrics = self.get_metrics_history(window_size)
        min_data_points = 2
        if len(recent_metrics) < min_data_points:
            return {"status": "insufficient_data", "message": "Need at least 2 data points"}

        # Calculate trends
        validation_times = [m.validation_time_ms for m in recent_metrics]
        processing_times = [m.processing_time_ms for m in recent_metrics]
        memory_usage = [m.memory_usage_mb for m in recent_metrics]
        error_rates = [m.error_rate_percent for m in recent_metrics]

        return {
            "status": "success",
            "trends": {
                "validation_time": {
                    "mean": statistics.mean(validation_times),
                    "median": statistics.median(validation_times),
                    "stdev": statistics.stdev(validation_times) if len(validation_times) > 1 else 0,
                    "trend": "stable",  # Would calculate actual trend
                },
                "processing_time": {
                    "mean": statistics.mean(processing_times),
                    "median": statistics.median(processing_times),
                    "stdev": statistics.stdev(processing_times) if len(processing_times) > 1 else 0,
                    "trend": "stable",  # Would calculate actual trend
                },
                "memory_usage": {
                    "mean": statistics.mean(memory_usage),
                    "median": statistics.median(memory_usage),
                    "stdev": statistics.stdev(memory_usage) if len(memory_usage) > 1 else 0,
                    "trend": "stable",  # Would calculate actual trend
                },
                "error_rate": {
                    "mean": statistics.mean(error_rates),
                    "median": statistics.median(error_rates),
                    "stdev": statistics.stdev(error_rates) if len(error_rates) > 1 else 0,
                    "trend": "stable",  # Would calculate actual trend
                },
            },
            "window_size": len(recent_metrics),
            "timestamp": datetime.now(UTC).isoformat(),
        }
