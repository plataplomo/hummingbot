"""WebSocket Error System Metrics Collector.

This module provides comprehensive metrics collection for the WebSocket error system,
enabling production monitoring and observability.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError


class MetricType(Enum):
    """Types of metrics collected."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMING = "timing"


@dataclass
class ErrorMetric:
    """Individual error metric data."""

    timestamp_ms: int
    error_code: WebSocketErrorCode
    severity: ErrorSeverity
    recovery_strategy: WebSocketRecoveryStrategy
    exchange: str
    channel: str | None = None
    processing_time_us: float | None = None
    recovery_time_ms: float | None = None
    retry_count: int = 0
    success: bool = True


@dataclass
class MetricsSummary:
    """Summary of collected metrics."""

    total_errors: int = 0
    errors_by_code: dict[str, int] = field(default_factory=lambda: dict[str, int]())
    errors_by_severity: dict[str, int] = field(default_factory=lambda: dict[str, int]())
    errors_by_exchange: dict[str, int] = field(default_factory=lambda: dict[str, int]())
    recovery_attempts: int = 0
    successful_recoveries: int = 0
    failed_recoveries: int = 0
    avg_processing_time_us: float = 0.0
    avg_recovery_time_ms: float = 0.0
    error_rate_per_second: float = 0.0
    peak_error_rate: float = 0.0
    last_error_timestamp_ms: int | None = None


class WebSocketErrorMetricsCollector:
    """Collects and aggregates metrics for WebSocket error system."""

    def __init__(self, window_size_minutes: int = 5) -> None:
        """Initialize metrics collector.

        Args:
            window_size_minutes: Time window for rate calculations
        """
        self.window_size_minutes = window_size_minutes
        self.metrics: list[ErrorMetric] = []
        self.start_time_ms = int(time.time() * 1000)

        # Counters
        self._error_counts: dict[str, int] = defaultdict(int)
        self._severity_counts: dict[str, int] = defaultdict(int)
        self._exchange_counts: dict[str, int] = defaultdict(int)
        self._recovery_counts: dict[str, int] = defaultdict(int)

        # Timing data
        self._processing_times: list[float] = []
        self._recovery_times: list[float] = []

        # Rate tracking
        self._error_timestamps_ms: list[int] = []
        self._peak_rate = 0.0

    def record_error(
        self,
        error: WebSocketStreamError,
        processing_time_us: float | None = None,
        recovery_time_ms: float | None = None,
        retry_count: int = 0,
        success: bool = True,
    ) -> None:
        """Record an error occurrence.

        Args:
            error: The WebSocket error that occurred
            processing_time_us: Time to process error in microseconds
            recovery_time_ms: Time to recover in milliseconds
            retry_count: Number of retry attempts
            success: Whether recovery was successful
        """
        timestamp_ms = int(time.time() * 1000)

        metric = ErrorMetric(
            timestamp_ms=timestamp_ms,
            error_code=error.code,
            severity=error.severity,
            recovery_strategy=error.recovery_strategy,
            exchange=error.context.exchange,
            channel=error.context.channel,
            processing_time_us=processing_time_us,
            recovery_time_ms=recovery_time_ms,
            retry_count=retry_count,
            success=success,
        )

        self.metrics.append(metric)
        self._update_counters(metric)
        self._update_timings(metric)
        self._update_rates(timestamp_ms)
        self._cleanup_old_metrics()

    def _update_counters(self, metric: ErrorMetric) -> None:
        """Update counter metrics."""
        self._error_counts[metric.error_code.name] += 1
        self._severity_counts[metric.severity.name] += 1
        self._exchange_counts[metric.exchange] += 1

        if metric.recovery_strategy != WebSocketRecoveryStrategy.NONE:
            self._recovery_counts["attempts"] += 1
            if metric.success:
                self._recovery_counts["successful"] += 1
            else:
                self._recovery_counts["failed"] += 1

    def _update_timings(self, metric: ErrorMetric) -> None:
        """Update timing metrics."""
        if metric.processing_time_us is not None:
            self._processing_times.append(metric.processing_time_us)
            # Keep only recent timings (last 1000)
            if len(self._processing_times) > 1000:
                self._processing_times.pop(0)

        if metric.recovery_time_ms is not None:
            self._recovery_times.append(metric.recovery_time_ms)
            # Keep only recent timings (last 1000)
            if len(self._recovery_times) > 1000:
                self._recovery_times.pop(0)

    def _update_rates(self, timestamp_ms: int) -> None:
        """Update rate metrics."""
        self._error_timestamps_ms.append(timestamp_ms)

        # Calculate current rate
        current_rate = self._calculate_error_rate()
        self._peak_rate = max(self._peak_rate, current_rate)

    def _calculate_error_rate(self) -> float:
        """Calculate current error rate per second."""
        if not self._error_timestamps_ms:
            return 0.0

        current_time_ms = int(time.time() * 1000)
        window_start_ms = current_time_ms - (60 * 1000)  # Last minute

        recent_errors = [ts for ts in self._error_timestamps_ms if ts > window_start_ms]

        if not recent_errors:
            return 0.0

        # Errors per second in the last minute
        return len(recent_errors) / 60.0

    def _cleanup_old_metrics(self) -> None:
        """Remove metrics outside the time window."""
        current_time_ms = int(time.time() * 1000)
        cutoff_time_ms = current_time_ms - (self.window_size_minutes * 60 * 1000)

        # Keep only recent metrics
        self.metrics = [m for m in self.metrics if m.timestamp_ms > cutoff_time_ms]

        # Clean up timestamps for rate calculation
        self._error_timestamps_ms = [ts for ts in self._error_timestamps_ms if ts > cutoff_time_ms]

    @property
    def has_errors(self) -> bool:
        """Check if any errors have been collected.

        Returns:
            True if errors have been collected, False otherwise
        """
        return len(self._error_timestamps_ms) > 0 or sum(self._error_counts.values()) > 0

    def get_summary(self) -> MetricsSummary:
        """Get summary of collected metrics.

        Returns:
            Summary of all collected metrics
        """
        total_errors = sum(self._error_counts.values())

        # Calculate averages
        avg_processing = (
            sum(self._processing_times) / len(self._processing_times)
            if self._processing_times
            else 0.0
        )
        avg_recovery = (
            sum(self._recovery_times) / len(self._recovery_times) if self._recovery_times else 0.0
        )

        return MetricsSummary(
            total_errors=total_errors,
            errors_by_code=dict(self._error_counts),
            errors_by_severity=dict(self._severity_counts),
            errors_by_exchange=dict(self._exchange_counts),
            recovery_attempts=self._recovery_counts.get("attempts", 0),
            successful_recoveries=self._recovery_counts.get("successful", 0),
            failed_recoveries=self._recovery_counts.get("failed", 0),
            avg_processing_time_us=avg_processing,
            avg_recovery_time_ms=avg_recovery,
            error_rate_per_second=self._calculate_error_rate(),
            peak_error_rate=self._peak_rate,
            last_error_timestamp_ms=(self.metrics[-1].timestamp_ms if self.metrics else None),
        )

    def get_metrics_by_timerange(self, start_time_ms: int, end_time_ms: int) -> list[ErrorMetric]:
        """Get metrics within a specific time range.

        Args:
            start_time_ms: Start of time range in milliseconds
            end_time_ms: End of time range in milliseconds

        Returns:
            List of metrics within the time range
        """
        return [m for m in self.metrics if start_time_ms <= m.timestamp_ms <= end_time_ms]

    def get_error_distribution(self) -> dict[str, float]:
        """Get percentage distribution of errors by code.

        Returns:
            Dictionary of error code to percentage
        """
        total = sum(self._error_counts.values())
        if total == 0:
            return {}

        return {code: (count / total) * 100 for code, count in self._error_counts.items()}

    def get_recovery_success_rate(self) -> float:
        """Get recovery success rate as percentage.

        Returns:
            Success rate percentage (0-100)
        """
        attempts = self._recovery_counts.get("attempts", 0)
        if attempts == 0:
            return 100.0  # No attempts means no failures

        successful = self._recovery_counts.get("successful", 0)
        return (successful / attempts) * 100

    def get_exchange_error_rates(self) -> dict[str, float]:
        """Get error rates by exchange.

        Returns:
            Dictionary of exchange to errors per minute
        """
        current_time_ms = int(time.time() * 1000)
        window_start_ms = current_time_ms - (60 * 1000)  # Last minute

        exchange_counts: dict[str, int] = defaultdict(int)
        for metric in self.metrics:
            if metric.timestamp_ms > window_start_ms:
                exchange_counts[metric.exchange] += 1

        # Convert to per-minute rates
        return {exchange: count for exchange, count in exchange_counts.items()}

    def get_severity_distribution(self) -> dict[str, float]:
        """Get percentage distribution by severity.

        Returns:
            Dictionary of severity to percentage
        """
        total = sum(self._severity_counts.values())
        if total == 0:
            return {}

        return {
            severity: (count / total) * 100 for severity, count in self._severity_counts.items()
        }

    def reset_metrics(self) -> None:
        """Reset all collected metrics."""
        self.metrics.clear()
        self._error_counts.clear()
        self._severity_counts.clear()
        self._exchange_counts.clear()
        self._recovery_counts.clear()
        self._processing_times.clear()
        self._recovery_times.clear()
        self._error_timestamps_ms.clear()
        self._peak_rate = 0.0
        self.start_time_ms = int(time.time() * 1000)

    def export_metrics(self) -> dict[str, Any]:
        """Export metrics in a format suitable for monitoring systems.

        Returns:
            Dictionary of metrics for export
        """
        summary = self.get_summary()

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "window_size_minutes": self.window_size_minutes,
            "total_errors": summary.total_errors,
            "error_rate_per_second": summary.error_rate_per_second,
            "peak_error_rate": summary.peak_error_rate,
            "recovery_success_rate": self.get_recovery_success_rate(),
            "errors_by_code": summary.errors_by_code,
            "errors_by_severity": summary.errors_by_severity,
            "errors_by_exchange": summary.errors_by_exchange,
            "performance": {
                "avg_processing_time_us": summary.avg_processing_time_us,
                "avg_recovery_time_ms": summary.avg_recovery_time_ms,
            },
            "recovery": {
                "attempts": summary.recovery_attempts,
                "successful": summary.successful_recoveries,
                "failed": summary.failed_recoveries,
            },
            "distributions": {
                "error_codes": self.get_error_distribution(),
                "severities": self.get_severity_distribution(),
            },
            "exchange_rates": self.get_exchange_error_rates(),
        }


class MetricsAggregator:
    """Aggregates metrics from multiple collectors."""

    def __init__(self) -> None:
        """Initialize metrics aggregator."""
        self.collectors: dict[str, WebSocketErrorMetricsCollector] = {}

    def register_collector(self, name: str, collector: WebSocketErrorMetricsCollector) -> None:
        """Register a metrics collector.

        Args:
            name: Name for the collector
            collector: The metrics collector instance
        """
        self.collectors[name] = collector

    def get_global_summary(self) -> dict[str, Any]:
        """Get aggregated summary from all collectors.

        Returns:
            Aggregated metrics from all collectors
        """
        if not self.collectors:
            return {}

        total_errors = 0
        total_recovery_attempts = 0
        total_successful_recoveries = 0
        all_processing_times: list[float] = []
        all_recovery_times: list[float] = []

        for collector in self.collectors.values():
            summary = collector.get_summary()
            total_errors += summary.total_errors
            total_recovery_attempts += summary.recovery_attempts
            total_successful_recoveries += summary.successful_recoveries

            if summary.avg_processing_time_us > 0:
                all_processing_times.append(summary.avg_processing_time_us)
            if summary.avg_recovery_time_ms > 0:
                all_recovery_times.append(summary.avg_recovery_time_ms)

        global_recovery_rate = (
            (total_successful_recoveries / total_recovery_attempts * 100)
            if total_recovery_attempts > 0
            else 100.0
        )

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "collectors": list(self.collectors.keys()),
            "global_stats": {
                "total_errors": total_errors,
                "total_recovery_attempts": total_recovery_attempts,
                "global_recovery_success_rate": global_recovery_rate,
                "avg_processing_time_us": (
                    sum(all_processing_times) / len(all_processing_times)
                    if all_processing_times
                    else 0.0
                ),
                "avg_recovery_time_ms": (
                    sum(all_recovery_times) / len(all_recovery_times) if all_recovery_times else 0.0
                ),
            },
            "per_collector": {
                name: collector.export_metrics() for name, collector in self.collectors.items()
            },
        }
