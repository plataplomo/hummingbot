"""WebSocket metrics collection system.

This module provides comprehensive metrics collection for WebSocket operations,
including message counts, error rates, processing times, and size distributions.
"""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.websocket.websocket_states import MessageProcessingResult


# Constants for histogram buckets and cleanup intervals
HISTOGRAM_BUCKET_5_SEC = 5
HISTOGRAM_BUCKET_10_SEC = 10
CLEANUP_INTERVAL_SECONDS = 60


class MetricType(StrEnum):
    """Types of metrics collected."""

    MESSAGE_COUNT = "message_count"
    ERROR_COUNT = "error_count"
    PROCESSING_TIME = "processing_time"
    MESSAGE_SIZE = "message_size"
    VALIDATION_ERROR = "validation_error"
    TRANSFORMATION_ERROR = "transformation_error"
    HANDLER_ERROR = "handler_error"
    CONNECTION_EVENT = "connection_event"


class MetricUnit(StrEnum):
    """Units for metric values."""

    COUNT = "count"
    MILLISECONDS = "ms"
    BYTES = "bytes"
    PERCENTAGE = "percent"


class MetricPoint(BaseModel):
    """A single metric data point."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metric_type: MetricType
    metric_name: str
    value: float
    unit: MetricUnit
    labels: dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True)


class MetricSummary(BaseModel):
    """Summary statistics for a metric."""

    metric_name: str
    count: int = 0
    total: float = 0
    min: float | None = None
    max: float | None = None
    average: float = Field(default=0, init=False)
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None
    unit: MetricUnit

    def model_post_init(self, __context: object, /) -> None:
        """Calculate average after initialization."""
        if self.count > 0:
            self.average = self.total / self.count
        else:
            self.average = 0


class WebSocketMetricsCollector:
    """Collects and aggregates WebSocket metrics."""

    def __init__(self, exchange_name: str, window_size: int = 300) -> None:
        """Initialize the metrics collector.

        Args:
            exchange_name: Name of the exchange for labeling
            window_size: Time window in seconds for metrics aggregation
        """
        self.exchange_name = exchange_name
        self.window_size = window_size

        # Counters
        self._message_counts: dict[str, int] = defaultdict(int)
        self._error_counts: dict[str, int] = defaultdict(int)

        # Histograms (store individual values for percentile calculation)
        self._processing_times: dict[str, list[float]] = defaultdict(list)
        self._message_sizes: dict[str, list[int]] = defaultdict(list)

        # Time series data
        self._time_series: list[MetricPoint] = []
        self._last_cleanup = time.time()

    def record_message(
        self,
        message_type: str,
        processing_time_ms: float,
        message_size: int,
        result: MessageProcessingResult = MessageProcessingResult.SUCCESS,
    ) -> None:
        """Record metrics for a processed message.

        Args:
            message_type: Type/channel of the message
            processing_time_ms: Processing time in milliseconds
            message_size: Size of the message in bytes
            result: Message processing result
        """
        # Record counts
        self._message_counts[message_type] += 1
        if not result.is_successful:
            self._error_counts[message_type] += 1

        # Record histograms
        self._processing_times[message_type].append(processing_time_ms)
        self._message_sizes[message_type].append(message_size)

        # Record time series
        timestamp = datetime.now(UTC)

        self._time_series.append(
            MetricPoint(
                timestamp=timestamp,
                metric_type=MetricType.MESSAGE_COUNT,
                metric_name="websocket_messages_total",
                value=1,
                unit=MetricUnit.COUNT,
                labels={
                    "exchange": self.exchange_name,
                    "message_type": message_type,
                    "status": "success" if result.is_successful else "error",
                },
            )
        )

        self._time_series.append(
            MetricPoint(
                timestamp=timestamp,
                metric_type=MetricType.PROCESSING_TIME,
                metric_name="websocket_processing_time",
                value=processing_time_ms,
                unit=MetricUnit.MILLISECONDS,
                labels={"exchange": self.exchange_name, "message_type": message_type},
            )
        )

        self._time_series.append(
            MetricPoint(
                timestamp=timestamp,
                metric_type=MetricType.MESSAGE_SIZE,
                metric_name="websocket_message_size",
                value=float(message_size),
                unit=MetricUnit.BYTES,
                labels={"exchange": self.exchange_name, "message_type": message_type},
            )
        )

        # Cleanup old data periodically
        self._cleanup_old_data()

    def record_error(
        self,
        error_type: str,
        message_type: str | None = None,
        error_details: str | None = None,
    ) -> None:
        """Record an error metric.

        Args:
            error_type: Type of error (validation, transformation, handler)
            message_type: Optional message type where error occurred
            error_details: Optional error details
        """
        error_key = f"{error_type}:{message_type or 'unknown'}"
        self._error_counts[error_key] += 1

        metric_type_map = {
            "validation": MetricType.VALIDATION_ERROR,
            "transformation": MetricType.TRANSFORMATION_ERROR,
            "handler": MetricType.HANDLER_ERROR,
        }

        self._time_series.append(
            MetricPoint(
                timestamp=datetime.now(UTC),
                metric_type=metric_type_map.get(error_type, MetricType.ERROR_COUNT),
                metric_name="websocket_errors_total",
                value=1,
                unit=MetricUnit.COUNT,
                labels={
                    "exchange": self.exchange_name,
                    "error_type": error_type,
                    "message_type": message_type or "unknown",
                    "details": error_details or "",
                },
            )
        )

    def record_connection_event(self, event_type: str) -> None:
        """Record a connection event.

        Args:
            event_type: Type of connection event (connected, disconnected, reconnect)
        """
        self._time_series.append(
            MetricPoint(
                timestamp=datetime.now(UTC),
                metric_type=MetricType.CONNECTION_EVENT,
                metric_name="websocket_connection_events",
                value=1,
                unit=MetricUnit.COUNT,
                labels={"exchange": self.exchange_name, "event_type": event_type},
            )
        )

    def get_summary(self, message_type: str | None = None) -> dict[str, MetricSummary]:
        """Get summary statistics for metrics.

        Args:
            message_type: Optional filter by message type

        Returns:
            Dictionary of metric summaries by metric name
        """
        summaries: dict[str, MetricSummary] = {}

        # Message count summary
        if message_type:
            count = self._message_counts.get(message_type, 0)
            summaries["message_count"] = MetricSummary(
                metric_name="message_count", count=count, total=float(count), unit=MetricUnit.COUNT
            )
        else:
            total_count = sum(self._message_counts.values())
            summaries["message_count"] = MetricSummary(
                metric_name="message_count",
                count=total_count,
                total=float(total_count),
                unit=MetricUnit.COUNT,
            )

        # Processing time summary
        processing_times = []
        if message_type and message_type in self._processing_times:
            processing_times = self._processing_times[message_type]
        else:
            processing_times = [t for times in self._processing_times.values() for t in times]

        if processing_times:
            sorted_times = sorted(processing_times)
            summaries["processing_time"] = MetricSummary(
                metric_name="processing_time",
                count=len(processing_times),
                total=sum(processing_times),
                min=sorted_times[0],
                max=sorted_times[-1],
                p50=self._percentile(sorted_times, 0.5),
                p95=self._percentile(sorted_times, 0.95),
                p99=self._percentile(sorted_times, 0.99),
                unit=MetricUnit.MILLISECONDS,
            )

        # Message size summary
        message_sizes = []
        if message_type and message_type in self._message_sizes:
            message_sizes = self._message_sizes[message_type]
        else:
            message_sizes = [s for sizes in self._message_sizes.values() for s in sizes]

        if message_sizes:
            sorted_sizes = sorted(message_sizes)
            summaries["message_size"] = MetricSummary(
                metric_name="message_size",
                count=len(message_sizes),
                total=float(sum(message_sizes)),
                min=float(sorted_sizes[0]),
                max=float(sorted_sizes[-1]),
                p50=float(self._percentile([float(x) for x in sorted_sizes], 0.5)),
                p95=float(self._percentile([float(x) for x in sorted_sizes], 0.95)),
                p99=float(self._percentile([float(x) for x in sorted_sizes], 0.99)),
                unit=MetricUnit.BYTES,
            )

        # Error rate summary
        total_messages = sum(self._message_counts.values())
        total_errors = sum(self._error_counts.values())
        if total_messages > 0:
            error_rate = (total_errors / total_messages) * 100
            summaries["error_rate"] = MetricSummary(
                metric_name="error_rate",
                count=total_errors,
                total=error_rate,
                unit=MetricUnit.PERCENTAGE,
            )

        return summaries

    def get_time_series(
        self,
        metric_type: MetricType | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[MetricPoint]:
        """Get time series data for metrics.

        Args:
            metric_type: Optional filter by metric type
            start_time: Optional start time filter
            end_time: Optional end time filter

        Returns:
            List of metric points
        """
        points = self._time_series

        if metric_type:
            points = [p for p in points if p.metric_type == metric_type]

        if start_time:
            points = [p for p in points if p.timestamp >= start_time]

        if end_time:
            points = [p for p in points if p.timestamp <= end_time]

        return points

    def reset(self) -> None:
        """Reset all metrics."""
        self._message_counts.clear()
        self._error_counts.clear()
        self._processing_times.clear()
        self._message_sizes.clear()
        self._time_series.clear()
        self._last_cleanup = time.time()

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus format.

        Returns:
            Metrics in Prometheus text format
        """
        lines: list[str] = []
        timestamp = int(time.time() * 1000)

        # Message counts
        lines.extend([
            "# HELP websocket_messages_total Total WebSocket messages processed",
            "# TYPE websocket_messages_total counter",
        ])
        for message_type, count in self._message_counts.items():
            lines.append(
                f'websocket_messages_total{{exchange="{self.exchange_name}",'
                f'message_type="{message_type}"}} {count} {timestamp}'
            )

        # Error counts
        lines.extend([
            "# HELP websocket_errors_total Total WebSocket errors",
            "# TYPE websocket_errors_total counter",
        ])
        for error_key, count in self._error_counts.items():
            error_type, message_type = error_key.split(":", 1)
            lines.append(
                f'websocket_errors_total{{exchange="{self.exchange_name}",'
                f'error_type="{error_type}",message_type="{message_type}"}} {count} {timestamp}'
            )

        # Processing time histogram
        lines.extend([
            "# HELP websocket_processing_time WebSocket message processing time",
            "# TYPE websocket_processing_time histogram",
        ])
        for message_type, times in self._processing_times.items():
            if times:
                lines.extend([
                    f'websocket_processing_time_bucket{{exchange="{self.exchange_name}",'
                    f'message_type="{message_type}",le="1"}} '
                    f"{sum(1 for t in times if t <= 1)} {timestamp}",
                    f'websocket_processing_time_bucket{{exchange="{self.exchange_name}",'
                    f'message_type="{message_type}",le="5"}} '
                    f"{sum(1 for t in times if t <= HISTOGRAM_BUCKET_5_SEC)} {timestamp}",
                    f'websocket_processing_time_bucket{{exchange="{self.exchange_name}",'
                    f'message_type="{message_type}",le="10"}} '
                    f"{sum(1 for t in times if t <= HISTOGRAM_BUCKET_10_SEC)} {timestamp}",
                    f'websocket_processing_time_bucket{{exchange="{self.exchange_name}",'
                    f'message_type="{message_type}",le="+Inf"}} {len(times)} {timestamp}',
                    f'websocket_processing_time_sum{{exchange="{self.exchange_name}",'
                    f'message_type="{message_type}"}} {sum(times)} {timestamp}',
                    f'websocket_processing_time_count{{exchange="{self.exchange_name}",'
                    f'message_type="{message_type}"}} {len(times)} {timestamp}',
                ])

        return "\n".join(lines)

    def _percentile(self, sorted_values: list[float | int], percentile: float) -> float:
        """Calculate percentile from sorted values.

        Args:
            sorted_values: List of values already sorted in ascending order
            percentile: Percentile to calculate (0.0 to 1.0)

        Returns:
            The calculated percentile value, or 0 if the list is empty.
        """
        if not sorted_values:
            return 0
        index = int((len(sorted_values) - 1) * percentile)
        if index >= len(sorted_values):
            index = len(sorted_values) - 1
        return float(sorted_values[index])

    def _cleanup_old_data(self) -> None:
        """Remove old data outside the window."""
        current_time = time.time()
        if current_time - self._last_cleanup < CLEANUP_INTERVAL_SECONDS:  # Cleanup every minute
            return

        cutoff_time = datetime.now(UTC).timestamp() - self.window_size
        cutoff_datetime = datetime.fromtimestamp(cutoff_time, tz=UTC)

        # Remove old time series data
        self._time_series = [p for p in self._time_series if p.timestamp >= cutoff_datetime]

        # Trim histogram data to last N values
        max_histogram_size = 10000
        for message_type in list(self._processing_times.keys()):
            if len(self._processing_times[message_type]) > max_histogram_size:
                times = self._processing_times[message_type]
                self._processing_times[message_type] = times[-max_histogram_size:]

        for message_type in list(self._message_sizes.keys()):
            if len(self._message_sizes[message_type]) > max_histogram_size:
                sizes = self._message_sizes[message_type]
                self._message_sizes[message_type] = sizes[-max_histogram_size:]

        self._last_cleanup = current_time
