"""Type-safe metrics collection for WebSocket errors.

Provides comprehensive metrics collection for WebSocket error events,
recovery attempts, and system health monitoring with full type safety.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.exceptions import WebSocketStreamError
from cyberdelta.apis.websocket.models.error_metrics import (
    AggregatedMetrics,
    CollectorStatistics,
    ConnectionMetrics,
    ErrorOccurrence,
    ErrorRateMetrics,
    RecoveryAttempt,
)
from cyberdelta.config.models.websocket_error_config import WebSocketErrorMetricsConfig


if TYPE_CHECKING:
    from logging import Logger


# ============================================================================
# Metrics Collector Implementation
# ============================================================================


class WebSocketErrorMetrics:
    """Type-safe metrics collector for WebSocket errors.

    Collects and aggregates metrics for WebSocket errors, recovery attempts,
    and connection health with full type safety and configurable collection.
    """

    def __init__(
        self,
        config: WebSocketErrorMetricsConfig,
        logger: Logger | None = None,
    ) -> None:
        """Initialize the metrics collector.

        Args:
            config: Metrics collection configuration
            logger: Optional logger for metrics operations
        """
        self.config = config
        self._logger = logger

        # Error occurrence tracking
        self._error_occurrences: deque[ErrorOccurrence] = deque(maxlen=config.metrics_buffer_size)

        # Recovery attempt tracking
        self._recovery_attempts: deque[RecoveryAttempt] = deque(maxlen=config.metrics_buffer_size)

        # Connection tracking
        self._connection_metrics: dict[str, ConnectionMetrics] = {}

        # Error rate buckets for time-series analysis
        self._error_rate_buckets: deque[ErrorRateMetrics] = deque(maxlen=config.error_rate_buckets)

        # Error chain tracking (for root cause analysis)
        self._error_chains: dict[str, list[ErrorOccurrence]] = defaultdict(list)

        # Statistics
        self._total_errors_recorded = 0
        self._total_recoveries_recorded = 0
        self._collection_start_time = time.time()

        if self._logger:
            self._logger.info(
                "WebSocket error metrics collector initialized with buffer_size=%d",
                config.metrics_buffer_size,
            )

    def record_error(
        self,
        error: WebSocketStreamError,
        recovery_successful: bool = False,
        recovery_duration_ms: int | None = None,
    ) -> None:
        """Record an error occurrence.

        Args:
            error: The WebSocket error that occurred
            recovery_successful: Whether recovery was successful
            recovery_duration_ms: Duration of recovery attempt
        """
        if not self.config.enable_metrics_collection:
            return

        # Create error occurrence record
        occurrence = ErrorOccurrence(
            timestamp_ms=int(error.timestamp.timestamp() * 1000),
            exchange=error.context.exchange,
            error_code=error.code,
            severity=error.severity,
            connection_id=error.context.connection_id,
            channel=error.context.channel,
            recovery_strategy=error.recovery_strategy,
            recovery_successful=recovery_successful,
            recovery_duration_ms=recovery_duration_ms,
        )

        self._error_occurrences.append(occurrence)
        self._total_errors_recorded += 1

        # Track error chains for root cause analysis
        if self.config.track_error_chains:
            chain_key = f"{error.context.connection_id}_{error.context.channel or 'global'}"
            self._error_chains[chain_key].append(occurrence)

            # Limit chain depth
            if len(self._error_chains[chain_key]) > self.config.max_error_chain_depth:
                self._error_chains[chain_key] = self._error_chains[chain_key][
                    -self.config.max_error_chain_depth :
                ]

        # Update connection metrics
        self._update_connection_error_count(error.context.connection_id)

        if self._logger:
            self._logger.debug(
                "Recorded error: %s on %s (connection: %s, recovery: %s)",
                error.code.name,
                error.context.exchange,
                error.context.connection_id,
                "successful" if recovery_successful else "failed",
            )

    def record_recovery_attempt(
        self,
        exchange: str,
        connection_id: str,
        strategy: WebSocketRecoveryStrategy,
        attempt_number: int,
        successful: bool,
        duration_ms: int,
        error_count_before: int = 0,
    ) -> None:
        """Record a recovery attempt.

        Args:
            exchange: Exchange name
            connection_id: Connection identifier
            strategy: Recovery strategy used
            attempt_number: Attempt number (1-based)
            successful: Whether recovery was successful
            duration_ms: Recovery duration in milliseconds
            error_count_before: Error count before recovery
        """
        if not self.config.enable_metrics_collection or not self.config.track_recovery_times:
            return

        # Create recovery attempt record
        attempt = RecoveryAttempt(
            timestamp_ms=int(time.time() * 1000),
            exchange=exchange,
            connection_id=connection_id,
            strategy=strategy,
            attempt_number=attempt_number,
            successful=successful,
            duration_ms=duration_ms,
            error_count_before=error_count_before,
        )

        self._recovery_attempts.append(attempt)
        self._total_recoveries_recorded += 1

        # Update connection metrics
        self._update_connection_recovery_count(connection_id, successful)

        if self._logger:
            self._logger.debug(
                "Recorded recovery attempt #%d: %s on %s "
                "(connection: %s, successful: %s, duration: %dms)",
                attempt_number,
                strategy.name,
                exchange,
                connection_id,
                successful,
                duration_ms,
            )

    def start_connection_tracking(
        self,
        connection_id: str,
        exchange: str,
    ) -> None:
        """Start tracking metrics for a connection.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name
        """
        if not self.config.enable_metrics_collection or not self.config.track_connection_durations:
            return

        metrics = ConnectionMetrics(
            connection_id=connection_id,
            exchange=exchange,
            connection_start_ms=int(time.time() * 1000),
        )

        self._connection_metrics[connection_id] = metrics

        if self._logger:
            self._logger.debug("Started connection tracking: %s on %s", connection_id, exchange)

    def end_connection_tracking(self, connection_id: str) -> None:
        """End tracking metrics for a connection.

        Args:
            connection_id: Connection identifier
        """
        if (
            not self.config.enable_metrics_collection
            or connection_id not in self._connection_metrics
        ):
            return

        # Update connection end time
        current_metrics = self._connection_metrics[connection_id]
        updated_metrics = current_metrics.model_copy(
            update={"connection_end_ms": int(time.time() * 1000)}
        )
        self._connection_metrics[connection_id] = updated_metrics

        if self._logger:
            duration_ms = (
                updated_metrics.connection_end_ms or 0
            ) - updated_metrics.connection_start_ms
            self._logger.debug(
                "Ended connection tracking: %s (duration: %dms)", connection_id, duration_ms
            )

    def update_error_rate_bucket(self) -> None:
        """Update the current error rate bucket with recent data."""
        if not self.config.enable_metrics_collection or not self.config.track_error_rates:
            return

        current_time_ms = int(time.time() * 1000)
        window_duration_ms = self.config.error_rate_window_ms // self.config.error_rate_buckets
        window_start_ms = current_time_ms - window_duration_ms

        # Count events in the current window
        total_events = 0
        error_events = 0

        for occurrence in self._error_occurrences:
            if occurrence.timestamp_ms >= window_start_ms:
                total_events += 1
                error_events += 1

        # Count successful messages (approximation)
        # In a real implementation, you'd track message counts separately
        estimated_total_events = max(total_events, error_events * 10)  # Rough estimate

        error_rate = error_events / estimated_total_events if estimated_total_events > 0 else 0.0

        bucket = ErrorRateMetrics(
            window_start_ms=window_start_ms,
            window_end_ms=current_time_ms,
            window_duration_ms=window_duration_ms,
            total_events=estimated_total_events,
            error_events=error_events,
            error_rate=error_rate,
        )

        self._error_rate_buckets.append(bucket)

    def get_aggregated_metrics(
        self,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> AggregatedMetrics:
        """Get aggregated metrics for a time period.

        Args:
            start_time_ms: Start time in milliseconds (default: 1 hour ago)
            end_time_ms: End time in milliseconds (default: now)

        Returns:
            Aggregated metrics for the specified period
        """
        if end_time_ms is None:
            end_time_ms = int(time.time() * 1000)

        if start_time_ms is None:
            start_time_ms = end_time_ms - (60 * 60 * 1000)  # 1 hour ago

        # Filter data to time period
        period_errors = [
            err
            for err in self._error_occurrences
            if start_time_ms <= err.timestamp_ms <= end_time_ms
        ]

        period_recoveries = [
            rec
            for rec in self._recovery_attempts
            if start_time_ms <= rec.timestamp_ms <= end_time_ms
        ]

        # Calculate aggregated metrics
        metrics = AggregatedMetrics(
            start_timestamp_ms=start_time_ms,
            end_timestamp_ms=end_time_ms,
            duration_ms=end_time_ms - start_time_ms,
        )

        # Error counts by code
        for error in period_errors:
            code_name = error.error_code.name
            metrics.error_counts_by_code[code_name] = (
                metrics.error_counts_by_code.get(code_name, 0) + 1
            )

        # Error counts by exchange
        for error in period_errors:
            exchange = error.exchange
            metrics.error_counts_by_exchange[exchange] = (
                metrics.error_counts_by_exchange.get(exchange, 0) + 1
            )

        # Recovery metrics
        for recovery in period_recoveries:
            strategy_name = recovery.strategy.name
            metrics.recovery_attempts_by_strategy[strategy_name] = (
                metrics.recovery_attempts_by_strategy.get(strategy_name, 0) + 1
            )

            if recovery.successful:
                metrics.successful_recoveries_by_strategy[strategy_name] = (
                    metrics.successful_recoveries_by_strategy.get(strategy_name, 0) + 1
                )

        # Timing metrics
        recovery_durations = [rec.duration_ms for rec in period_recoveries]
        if recovery_durations:
            metrics.average_recovery_duration_ms = sum(recovery_durations) / len(recovery_durations)
            sorted_durations = sorted(recovery_durations)
            median_index = len(sorted_durations) // 2
            metrics.median_recovery_duration_ms = float(sorted_durations[median_index])

        # Connection metrics
        active_connections = [
            conn
            for conn in self._connection_metrics.values()
            if conn.connection_end_ms is None or conn.connection_end_ms > start_time_ms
        ]

        metrics.total_connections = len(self._connection_metrics)
        metrics.active_connections = len(active_connections)

        if self._connection_metrics:
            connection_durations = [
                (conn.connection_end_ms or end_time_ms) - conn.connection_start_ms
                for conn in self._connection_metrics.values()
            ]
            metrics.average_connection_duration_ms = sum(connection_durations) / len(
                connection_durations
            )

        # Overall health
        total_events = len(period_errors) + max(len(period_errors) * 50, 100)  # Rough estimate
        metrics.overall_error_rate = len(period_errors) / total_events if total_events > 0 else 0.0

        total_recovery_attempts = len(period_recoveries)
        successful_recoveries = sum(1 for rec in period_recoveries if rec.successful)
        metrics.recovery_success_rate = (
            successful_recoveries / total_recovery_attempts if total_recovery_attempts > 0 else 0.0
        )

        return metrics

    def get_error_chains(
        self,
        connection_id: str | None = None,
        min_chain_length: int = 2,
    ) -> dict[str, list[ErrorOccurrence]]:
        """Get error chains for root cause analysis.

        Args:
            connection_id: Optional connection ID to filter by
            min_chain_length: Minimum chain length to include

        Returns:
            Dictionary of error chains by key
        """
        if not self.config.track_error_chains:
            return {}

        filtered_chains: dict[str, list[ErrorOccurrence]] = {}

        for key, chain in self._error_chains.items():
            # Filter by connection ID if specified
            if connection_id and not key.startswith(connection_id):
                continue

            # Filter by minimum chain length
            if len(chain) < min_chain_length:
                continue

            filtered_chains[key] = list(chain)

        return filtered_chains

    def get_statistics(self) -> CollectorStatistics:
        """Get collector statistics.

        Returns:
            Typed collector statistics
        """
        uptime_seconds = time.time() - self._collection_start_time

        return CollectorStatistics(
            uptime_seconds=int(uptime_seconds),
            total_errors_recorded=self._total_errors_recorded,
            total_recoveries_recorded=self._total_recoveries_recorded,
            current_error_buffer_size=len(self._error_occurrences),
            current_recovery_buffer_size=len(self._recovery_attempts),
            active_connections=len([
                conn for conn in self._connection_metrics.values() if conn.connection_end_ms is None
            ]),
            total_connections_tracked=len(self._connection_metrics),
            error_chains_tracked=len(self._error_chains),
            error_rate_buckets=len(self._error_rate_buckets),
            collection_enabled=self.config.enable_metrics_collection,
        )

    def clear_metrics(self) -> None:
        """Clear all collected metrics data."""
        self._error_occurrences.clear()
        self._recovery_attempts.clear()
        self._connection_metrics.clear()
        self._error_rate_buckets.clear()
        self._error_chains.clear()

        self._total_errors_recorded = 0
        self._total_recoveries_recorded = 0
        self._collection_start_time = time.time()

        if self._logger:
            self._logger.info("Cleared all metrics data")

    def export_metrics(self) -> dict[str, Any]:
        """Export metrics in a format suitable for monitoring systems.

        Returns:
            Dictionary of metrics for export
        """
        summary = self.get_aggregated_metrics()

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "collection_enabled": self.config.enable_metrics_collection,
            "total_errors": len(self._error_occurrences),
            "error_rate_per_second": self._calculate_current_error_rate(),
            "recovery_success_rate": self.get_recovery_success_rate(),
            "errors_by_code": summary.error_counts_by_code,
            "errors_by_exchange": summary.error_counts_by_exchange,
            "performance": {
                "avg_recovery_duration_ms": summary.average_recovery_duration_ms,
                "median_recovery_duration_ms": summary.median_recovery_duration_ms,
            },
            "recovery": {
                "attempts_by_strategy": summary.recovery_attempts_by_strategy,
                "successful_by_strategy": summary.successful_recoveries_by_strategy,
            },
            "distributions": {
                "error_codes": self.get_error_distribution(),
                "severities": self.get_severity_distribution(),
            },
            "exchange_rates": self.get_exchange_error_rates(),
            "connections": {
                "total": summary.total_connections,
                "active": summary.active_connections,
                "avg_duration_ms": summary.average_connection_duration_ms,
            },
            "health": {
                "overall_error_rate": summary.overall_error_rate,
                "recovery_success_rate": summary.recovery_success_rate,
            },
        }

    def get_error_distribution(self) -> dict[str, float]:
        """Get percentage distribution of errors by code.

        Returns:
            Dictionary of error code to percentage
        """
        if not self._error_occurrences:
            return {}

        code_counts: dict[str, int] = defaultdict(int)
        for error in self._error_occurrences:
            code_counts[error.error_code.name] += 1

        total = sum(code_counts.values())
        return {code: (count / total) * 100 for code, count in code_counts.items()}

    def get_severity_distribution(self) -> dict[str, float]:
        """Get percentage distribution by severity.

        Returns:
            Dictionary of severity to percentage
        """
        if not self._error_occurrences:
            return {}

        severity_counts: dict[str, int] = defaultdict(int)
        for error in self._error_occurrences:
            severity_counts[error.severity.name] += 1

        total = sum(severity_counts.values())
        return {severity: (count / total) * 100 for severity, count in severity_counts.items()}

    def get_recovery_success_rate(self) -> float:
        """Get recovery success rate as percentage.

        Returns:
            Success rate percentage (0-100)
        """
        if not self._recovery_attempts:
            return 100.0  # No attempts means no failures

        successful = sum(1 for attempt in self._recovery_attempts if attempt.successful)
        return (successful / len(self._recovery_attempts)) * 100

    def get_exchange_error_rates(self) -> dict[str, float]:
        """Get error rates by exchange in the last minute.

        Returns:
            Dictionary of exchange to errors per minute
        """
        current_time_ms = int(time.time() * 1000)
        window_start_ms = current_time_ms - (60 * 1000)  # Last minute

        exchange_counts: dict[str, int] = defaultdict(int)
        for error in self._error_occurrences:
            if error.timestamp_ms > window_start_ms:
                exchange_counts[error.exchange] += 1

        return dict(exchange_counts)

    def _calculate_current_error_rate(self) -> float:
        """Calculate current error rate per second.

        Returns:
            Current error rate per second
        """
        if not self._error_occurrences:
            return 0.0

        current_time_ms = int(time.time() * 1000)
        window_start_ms = current_time_ms - (60 * 1000)  # Last minute

        recent_errors = [
            error for error in self._error_occurrences if error.timestamp_ms > window_start_ms
        ]

        return len(recent_errors) / 60.0  # Errors per second in the last minute

    @property
    def has_errors(self) -> bool:
        """Check if any errors have been collected.

        Returns:
            True if errors have been collected, False otherwise
        """
        return len(self._error_occurrences) > 0 or self._total_errors_recorded > 0

    def get_summary(self) -> AggregatedMetrics:
        """Get summary of collected metrics (alias for get_aggregated_metrics).

        Returns:
            Aggregated metrics summary
        """
        return self.get_aggregated_metrics()

    # ========================================================================
    # Private Methods
    # ========================================================================

    def _update_connection_error_count(self, connection_id: str) -> None:
        """Update error count for a connection.

        Args:
            connection_id: Connection identifier
        """
        if connection_id in self._connection_metrics:
            current_metrics = self._connection_metrics[connection_id]
            updated_metrics = current_metrics.model_copy(
                update={"total_errors": current_metrics.total_errors + 1}
            )
            self._connection_metrics[connection_id] = updated_metrics

    def _update_connection_recovery_count(self, connection_id: str, successful: bool) -> None:
        """Update recovery count for a connection.

        Args:
            connection_id: Connection identifier
            successful: Whether recovery was successful
        """
        if connection_id in self._connection_metrics:
            current_metrics = self._connection_metrics[connection_id]
            update_dict = {"total_recoveries": current_metrics.total_recoveries + 1}

            if successful:
                update_dict["successful_recoveries"] = current_metrics.successful_recoveries + 1

            updated_metrics = current_metrics.model_copy(update=update_dict)
            self._connection_metrics[connection_id] = updated_metrics


# ============================================================================
# Metrics Aggregator
# ============================================================================


class MetricsAggregator:
    """Aggregates metrics from multiple WebSocketErrorMetrics collectors.

    Provides a unified view of metrics across different collectors,
    useful for system-wide monitoring and reporting.
    """

    def __init__(self) -> None:
        """Initialize metrics aggregator."""
        self.collectors: dict[str, WebSocketErrorMetrics] = {}

    def register_collector(self, name: str, collector: WebSocketErrorMetrics) -> None:
        """Register a metrics collector.

        Args:
            name: Name for the collector
            collector: The metrics collector instance
        """
        self.collectors[name] = collector

    def unregister_collector(self, name: str) -> None:
        """Unregister a metrics collector.

        Args:
            name: Name of the collector to remove
        """
        self.collectors.pop(name, None)

    def get_global_summary(self) -> dict[str, Any]:
        """Get aggregated summary from all collectors.

        Returns:
            Aggregated metrics from all collectors
        """
        if not self.collectors:
            return {
                "timestamp": datetime.now(UTC).isoformat(),
                "collectors": [],
                "global_stats": {
                    "total_errors": 0,
                    "total_recovery_attempts": 0,
                    "global_recovery_success_rate": 100.0,
                    "avg_recovery_duration_ms": 0.0,
                },
                "per_collector": {},
            }

        # Aggregate across all collectors
        total_errors = 0
        total_recovery_attempts = 0
        total_successful_recoveries = 0
        all_recovery_durations: list[float] = []

        for collector in self.collectors.values():
            stats = collector.get_statistics()

            total_errors += stats.total_errors_recorded
            total_recovery_attempts += stats.total_recoveries_recorded

            # Use public method to get recovery rate
            collector_success_rate = collector.get_recovery_success_rate()
            if stats.total_recoveries_recorded > 0:
                collector_successful = int(
                    stats.total_recoveries_recorded * (collector_success_rate / 100)
                )
                total_successful_recoveries += collector_successful

            # Get aggregated metrics for durations
            summary = collector.get_aggregated_metrics()
            if summary.average_recovery_duration_ms > 0:
                all_recovery_durations.append(summary.average_recovery_duration_ms)

        global_recovery_rate = (
            (total_successful_recoveries / total_recovery_attempts * 100)
            if total_recovery_attempts > 0
            else 100.0
        )

        avg_recovery_duration = (
            sum(all_recovery_durations) / len(all_recovery_durations)
            if all_recovery_durations
            else 0.0
        )

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "collectors": list(self.collectors.keys()),
            "global_stats": {
                "total_errors": total_errors,
                "total_recovery_attempts": total_recovery_attempts,
                "global_recovery_success_rate": global_recovery_rate,
                "avg_recovery_duration_ms": avg_recovery_duration,
            },
            "per_collector": {
                name: collector.export_metrics() for name, collector in self.collectors.items()
            },
        }

    def get_collector_names(self) -> list[str]:
        """Get list of registered collector names.

        Returns:
            List of collector names
        """
        return list(self.collectors.keys())

    def clear_all_metrics(self) -> None:
        """Clear metrics from all registered collectors."""
        for collector in self.collectors.values():
            collector.clear_metrics()
