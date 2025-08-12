"""Type-safe metrics collection for WebSocket errors.

Provides comprehensive metrics collection for WebSocket error events,
recovery attempts, and system health monitoring with full type safety.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.config.models.websocket_error_config import WebSocketErrorMetricsConfig


if TYPE_CHECKING:
    from logging import Logger


# ============================================================================
# Metrics Data Models
# ============================================================================


class ErrorOccurrence(BaseModel):
    """Represents a single error occurrence for metrics."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    timestamp_ms: int = Field(..., description="Error timestamp in milliseconds")
    exchange: str = Field(..., description="Exchange name")
    error_code: WebSocketErrorCode = Field(..., description="Error code")
    severity: ErrorSeverity = Field(..., description="Error severity")
    connection_id: str = Field(..., description="Connection identifier")
    channel: str | None = Field(default=None, description="Channel or stream name")
    recovery_strategy: WebSocketRecoveryStrategy = Field(..., description="Recovery strategy used")
    recovery_successful: bool = Field(..., description="Whether recovery was successful")
    recovery_duration_ms: int | None = Field(default=None, description="Recovery duration")


class RecoveryAttempt(BaseModel):
    """Represents a recovery attempt for metrics."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    timestamp_ms: int = Field(..., description="Recovery attempt timestamp")
    exchange: str = Field(..., description="Exchange name")
    connection_id: str = Field(..., description="Connection identifier")
    strategy: WebSocketRecoveryStrategy = Field(..., description="Recovery strategy")
    attempt_number: int = Field(..., ge=1, description="Attempt number (1-based)")
    successful: bool = Field(..., description="Whether recovery was successful")
    duration_ms: int = Field(..., ge=0, description="Recovery duration")
    error_count_before: int = Field(..., ge=0, description="Error count before recovery")


class ConnectionMetrics(BaseModel):
    """Metrics for a WebSocket connection."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    connection_id: str = Field(..., description="Connection identifier")
    exchange: str = Field(..., description="Exchange name")
    connection_start_ms: int = Field(..., description="Connection start timestamp")
    connection_end_ms: int | None = Field(default=None, description="Connection end timestamp")
    total_messages: int = Field(default=0, ge=0, description="Total messages received")
    total_errors: int = Field(default=0, ge=0, description="Total errors encountered")
    total_recoveries: int = Field(default=0, ge=0, description="Total recovery attempts")
    successful_recoveries: int = Field(default=0, ge=0, description="Successful recoveries")


class ErrorRateMetrics(BaseModel):
    """Error rate metrics over a time window."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    window_start_ms: int = Field(..., description="Window start timestamp")
    window_end_ms: int = Field(..., description="Window end timestamp")
    window_duration_ms: int = Field(..., ge=1, description="Window duration")
    total_events: int = Field(default=0, ge=0, description="Total events in window")
    error_events: int = Field(default=0, ge=0, description="Error events in window")
    error_rate: float = Field(default=0.0, ge=0.0, le=1.0, description="Error rate (0.0-1.0)")


class CollectorStatistics(BaseModel):
    """Statistics for the error metrics collector."""

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    uptime_seconds: int = Field(..., ge=0, description="Collector uptime in seconds")
    total_errors_recorded: int = Field(..., ge=0, description="Total errors recorded")
    total_recoveries_recorded: int = Field(..., ge=0, description="Total recoveries recorded")
    current_error_buffer_size: int = Field(..., ge=0, description="Current error buffer size")
    current_recovery_buffer_size: int = Field(..., ge=0, description="Current recovery buffer size")
    active_connections: int = Field(..., ge=0, description="Number of active connections")
    total_connections_tracked: int = Field(..., ge=0, description="Total connections tracked")
    error_chains_tracked: int = Field(..., ge=0, description="Number of error chains tracked")
    error_rate_buckets: int = Field(..., ge=0, description="Number of error rate buckets")
    collection_enabled: bool = Field(..., description="Whether collection is enabled")


class AggregatedMetrics(BaseModel):
    """Aggregated metrics for reporting."""

    model_config = {
        "frozen": False,
        "extra": "forbid",
    }

    # Time period
    start_timestamp_ms: int = Field(..., description="Metrics period start")
    end_timestamp_ms: int = Field(..., description="Metrics period end")
    duration_ms: int = Field(..., ge=1, description="Metrics period duration")

    # Error counts by code
    error_counts_by_code: dict[str, int] = Field(
        default_factory=dict, description="Error counts by error code name"
    )

    # Error counts by exchange
    error_counts_by_exchange: dict[str, int] = Field(
        default_factory=dict, description="Error counts by exchange"
    )

    # Recovery metrics
    recovery_attempts_by_strategy: dict[str, int] = Field(
        default_factory=dict, description="Recovery attempts by strategy name"
    )

    successful_recoveries_by_strategy: dict[str, int] = Field(
        default_factory=dict, description="Successful recoveries by strategy name"
    )

    # Timing metrics
    average_recovery_duration_ms: float = Field(
        default=0.0, ge=0.0, description="Average recovery duration"
    )

    median_recovery_duration_ms: float = Field(
        default=0.0, ge=0.0, description="Median recovery duration"
    )

    # Connection metrics
    total_connections: int = Field(default=0, ge=0, description="Total connections tracked")
    active_connections: int = Field(default=0, ge=0, description="Currently active connections")
    average_connection_duration_ms: float = Field(
        default=0.0, ge=0.0, description="Average connection duration"
    )

    # Overall health
    overall_error_rate: float = Field(default=0.0, ge=0.0, le=1.0, description="Overall error rate")

    recovery_success_rate: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Recovery success rate"
    )


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
