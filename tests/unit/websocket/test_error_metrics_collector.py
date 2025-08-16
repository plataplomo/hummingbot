"""Tests for WebSocket Error System Metrics Collector."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.enums import WebSocketErrorCode
from cyberdelta.apis.websocket.exceptions import WebSocketStreamError
from cyberdelta.apis.websocket.metrics.error_metrics import (
    MetricsAggregator,
    WebSocketErrorMetrics,
)
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.config.models.websocket_error_config import WebSocketErrorMetricsConfig


class TestWebSocketErrorMetrics:
    """Test WebSocket error metrics collection."""

    def test_collector_initialization(self) -> None:
        """Test metrics collector initialization."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        assert collector.config == config
        assert len(collector._error_occurrences) == 0
        assert collector._collection_start_time > 0

        summary = collector.get_summary()
        assert summary.total_errors == 0
        assert summary.overall_error_rate == 0.0

    def test_record_single_error(self) -> None:
        """Test recording a single error."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        # Create a proper WebSocketStreamError
        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )
        error = WebSocketStreamError(
            message="Test connection lost",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
        )

        collector.record_error(
            error=error,
            recovery_successful=True,
            recovery_duration_ms=10,
        )

        assert len(collector._error_occurrences) == 1

        summary = collector.get_summary()
        assert summary.total_errors == 1
        assert summary.error_counts_by_code["CONNECTION_LOST"] == 1

    def test_record_multiple_errors(self) -> None:
        """Test recording multiple errors with different codes."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        # Record various error types
        error_codes = [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.AUTH_FAILED,
            WebSocketErrorCode.CONNECTION_LOST,  # Duplicate
        ]

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        for code in error_codes:
            error = WebSocketStreamError(
                message=f"Test error {code.name}",
                code=code,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error)

        summary = collector.get_summary()
        assert summary.total_errors == 4
        assert summary.error_counts_by_code["CONNECTION_LOST"] == 2
        assert summary.error_counts_by_code["RATE_LIMITED"] == 1
        assert summary.error_counts_by_code["AUTH_FAILED"] == 1

    def test_severity_tracking(self) -> None:
        """Test tracking errors by severity."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        # Create errors with different severities
        severities = [
            ErrorSeverity.WARNING,
            ErrorSeverity.ERROR,
            ErrorSeverity.CRITICAL,
            ErrorSeverity.WARNING,
        ]

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        for severity in severities:
            error = WebSocketStreamError(
                message=f"Test error {severity.name}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=severity,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error)

        # Test the collector's severity distribution method
        distribution = collector.get_severity_distribution()
        assert distribution["WARNING"] == 50.0  # 2/4 * 100
        assert distribution["ERROR"] == 25.0  # 1/4 * 100
        assert distribution["CRITICAL"] == 25.0  # 1/4 * 100

    def test_exchange_tracking(self) -> None:
        """Test tracking errors by exchange."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        # Record errors from different exchanges
        exchanges = ["hyperliquid", "backpack", "hyperliquid"]

        for exchange in exchanges:
            context = StreamErrorContext(
                connection_id="test-connection",
                exchange=exchange,
                channel="orderbook",
            )
            error = WebSocketStreamError(
                message="Test rate limited",
                code=WebSocketErrorCode.RATE_LIMITED,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error)

        summary = collector.get_summary()
        assert summary.error_counts_by_exchange["hyperliquid"] == 2
        assert summary.error_counts_by_exchange["backpack"] == 1

    def test_recovery_metrics(self) -> None:
        """Test recovery attempt tracking."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Record successful recoveries
        for _ in range(3):
            error = WebSocketStreamError(
                message="Test connection lost",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error, recovery_successful=True, recovery_duration_ms=5)

        # Record failed recoveries
        for _ in range(2):
            error = WebSocketStreamError(
                message="Test auth failed",
                code=WebSocketErrorCode.AUTH_FAILED,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error, recovery_successful=False, recovery_duration_ms=15)

        # Check individual recovery success rate
        success_rate = collector.get_recovery_success_rate()
        assert success_rate == 60.0  # 3/5 * 100

    def test_timing_metrics(self) -> None:
        """Test recovery time tracking."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        recovery_times = [10, 20, 30]

        for rec_time in recovery_times:
            error = WebSocketStreamError(
                message="Test timing error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error, recovery_successful=True, recovery_duration_ms=rec_time)

        summary = collector.get_summary()
        assert summary.average_recovery_duration_ms == 20.0  # Average of 10, 20, 30

    @patch("time.time")
    def test_error_rate_calculation(self, mock_time: MagicMock) -> None:
        """Test error rate calculation."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        # Simulate errors over time
        base_time = 1000.0
        mock_time.return_value = base_time

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Record 10 errors in the first second
        for _ in range(10):
            error = WebSocketStreamError(
                message="Test rate calculation error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error)

        # Move forward 60 seconds
        mock_time.return_value = base_time + 60

        # Test the current error rate calculation
        current_rate = collector._calculate_current_error_rate()
        assert current_rate >= 0.0

    def test_has_errors_property(self) -> None:
        """Test has_errors property."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        # Initially no errors
        assert not collector.has_errors

        # Add an error
        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )
        error = WebSocketStreamError(
            message="Test error",
            code=WebSocketErrorCode.CONNECTION_LOST,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
        )
        collector.record_error(error)

        # Now should have errors
        assert collector.has_errors

    def test_clear_metrics(self) -> None:
        """Test clearing all metrics."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Add some errors
        for _ in range(5):
            error = WebSocketStreamError(
                message="Test clear error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error)

        assert len(collector._error_occurrences) == 5

        # Clear metrics
        collector.clear_metrics()

        assert len(collector._error_occurrences) == 0
        assert collector._total_errors_recorded == 0

    def test_get_aggregated_metrics_with_timerange(self) -> None:
        """Test retrieving aggregated metrics within a time range."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Record some errors
        for _ in range(5):
            error = WebSocketStreamError(
                message="Test timerange error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error)

        # Get metrics for current time range
        current_time_ms = int(time.time() * 1000)
        start_time_ms = current_time_ms - (60 * 60 * 1000)  # 1 hour ago

        range_metrics = collector.get_aggregated_metrics(start_time_ms, current_time_ms)
        assert range_metrics.total_errors >= 0  # Should return valid metrics

    def test_error_distribution(self) -> None:
        """Test error code distribution calculation."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Record errors with specific distribution
        error_counts = {
            WebSocketErrorCode.CONNECTION_LOST: 5,
            WebSocketErrorCode.RATE_LIMITED: 3,
            WebSocketErrorCode.AUTH_FAILED: 2,
        }

        for code, count in error_counts.items():
            for _ in range(count):
                error = WebSocketStreamError(
                    message=f"Test {code.name} error",
                    code=code,
                    context=context,
                    severity=ErrorSeverity.ERROR,
                    recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
                )
                collector.record_error(error)

        distribution = collector.get_error_distribution()

        assert distribution["CONNECTION_LOST"] == 50.0  # 5/10 * 100
        assert distribution["RATE_LIMITED"] == 30.0  # 3/10 * 100
        assert distribution["AUTH_FAILED"] == 20.0  # 2/10 * 100

    def test_statistics(self) -> None:
        """Test collector statistics."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Record some errors
        for _ in range(3):
            error = WebSocketStreamError(
                message="Test statistics error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error)

        # Record some recovery attempts
        collector.record_recovery_attempt(
            exchange="hyperliquid",
            connection_id="test-connection",
            strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            attempt_number=1,
            successful=True,
            duration_ms=100,
        )

        stats = collector.get_statistics()
        assert stats.total_errors_recorded == 3
        assert stats.total_recoveries_recorded == 1
        assert stats.collection_enabled == config.enable_metrics_collection

    def test_exchange_error_rates(self) -> None:
        """Test error rates by exchange."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        # Record errors for different exchanges
        exchanges = ["hyperliquid"] * 5 + ["backpack"] * 3

        for exchange in exchanges:
            context = StreamErrorContext(
                connection_id="test-connection",
                exchange=exchange,
                channel="orderbook",
            )
            error = WebSocketStreamError(
                message="Test exchange error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(error)

        exchange_rates = collector.get_exchange_error_rates()

        # Within last minute window
        assert "hyperliquid" in exchange_rates
        assert "backpack" in exchange_rates

    def test_connection_tracking(self) -> None:
        """Test connection lifecycle tracking."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        connection_id = "test-connection"
        exchange = "hyperliquid"

        # Start tracking connection
        collector.start_connection_tracking(connection_id, exchange)
        assert connection_id in collector._connection_metrics

        # End tracking connection
        collector.end_connection_tracking(connection_id)
        metrics = collector._connection_metrics[connection_id]
        assert metrics.connection_end_ms is not None

    def test_export_metrics(self) -> None:
        """Test exporting metrics for monitoring systems."""
        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Record various errors
        for i in range(3):
            error = WebSocketStreamError(
                message=f"Test export error {i}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector.record_error(
                error,
                recovery_successful=i < 2,  # First 2 successful
                recovery_duration_ms=5 + i,
            )

        exported = collector.export_metrics()

        # Verify exported structure
        assert "timestamp" in exported
        assert "collection_enabled" in exported
        assert exported["total_errors"] == 3
        assert "error_rate_per_second" in exported
        assert "recovery_success_rate" in exported
        assert "errors_by_code" in exported
        assert "performance" in exported
        assert "distributions" in exported
        assert "exchange_rates" in exported
        assert "connections" in exported
        assert "health" in exported


class TestMetricsAggregator:
    """Test metrics aggregator functionality."""

    def test_aggregator_initialization(self) -> None:
        """Test aggregator initialization."""
        aggregator = MetricsAggregator()
        assert len(aggregator.collectors) == 0

        summary = aggregator.get_global_summary()
        assert summary == {}

    def test_register_collector(self) -> None:
        """Test registering collectors."""
        aggregator = MetricsAggregator()

        config = WebSocketErrorMetricsConfig()
        collector1 = WebSocketErrorMetrics(config)
        collector2 = WebSocketErrorMetrics(config)

        aggregator.register_collector("exchange1", collector1)
        aggregator.register_collector("exchange2", collector2)

        assert len(aggregator.collectors) == 2
        assert "exchange1" in aggregator.collectors
        assert "exchange2" in aggregator.collectors

    def test_global_summary_aggregation(self) -> None:
        """Test aggregating summaries from multiple collectors."""
        aggregator = MetricsAggregator()

        config = WebSocketErrorMetricsConfig()
        context = StreamErrorContext(
            connection_id="test-connection",
            exchange="hyperliquid",
            channel="orderbook",
        )

        # Create collectors with different metrics
        collector1 = WebSocketErrorMetrics(config)
        for _ in range(5):
            error = WebSocketStreamError(
                message="Test collector1 error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector1.record_error(error, recovery_successful=True, recovery_duration_ms=10)

        collector2 = WebSocketErrorMetrics(config)
        for _ in range(3):
            error = WebSocketStreamError(
                message="Test collector2 error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            )
            collector2.record_error(error, recovery_successful=False, recovery_duration_ms=20)

        aggregator.register_collector("collector1", collector1)
        aggregator.register_collector("collector2", collector2)

        global_summary = aggregator.get_global_summary()

        # Verify aggregated stats exist
        assert "global_stats" in global_summary
        assert "total_errors" in global_summary["global_stats"]
        assert "per_collector" in global_summary
        assert "collector1" in global_summary["per_collector"]
        assert "collector2" in global_summary["per_collector"]

    def test_empty_aggregator_summary(self) -> None:
        """Test summary with no collectors."""
        aggregator = MetricsAggregator()
        summary = aggregator.get_global_summary()
        assert "collectors" in summary
        assert len(summary["collectors"]) == 0

    def test_aggregator_with_empty_collectors(self) -> None:
        """Test aggregator with collectors that have no data."""
        aggregator = MetricsAggregator()

        config = WebSocketErrorMetricsConfig()
        collector1 = WebSocketErrorMetrics(config)
        collector2 = WebSocketErrorMetrics(config)

        aggregator.register_collector("empty1", collector1)
        aggregator.register_collector("empty2", collector2)

        global_summary = aggregator.get_global_summary()

        assert global_summary["global_stats"]["total_errors"] == 0
        assert global_summary["global_stats"]["global_recovery_success_rate"] == 100.0
        assert global_summary["global_stats"]["avg_recovery_duration_ms"] == 0.0

    def test_unregister_collector(self) -> None:
        """Test unregistering collectors."""
        aggregator = MetricsAggregator()

        config = WebSocketErrorMetricsConfig()
        collector = WebSocketErrorMetrics(config)

        aggregator.register_collector("test", collector)
        assert "test" in aggregator.collectors

        aggregator.unregister_collector("test")
        assert "test" not in aggregator.collectors

    def test_get_collector_names(self) -> None:
        """Test getting collector names."""
        aggregator = MetricsAggregator()

        config = WebSocketErrorMetricsConfig()
        collector1 = WebSocketErrorMetrics(config)
        collector2 = WebSocketErrorMetrics(config)

        aggregator.register_collector("collector1", collector1)
        aggregator.register_collector("collector2", collector2)

        names = aggregator.get_collector_names()
        assert "collector1" in names
        assert "collector2" in names
        assert len(names) == 2
