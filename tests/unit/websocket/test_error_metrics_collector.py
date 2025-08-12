"""Tests for WebSocket Error System Metrics Collector."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_error_metrics_collector import (
    MetricsAggregator,
    WebSocketErrorMetricsCollector,
)
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestWebSocketErrorMetricsCollector:
    """Test WebSocket error metrics collection."""

    def test_collector_initialization(self) -> None:
        """Test metrics collector initialization."""
        collector = WebSocketErrorMetricsCollector(window_size_minutes=10)

        assert collector.window_size_minutes == 10
        assert len(collector.metrics) == 0
        assert collector.start_time_ms > 0

        summary = collector.get_summary()
        assert summary.total_errors == 0
        assert summary.error_rate_per_second == 0.0

    def test_record_single_error(self) -> None:
        """Test recording a single error."""
        collector = WebSocketErrorMetricsCollector()

        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST, message="Test connection lost"
        )

        collector.record_error(
            error=error,
            processing_time_us=150.5,
            recovery_time_ms=10.2,
            retry_count=2,
            success=True,
        )

        assert len(collector.metrics) == 1

        summary = collector.get_summary()
        assert summary.total_errors == 1
        assert summary.errors_by_code["CONNECTION_LOST"] == 1
        assert summary.recovery_attempts == 1
        assert summary.successful_recoveries == 1
        assert summary.avg_processing_time_us == 150.5
        assert summary.avg_recovery_time_ms == 10.2

    def test_record_multiple_errors(self) -> None:
        """Test recording multiple errors with different codes."""
        collector = WebSocketErrorMetricsCollector()

        # Record various error types
        error_codes = [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.AUTH_EXPIRED,
            WebSocketErrorCode.CONNECTION_LOST,  # Duplicate
        ]

        for code in error_codes:
            error = ErrorTestFactory.create_test_error(code=code)
            collector.record_error(error, processing_time_us=100)

        summary = collector.get_summary()
        assert summary.total_errors == 4
        assert summary.errors_by_code["CONNECTION_LOST"] == 2
        assert summary.errors_by_code["RATE_LIMITED"] == 1
        assert summary.errors_by_code["AUTH_EXPIRED"] == 1

    def test_severity_tracking(self) -> None:
        """Test tracking errors by severity."""
        collector = WebSocketErrorMetricsCollector()

        # Create errors with different severities
        severities = [
            ErrorSeverity.WARNING,
            ErrorSeverity.ERROR,
            ErrorSeverity.CRITICAL,
            ErrorSeverity.WARNING,
        ]

        for i, severity in enumerate(severities):
            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
            error.severity = severity
            collector.record_error(error)

        summary = collector.get_summary()
        assert summary.errors_by_severity["WARNING"] == 2
        assert summary.errors_by_severity["ERROR"] == 1
        assert summary.errors_by_severity["CRITICAL"] == 1

    def test_exchange_tracking(self) -> None:
        """Test tracking errors by exchange."""
        collector = WebSocketErrorMetricsCollector()

        # Record errors from different exchanges
        exchanges = ["hyperliquid", "backpack", "hyperliquid"]

        for exchange in exchanges:
            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED)
            error.context.exchange = exchange
            collector.record_error(error)

        summary = collector.get_summary()
        assert summary.errors_by_exchange["hyperliquid"] == 2
        assert summary.errors_by_exchange["backpack"] == 1

    def test_recovery_metrics(self) -> None:
        """Test recovery attempt tracking."""
        collector = WebSocketErrorMetricsCollector()

        # Record successful recoveries
        for _ in range(3):
            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
            collector.record_error(error, recovery_time_ms=5.0, success=True)

        # Record failed recoveries
        for _ in range(2):
            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.AUTH_FAILED)
            collector.record_error(error, recovery_time_ms=15.0, success=False)

        summary = collector.get_summary()
        assert summary.recovery_attempts == 5
        assert summary.successful_recoveries == 3
        assert summary.failed_recoveries == 2

        success_rate = collector.get_recovery_success_rate()
        assert success_rate == 60.0  # 3/5 * 100

    def test_timing_metrics(self) -> None:
        """Test processing and recovery time tracking."""
        collector = WebSocketErrorMetricsCollector()

        processing_times = [100.0, 200.0, 300.0]
        recovery_times = [10.0, 20.0, 30.0]

        for proc_time, rec_time in zip(processing_times, recovery_times, strict=False):
            error = ErrorTestFactory.create_test_error()
            collector.record_error(error, processing_time_us=proc_time, recovery_time_ms=rec_time)

        summary = collector.get_summary()
        assert summary.avg_processing_time_us == 200.0  # Average of 100, 200, 300
        assert summary.avg_recovery_time_ms == 20.0  # Average of 10, 20, 30

    @patch("time.time")
    def test_error_rate_calculation(self, mock_time) -> None:
        """Test error rate per second calculation."""
        collector = WebSocketErrorMetricsCollector()

        # Simulate errors over time
        base_time = 1000.0
        mock_time.return_value = base_time

        # Record 10 errors in the first second
        for _ in range(10):
            error = ErrorTestFactory.create_test_error()
            collector.record_error(error)

        # Move forward 60 seconds
        mock_time.return_value = base_time + 60

        # Rate should be 10 errors / 60 seconds
        rate = collector._calculate_error_rate()
        assert rate == pytest.approx(10 / 60.0, rel=0.01)

    def test_peak_rate_tracking(self) -> None:
        """Test peak error rate tracking."""
        collector = WebSocketErrorMetricsCollector()

        # Generate burst of errors
        for _ in range(100):
            error = ErrorTestFactory.create_test_error()
            collector.record_error(error)

        summary = collector.get_summary()
        assert summary.peak_error_rate > 0
        assert summary.peak_error_rate >= summary.error_rate_per_second

    @patch("time.time")
    def test_metrics_cleanup(self, mock_time) -> None:
        """Test old metrics cleanup based on window size."""
        collector = WebSocketErrorMetricsCollector(window_size_minutes=5)

        base_time = 1000.0
        mock_time.return_value = base_time

        # Record old error
        error1 = ErrorTestFactory.create_test_error()
        collector.record_error(error1)

        # Move forward 6 minutes (beyond window)
        mock_time.return_value = base_time + (6 * 60)

        # Record new error (triggers cleanup)
        error2 = ErrorTestFactory.create_test_error()
        collector.record_error(error2)

        # Old metric should be cleaned up
        assert len(collector.metrics) == 1
        assert collector.metrics[0].timestamp_ms > (base_time + 5 * 60) * 1000

    def test_get_metrics_by_timerange(self) -> None:
        """Test retrieving metrics within a time range."""
        collector = WebSocketErrorMetricsCollector()

        # Record errors at different times
        with patch("time.time") as mock_time:
            for i in range(5):
                mock_time.return_value = 1000.0 + i
                error = ErrorTestFactory.create_test_error()
                collector.record_error(error)

        # Get metrics for middle time range
        start_ms = 1001000  # 1001 seconds
        end_ms = 1003000  # 1003 seconds

        range_metrics = collector.get_metrics_by_timerange(start_ms, end_ms)
        assert len(range_metrics) == 3  # Errors at 1001, 1002, 1003

    def test_error_distribution(self) -> None:
        """Test error code distribution calculation."""
        collector = WebSocketErrorMetricsCollector()

        # Record errors with specific distribution
        error_counts = {
            WebSocketErrorCode.CONNECTION_LOST: 5,
            WebSocketErrorCode.RATE_LIMITED: 3,
            WebSocketErrorCode.AUTH_EXPIRED: 2,
        }

        for code, count in error_counts.items():
            for _ in range(count):
                error = ErrorTestFactory.create_test_error(code=code)
                collector.record_error(error)

        distribution = collector.get_error_distribution()

        assert distribution["CONNECTION_LOST"] == 50.0  # 5/10 * 100
        assert distribution["RATE_LIMITED"] == 30.0  # 3/10 * 100
        assert distribution["AUTH_EXPIRED"] == 20.0  # 2/10 * 100

    def test_severity_distribution(self) -> None:
        """Test severity distribution calculation."""
        collector = WebSocketErrorMetricsCollector()

        # Record errors with specific severity distribution
        severity_counts = {
            ErrorSeverity.WARNING: 6,
            ErrorSeverity.ERROR: 3,
            ErrorSeverity.CRITICAL: 1,
        }

        for severity, count in severity_counts.items():
            for _ in range(count):
                error = ErrorTestFactory.create_test_error()
                error.severity = severity
                collector.record_error(error)

        distribution = collector.get_severity_distribution()

        assert distribution["WARNING"] == 60.0  # 6/10 * 100
        assert distribution["ERROR"] == 30.0  # 3/10 * 100
        assert distribution["CRITICAL"] == 10.0  # 1/10 * 100

    def test_exchange_error_rates(self) -> None:
        """Test error rates by exchange."""
        collector = WebSocketErrorMetricsCollector()

        # Record errors for different exchanges
        exchanges = ["hyperliquid"] * 5 + ["backpack"] * 3

        for exchange in exchanges:
            error = ErrorTestFactory.create_test_error()
            error.context.exchange = exchange
            collector.record_error(error)

        exchange_rates = collector.get_exchange_error_rates()

        # Within last minute window
        assert "hyperliquid" in exchange_rates
        assert "backpack" in exchange_rates

    def test_reset_metrics(self) -> None:
        """Test resetting all metrics."""
        collector = WebSocketErrorMetricsCollector()

        # Record some errors
        for _ in range(5):
            error = ErrorTestFactory.create_test_error()
            collector.record_error(error, processing_time_us=100)

        # Verify metrics exist
        assert len(collector.metrics) == 5
        summary = collector.get_summary()
        assert summary.total_errors == 5

        # Reset metrics
        collector.reset_metrics()

        # Verify all cleared
        assert len(collector.metrics) == 0
        summary = collector.get_summary()
        assert summary.total_errors == 0
        assert summary.avg_processing_time_us == 0.0

    def test_export_metrics(self) -> None:
        """Test exporting metrics for monitoring systems."""
        collector = WebSocketErrorMetricsCollector(window_size_minutes=5)

        # Record various errors
        for i in range(3):
            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
            collector.record_error(
                error,
                processing_time_us=100.0 + i * 10,
                recovery_time_ms=5.0 + i,
                success=i < 2,  # First 2 successful
            )

        exported = collector.export_metrics()

        # Verify exported structure
        assert "timestamp" in exported
        assert exported["window_size_minutes"] == 5
        assert exported["total_errors"] == 3
        assert "error_rate_per_second" in exported
        assert "recovery_success_rate" in exported
        assert "errors_by_code" in exported
        assert "performance" in exported
        assert exported["performance"]["avg_processing_time_us"] > 0
        assert "distributions" in exported
        assert "exchange_rates" in exported


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

        collector1 = WebSocketErrorMetricsCollector()
        collector2 = WebSocketErrorMetricsCollector()

        aggregator.register_collector("exchange1", collector1)
        aggregator.register_collector("exchange2", collector2)

        assert len(aggregator.collectors) == 2
        assert "exchange1" in aggregator.collectors
        assert "exchange2" in aggregator.collectors

    def test_global_summary_aggregation(self) -> None:
        """Test aggregating summaries from multiple collectors."""
        aggregator = MetricsAggregator()

        # Create collectors with different metrics
        collector1 = WebSocketErrorMetricsCollector()
        for _ in range(5):
            error = ErrorTestFactory.create_test_error()
            collector1.record_error(
                error, processing_time_us=100, recovery_time_ms=10, success=True
            )

        collector2 = WebSocketErrorMetricsCollector()
        for _ in range(3):
            error = ErrorTestFactory.create_test_error()
            collector2.record_error(
                error, processing_time_us=200, recovery_time_ms=20, success=False
            )

        aggregator.register_collector("collector1", collector1)
        aggregator.register_collector("collector2", collector2)

        global_summary = aggregator.get_global_summary()

        # Verify aggregated stats
        assert global_summary["global_stats"]["total_errors"] == 8
        assert global_summary["global_stats"]["total_recovery_attempts"] == 8
        assert global_summary["global_stats"]["global_recovery_success_rate"] == 62.5  # 5/8

        # Verify per-collector data
        assert "collector1" in global_summary["per_collector"]
        assert "collector2" in global_summary["per_collector"]

    def test_empty_aggregator_summary(self) -> None:
        """Test summary with no collectors."""
        aggregator = MetricsAggregator()
        summary = aggregator.get_global_summary()
        assert summary == {}

    def test_aggregator_with_empty_collectors(self) -> None:
        """Test aggregator with collectors that have no data."""
        aggregator = MetricsAggregator()

        collector1 = WebSocketErrorMetricsCollector()
        collector2 = WebSocketErrorMetricsCollector()

        aggregator.register_collector("empty1", collector1)
        aggregator.register_collector("empty2", collector2)

        global_summary = aggregator.get_global_summary()

        assert global_summary["global_stats"]["total_errors"] == 0
        assert global_summary["global_stats"]["global_recovery_success_rate"] == 100.0
        assert global_summary["global_stats"]["avg_processing_time_us"] == 0.0
