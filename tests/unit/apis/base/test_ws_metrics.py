"""Tests for WebSocket metrics collection system."""

from datetime import UTC, datetime

import pytest

from cyberdelta.apis.base.websocket_states import MessageProcessingResult
from cyberdelta.apis.base.ws_metrics import (
    MetricPoint,
    MetricSummary,
    MetricType,
    MetricUnit,
    WebSocketMetricsCollector,
)


class TestMetricPoint:
    """Test MetricPoint model."""

    def test_metric_point_creation(self) -> None:
        """Test MetricPoint creation with basic fields."""
        point = MetricPoint(
            metric_type=MetricType.MESSAGE_COUNT,
            metric_name="test_metric",
            value=1.0,
            unit=MetricUnit.COUNT,
            labels={"exchange": "test", "type": "depth"},
        )

        assert point.metric_type == MetricType.MESSAGE_COUNT
        assert point.metric_name == "test_metric"
        assert point.value == 1.0
        assert point.unit == MetricUnit.COUNT
        assert point.labels == {"exchange": "test", "type": "depth"}
        assert isinstance(point.timestamp, datetime)

    def test_metric_point_defaults(self) -> None:
        """Test MetricPoint with default values."""
        point = MetricPoint(
            metric_type=MetricType.PROCESSING_TIME,
            metric_name="processing_time",
            value=5.5,
            unit=MetricUnit.MILLISECONDS,
        )

        assert point.labels == {}
        assert isinstance(point.timestamp, datetime)
        assert point.timestamp.tzinfo == UTC


class TestMetricSummary:
    """Test MetricSummary model."""

    def test_metric_summary_creation(self) -> None:
        """Test MetricSummary creation."""
        summary = MetricSummary(
            metric_name="test_metric",
            count=100,
            total=500.0,
            min=1.0,
            max=10.0,
            p50=5.0,
            p95=9.0,
            p99=9.8,
            unit=MetricUnit.MILLISECONDS,
        )

        assert summary.metric_name == "test_metric"
        assert summary.count == 100
        assert summary.total == 500.0
        assert summary.average == 5.0  # 500/100
        assert summary.min == 1.0
        assert summary.max == 10.0
        assert summary.p50 == 5.0
        assert summary.p95 == 9.0
        assert summary.p99 == 9.8
        assert summary.unit == MetricUnit.MILLISECONDS

    def test_average_calculation(self) -> None:
        """Test automatic average calculation."""
        summary = MetricSummary(metric_name="test", count=10, total=100.0, unit=MetricUnit.COUNT)

        assert summary.average == 10.0

    def test_zero_count_average(self) -> None:
        """Test average calculation with zero count."""
        summary = MetricSummary(metric_name="test", count=0, total=0.0, unit=MetricUnit.COUNT)

        assert summary.average == 0.0


class TestWebSocketMetricsCollector:
    """Test WebSocketMetricsCollector."""

    @pytest.fixture
    def collector(self) -> WebSocketMetricsCollector:
        """Create a test metrics collector."""
        return WebSocketMetricsCollector("test_exchange", window_size=300)

    def test_collector_initialization(self, collector: WebSocketMetricsCollector) -> None:
        """Test collector initialization through public interface."""
        assert collector.exchange_name == "test_exchange"
        assert collector.window_size == 300

        # Test initial state through public interface
        summary = collector.get_summary()
        assert "message_count" in summary
        assert summary["message_count"].count == 0

        # Check time series is empty initially
        time_series = collector.get_time_series()
        assert len(time_series) == 0

    def test_record_message_success(self, collector: WebSocketMetricsCollector) -> None:
        """Test recording successful message processing through public interface."""
        collector.record_message(
            message_type="depth",
            processing_time_ms=1.5,
            message_size=1024,
            result=MessageProcessingResult.SUCCESS,
        )

        # Check message counts through summary
        summary = collector.get_summary("depth")
        assert summary["message_count"].count == 1
        assert summary["processing_time"].count == 1
        assert abs(summary["processing_time"].total - 1.5) < 0.01
        assert summary["message_size"].count == 1
        assert summary["message_size"].total == 1024.0

        # Check time series points through public interface
        points = collector.get_time_series()
        assert len(points) == 3  # message_count, processing_time, message_size
        assert any(p.metric_type == MetricType.MESSAGE_COUNT for p in points)
        assert any(p.metric_type == MetricType.PROCESSING_TIME for p in points)
        assert any(p.metric_type == MetricType.MESSAGE_SIZE for p in points)

    def test_record_message_failure(self, collector: WebSocketMetricsCollector) -> None:
        """Test recording failed message processing through public interface."""
        collector.record_message(
            message_type="depth",
            processing_time_ms=2.0,
            message_size=512,
            result=MessageProcessingResult.FAILURE,
        )

        # Check through summary - failed messages still count as processed
        summary = collector.get_summary("depth")
        assert summary["message_count"].count == 1
        assert abs(summary["processing_time"].total - 2.0) < 0.01
        assert summary["message_size"].total == 512.0

        # Error rate should be reflected in the summary
        all_summary = collector.get_summary()
        assert "error_rate" in all_summary
        assert all_summary["error_rate"].total == 100.0  # 1 error out of 1 message = 100%

    def test_record_error(self, collector: WebSocketMetricsCollector) -> None:
        """Test recording errors through public interface."""
        collector.record_error(
            error_type="validation", message_type="depth", error_details="Invalid field"
        )

        # Check that error was recorded in time series
        error_points = collector.get_time_series(MetricType.VALIDATION_ERROR)
        assert len(error_points) == 1
        assert error_points[0].metric_name == "websocket_errors_total"
        assert error_points[0].labels["error_type"] == "validation"
        assert error_points[0].labels["message_type"] == "depth"

    def test_record_connection_event(self, collector: WebSocketMetricsCollector) -> None:
        """Test recording connection events through public interface."""
        collector.record_connection_event("connected")

        # Check through public interface
        connection_points = collector.get_time_series(MetricType.CONNECTION_EVENT)
        assert len(connection_points) == 1
        assert connection_points[0].labels["event_type"] == "connected"

    def test_get_summary_specific_type(self, collector: WebSocketMetricsCollector) -> None:
        """Test getting summary for specific message type."""
        # Record some test data
        collector.record_message("depth", 1.0, 100, MessageProcessingResult.SUCCESS)
        collector.record_message("depth", 2.0, 200, MessageProcessingResult.SUCCESS)
        collector.record_message("depth", 3.0, 300, MessageProcessingResult.FAILURE)
        collector.record_message("ticker", 0.5, 50, MessageProcessingResult.SUCCESS)

        summary = collector.get_summary("depth")

        assert summary["message_count"].count == 3
        assert summary["message_count"].total == 3.0
        assert summary["processing_time"].count == 3
        assert summary["processing_time"].min == 1.0
        assert summary["processing_time"].max == 3.0
        assert summary["message_size"].count == 3
        assert summary["message_size"].min == 100.0
        assert summary["message_size"].max == 300.0

    def test_get_summary_all_types(self, collector: WebSocketMetricsCollector) -> None:
        """Test getting summary for all message types."""
        # Record test data
        collector.record_message("depth", 1.0, 100, MessageProcessingResult.SUCCESS)
        collector.record_message("ticker", 2.0, 200, MessageProcessingResult.SUCCESS)
        collector.record_message("trades", 3.0, 300, MessageProcessingResult.FAILURE)

        summary = collector.get_summary()

        assert summary["message_count"].count == 3
        assert summary["message_count"].total == 3.0
        assert summary["processing_time"].count == 3
        assert summary["processing_time"].min == 1.0
        assert summary["processing_time"].max == 3.0
        assert summary["error_rate"].count == 1  # One failed message
        assert abs(summary["error_rate"].total - (100.0 / 3.0)) < 0.01  # Error rate percentage

    def test_get_time_series_filtering(self, collector: WebSocketMetricsCollector) -> None:
        """Test time series filtering."""
        collector.record_message("depth", 1.0, 100, MessageProcessingResult.SUCCESS)
        collector.record_error("validation", "depth")

        # Filter by metric type
        message_points = collector.get_time_series(MetricType.MESSAGE_COUNT)
        assert len(message_points) == 1
        assert message_points[0].metric_type == MetricType.MESSAGE_COUNT

        error_points = collector.get_time_series(MetricType.VALIDATION_ERROR)
        assert len(error_points) == 1
        assert error_points[0].metric_type == MetricType.VALIDATION_ERROR

    def test_export_prometheus(self, collector: WebSocketMetricsCollector) -> None:
        """Test Prometheus format export."""
        collector.record_message("depth", 1.5, 1024, MessageProcessingResult.SUCCESS)
        collector.record_error("validation", "depth")

        prometheus_text = collector.export_prometheus()

        # Check basic structure
        assert "# HELP websocket_messages_total" in prometheus_text
        assert "# TYPE websocket_messages_total counter" in prometheus_text
        assert "# HELP websocket_errors_total" in prometheus_text
        assert "# TYPE websocket_errors_total counter" in prometheus_text
        assert "# HELP websocket_processing_time" in prometheus_text
        assert "# TYPE websocket_processing_time histogram" in prometheus_text

        # Check metric values
        assert 'exchange="test_exchange"' in prometheus_text
        assert 'message_type="depth"' in prometheus_text

    def test_percentile_calculation_through_summary(
        self, collector: WebSocketMetricsCollector
    ) -> None:
        """Test percentile calculation through public summary interface."""
        # Record test data to generate percentiles
        for i in range(1, 11):
            collector.record_message("test", float(i), 100, MessageProcessingResult.SUCCESS)

        summary = collector.get_summary("test")

        # Verify percentiles are calculated correctly through summary
        processing_summary = summary["processing_time"]
        assert processing_summary.p50 == 5.0  # Median (50th percentile)
        assert processing_summary.p95 == 9.0  # 95th percentile
        assert processing_summary.p99 == 9.0  # 99th percentile

        # Test with empty data
        empty_collector = WebSocketMetricsCollector("empty", 300)
        empty_summary = empty_collector.get_summary("nonexistent")
        assert "processing_time" not in empty_summary

    def test_reset_metrics(self, collector: WebSocketMetricsCollector) -> None:
        """Test resetting all metrics through public interface."""
        collector.record_message("depth", 1.0, 100, MessageProcessingResult.SUCCESS)
        collector.record_error("validation", "depth")

        # Verify data exists before reset
        summary_before = collector.get_summary()
        time_series_before = collector.get_time_series()
        assert summary_before["message_count"].count > 0
        assert len(time_series_before) > 0

        collector.reset()

        # Verify data is cleared after reset
        summary_after = collector.get_summary()
        time_series_after = collector.get_time_series()
        assert summary_after["message_count"].count == 0
        assert len(time_series_after) == 0

    def test_data_retention_behavior_through_public_interface(
        self, collector: WebSocketMetricsCollector
    ) -> None:
        """Test that data is properly maintained through public interface."""
        # Add initial data
        collector.record_message("depth", 1.0, 100, MessageProcessingResult.SUCCESS)
        collector.record_error("validation", "depth")

        initial_time_series = collector.get_time_series()
        initial_summary = collector.get_summary()

        # Verify we have initial data
        assert len(initial_time_series) > 0
        assert initial_summary["message_count"].count > 0

        # Add more data - cleanup happens automatically during record operations
        for i in range(10):
            collector.record_message(
                f"type_{i}", float(i), 100 + i, MessageProcessingResult.SUCCESS
            )

        # Verify data accumulation through public interface
        current_time_series = collector.get_time_series()
        current_summary = collector.get_summary()

        # Should have more data than initially (at least the same + new messages)
        assert len(current_time_series) >= len(initial_time_series)
        assert current_summary["message_count"].count > initial_summary["message_count"].count

        # Specifically verify we added our 10 new messages
        assert current_summary["message_count"].count >= initial_summary["message_count"].count + 10


@pytest.mark.asyncio
class TestMetricsIntegration:
    """Test metrics integration with WebSocket processing."""

    async def test_metrics_with_processor(self) -> None:
        """Test metrics collection integration."""
        # This would be an integration test with actual processor
        # For now, just verify the basic flow works
        collector = WebSocketMetricsCollector("test")

        # Simulate processing flow
        processing_time_ms = 1.5
        message_size = 1024
        message_type = "depth"

        collector.record_message(
            message_type, processing_time_ms, message_size, MessageProcessingResult.SUCCESS
        )

        summary = collector.get_summary(message_type)
        assert summary["message_count"].count == 1
        assert summary["processing_time"].count == 1
        assert summary["message_size"].count == 1

        # Test error recording
        collector.record_error("validation", message_type, "Test error")
        error_summary = collector.get_summary(message_type)
        assert error_summary["error_rate"].count == 1
