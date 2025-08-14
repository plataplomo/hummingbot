"""Tests for WebSocket Processing Metrics Models.

This module tests the typed metrics models for WebSocket message processing.
"""

from unittest.mock import patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.websocket.ws_processing_metrics import ProcessingMetrics, ProcessorMetrics


class TestProcessingMetrics:
    """Test the ProcessingMetrics model."""

    def test_initialization(self) -> None:
        """Test metrics initialization with default values."""
        metrics = ProcessingMetrics()

        assert metrics.total_processed == 0
        assert metrics.validation_errors == 0
        assert metrics.transformation_errors == 0
        assert metrics.handler_errors == 0
        assert metrics.total_processing_time_seconds == 0.0
        assert isinstance(metrics.start_timestamp, float)

    def test_record_processing_time(self) -> None:
        """Test recording processing time."""
        metrics = ProcessingMetrics()

        # Record processing time
        metrics.record_processing_time(0.05)  # 50ms

        assert metrics.total_processed == 1
        assert metrics.total_processing_time_seconds == 0.05

        # Record another processing time
        metrics.record_processing_time(0.03)  # 30ms

        assert metrics.total_processed == 2
        assert metrics.total_processing_time_seconds == 0.08

    def test_record_errors(self) -> None:
        """Test recording different types of errors."""
        metrics = ProcessingMetrics()

        # Record validation errors
        metrics.record_validation_error()
        metrics.record_validation_error()

        assert metrics.validation_errors == 2
        assert metrics.transformation_errors == 0
        assert metrics.handler_errors == 0

        # Record transformation error
        metrics.record_transformation_error()

        assert metrics.validation_errors == 2
        assert metrics.transformation_errors == 1
        assert metrics.handler_errors == 0

        # Record handler error
        metrics.record_handler_error()

        assert metrics.validation_errors == 2
        assert metrics.transformation_errors == 1
        assert metrics.handler_errors == 1

    def test_get_uptime_seconds(self) -> None:
        """Test uptime calculation."""
        with patch("time.time") as mock_time:
            # Mock initial time
            mock_time.return_value = 1000.0
            metrics = ProcessingMetrics(start_timestamp=1000.0)

            # Mock time after 5 seconds
            mock_time.return_value = 1005.0
            uptime = metrics.get_uptime_seconds()

            assert uptime == 5.0

    def test_get_average_processing_time_ms(self) -> None:
        """Test average processing time calculation."""
        metrics = ProcessingMetrics()

        # No messages processed
        assert metrics.get_average_processing_time_ms() == 0.0

        # Process some messages
        metrics.record_processing_time(0.05)  # 50ms
        metrics.record_processing_time(0.03)  # 30ms

        # Average should be 40ms
        assert metrics.get_average_processing_time_ms() == 40.0

    def test_get_messages_per_second(self) -> None:
        """Test messages per second calculation."""
        with patch("time.time") as mock_time:
            # Mock initial time
            mock_time.return_value = 1000.0
            metrics = ProcessingMetrics(start_timestamp=1000.0)

            # Process some messages
            metrics.record_processing_time(0.01)
            metrics.record_processing_time(0.01)

            # Mock time after 2 seconds
            mock_time.return_value = 1002.0

            # Should be 1 message per second (2 messages / 2 seconds)
            assert metrics.get_messages_per_second() == 1.0

    def test_get_error_rate(self) -> None:
        """Test error rate calculation."""
        metrics = ProcessingMetrics()

        # No messages processed
        assert metrics.get_error_rate() == 0.0

        # Process 8 successful messages
        for _ in range(8):
            metrics.record_processing_time(0.01)

        # Add 2 errors (but not processing time, so total_processed stays 8)
        # The error rate calculation is errors / total_processed, not errors / (processed + errors)
        metrics.validation_errors = 1
        metrics.transformation_errors = 1

        # Error rate should be 0.25 (2 errors / 8 messages)
        assert metrics.get_error_rate() == 0.25

    def test_get_total_errors(self) -> None:
        """Test total errors calculation."""
        metrics = ProcessingMetrics()

        assert metrics.get_total_errors() == 0

        metrics.record_validation_error()
        metrics.record_transformation_error()
        metrics.record_handler_error()

        assert metrics.get_total_errors() == 3

    def test_get_success_rate(self) -> None:
        """Test success rate calculation."""
        metrics = ProcessingMetrics()

        # Process 8 successful messages
        for _ in range(8):
            metrics.record_processing_time(0.01)

        # Add 2 errors (error rate is 2/8 = 0.25)
        metrics.validation_errors = 1
        metrics.transformation_errors = 1

        # Success rate should be 0.75 (1 - 0.25)
        assert metrics.get_success_rate() == 0.75

    def test_reset(self) -> None:
        """Test metrics reset."""
        metrics = ProcessingMetrics()

        # Add some data
        metrics.record_processing_time(0.01)
        metrics.record_validation_error()

        assert metrics.total_processed == 1
        assert metrics.validation_errors == 1

        # Reset metrics
        new_metrics = metrics.reset()

        assert new_metrics.total_processed == 0
        assert new_metrics.validation_errors == 0
        assert new_metrics.transformation_errors == 0
        assert new_metrics.handler_errors == 0
        assert new_metrics.total_processing_time_seconds == 0.0

    def test_merge(self) -> None:
        """Test metrics merging."""
        metrics1 = ProcessingMetrics()
        metrics2 = ProcessingMetrics()

        # Add data to first metrics
        metrics1.record_processing_time(0.05)
        metrics1.record_validation_error()

        # Add data to second metrics
        metrics2.record_processing_time(0.03)
        metrics2.record_transformation_error()

        # Merge metrics
        merged = metrics1.merge(metrics2)

        assert merged.total_processed == 2
        assert merged.validation_errors == 1
        assert merged.transformation_errors == 1
        assert merged.handler_errors == 0
        assert merged.total_processing_time_seconds == 0.08


class TestProcessorMetrics:
    """Test the ProcessorMetrics model."""

    def test_from_processor(self) -> None:
        """Test creating processor metrics from components."""
        processing_metrics = ProcessingMetrics()
        processing_metrics.record_processing_time(0.05)

        processor_metrics = ProcessorMetrics.from_processor(
            processor_name="TestProcessor",
            raw_model_name="TestModel",
            transformer_type="TestTransformer",
            processing_metrics=processing_metrics,
        )

        assert processor_metrics.processor_name == "TestProcessor"
        assert processor_metrics.raw_model_name == "TestModel"
        assert processor_metrics.transformer_type == "TestTransformer"
        assert processor_metrics.processing_metrics.total_processed == 1
        assert processor_metrics.processing_metrics.total_processing_time_seconds == 0.05

    def test_model_validation(self) -> None:
        """Test Pydantic model validation."""
        processing_metrics = ProcessingMetrics()

        # Valid data should work
        processor_metrics = ProcessorMetrics(
            processor_name="TestProcessor",
            raw_model_name="TestModel",
            transformer_type="TestTransformer",
            processing_metrics=processing_metrics,
        )

        assert processor_metrics.processor_name == "TestProcessor"

        # Invalid data should raise ValidationError
        with pytest.raises(ValidationError):  # Pydantic ValidationError
            ProcessorMetrics(
                processor_name="",  # Invalid: empty string
                raw_model_name="TestModel",
                transformer_type="TestTransformer",
                processing_metrics=ProcessingMetrics(),
            )
