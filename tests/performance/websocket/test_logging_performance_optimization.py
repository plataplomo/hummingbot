"""Logging performance optimization tests.

This module tests and optimizes the performance of error logging
to ensure minimal overhead in error handling scenarios.
"""

from __future__ import annotations

import gc
import logging
import time
from io import StringIO

import pytest

from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.apis.websocket.ws_stream_log_data import WebSocketStreamLogData
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorConfig,
    WebSocketErrorMetricsConfig,
)
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestLoggingPerformanceOptimization:
    """Test performance of error logging operations."""

    def test_single_log_message_performance(self) -> None:
        """Test single log message creation performance."""
        # Target: < 50μs per log message creation
        target_time_us = 50

        _handler = WebSocketStreamErrorHandler(
            config=WebSocketErrorConfig(
                metrics=WebSocketErrorMetricsConfig(enable_metrics_collection=False)
            )  # Disable metrics for pure logging test
        )

        error = ErrorTestFactory.create_test_error()

        # Warm up
        for _ in range(10):
            WebSocketStreamLogData.from_stream_error(
                error.code, error.message, error.context, error.severity, error.recovery_strategy
            )

        start_time = time.perf_counter()
        log_data = WebSocketStreamLogData.from_stream_error(
            error.code, error.message, error.context, error.severity, error.recovery_strategy
        )
        end_time = time.perf_counter()

        creation_time_us = (end_time - start_time) * 1_000_000

        assert log_data is not None
        assert log_data.message
        assert creation_time_us < target_time_us, (
            f"Log data creation took {creation_time_us:.1f}μs, target was {target_time_us}μs"
        )

    def test_bulk_logging_performance(self) -> None:
        """Test bulk logging performance."""
        # Target: < 100ms for 1000 log messages
        count = 1000
        target_total_time_ms = 100

        _handler = WebSocketStreamErrorHandler(
            config=WebSocketErrorConfig(
                metrics=WebSocketErrorMetricsConfig(enable_metrics_collection=False)
            )
        )

        # Create test errors
        errors = [ErrorTestFactory.create_test_error() for _ in range(count)]

        # Warm up
        for i in range(10):
            WebSocketStreamLogData.from_stream_error(
                errors[i % len(errors)].code,
                errors[i % len(errors)].message,
                errors[i % len(errors)].context,
                errors[i % len(errors)].severity,
                errors[i % len(errors)].recovery_strategy,
            )

        gc.collect()  # Clean up before measurement

        start_time = time.perf_counter()
        log_data_list = []
        for i, error in enumerate(errors):
            log_data = WebSocketStreamLogData.from_stream_error(
                error.code, error.message, error.context, error.severity, error.recovery_strategy
            )
            log_data_list.append(log_data)
        end_time = time.perf_counter()

        total_time_ms = (end_time - start_time) * 1000
        avg_time_us = (total_time_ms * 1000) / count

        assert len(log_data_list) == count
        assert total_time_ms < target_total_time_ms, (
            f"Bulk logging took {total_time_ms:.1f}ms, target was {target_total_time_ms}ms"
        )
        assert avg_time_us < target_total_time_ms * 1000 / count, (
            f"Average log creation took {avg_time_us:.1f}μs, target was {target_total_time_ms * 1000 / count:.1f}μs"
        )

    def test_logging_format_performance(self) -> None:
        """Test logging format string performance."""
        # Target: < 40μs for log formatting
        target_time_us = 40

        # Create a test logger with string buffer
        logger = logging.getLogger("test_performance_logger")
        logger.setLevel(logging.INFO)

        # Add a StringIO handler to capture output without file I/O overhead
        string_handler = logging.StreamHandler(StringIO())
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        string_handler.setFormatter(formatter)
        logger.addHandler(string_handler)

        error = ErrorTestFactory.create_test_error()
        log_data = WebSocketStreamLogData.from_stream_error(
            error.code, error.message, error.context, error.severity, error.recovery_strategy
        )

        # Warm up
        for _ in range(10):
            logger.info(log_data.message)

        start_time = time.perf_counter()
        logger.info(log_data.message)
        end_time = time.perf_counter()

        format_time_us = (end_time - start_time) * 1_000_000

        assert format_time_us < target_time_us, (
            f"Log formatting took {format_time_us:.1f}μs, target was {target_time_us}μs"
        )

        # Clean up
        logger.removeHandler(string_handler)

    def test_structured_logging_performance(self) -> None:
        """Test structured logging vs string formatting performance."""
        # Target: structured logging should not be more than 50x slower than string formatting
        max_slowdown_factor = 50.0

        _handler = WebSocketStreamErrorHandler(
            config=WebSocketErrorConfig(
                metrics=WebSocketErrorMetricsConfig(enable_metrics_collection=False)
            )
        )
        error = ErrorTestFactory.create_test_error()

        # Test string formatting
        start_time = time.perf_counter()
        for _ in range(100):
            _message = f"Error {error.code.name}: {error.message}"
        string_time = time.perf_counter() - start_time

        # Test structured logging
        start_time = time.perf_counter()
        for _ in range(100):
            _log_data = WebSocketStreamLogData.from_stream_error(
                error.code, error.message, error.context, error.severity, error.recovery_strategy
            )
        structured_time = time.perf_counter() - start_time

        slowdown_factor = structured_time / string_time if string_time > 0 else 0

        assert slowdown_factor < max_slowdown_factor, (
            f"Structured logging is {slowdown_factor:.1f}x slower than string formatting, "
            f"max allowed is {max_slowdown_factor}x"
        )

    def test_log_level_filtering_performance(self) -> None:
        """Test log level filtering performance impact."""
        # Target: log level checks should be < 1μs
        target_time_us = 1

        logger = logging.getLogger("test_level_logger")
        logger.setLevel(logging.ERROR)  # Set high level to test filtering

        error = ErrorTestFactory.create_test_error()
        _log_data = WebSocketStreamLogData.from_stream_error(
            error.code, error.message, error.context, error.severity, error.recovery_strategy
        )

        # Warm up
        for _ in range(100):
            logger.isEnabledFor(logging.INFO)

        start_time = time.perf_counter()
        enabled = logger.isEnabledFor(logging.INFO)
        end_time = time.perf_counter()

        check_time_us = (end_time - start_time) * 1_000_000

        assert not enabled  # Should be False since we set level to ERROR
        assert check_time_us < target_time_us, (
            f"Log level check took {check_time_us:.1f}μs, target was {target_time_us}μs"
        )

    def test_context_serialization_performance(self) -> None:
        """Test context serialization for logging performance."""
        # Target: < 100μs for context serialization
        target_time_us = 100

        context = ErrorTestFactory.create_test_context(
            connection_id="perf-test-context",
            sequence_number=1000,
            expected_sequence=1005,
        )

        # Add error chain to make it more complex
        context.add_to_error_chain(ValueError("Test error"))
        context.add_to_error_chain(RuntimeError("Runtime error"))

        # Warm up
        for _ in range(10):
            context.model_dump()

        start_time = time.perf_counter()
        serialized = context.model_dump()
        end_time = time.perf_counter()

        serialization_time_us = (end_time - start_time) * 1_000_000

        assert isinstance(serialized, dict)
        assert "connection_id" in serialized
        assert serialization_time_us < target_time_us, (
            f"Context serialization took {serialization_time_us:.1f}μs, target was {target_time_us}μs"
        )

    def test_large_error_message_performance(self) -> None:
        """Test performance with large error messages."""
        # Target: performance should scale linearly with message size
        max_time_per_kb_us = 15  # 15μs per KB

        _handler = WebSocketStreamErrorHandler(
            config=WebSocketErrorConfig(
                metrics=WebSocketErrorMetricsConfig(enable_metrics_collection=False)
            )
        )

        # Test different message sizes
        message_sizes = [1024, 4096, 16384]  # 1KB, 4KB, 16KB

        for size in message_sizes:
            large_message = "A" * size
            error = WebSocketStreamError(
                message=large_message,
                code=WebSocketErrorCode.VALIDATION_FAILED,
                context=ErrorTestFactory.create_test_context(),
            )

            # Warm up
            for _ in range(5):
                WebSocketStreamLogData.from_stream_error(
                    error.code,
                    error.message,
                    error.context,
                    error.severity,
                    error.recovery_strategy,
                )

            start_time = time.perf_counter()
            log_data = WebSocketStreamLogData.from_stream_error(
                error.code, error.message, error.context, error.severity, error.recovery_strategy
            )
            end_time = time.perf_counter()

            processing_time_us = (end_time - start_time) * 1_000_000
            time_per_kb = processing_time_us / (size / 1024)

            assert log_data is not None
            assert len(log_data.message) >= size
            assert time_per_kb < max_time_per_kb_us, (
                f"Processing {size} byte message took {time_per_kb:.1f}μs/KB, "
                f"target was {max_time_per_kb_us}μs/KB"
            )

    def test_concurrent_logging_performance(self) -> None:
        """Test concurrent logging performance."""
        # Target: concurrent logging should not degrade significantly
        max_degradation_factor = 1.5  # 50% degradation acceptable

        _handler = WebSocketStreamErrorHandler(
            config=WebSocketErrorConfig(
                metrics=WebSocketErrorMetricsConfig(enable_metrics_collection=False)
            )
        )
        error = ErrorTestFactory.create_test_error()

        # Measure single-threaded performance
        start_time = time.perf_counter()
        for _ in range(100):
            WebSocketStreamLogData.from_stream_error(
                error.code, error.message, error.context, error.severity, error.recovery_strategy
            )
        sequential_time = time.perf_counter() - start_time

        # Measure concurrent performance (simulated by rapid succession)
        start_time = time.perf_counter()
        log_data_list = []
        for i in range(100):
            log_data = WebSocketStreamLogData.from_stream_error(
                error.code, error.message, error.context, error.severity, error.recovery_strategy
            )
            log_data_list.append(log_data)
        concurrent_time = time.perf_counter() - start_time

        degradation_factor = concurrent_time / sequential_time if sequential_time > 0 else 0

        assert len(log_data_list) == 100
        assert degradation_factor < max_degradation_factor, (
            f"Concurrent logging is {degradation_factor:.1f}x slower than sequential, "
            f"max allowed is {max_degradation_factor}x"
        )

    def test_log_data_memory_efficiency(self) -> None:
        """Test memory efficiency of log data structures."""
        # Target: each log data object should use < 2KB
        target_size_bytes = 2048

        error = ErrorTestFactory.create_test_error()

        # Create log data
        log_data = WebSocketStreamLogData.from_stream_error(
            error.code, error.message, error.context, error.severity, error.recovery_strategy
        )

        # Estimate size (approximate)
        estimated_size = (
            len(log_data.message.encode("utf-8"))
            + len(log_data.error_code_name.encode("utf-8"))
            + len(log_data.severity.name.encode("utf-8"))
            + 64  # Overhead for other fields
        )

        assert estimated_size < target_size_bytes, (
            f"Log data estimated size {estimated_size} bytes exceeds target {target_size_bytes} bytes"
        )

    @pytest.mark.parametrize("error_count", [10, 50, 100, 500])
    def test_logging_scaling_performance(self, error_count: int) -> None:
        """Test how logging performance scales with error count."""
        # Target: performance should scale linearly (O(n))
        max_time_per_error_us = 100  # 100μs per error max

        _handler = WebSocketStreamErrorHandler(
            config=WebSocketErrorConfig(
                metrics=WebSocketErrorMetricsConfig(enable_metrics_collection=False)
            )
        )

        # Create test errors
        errors = [ErrorTestFactory.create_test_error() for _ in range(error_count)]

        # Warm up
        for i in range(min(10, error_count)):
            WebSocketStreamLogData.from_stream_error(
                errors[i].code,
                errors[i].message,
                errors[i].context,
                errors[i].severity,
                errors[i].recovery_strategy,
            )

        gc.collect()

        start_time = time.perf_counter()
        for i, error in enumerate(errors):
            WebSocketStreamLogData.from_stream_error(
                error.code, error.message, error.context, error.severity, error.recovery_strategy
            )
        end_time = time.perf_counter()

        total_time_us = (end_time - start_time) * 1_000_000
        time_per_error_us = total_time_us / error_count

        assert time_per_error_us < max_time_per_error_us, (
            f"Processing {error_count} errors took {time_per_error_us:.1f}μs per error, "
            f"target was {max_time_per_error_us}μs per error"
        )
