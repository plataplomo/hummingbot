"""Adapter performance optimization tests.

This module tests and optimizes the performance of the WebSocket error adapter
to ensure minimal overhead in compatibility layer operations.
"""

from __future__ import annotations

import gc
import time

import pytest

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestAdapterPerformanceOptimization:
    """Test performance of WebSocket error adapter operations."""

    def test_single_error_conversion_performance(self) -> None:
        """Test single WebSocket to APIError conversion performance."""
        # Target: < 100µs per conversion
        target_time_us = 100

        ws_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST, message="Test connection lost error"
        )

        # Warm up
        for _ in range(10):
            WebSocketErrorAdapter.to_api_error(ws_error)

        start_time = time.perf_counter()
        api_error = WebSocketErrorAdapter.to_api_error(ws_error)
        end_time = time.perf_counter()

        conversion_time_us = (end_time - start_time) * 1_000_000

        assert isinstance(api_error, APIError)
        assert api_error.message == ws_error.message
        assert conversion_time_us < target_time_us, (
            f"Error conversion took {conversion_time_us:.1f}µs, target was {target_time_us}µs"
        )

    def test_bulk_error_conversion_performance(self) -> None:
        """Test bulk WebSocket error conversion performance."""
        # Target: < 50ms for 1000 conversions
        count = 1000
        target_total_time_ms = 50

        # Create different types of WebSocket errors
        error_types = [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.SEQUENCE_GAP,
            WebSocketErrorCode.VALIDATION_FAILED,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.AUTH_EXPIRED,
        ]

        ws_errors = []
        for i in range(count):
            error_code = error_types[i % len(error_types)]
            ws_error = ErrorTestFactory.create_test_error(
                code=error_code, message=f"Test error {i}"
            )
            ws_errors.append(ws_error)

        # Warm up
        for i in range(10):
            WebSocketErrorAdapter.to_api_error(ws_errors[i % len(ws_errors)])

        gc.collect()

        start_time = time.perf_counter()
        api_errors = []
        for ws_error in ws_errors:
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)
            api_errors.append(api_error)
        end_time = time.perf_counter()

        total_time_ms = (end_time - start_time) * 1000
        avg_time_us = (total_time_ms * 1000) / count

        assert len(api_errors) == count
        assert all(isinstance(err, APIError) for err in api_errors)
        assert total_time_ms < target_total_time_ms, (
            f"Bulk conversion took {total_time_ms:.1f}ms, target was {target_total_time_ms}ms"
        )
        assert avg_time_us < target_total_time_ms * 1000 / count, (
            f"Average conversion time was {avg_time_us:.1f}µs, "
            f"target was {target_total_time_ms * 1000 / count:.1f}µs"
        )

    def test_metadata_building_performance(self) -> None:
        """Test metadata building performance."""
        # Target: < 50µs for metadata building
        target_time_us = 50

        # Create WebSocket error with rich context
        context = ErrorTestFactory.create_test_context(
            connection_id="perf-test-connection",
            sequence_number=1000,
            expected_sequence=1005,
            active_subscriptions=25,
            raw_message_size=4096,
        )

        # Add error chain for complex metadata
        context.add_to_error_chain(ValueError("Test error 1"))
        context.add_to_error_chain(RuntimeError("Test error 2"))

        ws_error = WebSocketStreamError(
            message="Complex error for metadata test",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=context,
        )

        # Warm up
        for _ in range(10):
            WebSocketErrorAdapter._build_metadata(ws_error)

        start_time = time.perf_counter()
        metadata = WebSocketErrorAdapter._build_metadata(ws_error)
        end_time = time.perf_counter()

        build_time_us = (end_time - start_time) * 1_000_000

        assert isinstance(metadata, dict)
        assert "error_domain" in metadata
        assert "connection_id" in metadata
        assert "error_chain" in metadata
        assert len(metadata["error_chain"]) == 2

        assert build_time_us < target_time_us, (
            f"Metadata building took {build_time_us:.1f}µs, target was {target_time_us}µs"
        )

    def test_http_status_mapping_performance(self) -> None:
        """Test HTTP status code mapping performance."""
        # Target: < 10µs for status mapping
        target_time_us = 10

        # Test all WebSocket error codes
        all_error_codes = list(WebSocketErrorCode)

        for ws_code in all_error_codes:
            ws_error = ErrorTestFactory.create_test_error(code=ws_code)

            # Get API code for mapping
            api_code = WebSocketErrorAdapter.WS_TO_API_CODE_MAP.get(ws_code)
            if api_code is None:
                continue

            # Warm up
            for _ in range(10):
                WebSocketErrorAdapter._get_http_status_from_code(api_code, ws_code)

            start_time = time.perf_counter()
            status_code = WebSocketErrorAdapter._get_http_status_from_code(api_code, ws_code)
            end_time = time.perf_counter()

            mapping_time_us = (end_time - start_time) * 1_000_000

            assert isinstance(status_code, int)
            assert 200 <= status_code <= 599  # Valid HTTP status range

            assert mapping_time_us < target_time_us, (
                f"HTTP status mapping for {ws_code.name} took {mapping_time_us:.1f}µs, "
                f"target was {target_time_us}µs"
            )

    def test_legacy_monitoring_data_performance(self) -> None:
        """Test legacy monitoring data extraction performance."""
        # Target: < 200µs for monitoring data extraction
        target_time_us = 200

        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Warm up
        for _ in range(10):
            WebSocketErrorAdapter.get_legacy_monitoring_data(ws_error)

        start_time = time.perf_counter()
        monitoring_data = WebSocketErrorAdapter.get_legacy_monitoring_data(ws_error)
        end_time = time.perf_counter()

        extraction_time_us = (end_time - start_time) * 1_000_000

        assert isinstance(monitoring_data, dict)
        assert "error_type" in monitoring_data
        assert "error_code" in monitoring_data
        assert "severity" in monitoring_data
        assert "recovery_strategy" in monitoring_data

        assert extraction_time_us < target_time_us, (
            f"Legacy monitoring data extraction took {extraction_time_us:.1f}µs, "
            f"target was {target_time_us}µs"
        )

    def test_retryable_error_check_performance(self) -> None:
        """Test retryable error checking performance."""
        # Target: < 5µs for retryable check
        target_time_us = 5

        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Warm up
        for _ in range(10):
            WebSocketErrorAdapter.is_retryable_ws_error(ws_error)

        start_time = time.perf_counter()
        is_retryable = WebSocketErrorAdapter.is_retryable_ws_error(ws_error)
        end_time = time.perf_counter()

        check_time_us = (end_time - start_time) * 1_000_000

        assert isinstance(is_retryable, bool)
        assert check_time_us < target_time_us, (
            f"Retryable check took {check_time_us:.1f}µs, target was {target_time_us}µs"
        )

    def test_retry_delay_calculation_performance(self) -> None:
        """Test retry delay calculation performance."""
        # Target: < 5µs for retry delay calculation
        target_time_us = 5

        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED)

        # Warm up
        for _ in range(10):
            WebSocketErrorAdapter.get_retry_delay_seconds(ws_error)

        start_time = time.perf_counter()
        delay_seconds = WebSocketErrorAdapter.get_retry_delay_seconds(ws_error)
        end_time = time.perf_counter()

        calc_time_us = (end_time - start_time) * 1_000_000

        assert isinstance(delay_seconds, (int, float))
        assert delay_seconds >= 0
        assert calc_time_us < target_time_us, (
            f"Retry delay calculation took {calc_time_us:.1f}µs, target was {target_time_us}µs"
        )

    def test_circuit_breaker_check_performance(self) -> None:
        """Test circuit breaker checking performance."""
        # Target: < 2µs for circuit breaker check
        target_time_us = 2

        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Warm up
        for _ in range(10):
            WebSocketErrorAdapter.should_circuit_break(ws_error)

        start_time = time.perf_counter()
        should_break = WebSocketErrorAdapter.should_circuit_break(ws_error)
        end_time = time.perf_counter()

        check_time_us = (end_time - start_time) * 1_000_000

        assert isinstance(should_break, bool)
        assert check_time_us < target_time_us, (
            f"Circuit breaker check took {check_time_us:.1f}µs, target was {target_time_us}µs"
        )

    def test_alert_level_mapping_performance(self) -> None:
        """Test alert level mapping performance."""
        # Target: < 1µs for alert level mapping
        target_time_us = 1

        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Warm up
        for _ in range(10):
            WebSocketErrorAdapter.get_alert_level(ws_error)

        start_time = time.perf_counter()
        alert_level = WebSocketErrorAdapter.get_alert_level(ws_error)
        end_time = time.perf_counter()

        mapping_time_us = (end_time - start_time) * 1_000_000

        assert alert_level in ["info", "warning", "error", "critical"]
        assert mapping_time_us < target_time_us, (
            f"Alert level mapping took {mapping_time_us:.1f}µs, target was {target_time_us}µs"
        )

    @pytest.mark.parametrize("error_count", [10, 50, 100, 500])
    def test_adapter_scaling_performance(self, error_count: int) -> None:
        """Test how adapter scales with error count."""
        # Target: performance should scale linearly
        max_time_per_error_us = 100  # 100µs per error max

        # Create various types of errors
        error_codes = [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.SEQUENCE_GAP,
            WebSocketErrorCode.VALIDATION_FAILED,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.AUTH_EXPIRED,
        ]

        ws_errors = []
        for i in range(error_count):
            code = error_codes[i % len(error_codes)]
            ws_error = ErrorTestFactory.create_test_error(
                code=code, message=f"Scaling test error {i}"
            )
            ws_errors.append(ws_error)

        # Warm up
        for i in range(min(10, error_count)):
            WebSocketErrorAdapter.to_api_error(ws_errors[i])

        gc.collect()

        start_time = time.perf_counter()
        for ws_error in ws_errors:
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)
            # Also test additional operations
            WebSocketErrorAdapter.is_retryable_ws_error(ws_error)
            WebSocketErrorAdapter.get_alert_level(ws_error)
        end_time = time.perf_counter()

        total_time_us = (end_time - start_time) * 1_000_000
        time_per_error_us = total_time_us / error_count

        assert time_per_error_us < max_time_per_error_us, (
            f"Processing {error_count} errors took {time_per_error_us:.1f}µs per error, "
            f"target was {max_time_per_error_us}µs per error"
        )

    def test_adapter_memory_efficiency(self) -> None:
        """Test memory efficiency of adapter operations."""
        # Target: adapter should not accumulate excessive memory
        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Perform many adapter operations
        api_errors = []
        for i in range(1000):
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)

            # Only keep every 100th error to avoid excessive memory usage in test
            if i % 100 == 0:
                api_errors.append(api_error)

            # Test other operations
            WebSocketErrorAdapter.is_retryable_ws_error(ws_error)
            WebSocketErrorAdapter.get_alert_level(ws_error)

        # Memory usage should be reasonable
        assert len(api_errors) == 10  # Every 100th error

        # Verify all errors are properly converted
        for api_error in api_errors:
            assert isinstance(api_error, APIError)
            assert api_error.message == ws_error.message

    def test_concurrent_adapter_performance(self) -> None:
        """Test adapter performance under concurrent load simulation."""
        # Target: concurrent operations should not degrade significantly
        max_degradation_factor = 1.5  # 50% degradation acceptable

        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Measure sequential performance
        start_time = time.perf_counter()
        for _ in range(100):
            WebSocketErrorAdapter.to_api_error(ws_error)
        sequential_time = time.perf_counter() - start_time

        # Measure rapid succession (simulating concurrent load)
        start_time = time.perf_counter()
        api_errors = []
        for i in range(100):
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)
            api_errors.append(api_error)
        concurrent_time = time.perf_counter() - start_time

        degradation_factor = concurrent_time / sequential_time if sequential_time > 0 else 0

        assert len(api_errors) == 100
        assert all(isinstance(err, APIError) for err in api_errors)
        assert degradation_factor < max_degradation_factor, (
            f"Concurrent adapter operations are {degradation_factor:.1f}x slower than sequential, "
            f"max allowed is {max_degradation_factor}x"
        )

    def test_adapter_with_complex_context_performance(self) -> None:
        """Test adapter performance with complex error contexts."""
        # Target: complex contexts should not significantly impact performance
        max_slowdown_factor = 2.0  # 2x slowdown acceptable for complex contexts

        # Simple context
        simple_context = ErrorTestFactory.create_test_context()
        simple_error = WebSocketStreamError(
            message="Simple error", code=WebSocketErrorCode.CONNECTION_LOST, context=simple_context
        )

        # Complex context
        complex_context = ErrorTestFactory.create_test_context(
            connection_id="complex-perf-test-connection",
            sequence_number=1000,
            expected_sequence=1005,
            active_subscriptions=50,
            raw_message_size=8192,
        )

        # Add large error chain
        for i in range(10):
            complex_context.add_to_error_chain(ValueError(f"Error {i}"))

        complex_error = WebSocketStreamError(
            message="Complex error with rich context",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=complex_context,
        )

        # Measure simple context performance
        start_time = time.perf_counter()
        for _ in range(100):
            WebSocketErrorAdapter.to_api_error(simple_error)
        simple_time = time.perf_counter() - start_time

        # Measure complex context performance
        start_time = time.perf_counter()
        for _ in range(100):
            WebSocketErrorAdapter.to_api_error(complex_error)
        complex_time = time.perf_counter() - start_time

        slowdown_factor = complex_time / simple_time if simple_time > 0 else 0

        assert slowdown_factor < max_slowdown_factor, (
            f"Complex context processing is {slowdown_factor:.1f}x slower than simple, "
            f"max allowed is {max_slowdown_factor}x"
        )
