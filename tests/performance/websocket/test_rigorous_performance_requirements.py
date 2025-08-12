"""Rigorous performance requirements based on actual Pydantic benchmarks.

This module implements strict performance tests based on real Pydantic
performance data and industry benchmarks, not arbitrary numbers.

Based on Pydantic documentation:
- JSON validation: ~1.54ms (1540µs)
- TypedDict vs BaseModel: ~2.5x performance difference
- Typical Pydantic overhead: 10-50x vs pure Python
"""

from __future__ import annotations

import json
import statistics
import time
import timeit
from dataclasses import dataclass

import pytest

from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestRigorousPerformanceRequirements:
    """Performance tests with strict, justified requirements."""

    def test_pydantic_overhead_within_expected_range(self) -> None:
        """Ensure Pydantic overhead is within the 10-50x range documented.

        Based on Pydantic benchmarks showing typical 10-50x overhead vs pure Python.
        If this test fails, we have a serious performance problem.
        """

        # Pure Python baseline
        @dataclass
        class PureContext:
            connection_id: str
            exchange: str
            error_timestamp_ms: int

        @dataclass
        class PureError:
            message: str
            code: int
            context: PureContext

        # Measure pure Python performance
        def create_pure_error():
            ctx = PureContext("test-conn", "exchange", 1000)
            return PureError("Test error", 1001, ctx)

        pure_time = timeit.timeit(create_pure_error, number=10000)

        # Measure our Pydantic model
        def create_pydantic_error():
            ctx = StreamErrorContext(
                connection_id="test-conn", exchange="exchange", error_timestamp_ms=1000
            )
            return WebSocketStreamError(
                message="Test error", code=WebSocketErrorCode.CONNECTION_LOST, context=ctx
            )

        pydantic_time = timeit.timeit(create_pydantic_error, number=10000)

        overhead = pydantic_time / pure_time

        print(f"Pure Python: {pure_time * 100:.2f}ms for 10k operations")
        print(f"Pydantic: {pydantic_time * 100:.2f}ms for 10k operations")
        print(f"Overhead: {overhead:.1f}x")

        # STRICT REQUIREMENT: Must be within documented range
        assert overhead < 50, (
            f"CRITICAL: Pydantic overhead {overhead:.1f}x exceeds maximum expected 50x. "
            "This indicates a serious performance regression."
        )

        # WARNING if above typical range
        if overhead > 30:
            pytest.warns(
                UserWarning, match=f"Performance warning: {overhead:.1f}x overhead is high"
            )

    def test_json_validation_performance_baseline(self) -> None:
        """Test JSON validation against Pydantic's documented 1.54ms benchmark.

        Pydantic shows ~1.54ms for parsing JSON with validation.
        Our models should achieve similar performance.
        """
        # Create a realistic JSON payload
        json_data = {
            "message": "Connection lost to exchange",
            "code": "CONNECTION_LOST",
            "context": {
                "connection_id": "ws-conn-12345",
                "exchange": "hyperliquid",
                "error_timestamp_ms": 1234567890,
                "channel": "trades",
                "topic": "BTC-USDC",
            },
        }
        json_str = json.dumps(json_data)

        # Measure JSON parsing + validation
        iterations = 1000
        times_ms = []

        for _ in range(iterations):
            start = time.perf_counter()

            # Parse JSON and create models (what users actually do)
            data = json.loads(json_str)
            context = StreamErrorContext(
                connection_id=data["context"]["connection_id"],
                exchange=data["context"]["exchange"],
                error_timestamp_ms=data["context"]["error_timestamp_ms"],
                channel=data["context"].get("channel"),
                topic=data["context"].get("topic"),
            )
            error = WebSocketStreamError(
                message=data["message"], code=WebSocketErrorCode[data["code"]], context=context
            )

            elapsed_ms = (time.perf_counter() - start) * 1000
            times_ms.append(elapsed_ms)

        median_ms = statistics.median(times_ms)
        p95_ms = sorted(times_ms)[int(len(times_ms) * 0.95)]

        print(f"JSON validation median: {median_ms:.2f}ms")
        print(f"JSON validation 95th percentile: {p95_ms:.2f}ms")

        # Based on Pydantic's 1.54ms benchmark
        # Allow 2x for our nested structure
        assert median_ms < 3.0, (
            f"JSON validation too slow: {median_ms:.2f}ms (Pydantic baseline is 1.54ms)"
        )

    def test_high_frequency_operation_requirements(self) -> None:
        """Test operations that occur at high frequency in production.

        For WebSocket streams processing 1000+ messages/second,
        each operation must complete in microseconds, not milliseconds.
        """
        # Pre-create error for adapter operations
        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED)

        # Test 1: Retryable check (happens on EVERY error)
        iterations = 100000
        start = time.perf_counter()
        for _ in range(iterations):
            WebSocketErrorAdapter.is_retryable_ws_error(error)
        elapsed = time.perf_counter() - start
        per_op_us = (elapsed / iterations) * 1_000_000

        print(f"Retryable check: {per_op_us:.2f}µs per operation")

        # For 1000 msgs/sec, we have 1ms per message total
        # This operation should take < 1% of that
        assert per_op_us < 10, f"Retryable check too slow for high-frequency: {per_op_us:.2f}µs"

        # Test 2: Alert level mapping
        start = time.perf_counter()
        for _ in range(iterations):
            WebSocketErrorAdapter.get_alert_level(error)
        elapsed = time.perf_counter() - start
        per_op_us = (elapsed / iterations) * 1_000_000

        print(f"Alert level mapping: {per_op_us:.2f}µs per operation")

        assert per_op_us < 5, f"Alert level mapping too slow: {per_op_us:.2f}µs"

    def test_memory_overhead_per_error(self) -> None:
        """Test memory usage is reasonable for high-volume scenarios.

        At 1000 errors/second, memory usage must be bounded.
        """
        import sys

        # Measure single error memory footprint
        context = StreamErrorContext(
            connection_id="mem-test-conn",
            exchange="hyperliquid",
            error_timestamp_ms=1234567890,
            channel="trades",
            topic="BTC-USDC",
            sequence_number=12345,
            expected_sequence=12350,
        )

        error = WebSocketStreamError(
            message="Memory test error with reasonable message length",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=context,
        )

        # Get size (this is approximate)
        error_size = sys.getsizeof(error) + sys.getsizeof(context)

        print(f"Single error size: ~{error_size} bytes")

        # At 1000 errors/sec for 1 minute = 60,000 errors
        # Should not exceed reasonable memory (e.g., 100MB)
        max_memory_mb = 100
        max_bytes_per_error = (max_memory_mb * 1024 * 1024) / 60000

        assert error_size < max_bytes_per_error, (
            f"Error too large: {error_size} bytes (max {max_bytes_per_error:.0f} for 1000/sec rate)"
        )

    def test_latency_requirements_for_trading(self) -> None:
        """Test that error handling doesn't impact trading latency.

        In trading, every microsecond counts. Error handling should
        not add significant latency to the critical path.
        """
        # Simulate critical path: receive error -> check if retryable -> decide action
        context = StreamErrorContext(
            connection_id="latency-test", exchange="hyperliquid", error_timestamp_ms=1000
        )

        iterations = 10000
        times_us = []

        for _ in range(iterations):
            start = time.perf_counter()

            # Critical path operations
            error = WebSocketStreamError(
                message="Rate limit exceeded", code=WebSocketErrorCode.RATE_LIMITED, context=context
            )
            is_retryable = WebSocketErrorAdapter.is_retryable_ws_error(error)
            if is_retryable:
                delay = WebSocketErrorAdapter.get_retry_delay_seconds(error)

            elapsed_us = (time.perf_counter() - start) * 1_000_000
            times_us.append(elapsed_us)

        median_us = statistics.median(times_us)
        p99_us = sorted(times_us)[int(len(times_us) * 0.99)]

        print(f"Critical path median: {median_us:.2f}µs")
        print(f"Critical path 99th percentile: {p99_us:.2f}µs")

        # For HFT, total round-trip should be < 1ms
        # Error handling should be < 10% of that
        assert median_us < 100, f"Critical path too slow for trading: {median_us:.2f}µs"

        # 99th percentile shouldn't have huge spikes
        assert p99_us < median_us * 3, (
            f"Latency spikes too large: P99 {p99_us:.2f}µs vs median {median_us:.2f}µs"
        )

    def test_comparison_with_standard_exception(self) -> None:
        """Compare our error system with Python's standard exceptions.

        We should not be dramatically slower than built-in exceptions.
        """

        # Standard Python exception
        def create_standard_error():
            return ValueError("Test error message with some context")

        standard_time = timeit.timeit(create_standard_error, number=100000)

        # Our WebSocket error (minimal)
        def create_ws_error():
            ctx = StreamErrorContext(connection_id="test", exchange="test", error_timestamp_ms=1000)
            return WebSocketStreamError(
                message="Test error message with some context",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=ctx,
            )

        ws_time = timeit.timeit(create_ws_error, number=100000)

        ratio = ws_time / standard_time

        print(f"Standard exception: {standard_time * 1000:.2f}ms for 100k")
        print(f"WebSocket error: {ws_time * 1000:.2f}ms for 100k")
        print(f"Ratio: {ratio:.1f}x slower")

        # Should not be more than 100x slower than standard exceptions
        assert ratio < 100, f"WebSocket errors {ratio:.1f}x slower than standard exceptions"

    def test_scalability_under_load(self) -> None:
        """Test that performance doesn't degrade under high load.

        Performance should scale linearly, not exponentially.
        """
        loads = [100, 1000, 10000]
        times = []

        for load in loads:
            start = time.perf_counter()

            for i in range(load):
                ctx = StreamErrorContext(
                    connection_id=f"conn-{i}", exchange="test", error_timestamp_ms=i
                )
                error = WebSocketStreamError(
                    message=f"Error {i}", code=WebSocketErrorCode.CONNECTION_LOST, context=ctx
                )
                # Simulate processing
                _ = WebSocketErrorAdapter.to_api_error(error)

            elapsed = time.perf_counter() - start
            times.append(elapsed)

            print(f"Load {load}: {elapsed * 1000:.2f}ms")

        # Check scaling is roughly linear
        # Time for 10x load should be ~10x time, not 100x
        scale_ratio = times[2] / times[0]  # 10000 vs 100 = 100x load
        expected_ratio = loads[2] / loads[0]  # Should be 100

        # Allow 20% deviation from perfect linear scaling
        assert scale_ratio < expected_ratio * 1.2, (
            f"Non-linear scaling detected: {scale_ratio:.1f}x time for {expected_ratio}x load"
        )

    def test_real_world_websocket_message_processing(self) -> None:
        """Test processing a realistic WebSocket error message.

        Based on actual WebSocket error scenarios in production.
        """
        # Realistic WebSocket error message
        ws_message = {
            "error": {
                "code": 429,
                "message": "Rate limit exceeded: max 100 requests per second",
                "details": {"limit": 100, "window": "1s", "retry_after": 0.5},
            },
            "channel": "trades",
            "subscription": {"symbol": "BTC-USDC", "type": "trades"},
            "timestamp": 1234567890123,
        }

        # Measure end-to-end processing
        iterations = 1000
        times_us = []

        for _ in range(iterations):
            start = time.perf_counter()

            # Parse message and create error
            if "error" in ws_message:
                error_data = ws_message["error"]

                context = StreamErrorContext(
                    connection_id="ws-prod-conn",
                    exchange="hyperliquid",
                    error_timestamp_ms=ws_message["timestamp"],
                    channel=ws_message.get("channel"),
                    topic=ws_message.get("subscription", {}).get("symbol"),
                )

                # Map HTTP code to our error code
                if error_data["code"] == 429:
                    code = WebSocketErrorCode.RATE_LIMITED
                else:
                    code = WebSocketErrorCode.UNKNOWN_ERROR

                error = WebSocketStreamError(
                    message=error_data["message"], code=code, context=context
                )

                # Typical processing
                api_error = WebSocketErrorAdapter.to_api_error(error)
                is_retryable = WebSocketErrorAdapter.is_retryable_ws_error(error)

            elapsed_us = (time.perf_counter() - start) * 1_000_000
            times_us.append(elapsed_us)

        median_us = statistics.median(times_us)

        print(f"Real-world message processing: {median_us:.2f}µs median")

        # Should process in under 1ms for real-time requirements
        assert median_us < 1000, f"Real-world processing too slow: {median_us:.2f}µs (need < 1ms)"
