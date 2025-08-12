"""Realistic performance validation tests for WebSocket error system.

This module provides rigorous performance tests based on actual Pydantic
performance characteristics and real-world usage patterns.
"""

from __future__ import annotations

import gc
import json
import statistics
import time

from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestRealisticPerformanceValidation:
    """Rigorous performance tests based on real Pydantic benchmarks."""

    def test_pydantic_model_creation_overhead(self) -> None:
        """Test that our Pydantic models don't exceed expected overhead.

        Based on Pydantic docs: ~1.54ms for JSON parsing with validation.
        We should expect similar or better for direct model creation.
        """

        # Create a baseline Python class for comparison
        class PythonError:
            def __init__(self, message: str, code: int, context: dict):
                self.message = message
                self.code = code
                self.context = context

        # Baseline: Pure Python class
        iterations = 1000
        python_times = []

        for _ in range(iterations):
            start = time.perf_counter()
            err = PythonError(
                message="Test error",
                code=1001,
                context={"connection_id": "test", "exchange": "test"},
            )
            python_times.append((time.perf_counter() - start) * 1_000_000)

        python_median = statistics.median(python_times)

        # Our Pydantic model
        context = StreamErrorContext(
            connection_id="test-conn", exchange="hyperliquid", error_timestamp_ms=1000
        )

        pydantic_times = []
        for _ in range(iterations):
            start = time.perf_counter()
            err = WebSocketStreamError(
                message="Test error", code=WebSocketErrorCode.CONNECTION_LOST, context=context
            )
            pydantic_times.append((time.perf_counter() - start) * 1_000_000)

        pydantic_median = statistics.median(pydantic_times)

        # Calculate overhead
        overhead_factor = pydantic_median / python_median

        print(f"Pure Python median: {python_median:.2f}µs")
        print(f"Pydantic model median: {pydantic_median:.2f}µs")
        print(f"Overhead factor: {overhead_factor:.2f}x")

        # Pydantic should not be more than 100x slower than pure Python
        # (This is very generous - in practice it's usually 10-50x)
        assert overhead_factor < 100, (
            f"Pydantic overhead too high: {overhead_factor:.2f}x slower than pure Python"
        )

        # Also check absolute time - should be under 1ms for simple creation
        assert pydantic_median < 1000, (
            f"Pydantic model creation too slow: {pydantic_median:.2f}µs (should be < 1000µs)"
        )

    def test_validation_failure_performance(self) -> None:
        """Test performance when validation fails.

        Failed validations are often slower due to error message generation.
        This tests that we handle failures efficiently.
        """
        from pydantic import ValidationError

        # Test various validation failures
        failure_cases = [
            # Invalid connection_id (too short)
            lambda: StreamErrorContext(
                connection_id="x",  # Too short (min 8 chars)
                exchange="test",
                error_timestamp_ms=1000,
            ),
            # Invalid exchange (too long)
            lambda: StreamErrorContext(
                connection_id="valid-conn",
                exchange="x" * 50,  # Too long (max 32 chars)
                error_timestamp_ms=1000,
            ),
            # Invalid timestamp (negative)
            lambda: StreamErrorContext(
                connection_id="valid-conn", exchange="test", error_timestamp_ms=-1000
            ),
        ]

        failure_times = []

        for case_func in failure_cases:
            for _ in range(100):
                start = time.perf_counter()
                try:
                    case_func()
                except ValidationError:
                    pass  # Expected
                failure_times.append((time.perf_counter() - start) * 1_000_000)

        failure_median = statistics.median(failure_times)
        failure_p99 = sorted(failure_times)[int(len(failure_times) * 0.99)]

        print(f"Validation failure median: {failure_median:.2f}µs")
        print(f"Validation failure 99th percentile: {failure_p99:.2f}µs")

        # Validation failures should still be reasonably fast
        assert failure_median < 5000, (
            f"Validation failure too slow: {failure_median:.2f}µs (should be < 5000µs)"
        )

        # 99th percentile should not be catastrophically slow
        assert failure_p99 < 10000, (
            f"Validation failure 99th percentile too slow: {failure_p99:.2f}µs"
        )

    def test_json_serialization_performance(self) -> None:
        """Test JSON serialization/deserialization performance.

        Based on Pydantic docs showing ~1.54ms for JSON validation,
        we should achieve similar or better performance.
        """
        # Create a complex error with full context
        context = ErrorTestFactory.create_test_context(
            connection_id="json-test-conn",
            exchange="hyperliquid",
            channel="trades",
            topic="BTC-USDC",
            sequence_number=12345,
            expected_sequence=12350,
            active_subscriptions=10,
            raw_message_size=2048,
        )

        error = WebSocketStreamError(
            message="Test error for JSON performance",
            code=WebSocketErrorCode.SEQUENCE_GAP,
            context=context,
        )

        # Test serialization
        serialization_times = []
        for _ in range(1000):
            start = time.perf_counter()
            json_str = error.model_dump_json()
            serialization_times.append((time.perf_counter() - start) * 1_000_000)

        ser_median = statistics.median(serialization_times)

        # Test deserialization
        json_data = json.loads(error.model_dump_json())
        deserialization_times = []

        for _ in range(1000):
            start = time.perf_counter()
            WebSocketStreamError.model_validate(json_data)
            deserialization_times.append((time.perf_counter() - start) * 1_000_000)

        deser_median = statistics.median(deserialization_times)

        print(f"JSON serialization median: {ser_median:.2f}µs")
        print(f"JSON deserialization median: {deser_median:.2f}µs")

        # Based on Pydantic benchmarks (~1540µs for JSON parsing)
        # Our models should be in similar range
        assert ser_median < 2000, f"Serialization too slow: {ser_median:.2f}µs (should be < 2000µs)"
        assert deser_median < 2000, (
            f"Deserialization too slow: {deser_median:.2f}µs (should be < 2000µs)"
        )

    def test_memory_allocation_patterns(self) -> None:
        """Test memory allocation and garbage collection impact."""
        import tracemalloc

        # Start memory tracking
        tracemalloc.start()

        # Take snapshot before
        snapshot1 = tracemalloc.take_snapshot()

        # Create many errors
        errors = []
        for i in range(1000):
            context = StreamErrorContext(
                connection_id=f"conn-{i}", exchange="test", error_timestamp_ms=i
            )
            error = WebSocketStreamError(
                message=f"Error {i}", code=WebSocketErrorCode.CONNECTION_LOST, context=context
            )
            errors.append(error)

        # Take snapshot after
        snapshot2 = tracemalloc.take_snapshot()

        # Calculate memory usage
        top_stats = snapshot2.compare_to(snapshot1, "lineno")
        total_memory = sum(stat.size_diff for stat in top_stats)
        memory_per_error = total_memory / 1000

        print(f"Total memory for 1000 errors: {total_memory / 1024:.2f} KB")
        print(f"Memory per error: {memory_per_error:.2f} bytes")

        # Clean up
        errors.clear()
        gc.collect()
        tracemalloc.stop()

        # Each error should not use excessive memory
        # Pydantic models have overhead, but should be reasonable
        assert memory_per_error < 10000, (
            f"Memory per error too high: {memory_per_error:.2f} bytes (should be < 10KB)"
        )

    def test_nested_model_performance_impact(self) -> None:
        """Test performance impact of nested Pydantic models.

        Based on Pydantic docs: TypedDict is ~2.5x faster than nested BaseModel.
        Our nested structure should show similar characteristics.
        """
        from typing_extensions import TypedDict

        # Create a TypedDict version for comparison
        class ContextDict(TypedDict):
            connection_id: str
            exchange: str
            error_timestamp_ms: int
            channel: str | None
            topic: str | None

        # Measure TypedDict "validation" (just dict creation)
        typed_dict_times = []
        for i in range(1000):
            start = time.perf_counter()
            context_dict: ContextDict = {
                "connection_id": f"conn-{i}",
                "exchange": "test",
                "error_timestamp_ms": 1000,
                "channel": "trades",
                "topic": "BTC-USDC",
            }
            typed_dict_times.append((time.perf_counter() - start) * 1_000_000)

        typed_dict_median = statistics.median(typed_dict_times)

        # Measure our Pydantic model
        pydantic_times = []
        for i in range(1000):
            start = time.perf_counter()
            context = StreamErrorContext(
                connection_id=f"conn-{i}",
                exchange="test",
                error_timestamp_ms=1000,
                channel="trades",
                topic="BTC-USDC",
            )
            pydantic_times.append((time.perf_counter() - start) * 1_000_000)

        pydantic_median = statistics.median(pydantic_times)

        # Calculate overhead
        overhead_factor = pydantic_median / typed_dict_median

        print(f"TypedDict median: {typed_dict_median:.2f}µs")
        print(f"Pydantic model median: {pydantic_median:.2f}µs")
        print(f"Overhead factor: {overhead_factor:.2f}x")

        # Based on Pydantic docs, we expect 2-3x overhead for nested models
        # Our single model should be better, but let's be conservative
        assert overhead_factor < 50, (
            f"Pydantic overhead too high compared to TypedDict: {overhead_factor:.2f}x"
        )

    def test_real_world_error_patterns(self) -> None:
        """Test performance with realistic error patterns from production."""
        # Simulate real-world error scenarios
        scenarios = [
            # Connection lost with full context
            lambda: ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_LOST,
                message="WebSocket connection terminated unexpectedly",
            ),
            # Sequence gap with recovery info
            lambda: ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.SEQUENCE_GAP,
                message="Detected sequence gap: expected 1000, got 1005",
            ),
            # Rate limiting with retry info
            lambda: ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.RATE_LIMITED,
                message="Rate limit exceeded: 429 Too Many Requests",
            ),
            # Authentication failure
            lambda: ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.AUTH_FAILED,
                message="Authentication failed: Invalid API key",
            ),
        ]

        # Measure performance for each scenario
        scenario_times = {}

        for i, scenario_func in enumerate(scenarios):
            times = []
            for _ in range(500):
                start = time.perf_counter()
                error = scenario_func()
                # Also test conversion to APIError (common operation)
                api_error = WebSocketErrorAdapter.to_api_error(error)
                times.append((time.perf_counter() - start) * 1_000_000)

            scenario_times[f"scenario_{i}"] = {
                "median": statistics.median(times),
                "p95": sorted(times)[int(len(times) * 0.95)],
                "p99": sorted(times)[int(len(times) * 0.99)],
            }

        # Print results
        for scenario, metrics in scenario_times.items():
            print(f"{scenario}:")
            print(f"  Median: {metrics['median']:.2f}µs")
            print(f"  95th percentile: {metrics['p95']:.2f}µs")
            print(f"  99th percentile: {metrics['p99']:.2f}µs")

        # All scenarios should complete in reasonable time
        for scenario, metrics in scenario_times.items():
            assert metrics["median"] < 2000, (
                f"{scenario} median too slow: {metrics['median']:.2f}µs"
            )
            assert metrics["p99"] < 5000, (
                f"{scenario} 99th percentile too slow: {metrics['p99']:.2f}µs"
            )

    def test_high_throughput_scenario(self) -> None:
        """Test performance under high-throughput conditions."""
        # Simulate high-throughput error generation (e.g., during reconnection)
        errors_per_second_target = 1000  # Target: handle 1000 errors/second
        duration_seconds = 1
        total_errors = errors_per_second_target * duration_seconds

        # Pre-create context to avoid that overhead
        context = StreamErrorContext(
            connection_id="high-throughput-conn", exchange="hyperliquid", error_timestamp_ms=1000
        )

        # Measure throughput
        start_time = time.perf_counter()
        errors_created = 0

        while errors_created < total_errors:
            error = WebSocketStreamError(
                message=f"High throughput error {errors_created}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=context,
            )
            api_error = WebSocketErrorAdapter.to_api_error(error)
            errors_created += 1

        elapsed_time = time.perf_counter() - start_time
        actual_throughput = errors_created / elapsed_time

        print(f"Target throughput: {errors_per_second_target} errors/sec")
        print(f"Actual throughput: {actual_throughput:.2f} errors/sec")
        print(f"Time for {total_errors} errors: {elapsed_time * 1000:.2f}ms")

        # Should achieve at least 80% of target throughput
        assert actual_throughput > errors_per_second_target * 0.8, (
            f"Throughput too low: {actual_throughput:.2f} errors/sec "
            f"(target: {errors_per_second_target})"
        )

    def test_performance_regression_guard(self) -> None:
        """Guard against performance regressions with strict timing requirements."""
        # This test establishes baseline performance metrics that should not regress

        # Simple error creation
        simple_times = []
        context = StreamErrorContext(
            connection_id="regression-test", exchange="test", error_timestamp_ms=1000
        )

        for _ in range(100):
            start = time.perf_counter()
            error = WebSocketStreamError(
                message="Regression test", code=WebSocketErrorCode.CONNECTION_LOST, context=context
            )
            simple_times.append((time.perf_counter() - start) * 1_000_000)

        simple_p50 = statistics.median(simple_times)
        simple_p99 = sorted(simple_times)[99]

        # These are strict requirements - if they fail, we have a regression
        regression_limits = {
            "simple_creation_p50": 1000,  # 1ms median
            "simple_creation_p99": 3000,  # 3ms 99th percentile
        }

        print(
            f"Simple creation P50: {simple_p50:.2f}µs (limit: {regression_limits['simple_creation_p50']}µs)"
        )
        print(
            f"Simple creation P99: {simple_p99:.2f}µs (limit: {regression_limits['simple_creation_p99']}µs)"
        )

        assert simple_p50 < regression_limits["simple_creation_p50"], (
            f"Performance regression detected: P50 {simple_p50:.2f}µs exceeds limit"
        )
        assert simple_p99 < regression_limits["simple_creation_p99"], (
            f"Performance regression detected: P99 {simple_p99:.2f}µs exceeds limit"
        )
