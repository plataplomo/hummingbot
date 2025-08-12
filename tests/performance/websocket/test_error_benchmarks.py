"""Comprehensive performance benchmarks for WebSocket error system.

This module provides detailed performance benchmarks for the WebSocket error
handling system, establishing baseline metrics and performance regression tests.
"""

from __future__ import annotations

import gc
import statistics
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.apis.websocket.ws_stream_recovery import StreamRecoverySystem
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorConfig,
    WebSocketErrorRecoveryConfig,
)
from tests.utils.websocket.error_test_utils import ErrorTestFactory


@dataclass
class BenchmarkResult:
    """Benchmark result data."""

    operation: str
    iterations: int
    total_time_ms: float
    avg_time_us: float
    min_time_us: float
    max_time_us: float
    median_time_us: float
    std_dev_us: float
    percentile_95_us: float
    percentile_99_us: float


class PerformanceBenchmarks:
    """Performance benchmark utilities."""

    @staticmethod
    def run_benchmark(
        operation_name: str,
        operation: callable,
        iterations: int = 1000,
        warmup_iterations: int = 100,
    ) -> BenchmarkResult:
        """Run a performance benchmark."""
        # Warmup
        for _ in range(warmup_iterations):
            operation()

        # Collect garbage before benchmark
        gc.collect()

        # Benchmark
        times_us = []
        start_time = time.perf_counter()

        for _ in range(iterations):
            op_start = time.perf_counter()
            operation()
            op_end = time.perf_counter()
            times_us.append((op_end - op_start) * 1_000_000)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        # Calculate statistics
        times_us.sort()

        return BenchmarkResult(
            operation=operation_name,
            iterations=iterations,
            total_time_ms=total_time_ms,
            avg_time_us=statistics.mean(times_us),
            min_time_us=min(times_us),
            max_time_us=max(times_us),
            median_time_us=statistics.median(times_us),
            std_dev_us=statistics.stdev(times_us) if len(times_us) > 1 else 0,
            percentile_95_us=times_us[int(len(times_us) * 0.95)],
            percentile_99_us=times_us[int(len(times_us) * 0.99)],
        )

    @staticmethod
    def format_result(result: BenchmarkResult) -> str:
        """Format benchmark result for display."""
        return (
            f"{result.operation}:\n"
            f"  Iterations: {result.iterations}\n"
            f"  Total Time: {result.total_time_ms:.2f}ms\n"
            f"  Average: {result.avg_time_us:.2f}µs\n"
            f"  Median: {result.median_time_us:.2f}µs\n"
            f"  Min: {result.min_time_us:.2f}µs\n"
            f"  Max: {result.max_time_us:.2f}µs\n"
            f"  Std Dev: {result.std_dev_us:.2f}µs\n"
            f"  95th Percentile: {result.percentile_95_us:.2f}µs\n"
            f"  99th Percentile: {result.percentile_99_us:.2f}µs"
        )


class TestWebSocketErrorBenchmarks:
    """Comprehensive benchmarks for WebSocket error system."""

    def test_error_creation_benchmark(self) -> None:
        """Benchmark WebSocket error creation against baseline.

        Based on Pydantic docs showing 10-50x overhead vs pure Python.
        Our current implementation shows ~500x which needs investigation.
        """

        # Pure Python baseline for comparison
        @dataclass
        class BaselineError:
            message: str
            code: int
            connection_id: str
            exchange: str
            timestamp_ms: int

        # Measure baseline
        result_baseline = PerformanceBenchmarks.run_benchmark(
            "Baseline Error Creation",
            lambda: BaselineError("Test error", 1001, "bench-conn", "hyperliquid", 1000),
            iterations=10000,
        )

        print(PerformanceBenchmarks.format_result(result_baseline))

        # Create a simple context once for reuse
        simple_context = StreamErrorContext(
            connection_id="bench-conn", exchange="hyperliquid", error_timestamp_ms=1000
        )

        # Simple error creation
        result_simple = PerformanceBenchmarks.run_benchmark(
            "Simple WebSocket Error",
            lambda: WebSocketStreamError(
                message="Test error",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=simple_context,
            ),
            iterations=10000,
        )

        print(PerformanceBenchmarks.format_result(result_simple))

        # Complex error with context
        result_complex = PerformanceBenchmarks.run_benchmark(
            "Complex Error with Factory",
            lambda: ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.SEQUENCE_GAP, message="Complex error with full context"
            ),
            iterations=10000,
        )

        print(PerformanceBenchmarks.format_result(result_complex))

        # Calculate overhead factors
        simple_overhead = result_simple.avg_time_us / result_baseline.avg_time_us
        complex_overhead = result_complex.avg_time_us / result_baseline.avg_time_us

        print("\nOverhead Analysis:")
        print(f"Simple error: {simple_overhead:.1f}x slower than baseline")
        print(f"Complex error: {complex_overhead:.1f}x slower than baseline")

        # Based on Pydantic documentation and benchmarks:
        # - Typical Pydantic overhead: 10-50x
        # - Current implementation: ~500x (needs fixing)
        # - Allowing 100x as maximum acceptable (2x typical worst case)
        MAX_ACCEPTABLE_OVERHEAD = 100

        # These will likely fail with current implementation
        # but show what we should aim for
        assert simple_overhead < MAX_ACCEPTABLE_OVERHEAD, (
            f"Simple error overhead {simple_overhead:.1f}x exceeds max {MAX_ACCEPTABLE_OVERHEAD}x"
        )

        # For high-frequency trading, absolute time also matters
        # At 1000 msgs/sec, we have 1ms per message total budget
        assert result_simple.avg_time_us < 100, (
            f"Simple error creation {result_simple.avg_time_us:.1f}µs exceeds 100µs target "
            "(10% of 1ms message budget)"
        )

    def test_context_creation_benchmark(self) -> None:
        """Benchmark error context creation vs baseline.

        StreamErrorContext is a Pydantic model with 20+ fields.
        This tests its creation performance.
        """

        # Pure Python baseline
        @dataclass
        class BaselineContext:
            connection_id: str
            exchange: str
            error_timestamp_ms: int
            channel: str | None = None
            topic: str | None = None
            sequence_number: int | None = None

        # Baseline measurement
        result_baseline = PerformanceBenchmarks.run_benchmark(
            "Baseline Context",
            lambda: BaselineContext("conn-test", "hyperliquid", 1000),
            iterations=10000,
        )

        print(PerformanceBenchmarks.format_result(result_baseline))

        # Minimal context
        result_minimal = PerformanceBenchmarks.run_benchmark(
            "Minimal StreamErrorContext",
            lambda: StreamErrorContext(
                connection_id="conn-test", exchange="hyperliquid", error_timestamp_ms=1000
            ),
            iterations=10000,
        )

        print(PerformanceBenchmarks.format_result(result_minimal))

        # Full context
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        result_full = PerformanceBenchmarks.run_benchmark(
            "Full StreamErrorContext",
            lambda: ErrorTestFactory.create_test_context(
                connection_id="perf-test-connection",
                exchange="backpack",
                channel="orderbook",
                topic="ETH-USDC",
                sequence_number=5000,
                expected_sequence=5005,
                active_subscriptions=25,
                raw_message_size=4096,
                error_timestamp_ms=now_ms,
            ),
            iterations=10000,
        )

        print(PerformanceBenchmarks.format_result(result_full))

        # Calculate overheads
        minimal_overhead = result_minimal.avg_time_us / result_baseline.avg_time_us
        full_overhead = result_full.avg_time_us / result_baseline.avg_time_us

        print("\nContext Creation Overhead:")
        print(f"Minimal: {minimal_overhead:.1f}x vs baseline")
        print(f"Full: {full_overhead:.1f}x vs baseline")

        # Pydantic models with many fields can be expensive
        # Allow up to 50x for minimal, 100x for full
        assert minimal_overhead < 50, f"Minimal context overhead {minimal_overhead:.1f}x too high"
        assert full_overhead < 100, f"Full context overhead {full_overhead:.1f}x too high"

    def test_adapter_conversion_benchmark(self) -> None:
        """Benchmark WebSocket to APIError conversion.

        This operation happens frequently and should be fast.
        It's mostly mapping fields, so overhead should be minimal.
        """
        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED)

        result = PerformanceBenchmarks.run_benchmark(
            "WebSocket to APIError Conversion",
            lambda: WebSocketErrorAdapter.to_api_error(ws_error),
            iterations=10000,
        )

        print(PerformanceBenchmarks.format_result(result))

        # This is a mapping operation, should be very fast
        # Even with current slow error creation, conversion itself should be quick
        assert result.avg_time_us < 50, (
            f"Adapter conversion {result.avg_time_us:.1f}µs too slow for mapping operation"
        )
        assert result.percentile_95_us < 100, (
            f"95th percentile {result.percentile_95_us:.1f}µs shows high variability"
        )

    def test_adapter_operations_benchmark(self) -> None:
        """Benchmark various adapter operations.

        These are simple lookups/checks that happen on every error.
        They must be extremely fast for high-throughput scenarios.
        """
        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Retryable check
        result_retryable = PerformanceBenchmarks.run_benchmark(
            "Retryable Check",
            lambda: WebSocketErrorAdapter.is_retryable_ws_error(ws_error),
            iterations=50000,
        )

        print(PerformanceBenchmarks.format_result(result_retryable))

        # Alert level mapping
        result_alert = PerformanceBenchmarks.run_benchmark(
            "Alert Level Mapping",
            lambda: WebSocketErrorAdapter.get_alert_level(ws_error),
            iterations=50000,
        )

        print(PerformanceBenchmarks.format_result(result_alert))

        # Retry delay calculation
        result_delay = PerformanceBenchmarks.run_benchmark(
            "Retry Delay Calculation",
            lambda: WebSocketErrorAdapter.get_retry_delay_seconds(ws_error),
            iterations=50000,
        )

        print(PerformanceBenchmarks.format_result(result_delay))

        # These are simple operations - should be nanoseconds, not microseconds
        # At 1000 msgs/sec, these happen 1000 times and must not add up
        assert result_retryable.avg_time_us < 1, (
            f"Retryable check {result_retryable.avg_time_us:.2f}µs too slow for simple boolean check"
        )
        assert result_alert.avg_time_us < 1, (
            f"Alert level {result_alert.avg_time_us:.2f}µs too slow for enum mapping"
        )
        assert result_delay.avg_time_us < 2, (
            f"Retry delay {result_delay.avg_time_us:.2f}µs too slow for simple calculation"
        )

    @pytest.mark.asyncio
    async def test_recovery_system_benchmark(self) -> None:
        """Benchmark recovery system operations.

        Recovery system processes errors and determines recovery actions.
        This is async and involves state tracking.
        """
        recovery_config = WebSocketErrorRecoveryConfig(
            max_recovery_attempts=3,
            initial_backoff_ms=100,
            circuit_breaker_enabled=False,  # Disable for benchmark
        )

        recovery_system = StreamRecoverySystem(config=recovery_config)

        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Benchmark async recovery handling
        async def recovery_operation():
            await recovery_system.handle_stream_error(error)

        # Run async benchmark
        iterations = 1000
        times_us = []

        # Warmup
        for _ in range(100):
            await recovery_operation()

        gc.collect()
        start_time = time.perf_counter()

        for _ in range(iterations):
            op_start = time.perf_counter()
            await recovery_operation()
            op_end = time.perf_counter()
            times_us.append((op_end - op_start) * 1_000_000)

        total_time_ms = (time.perf_counter() - start_time) * 1000

        # Calculate statistics
        avg_time_us = statistics.mean(times_us)
        median_time_us = statistics.median(times_us)
        p99_time_us = sorted(times_us)[int(len(times_us) * 0.99)]

        print("Recovery System Benchmark:")
        print(f"  Iterations: {iterations}")
        print(f"  Total Time: {total_time_ms:.2f}ms")
        print(f"  Average: {avg_time_us:.2f}µs")
        print(f"  Median: {median_time_us:.2f}µs")
        print(f"  99th percentile: {p99_time_us:.2f}µs")

        # Recovery involves async operations and state management
        # Should be fast but not as fast as simple sync operations
        assert median_time_us < 1000, f"Recovery median {median_time_us:.1f}µs exceeds 1ms target"
        # P99 should not have huge spikes
        assert p99_time_us < median_time_us * 5, (
            f"Recovery P99 {p99_time_us:.1f}µs shows high variability (>5x median)"
        )

    def test_error_handler_benchmark(self) -> None:
        """Benchmark error handler operations.

        Error handler processes errors, logs them, and determines actions.
        Should be fast as it's on the critical path.
        """
        config = WebSocketErrorConfig()
        handler = WebSocketStreamErrorHandler(config=config)

        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.VALIDATION_FAILED)

        # Benchmark error handling
        result = PerformanceBenchmarks.run_benchmark(
            "Error Handler Process", lambda: handler.handle_error(error), iterations=5000
        )

        print(PerformanceBenchmarks.format_result(result))

        # Error handling is on critical path for message processing
        # Must be fast to not impact latency
        assert result.avg_time_us < 100, (
            f"Error handling {result.avg_time_us:.1f}µs too slow for critical path"
        )
        assert result.percentile_95_us < 200, (
            f"95th percentile {result.percentile_95_us:.1f}µs shows variability"
        )

    def test_bulk_operations_benchmark(self) -> None:
        """Benchmark bulk error operations.

        Tests throughput for processing many errors,
        simulating high message rate scenarios.
        """
        # Create 1000 errors
        errors = [
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode(1000 + (i % 10)), message=f"Bulk test error {i}"
            )
            for i in range(1000)
        ]

        # Benchmark bulk conversion
        def bulk_conversion():
            return [WebSocketErrorAdapter.to_api_error(e) for e in errors]

        result = PerformanceBenchmarks.run_benchmark(
            "Bulk Conversion (1000 errors)", bulk_conversion, iterations=100
        )

        print(PerformanceBenchmarks.format_result(result))

        # Calculate per-error time
        per_error_us = result.avg_time_us / 1000

        print(f"Per-error conversion: {per_error_us:.2f}µs")
        print(f"Throughput: {1_000_000 / per_error_us:.0f} errors/second")

        # For 1000 msgs/sec requirement, each error must process < 1ms
        assert per_error_us < 1000, (
            f"Per-error time {per_error_us:.1f}µs exceeds 1ms budget for 1000/sec rate"
        )

        # Total for 1000 should be reasonable
        assert result.avg_time_us < 1_000_000, (
            f"Bulk conversion {result.avg_time_us / 1000:.1f}ms too slow for 1000 errors"
        )

    def test_scaling_benchmark(self) -> None:
        """Benchmark scaling with different loads.

        CRITICAL: Performance must scale linearly, not exponentially.
        Exponential scaling would make the system unusable at high loads.
        """
        loads = [10, 100, 1000, 5000]
        results = []

        for load in loads:
            errors = [
                ErrorTestFactory.create_test_error(code=WebSocketErrorCode.SEQUENCE_GAP)
                for _ in range(load)
            ]

            def process_errors():
                for error in errors:
                    WebSocketErrorAdapter.to_api_error(error)

            result = PerformanceBenchmarks.run_benchmark(
                f"Processing {load} errors", process_errors, iterations=100
            )

            results.append(result)
            print(PerformanceBenchmarks.format_result(result))

            # Also show per-error time
            per_error = result.avg_time_us / load
            print(f"  Per-error: {per_error:.2f}µs")

        # Verify linear scaling (not exponential)
        print("\nScaling Analysis:")
        for i in range(1, len(results)):
            load_ratio = loads[i] / loads[i - 1]
            time_ratio = results[i].avg_time_us / results[i - 1].avg_time_us

            print(f"{loads[i - 1]} -> {loads[i]}: Load {load_ratio:.1f}x, Time {time_ratio:.1f}x")

            # CRITICAL: Must be roughly linear
            # Allow 30% deviation (some overhead is expected)
            assert time_ratio < load_ratio * 1.3, (
                f"Non-linear scaling detected: {time_ratio:.1f}x time for {load_ratio:.1f}x load. "
                "This would cause exponential degradation at high message rates."
            )

        # Also check absolute performance at highest load
        highest_load_per_error = results[-1].avg_time_us / loads[-1]
        assert highest_load_per_error < 1000, (
            f"At {loads[-1]} errors, per-error time {highest_load_per_error:.1f}µs "
            "exceeds 1ms budget"
        )

    def test_memory_efficiency_benchmark(self) -> None:
        """Benchmark memory efficiency of error operations.

        Tests that errors don't leak memory and GC works properly.
        Important for long-running services.
        """
        # Create and process many errors to test memory efficiency
        iterations = 10000

        def create_and_discard():
            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.AUTH_EXPIRED)
            api_error = WebSocketErrorAdapter.to_api_error(error)
            # Errors go out of scope and should be garbage collected

        # Force garbage collection before benchmark
        gc.collect()

        result = PerformanceBenchmarks.run_benchmark(
            "Create and Discard Errors", create_and_discard, iterations=iterations
        )

        print(PerformanceBenchmarks.format_result(result))

        # Check for performance degradation over time
        # If there's a memory leak, later iterations will be slower
        times = result.percentile_99_us / result.median_time_us
        print(f"P99/Median ratio: {times:.2f}x")

        # Force garbage collection after benchmark
        gc.collect()

        # Performance should be consistent (no memory leak)
        # If there's a leak, std dev will be high
        assert result.std_dev_us < result.avg_time_us * 0.5, (
            f"High variance (std dev {result.std_dev_us:.1f}µs) suggests memory issues"
        )

        # P99 should not be dramatically different from median
        assert times < 3, f"P99/Median ratio {times:.1f}x suggests memory pressure or GC issues"

    def test_concurrent_operations_benchmark(self) -> None:
        """Benchmark performance under concurrent-like load."""
        # Simulate rapid concurrent operations
        operations_count = 1000

        def rapid_operations():
            for i in range(operations_count):
                error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode(1000 + (i % 10)))
                api_error = WebSocketErrorAdapter.to_api_error(error)
                is_retryable = WebSocketErrorAdapter.is_retryable_ws_error(error)
                alert_level = WebSocketErrorAdapter.get_alert_level(error)

        result = PerformanceBenchmarks.run_benchmark(
            f"Rapid Operations ({operations_count} ops)", rapid_operations, iterations=100
        )

        print(PerformanceBenchmarks.format_result(result))

        # Calculate per-operation time
        per_op_us = result.avg_time_us / operations_count
        assert per_op_us < 200, f"Per-operation should be < 200µs, got {per_op_us:.2f}µs"

    def test_worst_case_scenario_benchmark(self) -> None:
        """Benchmark worst-case scenario with complex errors.

        Even in worst case (max fields, long messages, error chains),
        performance must remain acceptable.
        """

        # Create worst-case error with maximum complexity
        def create_worst_case_error():
            context = ErrorTestFactory.create_test_context(
                connection_id="worst-case-perf-test-connection-id-very-long",
                exchange="hyperliquid",
                channel="all_orderbooks_futures_perpetual",
                topic="BTC-USDC-PERP-FUTURES-20251231",
                sequence_number=999999999,
                expected_sequence=1000000000,
                active_subscriptions=100,
                raw_message_size=65536,
            )

            # Add large error chain
            for i in range(20):
                context.add_to_error_chain(ValueError(f"Nested error {i} with long message " * 10))

            return WebSocketStreamError(
                message="Worst case error with maximum complexity " * 10,
                code=WebSocketErrorCode.SEQUENCE_GAP,
                context=context,
                recovery_strategy=WebSocketRecoveryStrategy.FULL_RECONNECT,
            )

        worst_case_error = create_worst_case_error()

        # Benchmark worst-case conversion
        result = PerformanceBenchmarks.run_benchmark(
            "Worst-Case Error Conversion",
            lambda: WebSocketErrorAdapter.to_api_error(worst_case_error),
            iterations=1000,
        )

        print(PerformanceBenchmarks.format_result(result))

        # Even worst case must remain usable
        # 500µs allows 2000 worst-case errors/second
        assert result.median_time_us < 500, (
            f"Worst-case median {result.median_time_us:.1f}µs too slow"
        )

        # P99 should not explode
        assert result.percentile_99_us < 1000, (
            f"Worst-case P99 {result.percentile_99_us:.1f}µs exceeds 1ms"
        )

        # Ratio shows consistency even for complex errors
        ratio = result.percentile_99_us / result.median_time_us
        assert ratio < 3, f"Worst-case P99/median ratio {ratio:.1f}x shows high variability"

    def test_comparative_benchmark(self) -> None:
        """Compare WebSocket error system with legacy APIError.

        Our new system should not be dramatically slower than the old one,
        despite having more features and type safety.
        """
        # Create comparable errors
        ws_error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST, message="Connection lost to exchange"
        )

        # Benchmark WebSocket error creation
        result_ws = PerformanceBenchmarks.run_benchmark(
            "WebSocket Error Creation",
            lambda: ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_LOST, message="Connection lost to exchange"
            ),
            iterations=10000,
        )

        # Benchmark APIError creation (legacy)
        result_api = PerformanceBenchmarks.run_benchmark(
            "APIError Creation (Legacy)",
            lambda: APIError(
                message="Connection lost to exchange", error_code="CONNECTION_LOST", http_status=503
            ),
            iterations=10000,
        )

        print("\nComparative Benchmark:")
        print(PerformanceBenchmarks.format_result(result_ws))
        print(PerformanceBenchmarks.format_result(result_api))

        # Calculate performance ratio
        performance_ratio = result_ws.avg_time_us / result_api.avg_time_us
        print(f"\nPerformance Ratio (WS/API): {performance_ratio:.2f}x")

        # Analysis
        if performance_ratio > 10:
            print("WARNING: WebSocket errors are >10x slower than APIError!")
            print("This indicates a serious performance regression.")

        # We expect some overhead for richer type safety
        # But it shouldn't be extreme
        assert performance_ratio < 20, (
            f"WebSocket errors {performance_ratio:.1f}x slower than APIError. "
            "This overhead is unacceptable for production use."
        )
