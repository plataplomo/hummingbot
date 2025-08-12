"""Performance monitoring integration tests.

This module tests integration of performance monitoring for WebSocket error operations,
ensuring comprehensive metrics collection and proper monitoring system integration.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_recovery import StreamRecoverySystem
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class MockPerformanceMonitor:
    """Mock performance monitoring system."""

    def __init__(self) -> None:
        self.metrics: dict[str, list[float]] = {}
        self.counters: dict[str, int] = {}
        self.timers: dict[str, tuple[str, float]] = {}
        self.enabled = True
        self._timer_counter = 0

    def record_timing(self, operation: str, duration_ms: float) -> None:
        """Record operation timing."""
        if not self.enabled:
            return
        if operation not in self.metrics:
            self.metrics[operation] = []
        self.metrics[operation].append(duration_ms)

    def increment_counter(self, metric: str, count: int = 1) -> None:
        """Increment counter metric."""
        if not self.enabled:
            return
        self.counters[metric] = self.counters.get(metric, 0) + count

    def start_timer(self, operation: str) -> str:
        """Start a timing operation."""
        self._timer_counter += 1
        timer_id = f"timer_{self._timer_counter}"
        self.timers[timer_id] = (operation, time.perf_counter())
        return timer_id

    def stop_timer(self, timer_id: str) -> float:
        """Stop timing operation and record duration."""
        if timer_id not in self.timers:
            return 0.0

        operation, start_time = self.timers.pop(timer_id)
        duration_ms = (time.perf_counter() - start_time) * 1000

        self.record_timing(operation, duration_ms)
        return duration_ms

    def get_metrics(self) -> dict[str, Any]:
        """Get all collected metrics."""
        return {
            "timings": self.metrics.copy(),
            "counters": self.counters.copy(),
            "active_timers": len(self.timers),
        }

    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self.metrics.clear()
        self.counters.clear()
        self.timers.clear()


class TestPerformanceMonitoringIntegration:
    """Test performance monitoring integration for WebSocket errors."""

    def test_error_creation_monitoring(self) -> None:
        """Test monitoring of error creation performance.

        Based on current performance (~70µs per error),
        monitoring overhead should be minimal.
        """
        monitor = MockPerformanceMonitor()

        # Realistic target based on current performance
        # ~70µs for error + monitoring overhead
        target_time_ms = 0.1  # 100µs = 0.1ms
        operation_count = 100

        # Simulate monitoring integration
        start_time = time.perf_counter()
        for i in range(operation_count):
            timer_id = monitor.start_timer("error_creation")

            # Create error (simulating actual operation)
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_LOST, message=f"Test error {i}"
            )

            duration = monitor.stop_timer(timer_id)
            monitor.increment_counter("errors_created")

        total_time_ms = (time.perf_counter() - start_time) * 1000

        # Verify monitoring data
        metrics = monitor.get_metrics()
        assert "error_creation" in metrics["timings"]
        assert len(metrics["timings"]["error_creation"]) == operation_count
        assert metrics["counters"]["errors_created"] == operation_count

        # Verify performance
        avg_creation_time = sum(metrics["timings"]["error_creation"]) / len(
            metrics["timings"]["error_creation"]
        )
        assert avg_creation_time < target_time_ms, (
            f"Average error creation time {avg_creation_time:.2f}ms > target {target_time_ms}ms"
        )

        # Overall operation should be fast
        assert total_time_ms < operation_count * target_time_ms

    def test_error_adapter_monitoring(self) -> None:
        """Test monitoring of error adapter performance.

        Adapter operations are simple mappings and should be fast.
        Monitoring overhead must be minimal.
        """
        monitor = MockPerformanceMonitor()

        # Realistic target for simple operations
        # These are just field mappings, should be very fast
        target_time_us = 10  # 10µs for simple operations
        operation_count = 50

        ws_error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Monitor adapter operations
        for i in range(operation_count):
            timer_id = monitor.start_timer("adapter_conversion")

            # Convert WebSocket error to API error
            api_error = WebSocketErrorAdapter.to_api_error(ws_error)

            duration_ms = monitor.stop_timer(timer_id)
            monitor.increment_counter("adapter_conversions")

            # Also monitor other adapter operations
            retryable_timer = monitor.start_timer("adapter_retryable_check")
            is_retryable = WebSocketErrorAdapter.is_retryable_ws_error(ws_error)
            monitor.stop_timer(retryable_timer)

            alert_timer = monitor.start_timer("adapter_alert_level")
            alert_level = WebSocketErrorAdapter.get_alert_level(ws_error)
            monitor.stop_timer(alert_timer)

        # Verify monitoring data
        metrics = monitor.get_metrics()

        # Check all operations monitored
        assert "adapter_conversion" in metrics["timings"]
        assert "adapter_retryable_check" in metrics["timings"]
        assert "adapter_alert_level" in metrics["timings"]
        assert metrics["counters"]["adapter_conversions"] == operation_count

        # Verify performance targets
        for operation in ["adapter_conversion", "adapter_retryable_check", "adapter_alert_level"]:
            avg_time_ms = sum(metrics["timings"][operation]) / len(metrics["timings"][operation])
            avg_time_us = avg_time_ms * 1000

            assert avg_time_us < target_time_us, (
                f"Average {operation} time {avg_time_us:.1f}µs > target {target_time_us}µs"
            )

    @pytest.mark.asyncio
    async def test_recovery_system_monitoring(self) -> None:
        """Test monitoring of recovery system performance.

        Recovery operations are async and involve state management.
        Should still be fast enough to not impact message processing.
        """
        monitor = MockPerformanceMonitor()

        # Realistic target for async recovery operations
        # Should be <1ms to not impact latency
        target_time_ms = 1.0
        operation_count = 20

        from cyberdelta.config.models.websocket_error_config import WebSocketErrorRecoveryConfig

        recovery_config = WebSocketErrorRecoveryConfig()
        recovery_system = StreamRecoverySystem(config=recovery_config)

        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Monitor recovery operations
        for i in range(operation_count):
            timer_id = monitor.start_timer("recovery_handling")

            # Handle recovery (async)
            success = await recovery_system.handle_stream_error(error)

            duration_ms = monitor.stop_timer(timer_id)
            monitor.increment_counter("recovery_attempts")
            if success:
                monitor.increment_counter("recovery_successes")
            else:
                monitor.increment_counter("recovery_failures")

        # Verify monitoring data
        metrics = monitor.get_metrics()

        assert "recovery_handling" in metrics["timings"]
        assert metrics["counters"]["recovery_attempts"] == operation_count
        assert (
            "recovery_successes" in metrics["counters"]
            or "recovery_failures" in metrics["counters"]
        )

        # Verify performance
        avg_recovery_time = sum(metrics["timings"]["recovery_handling"]) / len(
            metrics["timings"]["recovery_handling"]
        )
        assert avg_recovery_time < target_time_ms, (
            f"Average recovery time {avg_recovery_time:.1f}ms > target {target_time_ms}ms"
        )

    def test_monitoring_overhead(self) -> None:
        """Test that monitoring adds minimal overhead.

        CRITICAL: Monitoring must not significantly impact performance.
        In production, monitoring is always on.
        """
        # Target: monitoring should add < 5% overhead
        # (Tighter than before - monitoring must be lightweight)
        max_overhead_percent = 5.0
        operation_count = 1000

        # Measure without monitoring
        start_time = time.perf_counter()
        for i in range(operation_count):
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.SEQUENCE_GAP, message=f"No monitor error {i}"
            )
            api_error = WebSocketErrorAdapter.to_api_error(error)
        no_monitor_time = time.perf_counter() - start_time

        # Measure with monitoring
        monitor = MockPerformanceMonitor()
        start_time = time.perf_counter()
        for i in range(operation_count):
            timer_id = monitor.start_timer("monitored_operation")

            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.SEQUENCE_GAP, message=f"Monitored error {i}"
            )
            api_error = WebSocketErrorAdapter.to_api_error(error)

            monitor.stop_timer(timer_id)
            monitor.increment_counter("monitored_operations")

        monitored_time = time.perf_counter() - start_time

        # Calculate overhead
        overhead_percent = ((monitored_time - no_monitor_time) / no_monitor_time) * 100

        print(f"\nMonitoring overhead: {overhead_percent:.1f}%")

        assert overhead_percent < max_overhead_percent, (
            f"Monitoring overhead {overhead_percent:.1f}% exceeds {max_overhead_percent}% target. "
            "This would significantly impact production performance."
        )

        # Verify monitoring collected data
        metrics = monitor.get_metrics()
        assert len(metrics["timings"]["monitored_operation"]) == operation_count
        assert metrics["counters"]["monitored_operations"] == operation_count

    def test_concurrent_monitoring(self) -> None:
        """Test monitoring under concurrent operations."""
        monitor = MockPerformanceMonitor()

        # Target: concurrent monitoring should not interfere
        max_variation_percent = 20.0  # 20% variation acceptable
        operation_count = 100

        # Sequential operations
        sequential_times = []
        for i in range(operation_count):
            timer_id = monitor.start_timer("sequential_op")

            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED)
            WebSocketErrorAdapter.to_api_error(error)

            duration = monitor.stop_timer(timer_id)
            sequential_times.append(duration)

        # Rapid operations (simulating concurrency)
        rapid_times = []
        start_time = time.perf_counter()
        for i in range(operation_count):
            timer_id = monitor.start_timer("rapid_op")

            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.RATE_LIMITED)
            api_error = WebSocketErrorAdapter.to_api_error(error)

            duration = monitor.stop_timer(timer_id)
            rapid_times.append(duration)
        rapid_total_time = time.perf_counter() - start_time

        # Analyze timing variation
        sequential_avg = sum(sequential_times) / len(sequential_times)
        rapid_avg = sum(rapid_times) / len(rapid_times)

        variation_percent = abs(rapid_avg - sequential_avg) / sequential_avg * 100

        assert variation_percent < max_variation_percent, (
            f"Concurrent monitoring variation {variation_percent:.1f}% > target {max_variation_percent}%"
        )

        # Verify all operations recorded
        metrics = monitor.get_metrics()
        total_operations = len(metrics["timings"]["sequential_op"]) + len(
            metrics["timings"]["rapid_op"]
        )
        assert total_operations == operation_count * 2

    def test_monitoring_memory_efficiency(self) -> None:
        """Test memory efficiency of monitoring system."""
        monitor = MockPerformanceMonitor()

        # Target: monitoring should not accumulate excessive memory
        operation_count = 10000
        memory_samples = []

        # Perform operations and sample memory usage
        for i in range(operation_count):
            timer_id = monitor.start_timer("memory_test")

            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_LOST, message=f"Memory test {i}"
            )

            monitor.stop_timer(timer_id)
            monitor.increment_counter("memory_test_ops")

            # Sample metrics size periodically
            if i % 1000 == 0:
                metrics = monitor.get_metrics()
                memory_samples.append(len(str(metrics)))

        # Verify memory usage is reasonable
        final_metrics = monitor.get_metrics()

        # Should have collected all timing data
        assert len(final_metrics["timings"]["memory_test"]) == operation_count
        assert final_metrics["counters"]["memory_test_ops"] == operation_count

        # Memory growth should be bounded (not exponential)
        if len(memory_samples) > 2:
            growth_rate = (memory_samples[-1] - memory_samples[0]) / len(memory_samples)
            # Growth should be reasonable (< 50KB per 1000 operations)
            assert growth_rate < 51200, (
                f"Memory growth rate too high: {growth_rate} bytes per 1000 ops"
            )

    def test_monitoring_error_handling(self) -> None:
        """Test monitoring system handles errors gracefully."""
        monitor = MockPerformanceMonitor()

        # Test monitoring with disabled system
        monitor.enabled = False
        timer_id = monitor.start_timer("disabled_test")
        monitor.record_timing("disabled_direct", 10.0)
        monitor.increment_counter("disabled_counter")
        monitor.stop_timer(timer_id)

        metrics = monitor.get_metrics()
        assert len(metrics["timings"]) == 0
        assert len(metrics["counters"]) == 0
        assert metrics["active_timers"] == 0

        # Re-enable and test normal operation
        monitor.enabled = True
        timer_id = monitor.start_timer("enabled_test")
        monitor.stop_timer(timer_id)

        metrics = monitor.get_metrics()
        assert "enabled_test" in metrics["timings"]
        assert len(metrics["timings"]["enabled_test"]) == 1

        # Test invalid timer handling
        invalid_duration = monitor.stop_timer("nonexistent_timer")
        assert invalid_duration == 0.0

    def test_monitoring_integration_patterns(self) -> None:
        """Test common monitoring integration patterns."""
        monitor = MockPerformanceMonitor()

        # Pattern 1: Context manager style (simulated)
        def monitored_operation(operation_name: str):
            """Simulate context manager pattern."""
            timer_id = monitor.start_timer(operation_name)
            try:
                # Simulate work
                error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.AUTH_EXPIRED)
                result = WebSocketErrorAdapter.to_api_error(error)
                monitor.increment_counter(f"{operation_name}_success")
                return result
            except Exception:
                monitor.increment_counter(f"{operation_name}_error")
                raise
            finally:
                monitor.stop_timer(timer_id)

        # Pattern 2: Decorator style (simulated)
        def monitor_decorator(func):
            def wrapper(*args, **kwargs):
                timer_id = monitor.start_timer(func.__name__)
                try:
                    result = func(*args, **kwargs)
                    monitor.increment_counter(f"{func.__name__}_calls")
                    return result
                finally:
                    monitor.stop_timer(timer_id)

            return wrapper

        @monitor_decorator
        def monitored_function():
            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.VALIDATION_FAILED)
            return error.get_recovery_strategy()

        # Test patterns
        api_error = monitored_operation("context_pattern")
        assert api_error is not None

        strategy = monitored_function()
        assert strategy is not None

        # Verify monitoring data
        metrics = monitor.get_metrics()

        # Context manager pattern
        assert "context_pattern" in metrics["timings"]
        assert "context_pattern_success" in metrics["counters"]

        # Decorator pattern
        assert "monitored_function" in metrics["timings"]
        assert "monitored_function_calls" in metrics["counters"]

    def test_performance_regression_detection(self) -> None:
        """Test monitoring can detect performance regressions."""
        monitor = MockPerformanceMonitor()

        # Establish baseline
        baseline_operations = 100
        for i in range(baseline_operations):
            timer_id = monitor.start_timer("baseline")

            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
            WebSocketErrorAdapter.to_api_error(error)

            monitor.stop_timer(timer_id)

        baseline_metrics = monitor.get_metrics()
        baseline_avg = sum(baseline_metrics["timings"]["baseline"]) / len(
            baseline_metrics["timings"]["baseline"]
        )

        # Simulate regression (add artificial delay)
        regression_operations = 50
        for i in range(regression_operations):
            timer_id = monitor.start_timer("regression_test")

            # Artificial delay to simulate regression
            time.sleep(0.001)  # 1ms delay

            error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
            WebSocketErrorAdapter.to_api_error(error)

            monitor.stop_timer(timer_id)

        final_metrics = monitor.get_metrics()
        regression_avg = sum(final_metrics["timings"]["regression_test"]) / len(
            final_metrics["timings"]["regression_test"]
        )

        # Detect regression
        regression_factor = regression_avg / baseline_avg

        # Should detect significant regression (> 2x slower)
        assert regression_factor > 2.0, (
            f"Failed to detect performance regression: {regression_factor:.1f}x slower"
        )

        # Monitoring should help identify the regression
        assert regression_avg > baseline_avg + 0.5  # At least 0.5ms slower

    @pytest.mark.parametrize(
        "error_code",
        [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.SEQUENCE_GAP,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.AUTH_EXPIRED,
            WebSocketErrorCode.VALIDATION_FAILED,
        ],
    )
    def test_per_error_type_monitoring(self, error_code: WebSocketErrorCode) -> None:
        """Test monitoring can track performance per error type."""
        monitor = MockPerformanceMonitor()

        operations_per_type = 20
        operation_name = f"error_type_{error_code.name.lower()}"

        for i in range(operations_per_type):
            timer_id = monitor.start_timer(operation_name)

            error = ErrorTestFactory.create_test_error(code=error_code)
            api_error = WebSocketErrorAdapter.to_api_error(error)

            monitor.stop_timer(timer_id)
            monitor.increment_counter(f"{operation_name}_count")

        # Verify per-type monitoring
        metrics = monitor.get_metrics()

        assert operation_name in metrics["timings"]
        assert len(metrics["timings"][operation_name]) == operations_per_type
        assert metrics["counters"][f"{operation_name}_count"] == operations_per_type

        # Performance should be consistent across error types
        avg_time_ms = sum(metrics["timings"][operation_name]) / len(
            metrics["timings"][operation_name]
        )
        assert avg_time_ms < 1.0, (
            f"Performance for {error_code.name} is too slow: {avg_time_ms:.2f}ms"
        )
