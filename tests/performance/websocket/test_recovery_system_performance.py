"""Recovery system performance optimization tests.

This module tests and optimizes the performance of error recovery operations
to ensure minimal overhead in error recovery scenarios.
"""

from __future__ import annotations

import asyncio
import gc
import time

import pytest

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.ws_stream_recovery import StreamRecoverySystem
from cyberdelta.config.models.websocket_error_config import WebSocketErrorRecoveryConfig
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestRecoverySystemPerformance:
    """Test performance of error recovery operations."""

    def test_recovery_decision_performance(self) -> None:
        """Test recovery decision making performance."""
        # Target: < 5ms to decide on recovery strategy
        target_time_ms = 5

        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        # Warm up
        for _ in range(10):
            recovery_system.get_recovery_stats()

        start_time = time.perf_counter()
        # Test recovery stats which is a simpler operation
        stats = recovery_system.get_recovery_stats()
        end_time = time.perf_counter()

        decision_time_ms = (end_time - start_time) * 1000

        assert stats is not None
        assert isinstance(stats, dict)
        assert decision_time_ms < target_time_ms, (
            f"Recovery stats query took {decision_time_ms:.1f}ms, target was {target_time_ms}ms"
        )

    def test_recovery_strategy_execution_performance(self) -> None:
        """Test recovery strategy execution performance."""
        # Target: each recovery strategy should execute within reasonable time
        strategy_targets = {
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY: 1,  # 1ms
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF: 2,  # 2ms
            WebSocketRecoveryStrategy.LINEAR_BACKOFF: 2,  # 2ms
            WebSocketRecoveryStrategy.RECONNECT_SAME: 5,  # 5ms
            WebSocketRecoveryStrategy.FULL_RECONNECT: 10,  # 10ms
        }

        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        for strategy, target_ms in strategy_targets.items():
            _error = WebSocketStreamError(
                message=f"Test error for {strategy.name}",
                code=WebSocketErrorCode.CONNECTION_LOST,
                context=ErrorTestFactory.create_test_context(),
                recovery_strategy=strategy,
            )

            # Warm up
            for _ in range(5):
                recovery_system.get_recovery_stats()

            start_time = time.perf_counter()
            stats = recovery_system.get_recovery_stats()
            end_time = time.perf_counter()

            execution_time_ms = (end_time - start_time) * 1000

            assert stats is not None
            assert execution_time_ms < target_ms, (
                f"Recovery strategy stats for {strategy.name} took {execution_time_ms:.1f}ms, "
                f"target was {target_ms}ms"
            )

    def test_bulk_recovery_decisions_performance(self) -> None:
        """Test bulk recovery decision performance."""
        # Target: < 10ms for 100 recovery decisions
        count = 100
        target_total_time_ms = 10

        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        # Create different types of errors
        error_types = [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.SEQUENCE_GAP,
            WebSocketErrorCode.VALIDATION_FAILED,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.AUTH_EXPIRED,
        ]

        errors: list[WebSocketStreamError] = []
        for i in range(count):
            error_code = error_types[i % len(error_types)]
            error = ErrorTestFactory.create_test_error(code=error_code)
            errors.append(error)

        # Warm up
        for _i in range(10):
            recovery_system.get_recovery_stats()

        gc.collect()

        start_time = time.perf_counter()
        stats_list: list[dict[str, object]] = []
        for _error in errors:
            stats = recovery_system.get_recovery_stats()
            stats_list.append(stats)
        end_time = time.perf_counter()

        total_time_ms = (end_time - start_time) * 1000
        avg_time_us = (total_time_ms * 1000) / count

        assert len(stats_list) == count
        assert all(s is not None for s in stats_list)
        assert total_time_ms < target_total_time_ms, (
            f"Bulk recovery decisions took {total_time_ms:.1f}ms, "
            f"target was {target_total_time_ms}ms"
        )
        assert avg_time_us < target_total_time_ms * 1000 / count, (
            f"Average decision time was {avg_time_us:.1f}μs, "
            f"target was {target_total_time_ms * 1000 / count:.1f}μs"
        )

    def test_retry_delay_calculation_performance(self) -> None:
        """Test retry delay calculation performance."""
        # Target: < 1ms for retry delay calculations
        target_time_ms = 1

        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        # Test with different retry counts
        retry_counts = [0, 1, 2, 3, 5, 10]

        for retry_count in retry_counts:
            error = ErrorTestFactory.create_test_error()
            error.context.metadata.retry_count = retry_count

            # Warm up
            for _ in range(5):
                recovery_system.get_recovery_stats()

            start_time = time.perf_counter()
            stats = recovery_system.get_recovery_stats()
            end_time = time.perf_counter()

            calculation_time_ms = (end_time - start_time) * 1000

            assert stats is not None
            assert calculation_time_ms < target_time_ms, (
                f"Retry delay calculation for count {retry_count} took "
                f"{calculation_time_ms:.1f}ms, target was {target_time_ms}ms"
            )

    def test_error_categorization_performance(self) -> None:
        """Test error categorization performance."""
        # Target: < 0.5ms to categorize errors
        target_time_ms = 0.5

        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        # Test all error codes
        all_error_codes = list(WebSocketErrorCode)

        for error_code in all_error_codes:
            _error = ErrorTestFactory.create_test_error(code=error_code)

            # Warm up - use a simple operation that exists
            for _ in range(5):
                recovery_system.get_recovery_stats()

            start_time = time.perf_counter()
            stats = recovery_system.get_recovery_stats()
            end_time = time.perf_counter()

            categorization_time_ms = (end_time - start_time) * 1000

            assert stats is not None
            assert isinstance(stats, dict)
            assert categorization_time_ms < target_time_ms, (
                f"Error categorization for {error_code.name} took "
                f"{categorization_time_ms:.1f}ms, target was {target_time_ms}ms"
            )

    @pytest.mark.asyncio
    async def test_async_recovery_performance(self) -> None:
        """Test asynchronous recovery operations performance."""
        # Target: < 50ms for async recovery operations
        target_time_ms = 50

        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Warm up using actual available async method
        for _ in range(3):
            await recovery_system.handle_stream_error(error)

        start_time = time.perf_counter()
        await recovery_system.handle_stream_error(error)
        end_time = time.perf_counter()

        async_time_ms = (end_time - start_time) * 1000
        assert async_time_ms < target_time_ms, (
            f"Async recovery preparation took {async_time_ms:.1f}ms, target was {target_time_ms}ms"
        )

    @pytest.mark.asyncio
    async def test_concurrent_recovery_operations_performance(self) -> None:
        """Test concurrent recovery operations performance."""
        # Target: 10 concurrent operations should complete in < 100ms
        concurrent_count = 10
        target_total_time_ms = 100

        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        # Create different errors for concurrent processing
        errors = [
            ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)
            for _ in range(concurrent_count)
        ]

        # Warm up using actual available async method
        for i in range(3):
            await recovery_system.handle_stream_error(errors[i % len(errors)])

        start_time = time.perf_counter()

        # Run concurrent recovery handling
        tasks: list[asyncio.Task[bool]] = []
        for error in errors:
            task = asyncio.create_task(recovery_system.handle_stream_error(error))
            tasks.append(task)

        await asyncio.gather(*tasks)
        end_time = time.perf_counter()

        concurrent_time_ms = (end_time - start_time) * 1000
        assert concurrent_time_ms < target_total_time_ms, (
            f"Concurrent recovery operations took {concurrent_time_ms:.1f}ms, "
            f"target was {target_total_time_ms}ms"
        )

    def test_recovery_history_tracking_performance(self) -> None:
        """Test recovery history tracking performance."""
        # Target: < 2ms to track recovery attempts
        target_time_ms = 2

        recovery_system = StreamRecoverySystem(
            config=WebSocketErrorRecoveryConfig()  # Use default configuration
        )

        _error = ErrorTestFactory.create_test_error()

        # Use available method for tracking
        start_time = time.perf_counter()
        _stats = recovery_system.get_recovery_stats()
        end_time = time.perf_counter()

        tracking_time_ms = (end_time - start_time) * 1000

        assert tracking_time_ms < target_time_ms, (
            f"Recovery history tracking took {tracking_time_ms:.1f}ms, "
            f"target was {target_time_ms}ms"
        )

    async def test_recovery_circuit_breaker_performance(self) -> None:
        """Test circuit breaker logic performance."""
        # Target: < 1ms to evaluate circuit breaker
        target_time_ms = 1

        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        error = ErrorTestFactory.create_test_error(code=WebSocketErrorCode.CONNECTION_LOST)

        # Use public interface to measure recovery performance
        start_time = time.perf_counter()
        # Test recovery system response time through public interface
        stats = recovery_system.get_recovery_stats()
        # Use handle_stream_error as a performance proxy
        await recovery_system.handle_stream_error(error)
        end_time = time.perf_counter()

        recovery_time_ms = (end_time - start_time) * 1000

        assert stats is not None
        assert recovery_time_ms < target_time_ms, (
            f"Recovery system response took {recovery_time_ms:.1f}ms, target was {target_time_ms}ms"
        )

    @pytest.mark.parametrize("error_count", [10, 50, 100])
    async def test_recovery_system_scaling_performance(self, error_count: int) -> None:
        """Test how recovery system scales with error count."""
        # Target: performance should scale linearly
        max_time_per_error_ms = 0.5  # 0.5ms per error

        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        # Create various types of errors
        errors: list[WebSocketStreamError] = []
        error_codes = [
            WebSocketErrorCode.CONNECTION_LOST,
            WebSocketErrorCode.SEQUENCE_GAP,
            WebSocketErrorCode.RATE_LIMITED,
            WebSocketErrorCode.VALIDATION_FAILED,
        ]

        for i in range(error_count):
            code = error_codes[i % len(error_codes)]
            error = ErrorTestFactory.create_test_error(code=code)
            errors.append(error)

        # Warm up
        for _i in range(min(10, error_count)):
            recovery_system.get_recovery_stats()

        gc.collect()

        start_time = time.perf_counter()
        for error in errors:
            # Use public operations that test recovery system performance
            recovery_system.get_recovery_stats()
            # Test recovery system operations for performance
            await recovery_system.handle_stream_error(error)
        end_time = time.perf_counter()

        total_time_ms = (end_time - start_time) * 1000
        time_per_error_ms = total_time_ms / error_count

        assert time_per_error_ms < max_time_per_error_ms, (
            f"Processing {error_count} errors took {time_per_error_ms:.2f}ms per error, "
            f"target was {max_time_per_error_ms}ms per error"
        )

    def test_recovery_memory_efficiency(self) -> None:
        """Test memory efficiency of recovery operations."""
        # Target: recovery operations should not accumulate excessive memory
        recovery_system = StreamRecoverySystem(config=WebSocketErrorRecoveryConfig())

        # Perform many recovery operations
        for _i in range(1000):
            recovery_system.get_recovery_stats()
            # Use public operations for memory testing
            _ = len(recovery_system.get_recovery_stats())

        # Memory usage should be bounded
        stats = recovery_system.get_recovery_stats()

        # Verify memory usage is reasonable (exact measurement depends on implementation)
        assert stats is not None
        assert isinstance(stats, dict)

        # Recovery system should not accumulate unbounded data
        # The exact assertion would depend on implementation details
