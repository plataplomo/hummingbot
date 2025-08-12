"""Performance baseline tests for WebSocket error system.

Establishes baseline performance metrics for error handling operations
to track performance improvements and prevent regressions.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestErrorPerformanceBaseline:
    """Establish performance baseline for error system."""

    @pytest.fixture
    def config(self) -> WebSocketErrorConfig:
        """Create test configuration."""
        return WebSocketErrorConfig(
            max_recovery_attempts=3,
            recovery_backoff_ms=100,
            enable_metrics_collection=True,
        )

    async def test_single_error_handling_baseline(self, config: WebSocketErrorConfig) -> None:
        """Test baseline performance for single error handling."""
        handler = WebSocketStreamErrorHandler(config=config)

        # Create test error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
        )

        # Measure handling time
        start = time.perf_counter()
        await handler.handle_stream_error(error)
        elapsed = time.perf_counter() - start

        # Baseline: Should handle single error in under 10ms
        assert elapsed < 0.01, f"Single error handling took {elapsed * 1000:.2f}ms"

    async def test_bulk_error_handling_baseline(self, config: WebSocketErrorConfig) -> None:
        """Test baseline performance for bulk error handling."""
        handler = WebSocketStreamErrorHandler(config=config)

        # Create 1000 test errors
        errors = [
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.RATE_LIMITED,
                message=f"Error {i}",
            )
            for i in range(1000)
        ]

        # Measure bulk handling time
        start = time.perf_counter()
        for error in errors:
            await handler.handle_stream_error(error)
        elapsed = time.perf_counter() - start

        # Baseline: Should handle 1000 errors in under 1 second
        assert elapsed < 1.0, f"Bulk error handling took {elapsed:.2f}s"

        # Calculate per-error time
        per_error_ms = (elapsed / 1000) * 1000
        assert per_error_ms < 1.0, f"Per-error time: {per_error_ms:.2f}ms"

    async def test_concurrent_error_handling_baseline(self, config: WebSocketErrorConfig) -> None:
        """Test baseline performance for concurrent error handling."""
        handler = WebSocketStreamErrorHandler(config=config)

        # Create test errors
        errors = [
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
                message=f"Concurrent error {i}",
            )
            for i in range(100)
        ]

        # Measure concurrent handling time
        start = time.perf_counter()
        tasks = [handler.handle_stream_error(error) for error in errors]
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

        # Baseline: Concurrent should be faster than sequential
        # 100 errors should take < 100ms with concurrency
        assert elapsed < 0.1, f"Concurrent handling took {elapsed * 1000:.2f}ms"

    async def test_error_context_creation_baseline(self) -> None:
        """Test baseline performance for error context creation."""
        # Measure context creation time
        iterations = 10000

        start = time.perf_counter()
        for _ in range(iterations):
            context = ErrorTestFactory.create_test_context()
        elapsed = time.perf_counter() - start

        # Baseline: Should create 10000 contexts in under 1 second
        assert elapsed < 1.0, f"Context creation took {elapsed:.2f}s"

        # Per-context time should be < 0.1ms
        per_context_us = (elapsed / iterations) * 1_000_000
        assert per_context_us < 100, f"Per-context time: {per_context_us:.2f}µs"

    async def test_error_creation_baseline(self) -> None:
        """Test baseline performance for error object creation."""
        iterations = 10000

        start = time.perf_counter()
        for i in range(iterations):
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.MESSAGE_PARSE_ERROR,
                message=f"Test error {i}",
            )
        elapsed = time.perf_counter() - start

        # Baseline: Should create 10000 errors in under 2 seconds
        assert elapsed < 2.0, f"Error creation took {elapsed:.2f}s"

        # Per-error time should be < 0.2ms
        per_error_us = (elapsed / iterations) * 1_000_000
        assert per_error_us < 200, f"Per-error time: {per_error_us:.2f}µs"

    async def test_metrics_collection_overhead(self, config: WebSocketErrorConfig) -> None:
        """Test overhead of metrics collection."""
        # Handler with metrics
        handler_with_metrics = WebSocketStreamErrorHandler(config=config)

        # Handler without metrics
        config_no_metrics = WebSocketErrorConfig(
            max_recovery_attempts=3,
            recovery_backoff_ms=100,
            enable_metrics_collection=False,
        )
        handler_no_metrics = WebSocketStreamErrorHandler(config=config_no_metrics)

        # Create test errors
        errors = [
            ErrorTestFactory.create_test_error(code=WebSocketErrorCode.SEQUENCE_GAP)
            for _ in range(100)
        ]

        # Measure with metrics
        start = time.perf_counter()
        for error in errors:
            await handler_with_metrics.handle_stream_error(error)
        with_metrics_time = time.perf_counter() - start

        # Measure without metrics
        start = time.perf_counter()
        for error in errors:
            await handler_no_metrics.handle_stream_error(error)
        without_metrics_time = time.perf_counter() - start

        # Metrics overhead should be < 20%
        overhead = (with_metrics_time - without_metrics_time) / without_metrics_time
        assert overhead < 0.2, f"Metrics overhead: {overhead * 100:.1f}%"

    async def test_recovery_operation_baseline(self, config: WebSocketErrorConfig) -> None:
        """Test baseline performance for recovery operations."""
        handler = WebSocketStreamErrorHandler(config=config)

        # Create error requiring recovery
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
        )

        # Measure recovery time (excluding actual backoff)
        start = time.perf_counter()
        # Just measure the decision and setup time, not actual recovery
        await handler.handle_stream_error(error)
        elapsed = time.perf_counter() - start

        # Recovery decision should be fast < 5ms
        assert elapsed < 0.005, f"Recovery decision took {elapsed * 1000:.2f}ms"

    def test_performance_summary(self) -> None:
        """Generate performance summary report."""
        summary = {
            "Single Error Handling": "< 10ms",
            "Bulk Error (1000)": "< 1s total, < 1ms per error",
            "Concurrent (100)": "< 100ms total",
            "Context Creation": "< 100µs per context",
            "Error Creation": "< 200µs per error",
            "Metrics Overhead": "< 20%",
            "Recovery Decision": "< 5ms",
        }

        print("\n=== WebSocket Error System Performance Baseline ===")
        for metric, target in summary.items():
            print(f"  {metric}: {target}")
        print("=" * 50)
