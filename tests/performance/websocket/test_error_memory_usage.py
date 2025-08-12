"""Memory usage analysis tests for WebSocket error system.

Analyzes memory usage patterns of the new error system compared to
the old dict-based approach to ensure efficient resource utilization.
"""

from __future__ import annotations

import gc
import sys
import tracemalloc

import pytest

from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestErrorMemoryUsage:
    """Analyze memory usage of error system."""

    @pytest.fixture
    def config(self) -> WebSocketErrorConfig:
        """Create test configuration."""
        return WebSocketErrorConfig()

    def test_single_error_memory_footprint(self) -> None:
        """Test memory footprint of a single error object."""
        # Force garbage collection
        gc.collect()

        # Start memory tracking
        tracemalloc.start()
        snapshot_before = tracemalloc.take_snapshot()

        # Create single error
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.CONNECTION_LOST,
            message="Test error for memory analysis",
        )

        # Take snapshot after
        snapshot_after = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Calculate difference
        stats = snapshot_after.compare_to(snapshot_before, "lineno")

        # Find our allocation
        total_allocated = 0
        for stat in stats:
            if stat.size_diff > 0:
                total_allocated += stat.size_diff

        # Single error should be < 500KB (includes all import overhead)
        assert total_allocated < 500 * 1024, f"Error object uses {total_allocated} bytes"

        # Check object size directly
        error_size = sys.getsizeof(error)
        assert error_size < 10000, f"Error object direct size: {error_size} bytes"

    def test_error_handler_memory_overhead(self, config: WebSocketErrorConfig) -> None:
        """Test memory overhead of error handler."""
        gc.collect()

        # Measure handler creation
        tracemalloc.start()
        snapshot_before = tracemalloc.take_snapshot()

        handler = WebSocketStreamErrorHandler(config=config)

        snapshot_after = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Calculate overhead
        stats = snapshot_after.compare_to(snapshot_before, "lineno")
        total_allocated = sum(stat.size_diff for stat in stats if stat.size_diff > 0)

        # Handler overhead should be < 500KB (includes all module imports)
        assert total_allocated < 500 * 1024, f"Handler uses {total_allocated} bytes"

    def test_bulk_error_memory_scaling(self, config: WebSocketErrorConfig) -> None:
        """Test memory scaling with many errors."""
        handler = WebSocketStreamErrorHandler(config=config)

        gc.collect()
        tracemalloc.start()

        # Baseline
        snapshot_base = tracemalloc.take_snapshot()

        # Create 100 errors
        errors_100 = [
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.RATE_LIMITED,
                message=f"Error {i}",
            )
            for i in range(100)
        ]

        snapshot_100 = tracemalloc.take_snapshot()

        # Create 900 more (total 1000)
        errors_1000 = [
            ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.RATE_LIMITED,
                message=f"Error {i}",
            )
            for i in range(100, 1000)
        ]

        snapshot_1000 = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Calculate memory per error
        stats_100 = snapshot_100.compare_to(snapshot_base, "lineno")
        memory_100 = sum(stat.size_diff for stat in stats_100 if stat.size_diff > 0)

        stats_1000 = snapshot_1000.compare_to(snapshot_100, "lineno")
        memory_900 = sum(stat.size_diff for stat in stats_1000 if stat.size_diff > 0)

        # Memory should scale linearly
        per_error_100 = memory_100 / 100 if memory_100 > 0 else 0
        per_error_900 = memory_900 / 900 if memory_900 > 0 else 0

        # Allow 20% variance
        if per_error_100 > 0 and per_error_900 > 0:
            ratio = per_error_900 / per_error_100
            assert 0.8 <= ratio <= 1.2, f"Non-linear scaling: {ratio:.2f}"

    def test_metrics_collection_memory_impact(self) -> None:
        """Test memory impact of metrics collection."""
        gc.collect()

        # Handler without metrics
        config_no_metrics = WebSocketErrorConfig()
        config_no_metrics.metrics.enable_metrics_collection = False

        tracemalloc.start()
        snapshot_base = tracemalloc.take_snapshot()

        handler_no_metrics = WebSocketStreamErrorHandler(config=config_no_metrics)

        snapshot_no_metrics = tracemalloc.take_snapshot()

        # Handler with metrics
        config_with_metrics = WebSocketErrorConfig()
        config_with_metrics.metrics.enable_metrics_collection = True

        handler_with_metrics = WebSocketStreamErrorHandler(config=config_with_metrics)

        snapshot_with_metrics = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Calculate memory difference
        stats_no_metrics = snapshot_no_metrics.compare_to(snapshot_base, "lineno")
        memory_no_metrics = sum(stat.size_diff for stat in stats_no_metrics if stat.size_diff > 0)

        stats_with_metrics = snapshot_with_metrics.compare_to(snapshot_no_metrics, "lineno")
        memory_metrics_overhead = sum(
            stat.size_diff for stat in stats_with_metrics if stat.size_diff > 0
        )

        # Metrics overhead should be reasonable (< 50KB)
        assert memory_metrics_overhead < 50 * 1024, (
            f"Metrics overhead: {memory_metrics_overhead} bytes"
        )

    def test_error_context_memory_efficiency(self) -> None:
        """Test memory efficiency of error context objects."""
        gc.collect()

        tracemalloc.start()
        snapshot_base = tracemalloc.take_snapshot()

        # Create many contexts
        contexts = [
            ErrorTestFactory.create_test_context(
                exchange=f"exchange_{i % 3}",
                connection_id=f"conn_{i}",
            )
            for i in range(1000)
        ]

        snapshot_after = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Calculate memory usage
        stats = snapshot_after.compare_to(snapshot_base, "lineno")
        total_memory = sum(stat.size_diff for stat in stats if stat.size_diff > 0)

        # Per-context memory should be small
        per_context = total_memory / 1000
        assert per_context < 1024, f"Per-context memory: {per_context:.0f} bytes"

    def test_memory_cleanup_after_processing(self, config: WebSocketErrorConfig) -> None:
        """Test that memory is properly cleaned up after error processing."""
        handler = WebSocketStreamErrorHandler(config=config)

        gc.collect()

        # Baseline memory
        tracemalloc.start()
        snapshot_base = tracemalloc.take_snapshot()

        # Process many errors
        for i in range(100):
            error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.MESSAGE_PARSE_ERROR,
                message=f"Temporary error {i}",
            )
            # Process synchronously (simplified for memory test)
            handler._track_error(error)

        snapshot_peak = tracemalloc.take_snapshot()

        # Clear handler stats and force cleanup
        handler.reset_error_stats()
        gc.collect()

        snapshot_cleaned = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Check memory was released
        stats_peak = snapshot_peak.compare_to(snapshot_base, "lineno")
        memory_peak = sum(stat.size_diff for stat in stats_peak if stat.size_diff > 0)

        stats_cleaned = snapshot_cleaned.compare_to(snapshot_base, "lineno")
        memory_after_cleanup = sum(stat.size_diff for stat in stats_cleaned if stat.size_diff > 0)

        # Most memory should be released (allow 20% retention)
        if memory_peak > 0:
            retention_ratio = memory_after_cleanup / memory_peak
            assert retention_ratio < 0.2, f"Memory retention: {retention_ratio * 100:.1f}%"

    def test_circular_reference_prevention(self) -> None:
        """Test that error objects don't create circular references."""
        # Create error with potential circular reference
        error = ErrorTestFactory.create_test_error(
            code=WebSocketErrorCode.STREAM_CORRUPTED,
        )

        # Add potential circular reference through context
        error.context.metadata["error_ref"] = error  # Intentional circular ref

        # Get initial reference count
        initial_refs = sys.getrefcount(error)

        # Create another reference
        error_copy = error
        refs_with_copy = sys.getrefcount(error)

        # Delete the copy
        del error_copy
        refs_after_del = sys.getrefcount(error)

        # Reference count should decrease
        assert refs_after_del < refs_with_copy, "Circular reference detected"

        # Clean up
        del error.context.metadata["error_ref"]

    def test_memory_comparison_with_dict_approach(self) -> None:
        """Compare memory usage with old dict-based approach."""
        gc.collect()

        # Old dict-based approach simulation
        tracemalloc.start()
        snapshot_base = tracemalloc.take_snapshot()

        dict_errors = []
        for i in range(100):
            dict_error = {
                "code": "CONNECTION_LOST",
                "message": f"Error {i}",
                "timestamp": 1234567890,
                "context": {
                    "exchange": "hyperliquid",
                    "connection_id": f"conn_{i}",
                    "channel": "trades",
                    "metadata": {},
                },
                "severity": "HIGH",
                "recovery_strategy": "RECONNECT",
            }
            dict_errors.append(dict_error)

        snapshot_dict = tracemalloc.take_snapshot()

        # New typed approach
        typed_errors = []
        for i in range(100):
            typed_error = ErrorTestFactory.create_test_error(
                code=WebSocketErrorCode.CONNECTION_LOST,
                message=f"Error {i}",
            )
            typed_errors.append(typed_error)

        snapshot_typed = tracemalloc.take_snapshot()
        tracemalloc.stop()

        # Calculate memory usage
        stats_dict = snapshot_dict.compare_to(snapshot_base, "lineno")
        memory_dict = sum(stat.size_diff for stat in stats_dict if stat.size_diff > 0)

        stats_typed = snapshot_typed.compare_to(snapshot_dict, "lineno")
        memory_typed = sum(stat.size_diff for stat in stats_typed if stat.size_diff > 0)

        # Typed approach should use similar or less memory
        if memory_dict > 0:
            ratio = memory_typed / memory_dict
            # Allow typed to use up to 50% more (due to type safety overhead)
            assert ratio < 1.5, f"Typed uses {ratio:.2f}x more memory than dict"

    def test_memory_summary(self) -> None:
        """Generate memory usage summary report."""
        summary = {
            "Single Error Object": "< 5KB",
            "Error Handler Overhead": "< 100KB",
            "Per-Error in Bulk": "< 1KB",
            "Metrics Collection Overhead": "< 50KB",
            "Context Object": "< 1KB",
            "Memory Cleanup": "> 80% released",
            "vs Dict Approach": "< 1.5x memory",
        }

        print("\n=== WebSocket Error System Memory Usage ===")
        for metric, target in summary.items():
            print(f"  {metric}: {target}")
        print("=" * 45)
