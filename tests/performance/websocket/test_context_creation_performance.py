"""Context creation performance optimization tests.

This module tests and optimizes the performance of error context creation
to ensure minimal overhead in error handling scenarios.
"""

from __future__ import annotations

import gc
import time
from datetime import UTC, datetime

import pytest

from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from tests.utils.websocket.error_test_utils import ErrorTestFactory


class TestContextCreationPerformance:
    """Test performance of error context creation."""

    def test_single_context_creation_performance(self) -> None:
        """Test single context creation meets performance targets."""
        # Target: < 100µs per context creation
        target_time_us = 100

        # Warm up
        for _ in range(10):
            ErrorTestFactory.create_test_context()

        # Measure actual performance
        start_time = time.perf_counter()
        context = ErrorTestFactory.create_test_context()
        end_time = time.perf_counter()

        creation_time_us = (end_time - start_time) * 1_000_000

        assert context is not None
        assert creation_time_us < target_time_us, (
            f"Context creation took {creation_time_us:.1f}µs, target was {target_time_us}µs"
        )

    def test_bulk_context_creation_performance(self) -> None:
        """Test bulk context creation performance."""
        # Target: < 1ms average per context for 1000 contexts
        count = 1000
        target_avg_time_us = 1000  # 1ms

        # Warm up
        for _ in range(10):
            ErrorTestFactory.create_test_context()

        gc.collect()  # Clean up before measurement

        start_time = time.perf_counter()
        contexts = []
        for i in range(count):
            context = ErrorTestFactory.create_test_context(
                connection_id=f"perf-test-{i}",
                sequence_number=i,
            )
            contexts.append(context)
        end_time = time.perf_counter()

        total_time_us = (end_time - start_time) * 1_000_000
        avg_time_us = total_time_us / count

        assert len(contexts) == count
        assert avg_time_us < target_avg_time_us, (
            f"Average context creation took {avg_time_us:.1f}µs, target was {target_avg_time_us}µs"
        )
        assert total_time_us < count * target_avg_time_us, (
            f"Total time {total_time_us:.1f}µs exceeded target {count * target_avg_time_us}µs"
        )

    def test_context_with_all_fields_performance(self) -> None:
        """Test context creation with all fields populated."""
        # Target: < 200µs even with all fields
        target_time_us = 200
        now_ms = int(datetime.now(UTC).timestamp() * 1000)

        # Warm up
        for _ in range(5):
            StreamErrorContext(
                connection_id="full-perf-test",
                exchange="hyperliquid",
                channel="trades",
                topic="BTC-USDC",
                sequence_number=1000,
                expected_sequence=1001,
                error_timestamp_ms=now_ms,
                connection_started_ms=now_ms - 30000,
                last_message_received_ms=now_ms - 1000,
                last_heartbeat_ms=now_ms - 5000,
                active_subscriptions=10,
                pending_messages=5,
                reconnect_count=2,
                raw_message_size=1024,
            )

        start_time = time.perf_counter()
        context = StreamErrorContext(
            connection_id="full-perf-test",
            exchange="hyperliquid",
            channel="trades",
            topic="BTC-USDC",
            sequence_number=1000,
            expected_sequence=1001,
            error_timestamp_ms=now_ms,
            connection_started_ms=now_ms - 30000,
            last_message_received_ms=now_ms - 1000,
            last_heartbeat_ms=now_ms - 5000,
            active_subscriptions=10,
            pending_messages=5,
            reconnect_count=2,
            raw_message_size=1024,
        )
        end_time = time.perf_counter()

        creation_time_us = (end_time - start_time) * 1_000_000

        assert context is not None
        assert context.connection_id == "full-perf-test"
        assert creation_time_us < target_time_us, (
            f"Full context creation took {creation_time_us:.1f}µs, target was {target_time_us}µs"
        )

    def test_context_validation_performance(self) -> None:
        """Test context validation performance."""
        # Target: validation should add < 50µs overhead
        target_validation_overhead_us = 50
        now_ms = int(datetime.now(UTC).timestamp() * 1000)

        # Test data
        context_data = {
            "connection_id": "validation-perf-test",
            "exchange": "hyperliquid",
            "channel": "trades",
            "topic": "BTC-USDC",
            "sequence_number": 1000,
            "error_timestamp_ms": now_ms,
        }

        # Warm up
        for _ in range(10):
            StreamErrorContext(**context_data)

        # Measure without validation (using model_construct)
        start_time = time.perf_counter()
        context_no_validation = StreamErrorContext.model_construct(**context_data)
        end_time = time.perf_counter()
        no_validation_time_us = (end_time - start_time) * 1_000_000

        # Measure with validation (normal constructor)
        start_time = time.perf_counter()
        context_with_validation = StreamErrorContext(**context_data)
        end_time = time.perf_counter()
        with_validation_time_us = (end_time - start_time) * 1_000_000

        validation_overhead_us = with_validation_time_us - no_validation_time_us

        assert context_no_validation is not None
        assert context_with_validation is not None
        assert validation_overhead_us < target_validation_overhead_us, (
            f"Validation overhead was {validation_overhead_us:.1f}µs, target was {target_validation_overhead_us}µs"
        )

    def test_context_method_call_performance(self) -> None:
        """Test performance of context method calls."""
        # Target: method calls should be < 10µs each
        target_method_time_us = 10

        context = ErrorTestFactory.create_test_context(
            sequence_number=1000,
            expected_sequence=1005,
            connection_started_ms=int(datetime.now(UTC).timestamp() * 1000) - 30000,
            last_message_received_ms=int(datetime.now(UTC).timestamp() * 1000) - 5000,
        )

        # Warm up
        for _ in range(10):
            context.get_sequence_gap_size()
            context.get_connection_duration_ms()
            context.get_time_since_last_message_ms()
            context.get_summary()

        # Test get_sequence_gap_size performance
        start_time = time.perf_counter()
        gap_size = context.get_sequence_gap_size()
        end_time = time.perf_counter()
        gap_time_us = (end_time - start_time) * 1_000_000

        # Test get_summary performance
        start_time = time.perf_counter()
        summary = context.get_summary()
        end_time = time.perf_counter()
        summary_time_us = (end_time - start_time) * 1_000_000

        # Test has_sequence_gap performance
        start_time = time.perf_counter()
        has_gap = context.has_sequence_gap()
        end_time = time.perf_counter()
        has_gap_time_us = (end_time - start_time) * 1_000_000

        assert gap_size == 5
        assert isinstance(summary, str)
        assert has_gap is True

        assert gap_time_us < target_method_time_us, (
            f"get_sequence_gap_size took {gap_time_us:.1f}µs, target was {target_method_time_us}µs"
        )
        assert summary_time_us < target_method_time_us * 2, (  # Summary can be a bit slower
            f"get_summary took {summary_time_us:.1f}µs, target was {target_method_time_us * 2}µs"
        )
        assert has_gap_time_us < target_method_time_us, (
            f"has_sequence_gap took {has_gap_time_us:.1f}µs, target was {target_method_time_us}µs"
        )

    def test_context_error_chain_performance(self) -> None:
        """Test error chain operation performance."""
        # Target: adding to error chain should be < 50µs
        target_add_time_us = 50

        context = ErrorTestFactory.create_test_context()
        test_error = ValueError("Performance test error")

        # Warm up
        for _ in range(5):
            temp_context = ErrorTestFactory.create_test_context()
            temp_context.add_to_error_chain(ValueError("Warmup"))

        start_time = time.perf_counter()
        context.add_to_error_chain(test_error)
        end_time = time.perf_counter()

        add_time_us = (end_time - start_time) * 1_000_000

        assert len(context.error_chain) == 1
        assert context.error_chain[0].error_class == "ValueError"
        assert add_time_us < target_add_time_us, (
            f"Adding to error chain took {add_time_us:.1f}µs, target was {target_add_time_us}µs"
        )

    @pytest.mark.parametrize("field_count", [5, 10, 15, 20])
    def test_context_scaling_with_field_count(self, field_count: int) -> None:
        """Test how context creation scales with number of fields."""
        # Performance should scale linearly with field count
        max_time_per_field_us = 20  # 20µs per field max

        base_data = {
            "connection_id": "scaling-test",
            "exchange": "hyperliquid",
            "error_timestamp_ms": int(datetime.now(UTC).timestamp() * 1000),
        }

        # Add fields based on field_count parameter
        if field_count >= 5:
            base_data.update({
                "channel": "trades",
                "topic": "BTC-USDC",
            })
        if field_count >= 10:
            base_data.update({
                "sequence_number": 1000,
                "expected_sequence": 1001,
                "connection_started_ms": int(datetime.now(UTC).timestamp() * 1000) - 30000,
                "last_message_received_ms": int(datetime.now(UTC).timestamp() * 1000) - 5000,
                "last_heartbeat_ms": int(datetime.now(UTC).timestamp() * 1000) - 10000,
            })
        if field_count >= 15:
            base_data.update({
                "active_subscriptions": 10,
                "pending_messages": 5,
                "reconnect_count": 2,
                "raw_message_size": 1024,
                "subscription_id": "sub-123",
            })
        if field_count >= 20:
            base_data.update({
                "last_received_sequence": 999,
                "is_authenticated": True,
                "message_id": "msg-456",
                "message_type": "trade_update",
                "user_id": "user-789",
            })

        # Warm up
        for _ in range(5):
            StreamErrorContext(**base_data)

        start_time = time.perf_counter()
        context = StreamErrorContext(**base_data)
        end_time = time.perf_counter()

        creation_time_us = (end_time - start_time) * 1_000_000
        time_per_field_us = creation_time_us / field_count

        assert context is not None
        assert time_per_field_us < max_time_per_field_us, (
            f"Time per field was {time_per_field_us:.1f}µs for {field_count} fields, "
            f"max allowed was {max_time_per_field_us}µs"
        )
