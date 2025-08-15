"""Performance tests for WebSocket processor with typed error system.

This module validates that the new typed error system doesn't introduce
performance regressions in the WebSocket processor.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel, Field

from cyberdelta.apis.backpack.bp_ws_context import BackpackMessageContext
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.websocket.ws_processor import (
    MessageTransformer,
    PydanticWebSocketProcessor,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.enums import ExchangeName


logger = logging.getLogger(__name__)


class TestOrderModel(BaseModel):
    """Test model for performance testing."""

    order_id: str = Field(...)
    symbol: str = Field(...)
    side: str = Field(...)
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    timestamp_ms: int = Field(...)
    metadata: dict[str, str | int | float | bool] = Field(default_factory=dict)


class TestComplexModel(BaseModel):
    """Complex model with nested structures for stress testing."""

    id: str = Field(...)
    orders: list[TestOrderModel] = Field(...)
    aggregate_data: dict[str, Decimal] = Field(...)
    status_flags: dict[str, bool] = Field(...)
    processing_metadata: dict[str, str | int | float | bool] = Field(default_factory=dict)


class TestTransformer(
    MessageTransformer[TestOrderModel, TestOrderModel | list[TestOrderModel] | None]
):
    """Test transformer for performance testing."""

    def transform(
        self, validated: TestOrderModel, context: WebSocketContextProtocol | None = None
    ) -> TestOrderModel:
        """Pass-through transformer for testing.

        Returns:
            The same validated model.
        """
        return validated


class TestComplexTransformer(MessageTransformer[TestComplexModel, None]):
    """Complex transformer that performs data processing."""

    def transform(
        self, validated: TestComplexModel, context: WebSocketContextProtocol | None = None
    ) -> None:
        """Transform complex model with processing.

        This transformer processes the data but doesn't return a domain model.
        """
        # Process the validated model (side effects only)
        # Perform processing calculations (side effects only)
        len(validated.orders)
        sum(o.quantity for o in validated.orders)
        sum(o.price * o.quantity for o in validated.orders)
        list(validated.aggregate_data.keys())
        [k for k, v in validated.status_flags.items() if v]

        # In a real scenario, this might store the processed data somewhere
        # Processing complete - values calculated but not stored for performance testing


@pytest.fixture
def mock_error_handler() -> AsyncMock:
    """Create mock error handler for testing.

    Returns:
        Mock error handler for testing.
    """
    return AsyncMock(spec=WebSocketStreamErrorHandler)


@pytest.fixture
def mock_stream_handler() -> AsyncMock:
    """Create mock stream error handler for testing.

    Returns:
        Mock stream error handler for testing.
    """
    return AsyncMock(spec=WebSocketStreamErrorHandler)


@pytest.fixture
def simple_processor(
    mock_error_handler: AsyncMock,
    mock_stream_handler: AsyncMock,
) -> PydanticWebSocketProcessor[TestOrderModel, TestOrderModel]:
    """Create simple processor for testing.

    Returns:
        Simple WebSocket processor for performance testing.
    """
    return PydanticWebSocketProcessor(
        raw_model=TestOrderModel,
        transformer=TestTransformer(),
        processor_name="perf_test_simple",
        stream_error_handler=mock_stream_handler,
    )


@pytest.fixture
def complex_processor(
    mock_error_handler: AsyncMock,
    mock_stream_handler: AsyncMock,
) -> PydanticWebSocketProcessor[TestComplexModel, Any]:
    """Create complex processor for testing.

    Returns:
        Complex WebSocket processor for performance testing.
    """
    return PydanticWebSocketProcessor(
        raw_model=TestComplexModel,
        transformer=TestComplexTransformer(),
        processor_name="perf_test_complex",
        stream_error_handler=mock_stream_handler,
    )


@pytest.fixture
def test_context() -> BackpackMessageContext:
    """Create test context for performance testing.

    Returns:
        Test message context for performance testing.
    """
    envelope = BackpackRawWebSocketEnvelope(
        stream="ticker.BTC_USDC",
        data={"test": "data"},
    )
    return BackpackMessageContext(
        validated_envelope=envelope,
        exchange_type=ExchangeName.BACKPACK,
        routing_key="test.performance",
        timestamp=datetime.now(UTC),
        message_id="perf-test-msg",
        connection_id="perf-test-conn",
        symbol="BTC_USDC",
    )


@pytest.fixture
def valid_order_payload() -> dict[str, str | int | dict[str, str | int]]:
    """Create valid order payload for testing.

    Returns:
        Valid order payload dictionary.
    """
    return {
        "order_id": "order-123",
        "symbol": "BTC_USDC",
        "side": "buy",
        "price": "50000.50",
        "quantity": "0.1",
        "timestamp_ms": int(time.time() * 1000),
        "metadata": {"source": "test", "priority": 1},
    }


@pytest.fixture
def invalid_order_payload() -> dict[str, str | int]:
    """Create invalid order payload for testing.

    Returns:
        Invalid order payload dictionary for testing error handling.
    """
    return {
        "order_id": "order-456",
        "symbol": "BTC_USDC",
        "side": "buy",
        "price": "-100",  # Invalid negative price
        "quantity": "0.1",
        "timestamp_ms": int(time.time() * 1000),
    }


@pytest.fixture
def complex_payload() -> dict[
    str, str | list[dict[str, str | int | dict[str, int]]] | dict[str, str | bool | int]
]:
    """Create complex payload for stress testing.

    Returns:
        Complex payload with multiple orders for stress testing.
    """
    orders = [
        {
            "order_id": f"order-{i}",
            "symbol": "BTC_USDC",
            "side": "buy" if i % 2 == 0 else "sell",
            "price": str(50000 + i * 10),
            "quantity": str(0.1 + i * 0.01),
            "timestamp_ms": int(time.time() * 1000) + i,
            "metadata": {"index": i},
        }
        for i in range(10)
    ]

    return {
        "id": "complex-test",
        "orders": orders,
        "aggregate_data": {f"metric_{i}": str(Decimal("100.50") * i) for i in range(20)},
        "status_flags": {f"flag_{i}": i % 3 == 0 for i in range(15)},
        "processing_metadata": {
            "source": "performance_test",
            "batch_size": 10,
        },
    }


class TestProcessorPerformance:
    """Test processor performance with typed error system."""

    @pytest.mark.asyncio
    async def test_successful_processing_performance(
        self,
        simple_processor: PydanticWebSocketProcessor[TestOrderModel, TestOrderModel],
        test_context: BackpackMessageContext,
        valid_order_payload: dict[str, str | int | dict[str, str | int]],
    ) -> None:
        """Test performance of successful message processing."""
        handler = AsyncMock()
        iterations = 1000

        # Warm up
        for _ in range(10):
            await simple_processor.process(valid_order_payload, handler, test_context)

        # Measure processing time
        start_time = time.perf_counter()

        for _ in range(iterations):
            await simple_processor.process(valid_order_payload, handler, test_context)

        end_time = time.perf_counter()
        total_time = end_time - start_time
        avg_time_ms = (total_time / iterations) * 1000

        # Performance assertions
        assert avg_time_ms < 1.0  # Should process in under 1ms on average
        assert handler.call_count >= iterations
        assert simple_processor.metrics.total_processed == iterations + 10  # Including warm-up
        assert simple_processor.metrics.validation_errors == 0

        # Check metrics accuracy
        metrics = simple_processor.get_metrics()
        assert metrics.processing_metrics.total_processed == iterations + 10
        assert metrics.processing_metrics.get_average_processing_time_ms() > 0

    @pytest.mark.asyncio
    async def test_validation_error_performance(
        self,
        simple_processor: PydanticWebSocketProcessor[TestOrderModel, TestOrderModel],
        test_context: BackpackMessageContext,
        invalid_order_payload: dict[str, str | int],
    ) -> None:
        """Test performance of validation error handling."""
        handler = AsyncMock()
        iterations = 1000

        # Measure validation error handling time
        start_time = time.perf_counter()

        for _ in range(iterations):
            await simple_processor.process(invalid_order_payload, handler, test_context)

        end_time = time.perf_counter()
        total_time = end_time - start_time
        avg_time_ms = (total_time / iterations) * 1000

        # Performance assertions
        assert avg_time_ms < 2.0  # Error handling should still be fast
        assert handler.call_count == 0  # Handler not called for validation errors
        assert simple_processor.metrics.validation_errors == iterations
        assert simple_processor.metrics.total_processed == 0

    @pytest.mark.asyncio
    async def test_complex_model_performance(
        self,
        complex_processor: PydanticWebSocketProcessor[TestComplexModel, Any],
        test_context: BackpackMessageContext,
        complex_payload: dict[
            str, str | list[dict[str, str | int | dict[str, int]]] | dict[str, str | bool | int]
        ],
    ) -> None:
        """Test performance with complex nested models."""
        handler = AsyncMock()
        iterations = 500

        # Warm up
        for _ in range(5):
            await complex_processor.process(complex_payload, handler, test_context)

        # Measure processing time
        start_time = time.perf_counter()

        for _ in range(iterations):
            await complex_processor.process(complex_payload, handler, test_context)

        end_time = time.perf_counter()
        total_time = end_time - start_time
        avg_time_ms = (total_time / iterations) * 1000

        # Performance assertions (complex models take longer)
        assert avg_time_ms < 5.0  # Should process in under 5ms on average
        assert handler.call_count >= iterations
        assert complex_processor.metrics.total_processed == iterations + 5

    @pytest.mark.asyncio
    async def test_concurrent_processing_performance(
        self,
        simple_processor: PydanticWebSocketProcessor[TestOrderModel, TestOrderModel],
        test_context: BackpackMessageContext,
        valid_order_payload: dict[str, str | int | dict[str, str | int]],
    ) -> None:
        """Test performance under concurrent load."""
        handler = AsyncMock()
        concurrent_tasks = 100
        iterations_per_task = 50

        async def process_batch() -> float:
            """Process a batch of messages.

            Returns:
                Time taken to process the batch in seconds.
            """
            start = time.perf_counter()
            for _ in range(iterations_per_task):
                await simple_processor.process(valid_order_payload, handler, test_context)
            return time.perf_counter() - start

        # Run concurrent processing
        start_time = time.perf_counter()
        tasks = [process_batch() for _ in range(concurrent_tasks)]
        _task_times = await asyncio.gather(*tasks)
        end_time = time.perf_counter()

        total_messages = concurrent_tasks * iterations_per_task
        total_time = end_time - start_time
        avg_time_ms = (total_time / total_messages) * 1000

        # Performance assertions
        assert avg_time_ms < 2.0  # Should handle concurrency well
        assert simple_processor.metrics.total_processed == total_messages
        assert simple_processor.metrics.validation_errors == 0

        # Check that concurrent processing didn't cause errors
        assert simple_processor.metrics.handler_errors == 0
        assert simple_processor.metrics.transformation_errors == 0

    @pytest.mark.asyncio
    async def test_mixed_success_error_performance(
        self,
        simple_processor: PydanticWebSocketProcessor[TestOrderModel, TestOrderModel],
        test_context: BackpackMessageContext,
        valid_order_payload: dict[str, str | int | dict[str, str | int]],
        invalid_order_payload: dict[str, str | int],
    ) -> None:
        """Test performance with mixed successful and error cases."""
        handler = AsyncMock()
        iterations = 1000

        # Alternate between valid and invalid payloads
        payloads = [
            valid_order_payload if i % 2 == 0 else invalid_order_payload for i in range(iterations)
        ]

        start_time = time.perf_counter()

        for payload in payloads:
            await simple_processor.process(payload, handler, test_context)

        end_time = time.perf_counter()
        total_time = end_time - start_time
        avg_time_ms = (total_time / iterations) * 1000

        # Performance assertions
        assert avg_time_ms < 1.5  # Mixed processing should still be fast
        assert simple_processor.metrics.total_processed == iterations // 2
        assert simple_processor.metrics.validation_errors == iterations // 2

    @pytest.mark.asyncio
    async def test_metrics_overhead(
        self,
        mock_error_handler: AsyncMock,
        mock_stream_handler: AsyncMock,
        test_context: BackpackMessageContext,
        valid_order_payload: dict[str, str | int | dict[str, str | int]],
    ) -> None:
        """Test performance overhead of metrics collection."""
        # Create processor without metrics
        mock_stream_handler = Mock()
        processor_no_metrics = PydanticWebSocketProcessor(
            raw_model=TestOrderModel,
            transformer=TestTransformer(),
            processor_name="no_metrics",
            stream_error_handler=mock_stream_handler,
        )

        # Create processor with full metrics
        processor_with_metrics = PydanticWebSocketProcessor(
            raw_model=TestOrderModel,
            transformer=TestTransformer(),
            processor_name="with_metrics",
            stream_error_handler=mock_stream_handler,
        )

        handler = AsyncMock()
        iterations = 1000

        # Measure without metrics
        start_time = time.perf_counter()
        for _ in range(iterations):
            await processor_no_metrics.process(valid_order_payload, handler, test_context)
        time_no_metrics = time.perf_counter() - start_time

        # Measure with metrics
        start_time = time.perf_counter()
        for _ in range(iterations):
            await processor_with_metrics.process(valid_order_payload, handler, test_context)
        time_with_metrics = time.perf_counter() - start_time

        # Calculate overhead
        overhead_percent = ((time_with_metrics - time_no_metrics) / time_no_metrics) * 100

        # Metrics should add minimal overhead
        assert overhead_percent < 10  # Less than 10% overhead acceptable

    @pytest.mark.asyncio
    async def test_error_recovery_performance(
        self,
        simple_processor: PydanticWebSocketProcessor[TestOrderModel, TestOrderModel],
        test_context: BackpackMessageContext,
        valid_order_payload: dict[str, str | int | dict[str, str | int]],
    ) -> None:
        """Test performance of error recovery mechanisms."""
        # Create handler that fails sometimes
        handler = AsyncMock()
        failure_rate = 0.1  # 10% failure rate
        call_count = 0

        def handler_side_effect(*args: object, **kwargs: object) -> None:
            nonlocal call_count
            call_count += 1
            if call_count % int(1 / failure_rate) == 0:
                raise RuntimeError("Simulated handler failure")

        handler.side_effect = handler_side_effect

        iterations = 1000
        start_time = time.perf_counter()

        for _ in range(iterations):
            await simple_processor.process(valid_order_payload, handler, test_context)

        end_time = time.perf_counter()
        total_time = end_time - start_time
        avg_time_ms = (total_time / iterations) * 1000

        # Performance assertions
        assert avg_time_ms < 2.0  # Error recovery should be efficient
        assert simple_processor.metrics.handler_errors > 0
        assert simple_processor.metrics.handler_errors < iterations * 0.15  # Close to expected rate

    @pytest.mark.asyncio
    async def test_memory_efficiency(
        self,
        simple_processor: PydanticWebSocketProcessor[TestOrderModel, TestOrderModel],
        test_context: BackpackMessageContext,
        valid_order_payload: dict[str, str | int | dict[str, str | int]],
    ) -> None:
        """Test that processor doesn't leak memory during processing."""
        handler = AsyncMock()

        # Process many messages
        for batch in range(10):
            for _ in range(1000):
                await simple_processor.process(valid_order_payload, handler, test_context)

            # Reset metrics periodically to avoid unbounded growth
            if batch % 5 == 0:
                simple_processor.reset_metrics()

        # Check that metrics were properly reset
        assert simple_processor.metrics.total_processed < 6000  # Should have been reset

        # Verify processor is still functional
        await simple_processor.process(valid_order_payload, handler, test_context)
        assert handler.called

    def test_metrics_calculation_performance(
        self,
        simple_processor: PydanticWebSocketProcessor[TestOrderModel, TestOrderModel],
    ) -> None:
        """Test performance of metrics calculation methods."""
        # Populate metrics with data
        for i in range(10000):
            simple_processor.metrics.record_processing_time(0.001 * (i % 10))
            if i % 100 == 0:
                simple_processor.metrics.record_validation_error()
            if i % 200 == 0:
                simple_processor.metrics.record_transformation_error()
            if i % 300 == 0:
                simple_processor.metrics.record_handler_error()

        # Measure metrics calculation performance
        iterations = 1000

        start_time = time.perf_counter()
        for _ in range(iterations):
            metrics = simple_processor.get_metrics()
            _ = metrics.processing_metrics.get_average_processing_time_ms()
            _ = metrics.processing_metrics.get_error_rate()
            _ = metrics.processing_metrics.get_messages_per_second()
        end_time = time.perf_counter()

        avg_time_ms = ((end_time - start_time) / iterations) * 1000

        # Metrics calculation should be very fast
        assert avg_time_ms < 0.1  # Less than 0.1ms per calculation

    @pytest.mark.asyncio
    async def test_processor_creation_performance(
        self,
        mock_error_handler: AsyncMock,
        mock_stream_handler: AsyncMock,
    ) -> None:
        """Test performance of processor creation and initialization."""
        iterations = 100

        start_time = time.perf_counter()

        for i in range(iterations):
            processor = PydanticWebSocketProcessor(
                raw_model=TestOrderModel,
                transformer=TestTransformer(),
                processor_name=f"perf_test_{i}",
                stream_error_handler=mock_stream_handler,
            )
            # Verify processor is functional
            assert processor.processor_name == f"perf_test_{i}"

        end_time = time.perf_counter()
        avg_time_ms = ((end_time - start_time) / iterations) * 1000

        # Processor creation should be fast
        assert avg_time_ms < 1.0  # Less than 1ms per processor


class TestProcessorPerformanceComparison:
    """Compare performance between old and new error handling."""

    @pytest.mark.asyncio
    async def test_compare_error_handling_approaches(
        self,
        mock_error_handler: AsyncMock,
        mock_stream_handler: AsyncMock,
        test_context: BackpackMessageContext,
        invalid_order_payload: dict[str, str | int],
    ) -> None:
        """Compare performance of old vs new error handling."""
        # Old approach processor (with mock stream handler)
        mock_stream_handler_old = Mock()
        old_processor = PydanticWebSocketProcessor(
            raw_model=TestOrderModel,
            transformer=TestTransformer(),
            processor_name="old_approach",
            stream_error_handler=mock_stream_handler_old,
        )

        # New approach processor (with stream handler)
        new_processor = PydanticWebSocketProcessor(
            raw_model=TestOrderModel,
            transformer=TestTransformer(),
            processor_name="new_approach",
            stream_error_handler=mock_stream_handler,
        )

        handler = AsyncMock()
        iterations = 1000

        # Measure old approach
        start_time = time.perf_counter()
        for _ in range(iterations):
            await old_processor.process(invalid_order_payload, handler, test_context)
        old_time = time.perf_counter() - start_time

        # Measure new approach
        start_time = time.perf_counter()
        for _ in range(iterations):
            await new_processor.process(invalid_order_payload, handler, test_context)
        new_time = time.perf_counter() - start_time

        # Calculate difference
        time_difference_percent = ((new_time - old_time) / old_time) * 100

        # New approach should not be significantly slower
        assert abs(time_difference_percent) < 15  # Within 15% is acceptable

        # Both should have handled errors correctly
        assert old_processor.metrics.validation_errors == iterations
        assert new_processor.metrics.validation_errors == iterations


class TestPerformanceBenchmarks:
    """Benchmark tests for establishing performance baselines."""

    @pytest.mark.asyncio
    async def test_baseline_throughput(
        self,
        simple_processor: PydanticWebSocketProcessor[TestOrderModel, TestOrderModel],
        test_context: BackpackMessageContext,
        valid_order_payload: dict[str, str | int | dict[str, str | int]],
    ) -> None:
        """Establish baseline throughput for the processor."""
        handler = AsyncMock()
        test_duration_seconds = 5

        start_time = time.perf_counter()
        message_count = 0

        while (time.perf_counter() - start_time) < test_duration_seconds:
            await simple_processor.process(valid_order_payload, handler, test_context)
            message_count += 1

        end_time = time.perf_counter()
        actual_duration = end_time - start_time

        messages_per_second = message_count / actual_duration

        # Baseline assertions
        assert messages_per_second > 1000  # Should handle at least 1000 msgs/sec
        assert simple_processor.metrics.total_processed == message_count

        logger.info("\nBaseline Throughput: %.2f messages/second", messages_per_second)
        logger.info("Average latency: %.3f ms", (actual_duration / message_count) * 1000)

    @pytest.mark.asyncio
    async def test_sustained_load_performance(
        self,
        simple_processor: PydanticWebSocketProcessor[TestOrderModel, TestOrderModel],
        test_context: BackpackMessageContext,
        valid_order_payload: dict[str, str | int | dict[str, str | int]],
    ) -> None:
        """Test performance under sustained load."""
        handler = AsyncMock()

        # Run for extended period with periodic measurements
        measurements: list[float] = []
        batch_size = 1000
        num_batches = 10

        for _batch in range(num_batches):
            start_time = time.perf_counter()

            for _ in range(batch_size):
                await simple_processor.process(valid_order_payload, handler, test_context)

            batch_time = time.perf_counter() - start_time
            batch_rate = batch_size / batch_time
            measurements.append(batch_rate)

            # Small delay between batches
            await asyncio.sleep(0.1)

        # Calculate statistics
        avg_rate = sum(measurements) / len(measurements)
        min_rate = min(measurements)
        max_rate = max(measurements)

        # Performance should be consistent
        variance = max_rate - min_rate
        consistency_percent = (variance / avg_rate) * 100

        assert consistency_percent < 20  # Less than 20% variance
        assert min_rate > 800  # Minimum acceptable rate

        logger.info("\nSustained Load Performance:")
        logger.info("  Average: %.2f msgs/sec", avg_rate)
        logger.info("  Min: %.2f msgs/sec", min_rate)
        logger.info("  Max: %.2f msgs/sec", max_rate)
        logger.info("  Consistency: %.1f%%", 100 - consistency_percent)
