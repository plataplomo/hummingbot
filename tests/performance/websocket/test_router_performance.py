"""Performance validation for WebSocket router with typed error system.

This test validates Step 45: Router Performance Validation.

These tests ensure the WebSocket router maintains acceptable performance
when integrated with the new typed error system, testing both successful
message routing and error handling scenarios.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router import (
    BaseWebSocketRouter,
    MessageHandler,
)
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.enums import ExchangeName


logger = logging.getLogger(__name__)


class TestEnvelopeModel(BaseModel):
    """Test envelope model for performance tests."""

    stream: str
    data: dict[str, Any]
    timestamp: int = 0


class TestRouterImpl(BaseWebSocketRouter[TestEnvelopeModel]):
    """Test router implementation for performance tests."""

    def _setup_processors(self) -> None:
        """Setup test processors."""

    def _extract_routing_key_from_envelope(self, envelope: TestEnvelopeModel) -> str | None:
        """Extract routing key from envelope.

        Returns:
            str | None: The routing key from the envelope stream field.
        """
        return envelope.stream or None

    def _extract_payload_from_envelope(self, envelope: TestEnvelopeModel) -> dict[str, Any]:
        """Extract payload from envelope.

        Returns:
            dict[str, Any]: The payload data from the envelope.
        """
        return envelope.data


class PerformanceProcessor:
    """High-performance test processor."""

    def __init__(self) -> None:
        """Initialize processor."""
        self.processed_count = 0
        self.processing_times: list[float] = []

    async def process(
        self,
        payload: dict[str, Any] | list[Any],
        handler: MessageHandler,
        context: WebSocketContextProtocol,
    ) -> None:
        """Process message with timing tracking."""
        start_time = time.perf_counter()
        self.processed_count += 1
        await handler(context)
        end_time = time.perf_counter()
        self.processing_times.append((end_time - start_time) * 1000)  # Convert to ms


@pytest.mark.asyncio
class TestWebSocketRouterPerformance:
    """Performance validation tests for WebSocket router."""

    @pytest.fixture
    def mock_legacy_error_handler(self) -> Mock:
        """Create mock legacy error handler.

        Returns:
            Mock: Configured legacy error handler mock with async methods.
        """
        mock = Mock()
        mock.handle_unroutable_message = AsyncMock()
        mock.handle_routing_error = AsyncMock()
        mock.handle_processing_error = AsyncMock()
        return mock

    @pytest.fixture
    def mock_stream_error_handler(self) -> Mock:
        """Create mock stream error handler.

        Returns:
            Mock: Stream error handler mock implementing WebSocketStreamErrorHandler.
        """
        mock = Mock(spec=WebSocketStreamErrorHandler)
        mock.handle_stream_error = AsyncMock()
        return mock

    @pytest.fixture
    def mock_typed_processor(self) -> Mock:
        """Create mock typed processor.

        Returns:
            Mock: Typed processor mock implementing TypeSafeWebSocketProcessor.
        """
        mock = Mock(spec=TypeSafeWebSocketProcessor)
        mock_context = Mock(spec=WebSocketContextProtocol)
        mock_context.connection_id = "test-conn-1234-abcd"
        mock_context.exchange_name = "hyperliquid"
        mock_context.message_id = "test-message-id"
        mock.create_typed_context.return_value = mock_context
        return mock

    @pytest.fixture
    def envelope_validator(self) -> Mock:
        """Create envelope validator.

        Returns:
            Mock: Validator mock that creates TestEnvelopeModel instances from message dictionaries.
        """

        def validator(message: dict[str, Any]) -> TestEnvelopeModel:
            """Validate and convert message to envelope model.

            Returns:
                TestEnvelopeModel: Validated envelope containing stream, data, and timestamp.
            """
            return TestEnvelopeModel(
                stream=message.get("stream", ""),
                data=message.get("data", {}),
                timestamp=message.get("timestamp", int(time.time())),
            )

        return Mock(side_effect=validator)

    @pytest.fixture
    def performance_router(
        self,
        mock_legacy_error_handler: Mock,
        mock_typed_processor: Mock,
        mock_stream_error_handler: Mock,
        envelope_validator: Mock,
    ) -> TestRouterImpl:
        """Create router configured for performance testing.

        Returns:
            TestRouterImpl: Router instance configured with mock dependencies for performance tests.
        """
        return TestRouterImpl(
            exchange_name=ExchangeName.HYPERLIQUID,
            typed_processor=mock_typed_processor,
            stream_error_handler=mock_stream_error_handler,
            envelope_validator=envelope_validator,
        )

    async def test_router_successful_message_processing_performance(
        self,
        performance_router: TestRouterImpl,
    ) -> None:
        """Test router performance for successful message processing."""
        # Setup processor and handler
        processor = PerformanceProcessor()
        handler = AsyncMock(spec=MessageHandler)

        performance_router.register_processor("ticker", processor)
        handlers: dict[str, MessageHandler] = {"ticker": cast(MessageHandler, handler)}

        # Test single message performance
        message = {
            "stream": "ticker",
            "data": {"symbol": "BTC-USD", "price": "50000"},
            "timestamp": int(time.time()),
        }

        # Single message timing
        start_time = time.perf_counter()
        await performance_router.route_message(message, handlers)
        end_time = time.perf_counter()
        single_message_time = (end_time - start_time) * 1000  # Convert to ms

        # Performance target: < 2ms per successful message (including typed system overhead)
        assert single_message_time < 2.0, (
            f"Single message took {single_message_time:.2f}ms, should be < 2ms"
        )

        # Verify processing occurred
        assert processor.processed_count == 1
        handler.assert_called_once()

    async def test_router_bulk_message_processing_performance(
        self,
        performance_router: TestRouterImpl,
    ) -> None:
        """Test router performance for bulk message processing."""
        # Setup processor and handler
        processor = PerformanceProcessor()
        handler = AsyncMock(spec=MessageHandler)

        performance_router.register_processor("ticker", processor)
        handlers: dict[str, MessageHandler] = {"ticker": cast(MessageHandler, handler)}

        # Process 100 messages in bulk
        messages: list[dict[str, object]] = [
            {
                "stream": "ticker",
                "data": {"symbol": "BTC-USD", "price": str(50000 + i), "seq": i},
                "timestamp": int(time.time()) + i,
            }
            for i in range(100)
        ]

        # Bulk processing timing
        start_time = time.perf_counter()
        for message in messages:
            await performance_router.route_message(message, handlers)
        end_time = time.perf_counter()

        bulk_processing_time = (end_time - start_time) * 1000  # Convert to ms
        avg_time_per_message = bulk_processing_time / 100

        # Performance targets:
        # - Total time < 500ms for 100 messages
        # - Average < 5ms per message (including typed system overhead)
        assert bulk_processing_time < 500.0, (
            f"Bulk processing took {bulk_processing_time:.2f}ms, should be < 500ms"
        )
        assert avg_time_per_message < 5.0, (
            f"Average per message {avg_time_per_message:.2f}ms, should be < 5ms"
        )

        # Verify all messages were processed
        assert processor.processed_count == 100
        assert handler.call_count == 100

    async def test_router_concurrent_message_processing_performance(
        self,
        performance_router: TestRouterImpl,
    ) -> None:
        """Test router performance under concurrent message processing."""
        # Setup processor and handler
        processor = PerformanceProcessor()
        handler = AsyncMock(spec=MessageHandler)

        performance_router.register_processor("ticker", processor)
        handlers: dict[str, MessageHandler] = {"ticker": cast(MessageHandler, handler)}

        # Create concurrent tasks
        async def process_message(i: int) -> float:
            """Process individual message and measure timing.

            Returns:
                float: Processing time in milliseconds.
            """
            message = {
                "stream": "ticker",
                "data": {"symbol": "BTC-USD", "price": str(50000 + i), "id": i},
                "timestamp": int(time.time()) + i,
            }
            start_time = time.perf_counter()
            await performance_router.route_message(message, handlers)
            end_time = time.perf_counter()
            return (end_time - start_time) * 1000

        # Process 50 messages concurrently
        start_time = time.perf_counter()
        task_times = await asyncio.gather(*[process_message(i) for i in range(50)])
        end_time = time.perf_counter()

        total_concurrent_time = (end_time - start_time) * 1000  # Convert to ms
        max_task_time = max(task_times)
        avg_task_time = sum(task_times) / len(task_times)

        # Performance targets for concurrent processing:
        # - Total time < 100ms (50 concurrent messages)
        # - Max individual task < 10ms
        # - Average task time < 5ms
        assert total_concurrent_time < 100.0, (
            f"Concurrent processing took {total_concurrent_time:.2f}ms, should be < 100ms"
        )
        assert max_task_time < 10.0, f"Max task time {max_task_time:.2f}ms, should be < 10ms"
        assert avg_task_time < 5.0, f"Average task time {avg_task_time:.2f}ms, should be < 5ms"

        # Verify all messages were processed
        assert processor.processed_count == 50
        assert handler.call_count == 50

    async def test_router_envelope_validation_error_performance(
        self,
        performance_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test router performance when handling envelope validation errors."""

        # Create failing envelope validator
        def failing_validator(message: dict[str, Any]) -> TestEnvelopeModel:
            raise ValidationError.from_exception_data(
                "TestEnvelopeModel",
                [{"type": "missing", "loc": ("stream",), "input": {}}],
            )

        performance_router.envelope_validator = Mock(side_effect=failing_validator)

        # Test single validation error performance
        message = {"data": {"invalid": "structure"}}
        handlers: dict[str, MessageHandler] = {}

        start_time = time.perf_counter()
        await performance_router.route_message(message, handlers)
        end_time = time.perf_counter()

        validation_error_time = (end_time - start_time) * 1000  # Convert to ms

        # Performance target: < 10ms per validation error (including typed error creation overhead)
        assert validation_error_time < 10.0, (
            f"Validation error took {validation_error_time:.2f}ms, should be < 10ms"
        )

        # Verify error was handled
        mock_stream_error_handler.handle_stream_error.assert_called_once()

    async def test_router_bulk_validation_error_performance(
        self,
        performance_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test router performance for bulk validation errors."""

        # Create failing envelope validator
        def failing_validator(message: dict[str, Any]) -> TestEnvelopeModel:
            raise ValidationError.from_exception_data(
                "TestEnvelopeModel",
                [{"type": "missing", "loc": ("stream",), "input": {}}],
            )

        performance_router.envelope_validator = Mock(side_effect=failing_validator)

        # Process 50 validation errors
        messages = [{"data": {"invalid": f"structure_{i}"}} for i in range(50)]
        handlers: dict[str, MessageHandler] = {}

        start_time = time.perf_counter()
        for message in messages:
            await performance_router.route_message(message, handlers)
        end_time = time.perf_counter()

        bulk_error_time = (end_time - start_time) * 1000  # Convert to ms
        avg_error_time = bulk_error_time / 50

        # Performance targets for bulk validation errors:
        # - Total time < 500ms for 50 errors
        # - Average < 10ms per error
        assert bulk_error_time < 500.0, (
            f"Bulk validation errors took {bulk_error_time:.2f}ms, should be < 500ms"
        )
        assert avg_error_time < 10.0, f"Average error time {avg_error_time:.2f}ms, should be < 10ms"

        # Verify all errors were handled
        assert mock_stream_error_handler.handle_stream_error.call_count == 50

    async def test_router_missing_processor_error_performance(
        self,
        performance_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test router performance when handling missing processor errors."""
        # Setup handler but no processor
        handler = AsyncMock(spec=MessageHandler)
        handlers: dict[str, MessageHandler] = {"ticker": cast(MessageHandler, handler)}

        # Test missing processor error performance
        message = {"stream": "ticker", "data": {"symbol": "BTC-USD"}, "timestamp": int(time.time())}

        start_time = time.perf_counter()
        await performance_router.route_message(message, handlers)
        end_time = time.perf_counter()

        missing_processor_time = (end_time - start_time) * 1000  # Convert to ms

        # Performance target: < 3ms per missing processor error
        assert missing_processor_time < 3.0, (
            f"Missing processor error took {missing_processor_time:.2f}ms, should be < 3ms"
        )

        # Verify error was handled
        mock_stream_error_handler.handle_stream_error.assert_called_once()

    async def test_router_mixed_success_error_performance(
        self,
        performance_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test router performance with mixed successful and error scenarios."""
        # Setup processor and handler for successful messages
        processor = PerformanceProcessor()
        handler = AsyncMock(spec=MessageHandler)

        performance_router.register_processor("ticker", processor)
        handlers: dict[str, MessageHandler] = {"ticker": cast(MessageHandler, handler)}

        # Create mixed messages (70% success, 30% missing processor)
        mixed_messages: list[dict[str, Any]] = []
        for i in range(100):
            if i % 10 < 7:  # 70% success
                mixed_messages.append({
                    "stream": "ticker",
                    "data": {"symbol": "BTC-USD", "price": str(50000 + i)},
                    "timestamp": int(time.time()) + i,
                })
            else:  # 30% missing processor
                mixed_messages.append({
                    "stream": "unknown_stream",
                    "data": {"symbol": "ETH-USD", "price": str(3000 + i)},
                    "timestamp": int(time.time()) + i,
                })

        # Process mixed messages
        start_time = time.perf_counter()
        for message in mixed_messages:
            await performance_router.route_message(message, handlers)
        end_time = time.perf_counter()

        mixed_processing_time = (end_time - start_time) * 1000  # Convert to ms
        avg_time_per_message = mixed_processing_time / 100

        # Performance targets for mixed scenario:
        # - Total time < 800ms for 100 mixed messages
        # - Average < 8ms per message (mix of success and errors)
        assert mixed_processing_time < 800.0, (
            f"Mixed processing took {mixed_processing_time:.2f}ms, should be < 800ms"
        )
        assert avg_time_per_message < 8.0, (
            f"Average mixed message time {avg_time_per_message:.2f}ms, should be < 8ms"
        )

        # Verify correct distribution of processing
        assert processor.processed_count == 70  # Successful messages
        assert handler.call_count == 70
        # 30 error messages should have been handled by error system

    async def test_router_memory_optimization_performance(
        self,
        performance_router: TestRouterImpl,
    ) -> None:
        """Test router performance with memory optimization enabled."""
        # Enable memory optimization
        optimization_enabled = performance_router.enable_high_frequency_mode()
        assert optimization_enabled is True

        # Setup processor and handler
        processor = PerformanceProcessor()
        handler = AsyncMock(spec=MessageHandler)

        performance_router.register_processor("ticker", processor)
        handlers: dict[str, MessageHandler] = {"ticker": cast(MessageHandler, handler)}

        # Process messages with memory optimization
        messages = [
            {
                "stream": "ticker",
                "data": {"symbol": "BTC-USD", "price": str(50000 + i), "id": i},
                "timestamp": int(time.time()) + i,
            }
            for i in range(100)
        ]

        # Memory optimized processing timing
        start_time = time.perf_counter()
        for message in messages:
            await performance_router.route_message(message, handlers)
        end_time = time.perf_counter()

        optimized_processing_time = (end_time - start_time) * 1000  # Convert to ms
        avg_optimized_time = optimized_processing_time / 100

        # Performance targets with memory optimization:
        # - Should be similar or better than non-optimized
        # - Total time < 400ms for 100 messages (better than bulk test)
        # - Average < 4ms per message
        assert optimized_processing_time < 400.0, (
            f"Optimized processing took {optimized_processing_time:.2f}ms, should be < 400ms"
        )
        assert avg_optimized_time < 4.0, (
            f"Average optimized time {avg_optimized_time:.2f}ms, should be < 4ms"
        )

        # Verify all messages were processed
        assert processor.processed_count == 100
        assert handler.call_count == 100

        # Verify memory stats are available
        memory_stats = performance_router.get_memory_stats()
        assert memory_stats is not None

    async def test_router_comprehensive_stats_performance(
        self,
        performance_router: TestRouterImpl,
    ) -> None:
        """Test performance of router comprehensive statistics collection."""
        # Setup multiple processors
        processor1 = PerformanceProcessor()
        processor2 = PerformanceProcessor()

        performance_router.register_processor("stream1", processor1)
        performance_router.register_processor("stream2", processor2)

        # Test stats collection performance
        stats_collection_times: list[float] = []

        for _ in range(100):
            start_time = time.perf_counter()
            stats = performance_router.get_comprehensive_stats()
            end_time = time.perf_counter()
            stats_collection_times.append((end_time - start_time) * 1000)  # Convert to ms

            # Verify stats structure is correct
            assert "exchange" in stats
            assert "processors" in stats
            assert "connection_id" in stats

        avg_stats_time = sum(stats_collection_times) / len(stats_collection_times)
        max_stats_time = max(stats_collection_times)

        # Performance targets for stats collection:
        # - Average < 1ms per stats collection
        # - Maximum < 5ms (even with complex stats)
        assert avg_stats_time < 1.0, (
            f"Average stats collection {avg_stats_time:.2f}ms, should be < 1ms"
        )
        assert max_stats_time < 5.0, f"Max stats collection {max_stats_time:.2f}ms, should be < 5ms"

    async def test_router_error_recovery_performance(
        self,
        performance_router: TestRouterImpl,
        mock_stream_error_handler: Mock,
    ) -> None:
        """Test router performance with error recovery system active."""
        # Verify error recovery is enabled
        assert performance_router.error_recovery is not None

        # Test successful operation notification performance
        success_notification_times: list[float] = []

        for _ in range(50):
            start_time = time.perf_counter()
            await performance_router.handle_successful_operation()
            end_time = time.perf_counter()
            success_notification_times.append((end_time - start_time) * 1000)  # Convert to ms

        avg_success_notification_time = sum(success_notification_times) / len(
            success_notification_times
        )
        max_success_notification_time = max(success_notification_times)

        # Performance targets for error recovery notifications:
        # - Average < 0.5ms per notification
        # - Maximum < 2ms
        assert avg_success_notification_time < 0.5, (
            f"Average success notification {avg_success_notification_time:.2f}ms, should be < 0.5ms"
        )
        assert max_success_notification_time < 2.0, (
            f"Max success notification {max_success_notification_time:.2f}ms, should be < 2ms"
        )

        # Test message send failure performance
        message = {"stream": "test_stream", "data": {"test": "data"}, "timestamp": int(time.time())}
        send_error = ConnectionError("Send failed")

        start_time = time.perf_counter()
        await performance_router.handle_message_send_failure(message, send_error)
        end_time = time.perf_counter()

        send_failure_time = (end_time - start_time) * 1000  # Convert to ms

        # Performance target: < 5ms for message send failure handling
        assert send_failure_time < 5.0, (
            f"Send failure handling took {send_failure_time:.2f}ms, should be < 5ms"
        )

        # Verify error was handled
        mock_stream_error_handler.handle_stream_error.assert_called_once()

    async def test_router_processor_registration_performance(
        self,
        performance_router: TestRouterImpl,
    ) -> None:
        """Test performance of processor registration and management operations."""
        # Test processor registration performance
        registration_times: list[float] = []
        processors: list[PerformanceProcessor] = []

        for i in range(100):
            processor = PerformanceProcessor()
            processors.append(processor)
            routing_key = f"stream_{i}"

            start_time = time.perf_counter()
            performance_router.register_processor(routing_key, processor)
            end_time = time.perf_counter()

            registration_times.append((end_time - start_time) * 1000)  # Convert to ms

        avg_registration_time = sum(registration_times) / len(registration_times)
        max_registration_time = max(registration_times)

        # Performance targets for processor registration:
        # - Average < 0.1ms per registration
        # - Maximum < 1ms
        assert avg_registration_time < 0.1, (
            f"Average registration {avg_registration_time:.2f}ms, should be < 0.1ms"
        )
        assert max_registration_time < 1.0, (
            f"Max registration {max_registration_time:.2f}ms, should be < 1ms"
        )

        # Test processor info retrieval performance
        info_retrieval_times: list[float] = []

        for _ in range(50):
            start_time = time.perf_counter()
            info = performance_router.get_processor_info()
            end_time = time.perf_counter()
            info_retrieval_times.append((end_time - start_time) * 1000)  # Convert to ms

            # Verify info structure
            assert info["total_processors"] == 100
            assert len(info["processors"]) == 100

        avg_info_time = sum(info_retrieval_times) / len(info_retrieval_times)
        max_info_time = max(info_retrieval_times)

        # Performance targets for processor info:
        # - Average < 2ms per info retrieval (with 100 processors)
        # - Maximum < 10ms
        assert avg_info_time < 2.0, f"Average info retrieval {avg_info_time:.2f}ms, should be < 2ms"
        assert max_info_time < 10.0, f"Max info retrieval {max_info_time:.2f}ms, should be < 10ms"

    async def test_router_high_frequency_scenario_performance(
        self,
        performance_router: TestRouterImpl,
    ) -> None:
        """Test router performance in high-frequency trading scenario."""
        # Enable high-frequency optimizations
        performance_router.enable_high_frequency_mode()

        # Setup high-performance processor
        processor = PerformanceProcessor()
        handler = AsyncMock(spec=MessageHandler)

        performance_router.register_processor("ticker", processor)
        handlers: dict[str, MessageHandler] = {"ticker": cast(MessageHandler, handler)}

        # Simulate high-frequency scenario: 1000 msgs/sec for 1 second
        messages = [
            {
                "stream": "ticker",
                "data": {
                    "symbol": "BTC-USD",
                    "price": str(50000 + (i % 100)),
                    "size": str(0.001 + (i % 10) * 0.001),
                    "timestamp": int(time.time() * 1000) + i,  # Microsecond precision
                },
                "timestamp": int(time.time()) + i,
            }
            for i in range(1000)
        ]

        # Process at high frequency
        start_time = time.perf_counter()

        # Process in batches to simulate realistic high-frequency load
        batch_size = 100
        for i in range(0, 1000, batch_size):
            batch = messages[i : i + batch_size]
            batch_tasks = [performance_router.route_message(message, handlers) for message in batch]
            await asyncio.gather(*batch_tasks)

        end_time = time.perf_counter()

        total_hft_time = (end_time - start_time) * 1000  # Convert to ms
        avg_time_per_message = total_hft_time / 1000
        messages_per_second = 1000 / (total_hft_time / 1000)

        # Performance targets for high-frequency scenario:
        # - Total time < 2000ms for 1000 messages (2 seconds acceptable)
        # - Average < 2ms per message
        # - Throughput > 500 messages/second
        assert total_hft_time < 2000.0, (
            f"HFT scenario took {total_hft_time:.2f}ms, should be < 2000ms"
        )
        assert avg_time_per_message < 2.0, (
            f"Average HFT message time {avg_time_per_message:.2f}ms, should be < 2ms"
        )
        assert messages_per_second > 500.0, (
            f"Throughput {messages_per_second:.2f} msgs/sec, should be > 500"
        )

        # Verify all messages were processed
        assert processor.processed_count == 1000
        assert handler.call_count == 1000

        # Verify memory optimization was beneficial
        memory_stats = performance_router.get_memory_stats()
        assert memory_stats is not None

        logger.info("High-frequency performance summary:")
        logger.info("  - Total time: %.2fms", total_hft_time)
        logger.info("  - Average per message: %.2fms", avg_time_per_message)
        logger.info("  - Throughput: %.2f messages/second", messages_per_second)
        logger.info("  - Memory stats: %s", memory_stats)
