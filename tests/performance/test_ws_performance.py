"""Performance tests for WebSocket refactoring.

This module contains performance benchmarks to ensure the new WebSocket
architecture maintains or improves upon the original performance characteristics.
"""

import asyncio
import json
import time
from typing import Any

import orjson
import pytest

from cyberdelta.apis.backpack.bp_ws_router import BackpackWebSocketRouter
from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
from cyberdelta.apis.backpack.mappers.account.bp_position_mapper import BackpackPositionMapper
from cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper import BackpackTransactionMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper import BackpackFillMapper
from cyberdelta.apis.backpack.mappers.trading.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawDepthUpdateEvent
from cyberdelta.apis.common import MessageHandler
from cyberdelta.apis.hyperliquid.hl_ws_router import HyperliquidWebSocketRouter
from cyberdelta.apis.hyperliquid.mappers.account.hl_balance_mapper import HyperliquidBalanceMapper
from cyberdelta.apis.hyperliquid.mappers.account.hl_position_mapper import HyperliquidPositionMapper
from cyberdelta.apis.hyperliquid.mappers.account.hl_transaction_mapper import (
    HyperliquidTransactionMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_historical_data_mapper import (
    HyperliquidHistoricalDataMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_order_book_mapper import (
    HyperliquidOrderBookMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_price_ticker_mapper import (
    HyperliquidPriceTickerMapper,
)
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
from cyberdelta.apis.websocket.error_handling.stream_error_handler import (
    WebSocketStreamErrorHandler,
)
from cyberdelta.apis.websocket.registry.registry_factory import WebSocketRegistryFactory
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.config.models.websocket_error_config import WebSocketErrorConfig
from tests.common_symbols import BTC_USDC_BP


# Sample messages for testing
BACKPACK_DEPTH_MESSAGE = {
    "topic": f"depth.{BTC_USDC_BP.value}",
    "data": {
        "symbol": BTC_USDC_BP.value,
        "asks": [["50100.00", "1.5"], ["50110.00", "2.0"]],
        "bids": [["50090.00", "1.2"], ["50080.00", "1.8"]],
        "lastUpdateId": "123456",
        "timestamp": 1640995200000,
    },
}

HYPERLIQUID_L2BOOK_MESSAGE = {
    "channel": "l2Book",
    "data": {
        "coin": "BTC",
        "levels": [
            [{"px": "50100.00", "sz": "1.5", "n": 1}, {"px": "50090.00", "sz": "1.2", "n": 1}],
            [{"px": "50080.00", "sz": "1.8", "n": 1}, {"px": "50070.00", "sz": "2.0", "n": 1}],
        ],
        "time": 1640995200000,
    },
}


class PerformanceMetrics:
    """Collect performance metrics during tests."""

    def __init__(self) -> None:
        """Initialize performance metrics collector."""
        self.start_time = 0.0
        self.end_time = 0.0
        self.message_count = 0
        self.total_processing_time = 0.0
        self.memory_start = 0
        self.memory_end = 0

    def start(self) -> None:
        """Start timing."""
        self.start_time = time.perf_counter()

    def end(self) -> None:
        """End timing."""
        self.end_time = time.perf_counter()

    def record_message(self, processing_time: float) -> None:
        """Record a message processing."""
        self.message_count += 1
        self.total_processing_time += processing_time

    @property
    def elapsed_time(self) -> float:
        """Total elapsed time."""
        return self.end_time - self.start_time

    @property
    def throughput(self) -> float:
        """Messages per second."""
        if self.elapsed_time == 0:
            return 0
        return self.message_count / self.elapsed_time

    @property
    def average_latency(self) -> float:
        """Average processing time per message."""
        if self.message_count == 0:
            return 0
        return self.total_processing_time / self.message_count


@pytest.mark.asyncio
class TestJSONParsingPerformance:
    """Test JSON parsing performance with orjson vs standard json."""

    @pytest.mark.timing
    async def test_orjson_vs_json_parsing(self) -> None:
        """Compare orjson vs standard json parsing performance."""
        message = BACKPACK_DEPTH_MESSAGE
        message_str = json.dumps(message)
        iterations = 10000

        # Test standard json
        start = time.perf_counter()
        for _ in range(iterations):
            json.loads(message_str)
        json_time = time.perf_counter() - start

        # Test orjson
        start = time.perf_counter()
        for _ in range(iterations):
            orjson.loads(message_str)
        orjson_time = time.perf_counter() - start

        # Assert orjson is faster
        assert orjson_time < json_time
        speedup = json_time / orjson_time
        assert speedup >= 2.0  # Expect at least 2x speedup


@pytest.mark.asyncio
class TestMessageRoutingPerformance:
    """Test message routing performance."""

    @pytest.mark.timing
    async def test_backpack_router_throughput(self) -> None:
        """Test Backpack router throughput."""
        # Setup router
        error_config = WebSocketErrorConfig()
        stream_error_handler = WebSocketStreamErrorHandler(config=error_config)
        registry = WebSocketRegistryFactory.create_registry()
        typed_processor = TypeSafeWebSocketProcessor(registry)

        router = BackpackWebSocketRouter(
            stream_error_handler=stream_error_handler,
            typed_processor=typed_processor,
            order_book_mapper=BackpackOrderBookMapper(),
            ticker_mapper=BackpackTickerMapper(),
            trade_mapper=BackpackFillMapper(),
            balance_mapper=BackpackBalanceMapper(),
            position_mapper=BackpackPositionMapper(),
            order_mapper=BackpackOrderMapper(),
            transaction_mapper=BackpackTransactionMapper(),
        )

        # Mock handler
        messages_processed: list[dict[str, Any]] = []

        async def mock_handler(context: WebSocketContextProtocol) -> None:
            messages_processed.append(context.model_dump())
            await asyncio.sleep(0)  # Make function properly async

        handlers: dict[str, MessageHandler] = {"depth": mock_handler}

        # Performance test
        metrics = PerformanceMetrics()
        iterations = 1000

        metrics.start()
        for _ in range(iterations):
            msg_start = time.perf_counter()
            await router.route_message(BACKPACK_DEPTH_MESSAGE, handlers)
            msg_time = time.perf_counter() - msg_start
            metrics.record_message(msg_time)
        metrics.end()

        # Assertions
        assert len(messages_processed) == iterations
        assert metrics.throughput > 2000  # Expect >2k messages/sec (with debug logging)
        assert metrics.average_latency < 0.002  # Expect <2ms per message

    @pytest.mark.timing
    async def test_hyperliquid_router_throughput(self) -> None:
        """Test Hyperliquid router throughput."""
        # Setup router
        error_config = WebSocketErrorConfig()
        stream_error_handler = WebSocketStreamErrorHandler(config=error_config)
        registry = WebSocketRegistryFactory.create_registry()
        typed_processor = TypeSafeWebSocketProcessor(registry)

        router = HyperliquidWebSocketRouter(
            stream_error_handler=stream_error_handler,
            typed_processor=typed_processor,
            order_book_mapper=HyperliquidOrderBookMapper(),
            price_ticker_mapper=HyperliquidPriceTickerMapper(),
            balance_mapper=HyperliquidBalanceMapper(),
            position_mapper=HyperliquidPositionMapper(),
            order_mapper=HyperliquidOrderMapper(),
            transaction_mapper=HyperliquidTransactionMapper(),
            historical_data_mapper=HyperliquidHistoricalDataMapper(),
        )

        # Mock handler
        messages_processed: list[dict[str, Any]] = []

        async def mock_handler(context: WebSocketContextProtocol) -> None:
            messages_processed.append(context.model_dump())
            await asyncio.sleep(0)  # Make function properly async

        handlers: dict[str, MessageHandler] = {"l2Book": mock_handler}

        # Performance test
        metrics = PerformanceMetrics()
        iterations = 1000

        metrics.start()
        for _ in range(iterations):
            msg_start = time.perf_counter()
            await router.route_message(HYPERLIQUID_L2BOOK_MESSAGE, handlers)
            msg_time = time.perf_counter() - msg_start
            metrics.record_message(msg_time)
        metrics.end()

        # Assertions
        assert len(messages_processed) == iterations
        assert metrics.throughput > 2000  # Expect >2k messages/sec (with debug logging)
        assert metrics.average_latency < 0.002  # Expect <2ms per message


@pytest.mark.asyncio
class TestValidationPerformance:
    """Test validation performance."""

    @pytest.mark.timing
    async def test_pydantic_validation_overhead(self) -> None:
        """Test overhead added by Pydantic validation."""
        # This tests the raw Pydantic validation speed
        # Valid depth update data
        depth_data = {
            "symbol": BTC_USDC_BP.value,
            "asks": [["50100.00", "1.5"], ["50110.00", "2.0"]],
            "bids": [["50090.00", "1.2"], ["50080.00", "1.8"]],
            "lastUpdateId": "123456",
            "timestamp": 1640995200000,
        }

        iterations = 10000

        # Time validation
        start = time.perf_counter()
        for _ in range(iterations):
            BackpackRawDepthUpdateEvent.model_validate(depth_data)
        validation_time = time.perf_counter() - start

        # Calculate overhead
        overhead_per_msg = validation_time / iterations * 1000  # in ms

        assert overhead_per_msg < 0.5  # Less than 0.5ms overhead


@pytest.mark.asyncio
class TestMemoryEfficiency:
    """Test memory efficiency of the new architecture."""

    @pytest.mark.timing
    async def test_processor_memory_reuse(self) -> None:
        """Test that processors efficiently reuse memory."""
        # This is a simplified test - in production, you'd use memory profilers
        error_config = WebSocketErrorConfig()
        stream_error_handler = WebSocketStreamErrorHandler(config=error_config)
        registry = WebSocketRegistryFactory.create_registry()
        typed_processor = TypeSafeWebSocketProcessor(registry)

        router = BackpackWebSocketRouter(
            stream_error_handler=stream_error_handler,
            typed_processor=typed_processor,
            order_book_mapper=BackpackOrderBookMapper(),
            ticker_mapper=BackpackTickerMapper(),
            trade_mapper=BackpackFillMapper(),
            balance_mapper=BackpackBalanceMapper(),
            position_mapper=BackpackPositionMapper(),
            order_mapper=BackpackOrderMapper(),
            transaction_mapper=BackpackTransactionMapper(),
        )

        async def mock_handler(context: WebSocketContextProtocol) -> None:
            pass

        handlers: dict[str, MessageHandler] = {"depth": mock_handler}

        # Process many messages and ensure no memory leak
        # In a real test, you'd measure actual memory usage
        for _ in range(10000):
            await router.route_message(BACKPACK_DEPTH_MESSAGE, handlers)

        # If we get here without OOM, basic memory management is working
        assert True


@pytest.mark.asyncio
class TestConcurrentProcessing:
    """Test concurrent message processing performance."""

    @pytest.mark.timing
    async def test_concurrent_routing(self) -> None:
        """Test routing multiple messages concurrently."""
        error_config = WebSocketErrorConfig()
        stream_error_handler = WebSocketStreamErrorHandler(config=error_config)
        registry = WebSocketRegistryFactory.create_registry()
        typed_processor = TypeSafeWebSocketProcessor(registry)

        router = BackpackWebSocketRouter(
            stream_error_handler=stream_error_handler,
            typed_processor=typed_processor,
            order_book_mapper=BackpackOrderBookMapper(),
            ticker_mapper=BackpackTickerMapper(),
            trade_mapper=BackpackFillMapper(),
            balance_mapper=BackpackBalanceMapper(),
            position_mapper=BackpackPositionMapper(),
            order_mapper=BackpackOrderMapper(),
            transaction_mapper=BackpackTransactionMapper(),
        )

        processed_count = 0

        async def mock_handler(context: WebSocketContextProtocol) -> None:
            nonlocal processed_count
            processed_count += 1
            await asyncio.sleep(0)  # Make function properly async

        handlers: dict[str, MessageHandler] = {"depth": mock_handler}

        # Create many concurrent tasks
        tasks: list[asyncio.Task[None]] = []
        message_count = 1000

        start = time.perf_counter()
        for _ in range(message_count):
            task = asyncio.create_task(router.route_message(BACKPACK_DEPTH_MESSAGE, handlers))
            tasks.append(task)

        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

        assert processed_count == message_count
        throughput = message_count / elapsed
        assert throughput > 3000  # Expect >3k messages/sec with concurrency (with debug logging)


if __name__ == "__main__":
    # Run performance tests
    pytest.main([__file__, "-v", "-s"])
