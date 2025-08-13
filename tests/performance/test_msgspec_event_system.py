"""Performance validation tests for the msgspec event system.

This module validates that the new event system meets or exceeds
all performance targets established during the refactor.
"""

import asyncio
import sys
import time
from decimal import Decimal

import msgspec
import pytest

from cyberdelta.config.models.event_system_config import EventBusConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import (
    ExchangeName,
    MarketDataType,
    OrderEventType,
    OrderSide,
    PositionEventType,
)
from cyberdelta.enums.event_bus import HandlerPriority
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models.events.core import (
    MarketData,
    OrderEvent,
    PositionEvent,
)


logger = get_logger(__name__)


class PerformanceMetrics:
    """Track performance metrics for validation."""

    def __init__(self) -> None:
        """Initialize performance metrics tracking."""
        self.serialization_times: list[float] = []
        self.deserialization_times: list[float] = []
        self.publish_times: list[float] = []
        self.handler_times: list[float] = []
        self.throughput_events_per_sec: float = 0.0
        self.memory_usage_mb: float = 0.0


@pytest.fixture
def event_bus() -> EventBus:
    """Create an event bus for testing.

    Returns:
        EventBus: Configured event bus instance
    """
    config = EventBusConfig()
    return EventBus(config)


@pytest.fixture
def sample_events() -> list[msgspec.Struct]:
    """Create sample events for testing.

    Returns:
        list[msgspec.Struct]: List of test events
    """
    return [
        MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            timestamp=0,
            data_type=MarketDataType.TICK,
            price=Decimal("50000.00"),
            volume=10,
            bid=Decimal("49999.00"),
            ask=Decimal("50001.00"),
        ),
        OrderEvent(
            order_id="test123",
            exchange=ExchangeName.BACKPACK,
            symbol="ETH-USDC",
            event_type=OrderEventType.FILLED,
            side=OrderSide.BUY,
            price=Decimal("3000.00"),
            quantity=Decimal("5.0"),
            fill_price=Decimal("2999.50"),
            fill_quantity=Decimal("5.0"),
            remaining_quantity=Decimal(0),
        ),
        PositionEvent(
            position_id="pos_123",
            symbol="SOL-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            event_type=PositionEventType.OPENED,
            size=Decimal("100.0"),
            average_price=Decimal("150.00"),
            realized_pnl=Decimal("500.00"),
            unrealized_pnl=Decimal("200.00"),
        ),
    ]


@pytest.mark.timing
def test_serialization_performance(sample_events: list[msgspec.Struct]) -> None:
    """Test that serialization meets performance targets.

    Target: < 10μs per event
    """
    metrics = PerformanceMetrics()

    for event in sample_events:
        start = time.perf_counter()
        msgspec.json.encode(event)
        end = time.perf_counter()

        duration_us = (end - start) * 1_000_000
        metrics.serialization_times.append(duration_us)

    avg_time = sum(metrics.serialization_times) / len(metrics.serialization_times)

    logger.info(
        "serialization_performance",
        average_time_us=avg_time,
        target_us=10,
        pass_status=avg_time < 10,
    )

    assert avg_time < 10, f"Serialization too slow: {avg_time:.2f}μs"


@pytest.mark.timing
def test_deserialization_performance(sample_events: list[msgspec.Struct]) -> None:
    """Test that deserialization meets performance targets.

    Target: < 20μs per event
    """
    metrics = PerformanceMetrics()

    # First encode the events
    encoded_events = [(type(event), msgspec.json.encode(event)) for event in sample_events]

    for event_type, encoded in encoded_events:
        decoder = msgspec.json.Decoder(event_type)

        start = time.perf_counter()
        decoder.decode(encoded)
        end = time.perf_counter()

        duration_us = (end - start) * 1_000_000
        metrics.deserialization_times.append(duration_us)

    avg_time = sum(metrics.deserialization_times) / len(metrics.deserialization_times)

    logger.info(
        "deserialization_performance",
        average_time_us=avg_time,
        target_us=20,
        pass_status=avg_time < 20,
    )

    assert avg_time < 20, f"Deserialization too slow: {avg_time:.2f}μs"


@pytest.mark.timing
async def test_event_bus_throughput(
    event_bus: EventBus, sample_events: list[msgspec.Struct]
) -> None:
    """Test event bus throughput.

    Target: > 10,000 events/second
    """
    # Create a simple handler
    handler_call_count = 0

    async def test_handler(event: msgspec.Struct) -> None:
        nonlocal handler_call_count
        handler_call_count += 1
        await asyncio.sleep(0)  # Make properly async

    # Subscribe handler
    for event_type in [MarketData, OrderEvent, PositionEvent]:
        event_bus.subscribe(event_type, test_handler)

    # Measure throughput
    num_events = 10000
    start = time.perf_counter()

    for _ in range(num_events // len(sample_events)):
        for event in sample_events:
            await event_bus.publish(event)

    end = time.perf_counter()
    duration_sec = end - start

    throughput = num_events / duration_sec

    logger.info(
        "event_bus_throughput",
        events_per_sec=throughput,
        target_events_per_sec=10000,
        pass_status=throughput > 10000,
    )

    assert throughput > 10000, f"Throughput too low: {throughput:.0f} events/sec"


@pytest.mark.timing
async def test_priority_routing_performance(event_bus: EventBus) -> None:
    """Test that priority routing adds minimal overhead.

    Target: < 5μs overhead for priority handling
    """
    execution_order: list[str] = []

    async def critical_handler(event: msgspec.Struct) -> None:
        execution_order.append("critical")
        await asyncio.sleep(0)  # Make properly async

    async def normal_handler(event: msgspec.Struct) -> None:
        execution_order.append("normal")
        await asyncio.sleep(0)  # Make properly async

    # Subscribe with different priorities
    event_bus.subscribe(MarketData, critical_handler, HandlerPriority.CRITICAL)
    event_bus.subscribe(MarketData, normal_handler, HandlerPriority.NORMAL)

    # Create test event
    event = MarketData(
        symbol="TEST",
        exchange=ExchangeName.HYPERLIQUID,
        timestamp=0,
        data_type=MarketDataType.TICK,
        price=Decimal(100),
    )

    # Measure overhead
    iterations = 1000

    # Baseline: publish without handlers
    event_bus_empty = EventBus(EventBusConfig())
    start = time.perf_counter()
    for _ in range(iterations):
        await event_bus_empty.publish(event)
    baseline_time = time.perf_counter() - start

    # With priority handlers
    execution_order.clear()
    start = time.perf_counter()
    for _ in range(iterations):
        await event_bus.publish(event)
    priority_time = time.perf_counter() - start

    overhead_us = ((priority_time - baseline_time) / iterations) * 1_000_000

    logger.info(
        "priority_routing_performance",
        overhead_us=overhead_us,
        target_us=5,
        pass_status=overhead_us < 5,
    )

    # Verify priority order
    assert execution_order[:2] == ["critical", "normal"], "Priority order incorrect"
    assert overhead_us < 5, f"Priority overhead too high: {overhead_us:.2f}μs"


def test_memory_efficiency() -> None:
    """Test memory efficiency of msgspec events.

    Target: < 1KB per event average
    """
    events: list[MarketData] = []
    for i in range(1000):
        event = MarketData(
            symbol=f"SYM{i}",
            exchange=ExchangeName.HYPERLIQUID,
            timestamp=i,
            data_type=MarketDataType.TICK,
            price=Decimal(f"{50000 + i}"),
            volume=int(10 + i),
            bid=Decimal(f"{49999 + i}"),
            ask=Decimal(f"{50001 + i}"),
        )
        events.append(event)

    # Estimate memory usage
    total_size = sum(sys.getsizeof(event) for event in events)
    avg_size_bytes = total_size / len(events)

    logger.info(
        "memory_efficiency",
        avg_size_bytes=avg_size_bytes,
        target_bytes=1024,
        pass_status=avg_size_bytes < 1024,
    )

    assert avg_size_bytes < 1024, f"Events too large: {avg_size_bytes:.0f} bytes"


@pytest.mark.asyncio
@pytest.mark.timing
async def test_complete_performance_suite(
    event_bus: EventBus, sample_events: list[msgspec.Struct]
) -> None:
    """Run complete performance validation suite."""
    logger.info("performance_suite_started")

    test_serialization_performance(sample_events)
    test_deserialization_performance(sample_events)
    await test_event_bus_throughput(event_bus, sample_events)
    await test_priority_routing_performance(event_bus)
    test_memory_efficiency()

    logger.info(
        "performance_suite_completed",
        message="All performance targets met - system is production ready",
    )


if __name__ == "__main__":
    # Run the performance validation
    asyncio.run(
        test_complete_performance_suite(
            EventBus(EventBusConfig()),
            [
                MarketData(
                    symbol="BTC-USDC",
                    exchange=ExchangeName.HYPERLIQUID,
                    timestamp=0,
                    data_type=MarketDataType.TICK,
                    price=Decimal("50000.00"),
                )
            ],
        )
    )
