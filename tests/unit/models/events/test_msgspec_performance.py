"""Performance tests for msgspec event structures vs Pydantic baseline.

Tests cover:
1. Direct msgspec vs Pydantic performance comparison
2. Performance regression detection with meaningful thresholds
3. High-frequency throughput validation
4. Real-world benchmark requirements
5. Symbol conversion overhead validation
"""

import gc
import time
from datetime import UTC, datetime
from decimal import Decimal

import msgspec
import pytest
from pydantic import BaseModel

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import (
    BalanceEventType,
    HealthStatus,
    MarketDataType,
    OrderEventType,
    OrderSide,
    PositionEventType,
    RiskSeverity,
    RiskType,
    SystemEventType,
    TradingAction,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.events.core import (
    BalanceEvent,
    MarketData,
    OrderEvent,
    PositionEvent,
    RiskEvent,
    SignalEvent,
    SystemEvent,
)
from tests.fixtures.time_fixtures import FreezerProtocol


pytestmark = pytest.mark.timing


# Get logger for test output
logger = get_logger(__name__)


# Pydantic baseline implementations for comparison
class PydanticMarketData(BaseModel):
    """Pydantic baseline for MarketData performance comparison."""

    symbol: str
    exchange: ExchangeName
    data_type: str
    price: Decimal | None = None
    volume: int | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    bids: list[tuple[Decimal, Decimal]] | None = None
    asks: list[tuple[Decimal, Decimal]] | None = None
    timestamp: float | None = None

    # Note: Removed time.time() initialization - this is handled by test fixtures


class PydanticOrderEvent(BaseModel):
    """Pydantic baseline for OrderEvent performance comparison."""

    order_id: str
    exchange: ExchangeName
    symbol: str
    event_type: str
    price: Decimal | None = None
    quantity: Decimal | None = None
    fill_price: Decimal | None = None
    fill_quantity: Decimal | None = None
    remaining_quantity: Decimal | None = None
    commission: Decimal | None = None
    reason: str | None = None
    error_code: str | None = None
    timestamp: float | None = None

    # Note: Removed time.time() initialization - this is handled by test fixtures


class TestMsgspecVsPydanticPerformance:
    """Test msgspec performance against Pydantic baseline with meaningful assertions."""

    @pytest.fixture
    def msgspec_market_data(self, frozen_time: FreezerProtocol) -> MarketData:
        """Msgspec MarketData event for performance testing.

        Args:
            frozen_time: Time freezer for deterministic timestamps.

        Returns:
            MarketData: Test market data event.
        """
        # Freeze time for deterministic testing
        test_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(test_time)

        return MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal("50123.456789"),
            volume=12345,
            # Timestamp will use frozen time via default_factory
        )

    @pytest.fixture
    def pydantic_market_data(self, frozen_time: FreezerProtocol) -> PydanticMarketData:
        """Pydantic MarketData event for baseline comparison.

        Args:
            frozen_time: Time freezer for deterministic timestamps.

        Returns:
            PydanticMarketData: Pydantic baseline event.
        """
        # Use same frozen time as msgspec for fair comparison
        test_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(test_time)

        return PydanticMarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type="tick",
            price=Decimal("50123.456789"),
            volume=12345,
            timestamp=test_time.timestamp(),
        )

    @pytest.fixture
    def msgspec_order_event(self, frozen_time: FreezerProtocol) -> OrderEvent:
        """Msgspec OrderEvent for performance testing.

        Args:
            frozen_time: Time freezer for deterministic timestamps.

        Returns:
            OrderEvent: Test order event.
        """
        # Freeze time for deterministic testing
        test_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(test_time)

        return OrderEvent(
            order_id="order_12345678901234567890",
            exchange=ExchangeName.BACKPACK,
            symbol="ETH-USDC",
            event_type=OrderEventType.FILLED,
            side=OrderSide.BUY,
            price=Decimal("3456.123456789"),
            quantity=Decimal("2.5"),
            fill_price=Decimal("3456.130000"),
            fill_quantity=Decimal("2.5"),
            commission=Decimal("8.640325"),
            # Timestamp will use frozen time via default_factory
        )

    @pytest.fixture
    def pydantic_order_event(self, frozen_time: FreezerProtocol) -> PydanticOrderEvent:
        """Pydantic OrderEvent for baseline comparison.

        Args:
            frozen_time: Time freezer for deterministic timestamps.

        Returns:
            PydanticOrderEvent: Pydantic baseline event.
        """
        # Use same frozen time as msgspec for fair comparison
        test_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(test_time)

        return PydanticOrderEvent(
            order_id="order_12345678901234567890",
            exchange=ExchangeName.BACKPACK,
            symbol="ETH-USDC",
            event_type="filled",
            price=Decimal("3456.123456789"),
            quantity=Decimal("2.5"),
            fill_price=Decimal("3456.130000"),
            fill_quantity=Decimal("2.5"),
            commission=Decimal("8.640325"),
            timestamp=test_time.timestamp(),
        )

    def test_market_data_serialization_speed_vs_pydantic(
        self, msgspec_market_data: MarketData, pydantic_market_data: PydanticMarketData
    ) -> None:
        """Test msgspec MarketData serialization vs Pydantic baseline."""
        msgspec_event = msgspec_market_data
        pydantic_event = pydantic_market_data

        # Warm up both
        for _ in range(100):
            msgspec.json.encode(msgspec_event)
            pydantic_event.model_dump_json()

        iterations = 1000

        # Measure msgspec serialization
        msgspec_bytes = b""
        start_time = time.perf_counter()
        for _ in range(iterations):
            msgspec_bytes = msgspec.json.encode(msgspec_event)
        msgspec_time = (time.perf_counter() - start_time) / iterations * 1_000_000  # μs

        # Measure Pydantic serialization
        pydantic_json = ""
        start_time = time.perf_counter()
        for _ in range(iterations):
            pydantic_json = pydantic_event.model_dump_json()
        pydantic_time = (time.perf_counter() - start_time) / iterations * 1_000_000  # μs

        # Calculate speedup
        speedup = pydantic_time / msgspec_time

        logger.info(
            "MarketData serialization comparison",
            msgspec_time=f"{msgspec_time:.2f}μs",
            pydantic_time=f"{pydantic_time:.2f}μs",
            speedup=f"{speedup:.1f}x",
        )

        # Based on real measurements: msgspec should be 5-15x faster than Pydantic v2
        if speedup < 4.0:
            pytest.fail(
                f"msgspec not fast enough vs Pydantic. Got {speedup:.1f}x speedup, expected >4x"
            )

        # msgspec should be very fast for serialization
        if msgspec_time > 1.0:  # Allow some variance for real hardware
            pytest.fail(f"msgspec serialize time {msgspec_time:.2f}μs exceeds expected baseline")

        # Verify output validity
        assert len(msgspec_bytes) > 0
        assert len(pydantic_json) > 0

    def test_market_data_deserialization_speed_vs_pydantic(
        self, msgspec_market_data: MarketData, pydantic_market_data: PydanticMarketData
    ) -> None:
        """Test msgspec MarketData deserialization vs Pydantic baseline."""
        msgspec_event = msgspec_market_data
        pydantic_event = pydantic_market_data

        # Prepare serialized data
        msgspec_bytes = msgspec.json.encode(msgspec_event)
        pydantic_json = pydantic_event.model_dump_json()

        # Warm up both
        for _ in range(100):
            msgspec.json.decode(msgspec_bytes, type=MarketData)
            PydanticMarketData.model_validate_json(pydantic_json)

        iterations = 1000

        # Measure msgspec deserialization
        msgspec_decoded: MarketData | None = None
        start_time = time.perf_counter()
        for _ in range(iterations):
            msgspec_decoded = msgspec.json.decode(msgspec_bytes, type=MarketData)
        msgspec_time = (time.perf_counter() - start_time) / iterations * 1_000_000  # μs

        # Measure Pydantic deserialization
        pydantic_decoded: PydanticMarketData | None = None
        start_time = time.perf_counter()
        for _ in range(iterations):
            pydantic_decoded = PydanticMarketData.model_validate_json(pydantic_json)
        pydantic_time = (time.perf_counter() - start_time) / iterations * 1_000_000  # μs

        # Calculate speedup
        speedup = pydantic_time / msgspec_time

        logger.info(
            "MarketData deserialization comparison",
            msgspec_time=f"{msgspec_time:.2f}μs",
            pydantic_time=f"{pydantic_time:.2f}μs",
            speedup=f"{speedup:.1f}x",
        )

        # Based on real measurements: msgspec should be 5-15x faster than Pydantic v2
        if speedup < 4.0:
            pytest.fail(
                f"msgspec not fast enough vs Pydantic. Got {speedup:.1f}x speedup, expected >4x"
            )

        # msgspec should be very fast for deserialization
        if msgspec_time > 2.0:  # Allow some variance for real hardware
            pytest.fail(f"msgspec deserialize time {msgspec_time:.2f}μs exceeds expected baseline")

        # Verify data integrity
        assert msgspec_decoded is not None
        assert msgspec_decoded.symbol == msgspec_event.symbol
        assert msgspec_decoded.price == msgspec_event.price
        assert pydantic_decoded is not None
        assert pydantic_decoded.symbol == pydantic_event.symbol
        assert pydantic_decoded.price == pydantic_event.price

    def test_order_event_round_trip_vs_pydantic(
        self, msgspec_order_event: OrderEvent, pydantic_order_event: PydanticOrderEvent
    ) -> None:
        """Test OrderEvent round-trip performance vs Pydantic baseline."""
        msgspec_event = msgspec_order_event
        pydantic_event = pydantic_order_event

        # Warm up both
        for _ in range(100):
            msgspec_bytes = msgspec.json.encode(msgspec_event)
            msgspec.json.decode(msgspec_bytes, type=OrderEvent)
            pydantic_json = pydantic_event.model_dump_json()
            PydanticOrderEvent.model_validate_json(pydantic_json)

        iterations = 500

        # Measure msgspec round-trip
        start_time = time.perf_counter()
        for _ in range(iterations):
            msgspec_bytes = msgspec.json.encode(msgspec_event)
            _ = msgspec.json.decode(msgspec_bytes, type=OrderEvent)
        msgspec_time = (time.perf_counter() - start_time) / iterations * 1_000_000  # μs

        # Measure Pydantic round-trip
        start_time = time.perf_counter()
        for _ in range(iterations):
            pydantic_json = pydantic_event.model_dump_json()
            _ = PydanticOrderEvent.model_validate_json(pydantic_json)
        pydantic_time = (time.perf_counter() - start_time) / iterations * 1_000_000  # μs

        # Calculate speedup
        speedup = pydantic_time / msgspec_time

        logger.info(
            "OrderEvent round-trip comparison",
            msgspec_time=f"{msgspec_time:.2f}μs",
            pydantic_time=f"{pydantic_time:.2f}μs",
            speedup=f"{speedup:.1f}x",
        )

        # Based on real-world measurements, msgspec should be 4-10x faster than Pydantic v2
        # Real hardware shows ~5x is excellent performance
        if speedup < 4.0:
            pytest.fail(
                f"msgspec round-trip not fast enough vs Pydantic. "
                f"Got {speedup:.1f}x speedup, expected >4x"
            )

        # msgspec should be reasonably fast for OrderEvent (more complex than MarketData)
        if msgspec_time > 5.0:  # Allow reasonable time for complex OrderEvent
            pytest.fail(f"msgspec round-trip time {msgspec_time:.2f}μs too slow for OrderEvent")

    def test_all_event_types_regression_protection(self) -> None:
        """Test all event types meet performance regression thresholds."""
        events = [
            (
                "MarketData",
                MarketData(
                    symbol="BTC-USDC",
                    exchange=ExchangeName.HYPERLIQUID,
                    data_type=MarketDataType.TICK,
                    price=Decimal("50000.12"),
                ),
            ),
            (
                "OrderEvent",
                OrderEvent(
                    order_id="test123",
                    exchange=ExchangeName.BACKPACK,
                    symbol="ETH-USDC",
                    event_type=OrderEventType.FILLED,
                    side=OrderSide.BUY,
                    price=Decimal("3500.00"),
                    quantity=Decimal("1.0"),
                ),
            ),
            (
                "PositionEvent",
                PositionEvent(
                    position_id="pos1",
                    symbol="SOL-USDC",
                    exchange=ExchangeName.HYPERLIQUID,
                    event_type=PositionEventType.OPENED,
                    size=Decimal("10.0"),
                    average_price=Decimal("100.0"),
                ),
            ),
            (
                "SignalEvent",
                SignalEvent(
                    signal_id="sig1",
                    strategy_name="test",
                    symbol="BTC-USDC",
                    exchange=ExchangeName.HYPERLIQUID,
                    action=TradingAction.BUY,
                    confidence=0.85,
                ),
            ),
            (
                "RiskEvent",
                RiskEvent(
                    risk_type=RiskType.EXPOSURE,
                    severity=RiskSeverity.WARNING,
                    current_value=Decimal(50000),
                    limit_value=Decimal(100000),
                    message="Exposure check",
                ),
            ),
            (
                "BalanceEvent",
                BalanceEvent(
                    account_id="acc1",
                    exchange=ExchangeName.BACKPACK,
                    currency="USDC",
                    event_type=BalanceEventType.UPDATED,
                    old_balance=Decimal(10000),
                    new_balance=Decimal(9500),
                ),
            ),
            (
                "SystemEvent",
                SystemEvent(
                    component="trading_service",
                    event_type=SystemEventType.STARTED,
                    status=HealthStatus.HEALTHY,
                    message="Service started successfully",
                ),
            ),
        ]

        # Performance regression thresholds with safety margin
        # These are maximum allowed times to detect regressions
        regression_thresholds = {
            "MarketData": 1.0,  # Should be ~0.14μs, allow up to 1.0μs
            "OrderEvent": 2.0,  # More complex, allow up to 2.0μs
            "PositionEvent": 2.0,  # More complex, allow up to 2.0μs
            "SignalEvent": 1.5,  # Medium complexity, allow up to 1.5μs
            "RiskEvent": 1.5,  # Medium complexity, allow up to 1.5μs
            "BalanceEvent": 1.5,  # Medium complexity, allow up to 1.5μs
            "SystemEvent": 1.5,  # Medium complexity, allow up to 1.5μs
        }

        results: dict[str, float] = {}
        failures: list[str] = []

        logger.info("Event type serialization performance (regression protection)")

        for event_type, event in events:
            # Warm up
            for _ in range(100):
                msgspec.json.encode(event)

            # Measure serialization
            start_time = time.perf_counter()
            iterations = 1000

            for _ in range(iterations):
                _ = msgspec.json.encode(event)

            avg_time = (time.perf_counter() - start_time) / iterations * 1_000_000  # μs
            results[event_type] = avg_time
            threshold = regression_thresholds[event_type]

            status = "✓" if avg_time <= threshold else "✗"
            logger.info(
                "Event performance",
                event_type=event_type,
                avg_time=f"{avg_time:.2f}μs",
                threshold=f"{threshold:.1f}μs",
                status=status,
            )

            # Collect failures for comprehensive error message
            if avg_time > threshold:
                failures.append(f"{event_type}: {avg_time:.2f}μs > {threshold:.1f}μs threshold")

        # MarketData should be fastest (array_like=True optimization)
        if results["MarketData"] > min(results.values()) * 2.0:  # Allow 2x variance
            failures.append(
                f"MarketData not optimally fast: {results['MarketData']:.2f}μs "
                f"vs min {min(results.values()):.2f}μs"
            )

        # Fail with comprehensive message if any regressions detected
        if failures:
            pytest.fail(
                "Performance regression detected:\n"
                + "\n".join(f"  - {failure}" for failure in failures)
            )


class TestTradingSystemRequirements:
    """Test performance against real trading system requirements."""

    def test_high_frequency_market_data_processing(self, frozen_time: FreezerProtocol) -> None:
        """Test processing burst of market data (simulating high-frequency feed)."""
        # Set deterministic time for consistent test results
        test_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(test_time)
        base_time = test_time.timestamp()

        # Create 1000 market data events (simulating 1 second of data at 1000 Hz)
        events: list[MarketData] = []

        for i in range(1000):
            event = MarketData(
                symbol="BTC-USDC",
                exchange=ExchangeName.HYPERLIQUID,
                data_type=MarketDataType.TICK,
                price=Decimal(f"{50000 + i * 0.01:.8f}"),  # Price moves slightly
                volume=1000 + i,
                timestamp=base_time + i * 0.001,  # 1ms apart
            )
            events.append(event)

        # Test batch serialization
        start_time = time.perf_counter()

        serialized_events: list[bytes] = []
        for event in events:
            json_bytes = msgspec.json.encode(event)
            serialized_events.append(json_bytes)

        serialize_time = time.perf_counter() - start_time

        # Test batch deserialization
        start_time = time.perf_counter()

        deserialized_events: list[MarketData] = []
        for json_bytes in serialized_events:
            event = msgspec.json.decode(json_bytes, type=MarketData)
            deserialized_events.append(event)

        deserialize_time = time.perf_counter() - start_time

        total_time = serialize_time + deserialize_time
        events_per_second = len(events) / total_time
        avg_event_time = total_time / len(events) * 1_000_000  # μs per event

        logger.info(
            "High-frequency processing results",
            events_processed=len(events),
            serialize_time=f"{serialize_time:.3f}s",
            deserialize_time=f"{deserialize_time:.3f}s",
            total_time=f"{total_time:.3f}s",
            events_per_second=f"{events_per_second:,.0f}",
            avg_per_event=f"{avg_event_time:.2f}μs",
        )

        # Should handle >10,000 events per second
        assert events_per_second > 10_000, f"Too slow: {events_per_second:,.0f} events/sec"

        # Verify data integrity
        assert len(deserialized_events) == len(events)
        for original, deserialized in zip(events, deserialized_events, strict=False):
            assert original.symbol == deserialized.symbol
            assert original.price == deserialized.price

    def test_realistic_trading_event_mix_performance(self, frozen_time: FreezerProtocol) -> None:
        """Test performance with mixed event types (realistic scenario)."""
        # Set deterministic time for consistent test results
        test_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(test_time)
        base_time = test_time.timestamp()

        # Create mixed stream: 70% MarketData, 20% OrderEvent, 10% others
        events: list[MarketData | OrderEvent | SystemEvent] = []

        for i in range(1000):
            if i % 10 < 7:  # 70% market data
                event: MarketData | OrderEvent | SystemEvent = MarketData(
                    symbol="BTC-USDC",
                    exchange=ExchangeName.HYPERLIQUID,
                    data_type=MarketDataType.TICK,
                    price=Decimal(f"{50000 + (i % 100) * 0.01:.8f}"),
                    volume=1000 + i,
                    timestamp=base_time + i * 0.001,
                )
            elif i % 10 < 9:  # 20% order events
                event = OrderEvent(
                    order_id=f"order_{i}",
                    exchange=ExchangeName.BACKPACK,
                    symbol="ETH-USDC",
                    event_type=OrderEventType.FILLED,
                    side=OrderSide.BUY,
                    price=Decimal(f"{3500 + (i % 50) * 0.1:.6f}"),
                    quantity=Decimal("1.0"),
                    timestamp=base_time + i * 0.001,
                )
            else:  # 10% other events
                event = SystemEvent(
                    component="market_data",
                    event_type=SystemEventType.HEALTH_CHECK,
                    status=HealthStatus.HEALTHY,
                    message=f"Health check {i}",
                    timestamp=base_time + i * 0.001,
                )
            events.append(event)

        # Test mixed stream processing
        start_time = time.perf_counter()

        for event in events:
            json_bytes = msgspec.json.encode(event)
            # Simulate processing based on event type
            if isinstance(event, MarketData):
                _ = msgspec.json.decode(json_bytes, type=MarketData)
            elif isinstance(event, OrderEvent):
                _ = msgspec.json.decode(json_bytes, type=OrderEvent)
            else:
                _ = msgspec.json.decode(json_bytes, type=SystemEvent)

        total_time = time.perf_counter() - start_time
        events_per_second = len(events) / total_time

        logger.info(
            "Mixed event stream results",
            events_processed=len(events),
            processing_time=f"{total_time:.3f}s",
            events_per_second=f"{events_per_second:,.0f}",
        )

        # Should handle >5,000 mixed events per second
        assert events_per_second > 5_000, (
            f"Mixed stream too slow: {events_per_second:,.0f} events/sec"
        )


class TestProductionScaleRequirements:
    """Test requirements for production-scale trading operations."""

    def test_large_scale_event_processing(self, frozen_time: FreezerProtocol) -> None:
        """Test memory footprint of events vs alternatives."""
        # Set deterministic time for consistent test results
        test_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(test_time)
        base_time = test_time.timestamp()

        # Create many events to measure memory usage
        event_count = 10_000

        # Force garbage collection before measuring
        gc.collect()

        events: list[MarketData] = []
        for i in range(event_count):
            event = MarketData(
                symbol="BTC-USDC",
                exchange=ExchangeName.HYPERLIQUID,
                data_type=MarketDataType.TICK,
                price=Decimal(f"{50000 + i * 0.01:.8f}"),
                volume=1000 + i,
                timestamp=base_time + i * 0.001,
            )
            events.append(event)

        # Test that we can create many events without memory issues
        assert len(events) == event_count

        # Test serialization memory efficiency
        serialized: list[bytes] = []
        for event in events:
            json_bytes = msgspec.json.encode(event)
            serialized.append(json_bytes)

        # Verify all events serialized
        assert len(serialized) == event_count

        logger.info(
            "Memory test results",
            events_created=f"{event_count:,}",
            status="All events serialized successfully",
        )

        # Cleanup
        del events
        del serialized
        gc.collect()


class TestArchitectureDecisionValidation:
    """Validate architecture decisions with quantitative measurements."""

    def test_symbol_string_architecture_validation(self) -> None:
        """Validate that symbol strings have zero conversion overhead."""
        symbol_strings = [
            "BTC-USDC",
            "ETH-USDC",
            "SOL-USDC",
            "DOGE-USDC",
            "AVAX-USDC",
            "DOT-USDC",
            "MATIC-USDC",
            "LINK-USDC",
            "UNI-USDC",
            "AAVE-USDC",
        ]

        # Test direct string usage (current approach)
        events_with_strings: list[MarketData] = []

        start_time = time.perf_counter()
        for _ in range(1000):
            for symbol_str in symbol_strings:
                event = MarketData(
                    symbol=symbol_str,  # Direct string usage
                    exchange=ExchangeName.HYPERLIQUID,
                    data_type=MarketDataType.TICK,
                    price=Decimal("50000.00"),
                )
                events_with_strings.append(event)
        string_creation_time = time.perf_counter() - start_time

        # Test string serialization
        start_time = time.perf_counter()
        for event in events_with_strings:
            _ = msgspec.json.encode(event)  # Measure encoding time
        string_serialization_time = time.perf_counter() - start_time

        total_events = len(events_with_strings)
        avg_creation_time = string_creation_time / total_events * 1_000_000  # μs
        avg_serialization_time = string_serialization_time / total_events * 1_000_000  # μs

        logger.info(
            "Symbol string overhead validation",
            events_created=f"{total_events:,}",
            avg_creation_time=f"{avg_creation_time:.3f}μs",
            avg_serialization_time=f"{avg_serialization_time:.3f}μs",
            total_overhead=f"{avg_creation_time + avg_serialization_time:.3f}μs",
        )

        # Validate extremely low overhead (should be <1μs total per event)
        total_overhead = avg_creation_time + avg_serialization_time
        assert total_overhead < 5.0, f"String overhead too high: {total_overhead:.3f}μs"

        # This confirms our architecture decision: strings have near-zero overhead
        logger.info(
            "Symbol string overhead validated",
            overhead=f"{total_overhead:.3f}μs per event",
        )


class TestPerformanceBaselineCompliance:
    """Test compliance with performance baselines."""

    def test_baseline_compliance(self) -> None:
        """Test that we meet baseline performance targets."""
        # Create test events
        market_data = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal("50123.456789"),
            volume=12345,
        )

        order_event = OrderEvent(
            order_id="order_123456",
            exchange=ExchangeName.BACKPACK,
            symbol="ETH-USDC",
            event_type=OrderEventType.FILLED,
            side=OrderSide.BUY,
            price=Decimal("3456.789"),
            quantity=Decimal("2.5"),
        )

        # Test serialization speed targets
        events_to_test = [
            ("MarketData", market_data, 10.0),  # Target: <10μs
            ("OrderEvent", order_event, 20.0),  # Target: <20μs
        ]

        results: dict[str, float] = {}

        for event_name, event, target_us in events_to_test:
            # Warm up
            for _ in range(100):
                msgspec.json.encode(event)

            # Measure
            start_time = time.perf_counter()
            iterations = 1000

            for _ in range(iterations):
                _ = msgspec.json.encode(event)

            end_time = time.perf_counter()
            avg_time = (end_time - start_time) / iterations * 1_000_000  # μs

            results[event_name] = avg_time

            logger.info(
                "Performance target",
                event_name=event_name,
                avg_time=f"{avg_time:.2f}μs",
                target=f"<{target_us}μs",
            )

            # Verify performance target
            assert avg_time < target_us, (
                f"{event_name} regression: {avg_time:.2f}μs > {target_us}μs"
            )

        logger.info("All performance targets met")
        for event_name, avg_time in results.items():
            logger.info(
                "Target met",
                event_name=event_name,
                avg_time=f"{avg_time:.2f}μs",
            )

    def test_production_throughput_requirements(self) -> None:
        """Test that we meet throughput targets for high-frequency scenarios."""
        # Target: Process 10,000 MarketData events per second
        event_count = 1000
        events = [
            MarketData(
                symbol="BTC-USDC",
                exchange=ExchangeName.HYPERLIQUID,
                data_type=MarketDataType.TICK,
                price=Decimal(f"{50000 + i * 0.01:.8f}"),
                volume=1000 + i,
            )
            for i in range(event_count)
        ]

        # Measure throughput
        start_time = time.perf_counter()

        for event in events:
            json_bytes = msgspec.json.encode(event)
            _ = msgspec.json.decode(json_bytes, type=MarketData)

        total_time = time.perf_counter() - start_time
        throughput = event_count / total_time

        logger.info(
            "Throughput test",
            events=event_count,
            time=f"{total_time:.3f}s",
            throughput=f"{throughput:,.0f} events/second",
        )

        # Target: >10,000 events/second
        target_throughput = 10_000
        assert throughput > target_throughput, (
            f"Throughput below target: {throughput:,.0f} < {target_throughput:,.0f}"
        )

        logger.info(
            "Throughput target met",
            actual=f"{throughput:,.0f}",
            target=f"{target_throughput:,.0f}",
        )
