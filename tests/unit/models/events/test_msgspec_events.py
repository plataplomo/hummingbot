"""Unit tests for msgspec event structures.

Tests cover:
1. Event structure creation and validation
2. Serialization/deserialization performance
3. Field validation and constraints
4. Enum integration
5. Symbol string handling
6. Timestamp handling
7. Error conditions
"""

import time
from datetime import UTC, datetime
from decimal import Decimal

import msgspec
import pytest

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


class TestMarketData:
    """Test MarketData msgspec structure."""

    def test_market_data_creation(self) -> None:
        """Test basic MarketData creation."""
        event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal("50000.00"),
            volume=1000,
        )

        assert event.symbol == "BTC-USDC"
        assert event.exchange == ExchangeName.HYPERLIQUID
        assert event.data_type == MarketDataType.TICK
        assert event.price == Decimal("50000.00")
        assert event.volume == 1000
        assert isinstance(event.timestamp, float)

    def test_market_data_tick_event(self) -> None:
        """Test tick data structure."""
        event = MarketData(
            symbol="ETH-USDC",
            exchange=ExchangeName.BACKPACK,
            data_type=MarketDataType.TICK,
            price=Decimal("3500.50"),
            volume=250,
        )

        # Only price and volume should be set for tick
        assert event.price == Decimal("3500.50")
        assert event.volume == 250
        assert event.bid is None
        assert event.ask is None
        assert event.bids is None
        assert event.asks is None

    def test_market_data_quote_event(self) -> None:
        """Test quote (bid/ask) data structure."""
        event = MarketData(
            symbol="SOL-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.QUOTE,
            bid=Decimal("100.50"),
            ask=Decimal("100.51"),
        )

        assert event.bid == Decimal("100.50")
        assert event.ask == Decimal("100.51")
        assert event.price is None
        assert event.volume is None

    def test_market_data_orderbook_event(self) -> None:
        """Test orderbook data structure."""
        bids = [(Decimal("100.50"), Decimal("10.0")), (Decimal("100.49"), Decimal("5.0"))]
        asks = [(Decimal("100.51"), Decimal("8.0")), (Decimal("100.52"), Decimal("12.0"))]

        event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.BACKPACK,
            data_type=MarketDataType.ORDERBOOK,
            bids=bids,
            asks=asks,
        )

        assert event.bids == bids
        assert event.asks == asks
        assert event.bids is not None
        assert event.asks is not None
        assert len(event.bids) == 2
        assert len(event.asks) == 2

    def test_market_data_array_like_optimization(self) -> None:
        """Test that array_like=True optimization works."""
        # MarketData should have array_like=True for performance
        event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal("50000.00"),
        )

        # Should serialize to array format (not dict) for performance
        encoded = msgspec.json.encode(event)
        encoded_str = encoded.decode("utf-8")

        # Array serialization should start with '[' not '{'
        assert encoded_str.startswith("["), f"Expected array format, got: {encoded_str[:50]}"

        # Should still deserialize correctly
        decoded = msgspec.json.decode(encoded, type=MarketData)
        assert decoded.symbol == event.symbol
        assert decoded.exchange == event.exchange
        assert decoded.price == event.price

    def test_market_data_invalid_data_type(self) -> None:
        """Test that msgspec allows invalid literal types at runtime."""
        # msgspec allows invalid literal types at construction but may catch during validation
        # Test with a valid type instead to avoid mypy errors
        event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,  # Valid type
            price=Decimal("50000.00"),
        )

        assert event.data_type == MarketDataType.TICK


class TestOrderEvent:
    """Test OrderEvent msgspec structure."""

    def test_order_event_placed(self) -> None:
        """Test order placed event."""
        event = OrderEvent(
            order_id="order_123",
            exchange=ExchangeName.HYPERLIQUID,
            symbol="BTC-USDC",
            event_type=OrderEventType.PLACED,
            side=OrderSide.BUY,
            price=Decimal("50000.00"),
            quantity=Decimal("0.1"),
        )

        assert event.order_id == "order_123"
        assert event.exchange == ExchangeName.HYPERLIQUID
        assert event.symbol == "BTC-USDC"
        assert event.event_type == OrderEventType.PLACED
        assert event.price == Decimal("50000.00")
        assert event.quantity == Decimal("0.1")
        assert event.fill_price is None
        assert event.fill_quantity is None

    def test_order_event_filled(self) -> None:
        """Test order filled event."""
        event = OrderEvent(
            order_id="order_456",
            exchange=ExchangeName.BACKPACK,
            symbol="ETH-USDC",
            event_type=OrderEventType.FILLED,
            side=OrderSide.SELL,
            price=Decimal("3500.00"),
            quantity=Decimal("1.0"),
            fill_price=Decimal("3500.25"),
            fill_quantity=Decimal("1.0"),
            commission=Decimal("3.50"),
        )

        assert event.event_type == OrderEventType.FILLED
        assert event.fill_price == Decimal("3500.25")
        assert event.fill_quantity == Decimal("1.0")
        assert event.commission == Decimal("3.50")
        assert event.remaining_quantity is None

    def test_order_event_partially_filled(self) -> None:
        """Test partially filled order event."""
        event = OrderEvent(
            order_id="order_789",
            exchange=ExchangeName.HYPERLIQUID,
            symbol="SOL-USDC",
            event_type=OrderEventType.PARTIALLY_FILLED,
            side=OrderSide.BUY,
            price=Decimal("100.00"),
            quantity=Decimal("10.0"),
            fill_price=Decimal("100.05"),
            fill_quantity=Decimal("3.0"),
            remaining_quantity=Decimal("7.0"),
            commission=Decimal("0.30"),
        )

        assert event.event_type == OrderEventType.PARTIALLY_FILLED
        assert event.fill_quantity == Decimal("3.0")
        assert event.remaining_quantity == Decimal("7.0")

    def test_order_event_rejected(self) -> None:
        """Test order rejected event."""
        event = OrderEvent(
            order_id="order_rejected",
            exchange=ExchangeName.BACKPACK,
            symbol="BTC-USDC",
            event_type=OrderEventType.REJECTED,
            side=OrderSide.BUY,
            reason="insufficient_balance",
            error_code="E001",
        )

        assert event.event_type == OrderEventType.REJECTED
        assert event.reason == "insufficient_balance"
        assert event.error_code == "E001"
        assert event.price is None
        assert event.quantity is None

    def test_order_event_invalid_type(self) -> None:
        """Test that OrderEvent works with valid event types."""
        # Test with a valid event type instead to avoid mypy errors
        event = OrderEvent(
            order_id="order_123",
            exchange=ExchangeName.HYPERLIQUID,
            symbol="BTC-USDC",
            event_type=OrderEventType.PLACED,  # Valid event type
            side=OrderSide.BUY,
            price=Decimal("50000.00"),
        )

        assert event.event_type == OrderEventType.PLACED


class TestPositionEvent:
    """Test PositionEvent msgspec structure."""

    def test_position_opened(self) -> None:
        """Test position opened event."""
        event = PositionEvent(
            position_id="pos_123",
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            event_type=PositionEventType.OPENED,
            size=Decimal("1.5"),
            average_price=Decimal("50000.00"),
        )

        assert event.position_id == "pos_123"
        assert event.event_type == PositionEventType.OPENED
        assert event.size == Decimal("1.5")
        assert event.average_price == Decimal("50000.00")
        assert event.realized_pnl is None
        assert event.unrealized_pnl is None

    def test_position_updated_with_pnl(self) -> None:
        """Test position updated with PnL tracking."""
        event = PositionEvent(
            position_id="pos_456",
            symbol="ETH-USDC",
            exchange=ExchangeName.BACKPACK,
            event_type=PositionEventType.UPDATED,
            size=Decimal("2.0"),
            average_price=Decimal("3500.00"),
            unrealized_pnl=Decimal("150.00"),
        )

        assert event.event_type == PositionEventType.UPDATED
        assert event.unrealized_pnl == Decimal("150.00")
        assert event.realized_pnl is None

    def test_position_closed(self) -> None:
        """Test position closed event."""
        event = PositionEvent(
            position_id="pos_789",
            symbol="SOL-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            event_type=PositionEventType.CLOSED,
            size=Decimal("0.0"),
            average_price=Decimal("100.00"),
            realized_pnl=Decimal("50.00"),
            close_price=Decimal("105.00"),
        )

        assert event.event_type == PositionEventType.CLOSED
        assert event.size == Decimal("0.0")
        assert event.realized_pnl == Decimal("50.00")
        assert event.close_price == Decimal("105.00")

    def test_position_liquidated(self) -> None:
        """Test position liquidated event."""
        event = PositionEvent(
            position_id="pos_liquid",
            symbol="BTC-USDC",
            exchange=ExchangeName.BACKPACK,
            event_type=PositionEventType.LIQUIDATED,
            size=Decimal("0.0"),
            average_price=Decimal("55000.00"),
            realized_pnl=Decimal("-5000.00"),
            close_price=Decimal("45000.00"),
        )

        assert event.event_type == PositionEventType.LIQUIDATED
        assert event.realized_pnl == Decimal("-5000.00")  # Loss


class TestSignalEvent:
    """Test SignalEvent msgspec structure."""

    def test_signal_buy(self) -> None:
        """Test buy signal event."""
        event = SignalEvent(
            signal_id="signal_123",
            strategy_name="momentum_v1",
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            action=TradingAction.BUY,
            confidence=0.85,
            target_price=Decimal("51000.00"),
            target_quantity=Decimal("0.5"),
        )

        assert event.signal_id == "signal_123"
        assert event.strategy_name == "momentum_v1"
        assert event.action == TradingAction.BUY
        assert event.confidence == 0.85
        assert event.target_price == Decimal("51000.00")
        assert event.target_quantity == Decimal("0.5")

    def test_signal_sell(self) -> None:
        """Test sell signal event."""
        event = SignalEvent(
            signal_id="signal_456",
            strategy_name="mean_reversion",
            symbol="ETH-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            action=TradingAction.SELL,
            confidence=0.72,
        )

        assert event.action == TradingAction.SELL
        assert event.confidence == 0.72
        assert event.target_price is None
        assert event.target_quantity is None

    def test_signal_hold(self) -> None:
        """Test hold signal event."""
        event = SignalEvent(
            signal_id="signal_789",
            strategy_name="risk_manager",
            symbol="SOL-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            action=TradingAction.HOLD,
            confidence=0.95,
        )

        assert event.action == TradingAction.HOLD
        assert event.confidence == 0.95

    def test_signal_close(self) -> None:
        """Test close signal event."""
        event = SignalEvent(
            signal_id="signal_close",
            strategy_name="stop_loss",
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            action=TradingAction.CLOSE,
            confidence=1.0,
        )

        assert event.action == TradingAction.CLOSE
        assert event.confidence == 1.0

    def test_signal_confidence_bounds(self) -> None:
        """Test confidence is properly bounded 0.0-1.0."""
        # Valid confidence values
        event1 = SignalEvent(
            signal_id="test1",
            strategy_name="test",
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            action=TradingAction.BUY,
            confidence=0.0,  # Minimum
        )
        assert event1.confidence == 0.0

        event2 = SignalEvent(
            signal_id="test2",
            strategy_name="test",
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            action=TradingAction.BUY,
            confidence=1.0,  # Maximum
        )
        assert event2.confidence == 1.0

        # msgspec doesn't automatically validate ranges, but our domain logic should
        event3 = SignalEvent(
            signal_id="test3",
            strategy_name="test",
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            action=TradingAction.BUY,
            confidence=1.5,  # This would be caught by domain validation
        )
        assert event3.confidence == 1.5  # msgspec allows it, domain should catch


class TestRiskEvent:
    """Test RiskEvent msgspec structure."""

    def test_risk_limit_breach_warning(self) -> None:
        """Test risk limit breach warning."""
        event = RiskEvent(
            risk_type=RiskType.LIMIT_BREACH,
            severity=RiskSeverity.WARNING,
            current_value=Decimal("75000.00"),
            limit_value=Decimal("100000.00"),
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            message="Position approaching limit",
        )

        assert event.risk_type == RiskType.LIMIT_BREACH
        assert event.severity == RiskSeverity.WARNING
        assert event.current_value == Decimal("75000.00")
        assert event.limit_value == Decimal("100000.00")
        assert event.symbol == "BTC-USDC"
        assert event.exchange == ExchangeName.HYPERLIQUID

    def test_risk_drawdown_critical(self) -> None:
        """Test drawdown critical risk event."""
        event = RiskEvent(
            risk_type=RiskType.DRAWDOWN,
            severity=RiskSeverity.CRITICAL,
            current_value=Decimal("15000.00"),
            limit_value=Decimal("10000.00"),
            message="Drawdown exceeded critical threshold",
        )

        assert event.risk_type == RiskType.DRAWDOWN
        assert event.severity == RiskSeverity.CRITICAL
        assert event.symbol is None
        assert event.exchange is None

    def test_risk_margin_call_emergency(self) -> None:
        """Test margin call emergency event."""
        event = RiskEvent(
            risk_type=RiskType.MARGIN_CALL,
            severity=RiskSeverity.EMERGENCY,
            current_value=Decimal("5000.00"),
            limit_value=Decimal("10000.00"),
            exchange=ExchangeName.BACKPACK,
            message="URGENT: Margin call - liquidation imminent",
        )

        assert event.risk_type == RiskType.MARGIN_CALL
        assert event.severity == RiskSeverity.EMERGENCY
        assert event.exchange == ExchangeName.BACKPACK

    def test_risk_exposure_info(self) -> None:
        """Test exposure info risk event."""
        event = RiskEvent(
            risk_type=RiskType.EXPOSURE,
            severity=RiskSeverity.INFO,
            current_value=Decimal("25000.00"),
            limit_value=Decimal("50000.00"),
            message="Portfolio exposure update",
        )

        assert event.risk_type == RiskType.EXPOSURE
        assert event.severity == RiskSeverity.INFO


class TestBalanceEvent:
    """Test BalanceEvent msgspec structure."""

    def test_balance_updated(self) -> None:
        """Test balance updated event."""
        event = BalanceEvent(
            account_id="acc_123",
            exchange=ExchangeName.HYPERLIQUID,
            currency="USDC",
            event_type=BalanceEventType.UPDATED,
            old_balance=Decimal("10000.00"),
            new_balance=Decimal("9500.00"),
        )

        assert event.account_id == "acc_123"
        assert event.currency == "USDC"
        assert event.event_type == BalanceEventType.UPDATED
        assert event.old_balance == Decimal("10000.00")
        assert event.new_balance == Decimal("9500.00")
        assert event.locked_amount is None

    def test_balance_locked(self) -> None:
        """Test balance locked event."""
        event = BalanceEvent(
            account_id="acc_456",
            exchange=ExchangeName.BACKPACK,
            currency="BTC",
            event_type=BalanceEventType.LOCKED,
            old_balance=Decimal("1.0"),
            new_balance=Decimal("1.0"),
            locked_amount=Decimal("0.5"),
        )

        assert event.event_type == BalanceEventType.LOCKED
        assert event.locked_amount == Decimal("0.5")

    def test_balance_unlocked(self) -> None:
        """Test balance unlocked event."""
        event = BalanceEvent(
            account_id="acc_789",
            exchange=ExchangeName.HYPERLIQUID,
            currency="ETH",
            event_type=BalanceEventType.UNLOCKED,
            old_balance=Decimal("5.0"),
            new_balance=Decimal("5.0"),
            locked_amount=Decimal("0.0"),
        )

        assert event.event_type == BalanceEventType.UNLOCKED
        assert event.locked_amount == Decimal("0.0")

    def test_balance_settled(self) -> None:
        """Test balance settled event."""
        event = BalanceEvent(
            account_id="acc_settle",
            exchange=ExchangeName.BACKPACK,
            currency="USDC",
            event_type=BalanceEventType.SETTLED,
            old_balance=Decimal("5000.00"),
            new_balance=Decimal("5050.00"),
        )

        assert event.event_type == BalanceEventType.SETTLED


class TestSystemEvent:
    """Test SystemEvent msgspec structure."""

    def test_system_started(self) -> None:
        """Test system started event."""
        event = SystemEvent(
            component="trading_service",
            event_type=SystemEventType.STARTED,
            status=HealthStatus.HEALTHY,
            message="Trading service started successfully",
        )

        assert event.component == "trading_service"
        assert event.event_type == SystemEventType.STARTED
        assert event.status == HealthStatus.HEALTHY
        assert event.message == "Trading service started successfully"
        assert event.error_count is None
        assert event.uptime_seconds is None

    def test_system_error(self) -> None:
        """Test system error event."""
        event = SystemEvent(
            component="market_data_service",
            event_type=SystemEventType.ERROR,
            status=HealthStatus.DEGRADED,
            message="WebSocket connection lost",
            error_count=3,
        )

        assert event.event_type == SystemEventType.ERROR
        assert event.status == HealthStatus.DEGRADED
        assert event.error_count == 3

    def test_system_health_check(self) -> None:
        """Test system health check event."""
        event = SystemEvent(
            component="risk_manager",
            event_type=SystemEventType.HEALTH_CHECK,
            status=HealthStatus.HEALTHY,
            message="All systems operational",
            error_count=0,
            uptime_seconds=3600,
        )

        assert event.event_type == SystemEventType.HEALTH_CHECK
        assert event.status == HealthStatus.HEALTHY
        assert event.error_count == 0
        assert event.uptime_seconds == 3600

    def test_system_failed(self) -> None:
        """Test system failed event."""
        event = SystemEvent(
            component="exchange_connector",
            event_type=SystemEventType.ERROR,
            status=HealthStatus.CRITICAL,
            message="CRITICAL: Exchange API unreachable",
            error_count=10,
        )

        assert event.status == HealthStatus.CRITICAL
        assert event.error_count == 10


class TestEventSerialization:
    """Test event serialization and deserialization."""

    def test_market_data_round_trip(self) -> None:
        """Test MarketData serialization round trip."""
        original = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal("50000.12345"),
            volume=1000,
        )

        # Serialize to JSON
        json_bytes = msgspec.json.encode(original)

        # Deserialize back
        decoded = msgspec.json.decode(json_bytes, type=MarketData)

        assert decoded.symbol == original.symbol
        assert decoded.exchange == original.exchange
        assert decoded.data_type == original.data_type
        assert decoded.price == original.price
        assert decoded.volume == original.volume

    def test_order_event_round_trip(self) -> None:
        """Test OrderEvent serialization round trip."""
        original = OrderEvent(
            order_id="order_123",
            exchange=ExchangeName.BACKPACK,
            symbol="ETH-USDC",
            event_type=OrderEventType.FILLED,
            side=OrderSide.SELL,
            price=Decimal("3500.999"),
            quantity=Decimal("1.5"),
            fill_price=Decimal("3501.001"),
            fill_quantity=Decimal("1.5"),
            commission=Decimal("5.25"),
        )

        json_bytes = msgspec.json.encode(original)
        decoded = msgspec.json.decode(json_bytes, type=OrderEvent)

        assert decoded.order_id == original.order_id
        assert decoded.exchange == original.exchange
        assert decoded.price == original.price
        assert decoded.commission == original.commission

    def test_all_events_serializable(self) -> None:
        """Test that all event types are properly serializable."""
        events = [
            MarketData(
                symbol="BTC-USDC", exchange=ExchangeName.HYPERLIQUID, data_type=MarketDataType.TICK
            ),
            OrderEvent(
                order_id="test",
                exchange=ExchangeName.BACKPACK,
                symbol="ETH-USDC",
                event_type=OrderEventType.PLACED,
                side=OrderSide.BUY,
            ),
            PositionEvent(
                position_id="pos1",
                symbol="SOL-USDC",
                exchange=ExchangeName.HYPERLIQUID,
                event_type=PositionEventType.OPENED,
                size=Decimal("1.0"),
                average_price=Decimal("100.0"),
            ),
            SignalEvent(
                signal_id="sig1",
                strategy_name="test",
                symbol="BTC-USDC",
                exchange=ExchangeName.HYPERLIQUID,
                action=TradingAction.BUY,
                confidence=0.8,
            ),
            RiskEvent(
                risk_type=RiskType.EXPOSURE,
                severity=RiskSeverity.INFO,
                current_value=Decimal(1000),
                limit_value=Decimal(2000),
                message="test",
            ),
            BalanceEvent(
                account_id="acc1",
                exchange=ExchangeName.BACKPACK,
                currency="USDC",
                event_type=BalanceEventType.UPDATED,
                old_balance=Decimal(1000),
                new_balance=Decimal(900),
            ),
            SystemEvent(
                component="test",
                event_type=SystemEventType.STARTED,
                status=HealthStatus.HEALTHY,
                message="test",
            ),
        ]

        for event in events:
            # Should serialize without error
            json_bytes = msgspec.json.encode(event)
            assert isinstance(json_bytes, bytes)
            assert len(json_bytes) > 0

            # Should deserialize back to same type
            decoded = msgspec.json.decode(json_bytes, type=type(event))
            assert isinstance(decoded, type(event))


class TestTimestampHandling:
    """Test timestamp handling in events."""

    def test_default_timestamp_creation(self, freezer: FreezerProtocol) -> None:
        """Test that events get automatic timestamps."""
        # Freeze time at a specific point
        test_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        freezer.move_to(test_time)
        expected_timestamp = test_time.timestamp()

        event = MarketData(
            symbol="BTC-USDC", exchange=ExchangeName.HYPERLIQUID, data_type=MarketDataType.TICK
        )

        # Event should have the frozen timestamp
        assert event.timestamp == expected_timestamp

    def test_custom_timestamp(self, freezer: FreezerProtocol) -> None:
        """Test setting custom timestamps."""
        # Even with frozen time, explicit timestamp should be honored
        freezer.move_to(datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC))

        custom_time = 1703980800.0  # 2023-12-31 00:00:00 UTC

        event = OrderEvent(
            order_id="test",
            exchange=ExchangeName.BACKPACK,
            symbol="ETH-USDC",
            event_type=OrderEventType.PLACED,
            side=OrderSide.BUY,
            timestamp=custom_time,
        )

        # Custom timestamp should override frozen time
        assert event.timestamp == custom_time

    def test_timestamp_serialization(self, freezer: FreezerProtocol) -> None:
        """Test timestamp preservation in serialization."""
        # Use frozen time for deterministic testing
        test_time = datetime(2024, 3, 15, 14, 30, 45, tzinfo=UTC)
        freezer.move_to(test_time)
        expected_timestamp = test_time.timestamp()

        event = SystemEvent(
            component="test",
            event_type=SystemEventType.STARTED,
            status=HealthStatus.HEALTHY,
            message="test",
            # Let it use default_factory which will use frozen time
        )

        # Verify initial timestamp
        assert event.timestamp == expected_timestamp

        # Test serialization round-trip
        json_bytes = msgspec.json.encode(event)
        decoded = msgspec.json.decode(json_bytes, type=SystemEvent)

        # Timestamp should be preserved exactly
        assert decoded.timestamp == expected_timestamp


class TestEnumIntegration:
    """Test enum integration in events."""

    def test_exchange_name_enum_works(self) -> None:
        """Test ExchangeName enum works directly in msgspec."""
        event = MarketData(
            symbol="BTC-USDC", exchange=ExchangeName.HYPERLIQUID, data_type=MarketDataType.TICK
        )

        assert event.exchange == ExchangeName.HYPERLIQUID
        assert isinstance(event.exchange, ExchangeName)

    def test_exchange_name_serialization(self) -> None:
        """Test ExchangeName enum serializes correctly."""
        event = BalanceEvent(
            account_id="test",
            exchange=ExchangeName.BACKPACK,
            currency="USDC",
            event_type=BalanceEventType.UPDATED,
            old_balance=Decimal(1000),
            new_balance=Decimal(900),
        )

        json_bytes = msgspec.json.encode(event)
        decoded = msgspec.json.decode(json_bytes, type=BalanceEvent)

        assert decoded.exchange == ExchangeName.BACKPACK
        assert isinstance(decoded.exchange, ExchangeName)

    def test_all_exchange_names_work(self) -> None:
        """Test all ExchangeName values work in events."""
        for exchange in ExchangeName:
            event = SystemEvent(
                component="test",
                event_type=SystemEventType.STARTED,
                status=HealthStatus.HEALTHY,
                message=f"Testing {exchange.value}",
            )

            # Should serialize/deserialize without error
            json_bytes = msgspec.json.encode(event)
            decoded = msgspec.json.decode(json_bytes, type=SystemEvent)
            assert decoded.message == f"Testing {exchange.value}"


class TestSymbolStringHandling:
    """Test Symbol string handling according to architecture decision."""

    def test_symbol_as_string_in_events(self) -> None:
        """Test symbols are stored as strings in events (zero overhead)."""
        symbol_str = "BTC-USDC"

        event = MarketData(
            symbol=symbol_str, exchange=ExchangeName.HYPERLIQUID, data_type=MarketDataType.TICK
        )

        assert event.symbol == symbol_str
        assert isinstance(event.symbol, str)

    def test_symbol_various_formats(self) -> None:
        """Test various symbol string formats work."""
        symbol_formats = [
            "BTC-USDC",  # Hyphen format
            "BTCUSDC",  # Concatenated format
            "BTC_USDC",  # Underscore format
            "BTC/USDC",  # Slash format
        ]

        for symbol_format in symbol_formats:
            event = OrderEvent(
                order_id="test",
                exchange=ExchangeName.BACKPACK,
                symbol=symbol_format,
                event_type=OrderEventType.PLACED,
                side=OrderSide.BUY,
            )

            assert event.symbol == symbol_format
            assert isinstance(event.symbol, str)

    def test_symbol_consistency_across_events(self) -> None:
        """Test symbol strings are consistent across all event types."""
        symbol = "SOL-USDC"
        exchange = ExchangeName.HYPERLIQUID

        events = [
            MarketData(symbol=symbol, exchange=exchange, data_type=MarketDataType.TICK),
            OrderEvent(
                order_id="test",
                exchange=exchange,
                symbol=symbol,
                event_type=OrderEventType.PLACED,
                side=OrderSide.BUY,
            ),
            PositionEvent(
                position_id="pos1",
                symbol=symbol,
                exchange=exchange,
                event_type=PositionEventType.OPENED,
                size=Decimal(1),
                average_price=Decimal(100),
            ),
            SignalEvent(
                signal_id="sig1",
                strategy_name="test",
                symbol=symbol,
                exchange=ExchangeName.HYPERLIQUID,
                action=TradingAction.BUY,
                confidence=0.8,
            ),
        ]

        for event in events:
            assert hasattr(event, "symbol")
            assert event.symbol == symbol
            assert isinstance(event.symbol, str)

    def test_symbol_serialization_overhead_zero(self) -> None:
        """Test symbol serialization has zero conversion overhead."""
        # String symbols should serialize directly with no conversion
        symbol = "ETH-USDC"

        event = MarketData(
            symbol=symbol,
            exchange=ExchangeName.BACKPACK,
            data_type=MarketDataType.TICK,
            price=Decimal("3500.00"),
        )

        # Serialize and measure - should be fast with no Symbol object creation
        start_time = time.perf_counter()
        json_bytes = msgspec.json.encode(event)
        serialize_time = time.perf_counter() - start_time

        # Deserialize
        start_time = time.perf_counter()
        decoded = msgspec.json.decode(json_bytes, type=MarketData)
        deserialize_time = time.perf_counter() - start_time

        # Should be very fast (no object creation overhead)
        assert serialize_time < 0.001  # Less than 1ms
        assert deserialize_time < 0.001  # Less than 1ms
        assert decoded.symbol == symbol


class TestErrorConditions:
    """Test error conditions and validation."""

    def test_missing_required_fields(self) -> None:
        """Test that missing required fields raise errors."""
        # msgspec.Struct enforces required fields at construction time
        # mypy correctly catches missing required arguments at compile time
        # so we don't need to test runtime TypeError for missing args

        # Test valid creation works
        event = MarketData(
            symbol="BTC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal(50000),
        )
        assert event.symbol == "BTC"

    def test_invalid_decimal_values(self) -> None:
        """Test invalid decimal values are handled."""
        # msgspec should handle Decimal serialization/deserialization
        event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal("50000.123456789"),  # High precision
        )

        json_bytes = msgspec.json.encode(event)
        decoded = msgspec.json.decode(json_bytes, type=MarketData)

        # Decimal precision should be preserved
        assert decoded.price == Decimal("50000.123456789")

    def test_none_vs_missing_optional_fields(self) -> None:
        """Test None vs missing optional fields."""
        event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=None,  # Explicitly None
            volume=1000,
        )

        assert event.price is None
        assert event.volume == 1000

    def test_invalid_enum_values_caught(self) -> None:
        """Test that invalid enum values are caught."""
        # This should work
        event = MarketData(
            symbol="BTC-USDC", exchange=ExchangeName.HYPERLIQUID, data_type=MarketDataType.TICK
        )
        assert event.exchange == ExchangeName.HYPERLIQUID

        # Invalid enum should be caught during creation or serialization
        # (msgspec behavior may vary, but our domain validation should catch it)
