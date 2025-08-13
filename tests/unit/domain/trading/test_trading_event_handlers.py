"""Unit tests for trading event handlers.

Tests for TradingOrderEventHandler and TradingPositionEventHandler with
proper mocking of dependencies and configuration-driven behavior.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from cyberdelta.config.models.event_system_config import (
    EventHandlerConfig,
    EventRetryConfig,
)
from cyberdelta.domain.trading.trading_event_handlers import (
    TradingOrderEventHandler,
    TradingPositionEventHandler,
)
from cyberdelta.enums import OrderEventType, OrderSide, PositionEventType
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.enums.event_bus import HandlerPriority
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.events.core import OrderEvent, PositionEvent


@pytest.fixture
def event_config() -> EventHandlerConfig:
    """Create test event handler configuration.

    Returns:
        EventHandlerConfig: Test configuration instance.
    """
    return EventHandlerConfig(
        retry_config=EventRetryConfig(
            max_attempts=2,
            initial_delay_sec=0.1,
            max_delay_sec=1.0,
            exponential_base=2.0,
            jitter=False,
        ),
        max_consecutive_errors=5,
        auto_degrade_after_errors=5,
        auto_fault_after_errors=10,
    )


@pytest.fixture
def mock_event_bus() -> AsyncMock:
    """Create mock event bus.

    Returns:
        AsyncMock: Mock event bus instance.
    """
    bus = AsyncMock()
    bus.subscribe = AsyncMock()
    bus.unsubscribe = AsyncMock()
    bus.publish = AsyncMock()
    return bus


@pytest.fixture
def mock_trading_service() -> AsyncMock:
    """Create mock trading service.

    Returns:
        AsyncMock: Mock trading service instance.
    """
    service = AsyncMock()
    service.get_active_orders = AsyncMock(return_value=[])
    service.update_order_status = AsyncMock()
    service.process_order_fill = AsyncMock()
    service.complete_order = AsyncMock()
    service.update_order_amendment = AsyncMock()
    service.track_new_position = AsyncMock()
    service.update_position = AsyncMock()
    service.close_position = AsyncMock()
    service.handle_liquidation = AsyncMock()
    return service


@pytest.fixture
def mock_symbol_service() -> Mock:
    """Create mock symbol service.

    Returns:
        Mock: Mock symbol service instance.
    """
    service = Mock()

    def create_symbol(symbol_str: str, exchange: ExchangeName) -> str:
        """Mock symbol creation - returns formatted string.

        Returns:
            str: Formatted symbol string.
        """
        return f"Symbol({symbol_str}@{exchange.value})"

    service.create_symbol = Mock(side_effect=create_symbol)
    return service


class TestTradingOrderEventHandler:
    """Tests for TradingOrderEventHandler."""

    @pytest.mark.asyncio
    async def test_handler_initialization(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test handler initialization."""
        handler = TradingOrderEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )

        assert handler.handler_id == "trading_order_handler"
        assert handler.state == ComponentState.PRE_INITIALIZED
        # Verify cache is empty via metrics instead of private access
        metrics = handler.get_metrics()
        assert metrics["cache_size"] == 0
        assert handler.config == event_config

    @pytest.mark.asyncio
    async def test_handler_start_subscribes_to_order_events(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test handler subscribes to order events on start."""
        handler = TradingOrderEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )

        await handler.start()

        # Verify subscription
        mock_event_bus.subscribe.assert_called_with(
            OrderEvent, handler.handle_event, priority=HandlerPriority.HIGH
        )
        assert handler.state == ComponentState.RUNNING

    @pytest.mark.asyncio
    async def test_order_placed_event_handling(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test handling of order placed event."""
        handler = TradingOrderEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )
        await handler.start()

        # Create order placed event
        event = OrderEvent(
            timestamp=1234567890,
            exchange=ExchangeName.HYPERLIQUID,
            symbol="BTC",
            order_id="order123",
            event_type=OrderEventType.PLACED,
            side=OrderSide.BUY,
            price=Decimal("50000.00"),
            quantity=Decimal("0.1"),
        )

        await handler.handle_event(event)

        # Verify symbol creation and caching
        mock_symbol_service.create_symbol.assert_called_once_with("BTC", ExchangeName.HYPERLIQUID)

        # Verify trading service call
        expected_symbol = "Symbol(BTC@hyperliquid)"
        mock_trading_service.update_order_status.assert_called_once_with(
            "order123", "placed", expected_symbol
        )

        # Verify symbol is cached by checking cache metrics
        metrics = handler.get_metrics()
        assert metrics["cache_size"] == 1

    @pytest.mark.asyncio
    async def test_order_fill_event_handling(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test handling of order fill event."""
        handler = TradingOrderEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )
        await handler.start()

        # Create order fill event
        event = OrderEvent(
            timestamp=1234567890,
            exchange=ExchangeName.BACKPACK,
            symbol="ETH",
            order_id="order456",
            event_type=OrderEventType.FILLED,
            side=OrderSide.SELL,
            fill_price=Decimal("3000.00"),
            fill_quantity=Decimal("1.0"),
            commission=Decimal("3.00"),
        )

        await handler.handle_event(event)

        # Verify trading service call
        expected_symbol = "Symbol(ETH@backpack)"
        mock_trading_service.process_order_fill.assert_called_once_with(
            "order456", Decimal("3000.00"), Decimal("1.0"), Decimal("3.00"), expected_symbol
        )

    @pytest.mark.asyncio
    async def test_order_cancellation_event_handling(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test handling of order cancellation event."""
        handler = TradingOrderEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )
        await handler.start()

        # Create order cancelled event
        event = OrderEvent(
            timestamp=1234567890,
            exchange=ExchangeName.HYPERLIQUID,
            symbol="SOL",
            order_id="order789",
            event_type=OrderEventType.CANCELLED,
            side=OrderSide.BUY,
            reason="User requested",
        )

        await handler.handle_event(event)

        # Verify trading service call
        expected_symbol = "Symbol(SOL@hyperliquid)"
        mock_trading_service.complete_order.assert_called_once_with(
            "order789", "cancelled", expected_symbol, error_code=None, reason="User requested"
        )

    @pytest.mark.asyncio
    async def test_degraded_mode_only_processes_cancellations(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test degraded mode only processes cancellation events."""
        handler = TradingOrderEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )
        await handler.start()

        # Force handler into degraded mode
        await handler.degrade()
        assert handler.state == ComponentState.DEGRADED

        # Try to process placed event - should be skipped
        placed_event = OrderEvent(
            timestamp=1234567890,
            exchange=ExchangeName.HYPERLIQUID,
            symbol="BTC",
            order_id="order1",
            event_type=OrderEventType.PLACED,
            side=OrderSide.BUY,
        )
        await handler.handle_event(placed_event)

        # Verify placed event was not processed
        mock_trading_service.update_order_status.assert_not_called()

        # Process cancelled event - should work
        cancelled_event = OrderEvent(
            timestamp=1234567890,
            exchange=ExchangeName.HYPERLIQUID,
            symbol="BTC",
            order_id="order2",
            event_type=OrderEventType.CANCELLED,
            side=OrderSide.SELL,
        )
        await handler.handle_event(cancelled_event)

        # Verify cancellation was processed
        mock_trading_service.complete_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_symbol_caching_improves_performance(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test symbol caching reduces symbol service calls."""
        handler = TradingOrderEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )
        await handler.start()

        # Process same symbol multiple times
        for i in range(3):
            event = OrderEvent(
                timestamp=1234567890,
                exchange=ExchangeName.HYPERLIQUID,
                symbol="BTC",
                order_id=f"order{i}",
                event_type=OrderEventType.PLACED,
                side=OrderSide.BUY,
            )
            await handler.handle_event(event)

        # Symbol service should only be called once due to caching
        mock_symbol_service.create_symbol.assert_called_once_with("BTC", ExchangeName.HYPERLIQUID)

        # Check cache metrics
        metrics = handler.get_metrics()
        assert metrics["cache_hits"] == 2
        assert metrics["cache_misses"] == 1

    @pytest.mark.asyncio
    async def test_cache_warming_on_startup(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test cache warming with active orders on startup."""
        # Set up active orders
        mock_trading_service.get_active_orders.return_value = [
            {"symbol": "BTC", "exchange": ExchangeName.HYPERLIQUID, "order_id": "active1"},
            {"symbol": "ETH", "exchange": ExchangeName.BACKPACK, "order_id": "active2"},
        ]

        handler = TradingOrderEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )

        await handler.start()

        # Verify cache warming occurred
        mock_trading_service.get_active_orders.assert_called_once()

        # Verify symbols were pre-cached by checking cache size
        metrics = handler.get_metrics()
        assert metrics["cache_size"] == 2


class TestTradingPositionEventHandler:
    """Tests for TradingPositionEventHandler."""

    @pytest.mark.asyncio
    async def test_position_opened_event_handling(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test handling of position opened event."""
        handler = TradingPositionEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )
        await handler.start()

        # Create position opened event
        event = PositionEvent(
            timestamp=1234567890,
            exchange=ExchangeName.HYPERLIQUID,
            symbol="BTC",
            position_id="pos123",
            event_type=PositionEventType.OPENED,
            size=Decimal("0.5"),
            average_price=Decimal("50000.00"),
        )

        await handler.handle_event(event)

        # Verify trading service call
        expected_symbol = "Symbol(BTC@hyperliquid)"
        mock_trading_service.track_new_position.assert_called_once_with(
            "pos123", expected_symbol, Decimal("0.5"), Decimal("50000.00")
        )

    @pytest.mark.asyncio
    async def test_position_updated_with_pnl_handling(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test handling of position update with PnL."""
        handler = TradingPositionEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )
        await handler.start()

        # Create position updated event with PnL
        event = PositionEvent(
            timestamp=1234567890,
            exchange=ExchangeName.BACKPACK,
            symbol="ETH",
            position_id="pos456",
            event_type=PositionEventType.UPDATED,
            size=Decimal("2.0"),
            average_price=Decimal("3000.00"),
            unrealized_pnl=Decimal("150.00"),
        )

        await handler.handle_event(event)

        # Verify trading service call
        expected_symbol = "Symbol(ETH@backpack)"
        mock_trading_service.update_position.assert_called_once_with(
            "pos456", expected_symbol, Decimal("2.0"), Decimal("3000.00"), Decimal("150.00")
        )

    @pytest.mark.asyncio
    async def test_position_closed_with_realized_pnl(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test handling of position closed with realized PnL."""
        handler = TradingPositionEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )
        await handler.start()

        # Create position closed event
        event = PositionEvent(
            timestamp=1234567890,
            exchange=ExchangeName.HYPERLIQUID,
            symbol="SOL",
            position_id="pos789",
            event_type=PositionEventType.CLOSED,
            size=Decimal(0),
            average_price=Decimal(0),
            close_price=Decimal("150.00"),
            realized_pnl=Decimal("500.00"),
        )

        await handler.handle_event(event)

        # Verify trading service call
        expected_symbol = "Symbol(SOL@hyperliquid)"
        mock_trading_service.close_position.assert_called_once_with(
            "pos789", expected_symbol, Decimal("150.00"), Decimal("500.00")
        )

    @pytest.mark.asyncio
    async def test_position_liquidation_handling(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test handling of position liquidation event."""
        handler = TradingPositionEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )
        await handler.start()

        # Create position liquidated event
        event = PositionEvent(
            timestamp=1234567890,
            exchange=ExchangeName.HYPERLIQUID,
            symbol="BTC",
            position_id="pos_liq",
            event_type=PositionEventType.LIQUIDATED,
            size=Decimal(0),
            average_price=Decimal(0),
            close_price=Decimal("45000.00"),
            realized_pnl=Decimal("-5000.00"),
        )

        await handler.handle_event(event)

        # Verify trading service call (critical event)
        expected_symbol = "Symbol(BTC@hyperliquid)"
        mock_trading_service.handle_liquidation.assert_called_once_with(
            "pos_liq", expected_symbol, Decimal("45000.00"), Decimal("-5000.00")
        )

    @pytest.mark.asyncio
    async def test_position_handler_subscribes_with_high_priority(
        self,
        mock_event_bus: AsyncMock,
        mock_trading_service: AsyncMock,
        mock_symbol_service: Mock,
        event_config: EventHandlerConfig,
    ) -> None:
        """Test position handler subscribes with HIGH priority."""
        handler = TradingPositionEventHandler(
            event_bus=mock_event_bus,
            trading_service=mock_trading_service,
            symbol_service=mock_symbol_service,
            config=event_config,
        )

        await handler.start()

        # Verify subscription with HIGH priority
        mock_event_bus.subscribe.assert_called_with(
            PositionEvent, handler.handle_event, priority=HandlerPriority.HIGH
        )
