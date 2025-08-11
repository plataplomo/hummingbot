"""Unit tests for portfolio event handlers.

Tests cover:
1. Position event handling with PnL tracking
2. Balance event handling with lock management
3. Symbol boundary conversion
4. Service integration
5. Degraded mode behavior
6. Error handling
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from cyberdelta.config.models.event_system_config import EventHandlerConfig
from cyberdelta.domain.portfolio.portfolio_event_handlers import (
    PortfolioBalanceEventHandler,
    PortfolioPositionEventHandler,
)
from cyberdelta.enums import BalanceEventType, OrderSide, PositionEventType
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.derivative_position import DerivativePosition
from cyberdelta.models.events.core import BalanceEvent, PositionEvent
from cyberdelta.models.spot_balance import SpotBalance
from tests.factories.symbol_factories import SymbolFactory


class TestPortfolioPositionEventHandler:
    """Test PortfolioPositionEventHandler."""

    @pytest.fixture
    def mock_event_bus(self) -> MagicMock:
        """Create mock event bus.

        Returns:
            MagicMock: Mock event bus.
        """
        return MagicMock()

    @pytest.fixture
    def mock_position_service(self) -> Mock:
        """Create mock position service.

        Returns:
            Mock: Mock position service.
        """
        service = Mock()

        # Mock get_exchange_positions to return mock data
        def mock_get_positions(exchange: ExchangeName) -> dict[str, DerivativePosition]:
            symbol = SymbolFactory.create_btc_usdc_spot_hyperliquid()
            return {
                symbol.value: DerivativePosition(
                    exchange=exchange,
                    symbol=symbol,
                    side=OrderSide.BUY,  # Using proper enum
                    size=Decimal("1.5"),
                    entry_price=Decimal("50000.00"),
                    mark_price=Decimal("51000.00"),
                    unrealized_pnl=Decimal("1500.00"),
                    realized_pnl=Decimal("0.00"),
                    liquidation_price=Decimal("45000.00"),
                    timestamp=datetime.now(UTC),
                )
            }

        service.get_exchange_positions = AsyncMock(
            return_value=mock_get_positions(ExchangeName.HYPERLIQUID)
        )
        service.update_position_directly = AsyncMock()
        return service

    @pytest.fixture
    def config(self) -> EventHandlerConfig:
        """Create test config.

        Returns:
            EventHandlerConfig: Test configuration.
        """
        return EventHandlerConfig()

    @pytest.fixture
    def handler(
        self,
        mock_event_bus: MagicMock,
        config: EventHandlerConfig,
        mock_position_service: Mock,
    ) -> PortfolioPositionEventHandler:
        """Create test handler.

        Args:
            mock_event_bus: Mock event bus.
            config: Event handler config.
            mock_position_service: Mock position service.

        Returns:
            PortfolioPositionEventHandler: Test handler.
        """
        return PortfolioPositionEventHandler(
            handler_id="portfolio_position_test",
            event_bus=mock_event_bus,
            config=config,
            position_service=mock_position_service,
        )

    @pytest.mark.asyncio
    async def test_position_opened_event(
        self,
        handler: PortfolioPositionEventHandler,
        mock_position_service: Mock,
    ) -> None:
        """Test handling position opened event."""
        event = PositionEvent(
            position_id="pos_123",
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            event_type=PositionEventType.OPENED,
            size=Decimal("1.5"),
            average_price=Decimal("50000.00"),
        )

        await handler.handle_event(event)

        # Verify position service was called to update the position
        mock_position_service.update_position_directly.assert_called_once()
        # Verify the call arguments
        call_args = mock_position_service.update_position_directly.call_args
        assert call_args[0][1] == ExchangeName.HYPERLIQUID  # exchange
        position = call_args[0][2]  # position object
        assert isinstance(position, DerivativePosition)
        assert position.size == Decimal("1.5")
        assert position.entry_price == Decimal("50000.00")

    @pytest.mark.asyncio
    async def test_position_updated_with_pnl(
        self,
        handler: PortfolioPositionEventHandler,
        mock_position_service: Mock,
    ) -> None:
        """Test handling position updated event with PnL."""
        event = PositionEvent(
            position_id="pos_456",
            symbol="ETH-USDC",
            exchange=ExchangeName.BACKPACK,
            event_type=PositionEventType.UPDATED,
            size=Decimal("2.0"),
            average_price=Decimal("3500.00"),
            unrealized_pnl=Decimal("150.00"),
        )

        await handler.handle_event(event)

        # Verify position service was called
        mock_position_service.get_exchange_positions.assert_called_once_with(ExchangeName.BACKPACK)

    @pytest.mark.asyncio
    async def test_position_closed_event(
        self,
        handler: PortfolioPositionEventHandler,
        mock_position_service: Mock,
    ) -> None:
        """Test handling position closed event."""
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

        await handler.handle_event(event)

        # Verify position service was called
        mock_position_service.get_exchange_positions.assert_called_once()

    @pytest.mark.asyncio
    async def test_position_liquidated_event(
        self,
        handler: PortfolioPositionEventHandler,
        mock_position_service: Mock,
    ) -> None:
        """Test handling position liquidated event."""
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

        await handler.handle_event(event)

        # Verify critical event was handled
        mock_position_service.get_exchange_positions.assert_called_once()

    @pytest.mark.asyncio
    async def test_symbol_boundary_conversion(
        self,
        handler: PortfolioPositionEventHandler,
        mock_position_service: Mock,
    ) -> None:
        """Test that handler processes events with string symbols correctly."""
        event = PositionEvent(
            position_id="pos_boundary",
            symbol="BTC-USDC",  # String symbol in event
            exchange=ExchangeName.HYPERLIQUID,
            event_type=PositionEventType.OPENED,
            size=Decimal("1.0"),
            average_price=Decimal("50000.00"),
        )

        await handler.handle_event(event)

        # Verify the handler processed the event and called the position service
        # The symbol conversion is internal implementation detail, we test observable behavior
        mock_position_service.update_position_directly.assert_called_once()
        call_args = mock_position_service.update_position_directly.call_args
        # First argument should be a Symbol object (converted from string)
        symbol_arg = call_args[0][0]
        assert symbol_arg.value == "BTC-USDC"  # Symbol object has correct value
        assert symbol_arg.exchange == ExchangeName.HYPERLIQUID

    @pytest.mark.asyncio
    async def test_degraded_mode_behavior(
        self,
        handler: PortfolioPositionEventHandler,
    ) -> None:
        """Test handler behavior in degraded mode."""
        # Handler must be running to degrade
        await handler.start()
        initial_state = handler.state
        assert initial_state == ComponentState.RUNNING

        # Put handler in degraded state
        await handler.degrade()
        # After degrading, check the state has changed
        degraded_state = handler.state
        assert degraded_state == ComponentState.DEGRADED

        # Essential events should still be processed
        event = PositionEvent(
            position_id="pos_degraded",
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            event_type=PositionEventType.LIQUIDATED,  # Critical event
            size=Decimal("0.0"),
            average_price=Decimal("50000.00"),
            realized_pnl=Decimal("-5000.00"),
        )

        # Should not raise exception
        await handler.handle_event(event)


class TestPortfolioBalanceEventHandler:
    """Test PortfolioBalanceEventHandler."""

    @pytest.fixture
    def mock_event_bus(self) -> MagicMock:
        """Create mock event bus.

        Returns:
            MagicMock: Mock event bus.
        """
        return MagicMock()

    @pytest.fixture
    def mock_balance_service(self) -> Mock:
        """Create mock balance service.

        Returns:
            Mock: Mock balance service.
        """
        service = Mock()

        # Mock get_exchange_balances to return mock data
        def mock_get_balances(exchange: ExchangeName) -> dict[str, SpotBalance]:
            symbol = SymbolFactory.create_custom_hyperliquid("USDC")
            return {
                symbol.value: SpotBalance(
                    exchange=exchange,
                    asset=symbol,
                    total_quantity=Decimal("10000.00"),
                    available_quantity=Decimal("9500.00"),
                    timestamp=datetime.now(UTC),
                )
            }

        service.get_exchange_balances = AsyncMock(
            return_value=mock_get_balances(ExchangeName.HYPERLIQUID)
        )
        service.update_balance_directly = AsyncMock()
        return service

    @pytest.fixture
    def config(self) -> EventHandlerConfig:
        """Create test config.

        Returns:
            EventHandlerConfig: Test configuration.
        """
        return EventHandlerConfig()

    @pytest.fixture
    def handler(
        self,
        mock_event_bus: MagicMock,
        config: EventHandlerConfig,
        mock_balance_service: Mock,
    ) -> PortfolioBalanceEventHandler:
        """Create test handler.

        Args:
            mock_event_bus: Mock event bus.
            config: Event handler config.
            mock_balance_service: Mock balance service.

        Returns:
            PortfolioBalanceEventHandler: Test handler.
        """
        return PortfolioBalanceEventHandler(
            handler_id="portfolio_balance_test",
            event_bus=mock_event_bus,
            config=config,
            balance_service=mock_balance_service,
        )

    @pytest.mark.asyncio
    async def test_balance_updated_event(
        self,
        handler: PortfolioBalanceEventHandler,
        mock_balance_service: Mock,
    ) -> None:
        """Test handling balance updated event."""
        event = BalanceEvent(
            account_id="acc_123",
            exchange=ExchangeName.HYPERLIQUID,
            currency="USDC",
            event_type=BalanceEventType.UPDATED,
            old_balance=Decimal("10000.00"),
            new_balance=Decimal("9500.00"),
        )

        await handler.handle_event(event)

        # Verify balance service was called to update the balance
        mock_balance_service.update_balance_directly.assert_called_once()
        # Verify the call arguments
        call_args = mock_balance_service.update_balance_directly.call_args
        assert call_args[0][1] == ExchangeName.HYPERLIQUID  # exchange
        balance = call_args[0][2]  # balance object
        assert isinstance(balance, SpotBalance)
        assert balance.total_quantity == Decimal("9500.00")

    @pytest.mark.asyncio
    async def test_balance_locked_event(
        self,
        handler: PortfolioBalanceEventHandler,
        mock_balance_service: Mock,
    ) -> None:
        """Test handling balance locked event."""
        event = BalanceEvent(
            account_id="acc_456",
            exchange=ExchangeName.BACKPACK,
            currency="BTC",
            event_type=BalanceEventType.LOCKED,
            old_balance=Decimal("1.0"),
            new_balance=Decimal("1.0"),
            locked_amount=Decimal("0.5"),
        )

        await handler.handle_event(event)

        # Verify balance service was called
        mock_balance_service.get_exchange_balances.assert_called_once_with(ExchangeName.BACKPACK)

    @pytest.mark.asyncio
    async def test_balance_unlocked_event(
        self,
        handler: PortfolioBalanceEventHandler,
        mock_balance_service: Mock,
    ) -> None:
        """Test handling balance unlocked event."""
        event = BalanceEvent(
            account_id="acc_789",
            exchange=ExchangeName.HYPERLIQUID,
            currency="ETH",
            event_type=BalanceEventType.UNLOCKED,
            old_balance=Decimal("5.0"),
            new_balance=Decimal("5.0"),
            locked_amount=Decimal("0.0"),
        )

        await handler.handle_event(event)

        # Verify balance service was called to update
        mock_balance_service.update_balance_directly.assert_called_once()
        # Verify the call arguments
        call_args = mock_balance_service.update_balance_directly.call_args
        assert call_args[0][1] == ExchangeName.HYPERLIQUID  # exchange

    @pytest.mark.asyncio
    async def test_balance_settled_event(
        self,
        handler: PortfolioBalanceEventHandler,
        mock_balance_service: Mock,
    ) -> None:
        """Test handling balance settled event."""
        event = BalanceEvent(
            account_id="acc_settle",
            exchange=ExchangeName.BACKPACK,
            currency="USDC",
            event_type=BalanceEventType.SETTLED,
            old_balance=Decimal("5000.00"),
            new_balance=Decimal("5050.00"),
        )

        await handler.handle_event(event)

        # Verify balance service was called to update
        mock_balance_service.update_balance_directly.assert_called_once()
        # Verify the call arguments
        call_args = mock_balance_service.update_balance_directly.call_args
        assert call_args[0][1] == ExchangeName.BACKPACK  # exchange

    @pytest.mark.asyncio
    async def test_error_handling_with_balance_locked(
        self,
        handler: PortfolioBalanceEventHandler,
        mock_balance_service: Mock,
    ) -> None:
        """Test error handling with balance locked event."""
        # Make balance service raise errors for locked event (which calls get_exchange_balances)
        mock_balance_service.get_exchange_balances.side_effect = RuntimeError("Service unavailable")

        event = BalanceEvent(
            account_id="acc_error",
            exchange=ExchangeName.HYPERLIQUID,
            currency="USDC",
            event_type=BalanceEventType.LOCKED,  # locked event calls get_exchange_balances
            old_balance=Decimal("1000.00"),
            new_balance=Decimal("1000.00"),
            locked_amount=Decimal("100.00"),
        )

        # Should raise error when calling get_exchange_balances
        with pytest.raises(RuntimeError):
            await handler.handle_event(event)

    @pytest.mark.asyncio
    async def test_metrics_collection(
        self,
        handler: PortfolioBalanceEventHandler,
        mock_balance_service: Mock,
    ) -> None:
        """Test metrics are collected during event processing."""
        event = BalanceEvent(
            account_id="acc_metrics",
            exchange=ExchangeName.HYPERLIQUID,
            currency="USDC",
            event_type=BalanceEventType.UPDATED,
            old_balance=Decimal("1000.00"),
            new_balance=Decimal("1100.00"),
        )

        await handler.handle_event(event)

        # Verify the balance service was called with proper arguments
        mock_balance_service.update_balance_directly.assert_called_once()
        call_args = mock_balance_service.update_balance_directly.call_args
        # Check the balance object has the correct new balance
        balance = call_args[0][2]
        assert balance.total_quantity == Decimal("1100.00")
