"""Unit tests for market domain event handlers - CLAUDE.md compliant.

Tests market data processing, orderbook handling, and symbol caching
functionality using ONLY public behavior and proper lifecycle testing.

Following CLAUDE.md Line 72: Testing ONLY via public behavior exposed by event handlers.
NO private method access. Tests interact only through public interfaces.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.models.event_system_config import EventHandlerConfig
from cyberdelta.domain.market.market_event_handlers import (
    MarketDataEventHandler,
    MarketOrderbookEventHandler,
)
from cyberdelta.enums import MarketDataType
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.infrastructure.event_bus import EventBus
from cyberdelta.models.events.core import MarketData
from tests.factories.symbol_factories import SymbolFactory
from tests.fixtures.time_fixtures import FreezerProtocol


class TestMarketDataEventHandlerCompliant:
    """Test suite for MarketDataEventHandler using ONLY public behavior."""

    @pytest.fixture
    def mock_market_service(self) -> AsyncMock:
        """Create mock market data service.

        Returns:
            AsyncMock: Mock market data service for testing
        """
        service = AsyncMock()
        service.update_ticker = AsyncMock()
        service.update_orderbook = AsyncMock()
        return service

    @pytest.fixture
    def mock_event_bus(self) -> MagicMock:
        """Create mock event bus.

        Returns:
            MagicMock: Mock event bus for testing
        """
        bus = MagicMock(spec=EventBus)
        bus.subscribe = MagicMock()
        bus.unsubscribe = MagicMock()
        return bus

    @pytest.fixture
    def handler_config(self) -> EventHandlerConfig:
        """Create handler configuration.

        Returns:
            EventHandlerConfig: Handler configuration for testing
        """
        return EventHandlerConfig()

    @pytest.fixture
    def app_config(self) -> AppSettings:
        """Create application configuration.

        Returns:
            AppSettings: Application configuration for testing
        """
        return AppSettings()

    @pytest.fixture
    def handler(
        self,
        mock_event_bus: MagicMock,
        handler_config: EventHandlerConfig,
        app_config: AppSettings,
        mock_market_service: AsyncMock,
    ) -> MarketDataEventHandler:
        """Create market data event handler.

        Returns:
            MarketDataEventHandler: Handler instance for testing
        """
        return MarketDataEventHandler(
            handler_id="test-market-handler",
            event_bus=mock_event_bus,
            config=handler_config,
            app_config=app_config,
            market_service=mock_market_service,
        )

    def test_handler_initialization(
        self,
        handler: MarketDataEventHandler,
        app_config: AppSettings,
        mock_market_service: AsyncMock,
    ) -> None:
        """Test handler initializes correctly with configuration."""
        assert handler.handler_id == "test-market-handler"
        # Test public behavior only - private field access violates CLAUDE.md Line 72
        assert handler.get_processing_metrics()["ticks_processed"] == 0

    @pytest.mark.anyio
    async def test_handler_lifecycle(
        self,
        handler: MarketDataEventHandler,
        mock_event_bus: MagicMock,
    ) -> None:
        """Test handler lifecycle through public start/stop methods."""
        # Start handler
        await handler.start()
        assert mock_event_bus.subscribe.called

        # Stop handler
        await handler.stop()
        assert mock_event_bus.unsubscribe.called

    @pytest.mark.anyio
    async def test_tick_processing_through_public_interface(
        self,
        handler: MarketDataEventHandler,
        mock_market_service: AsyncMock,
        freezer: FreezerProtocol,
    ) -> None:
        """Test tick processing through public handle_event method."""
        freezer.move_to("2024-01-01T12:00:00Z")

        # Start handler to enable processing
        await handler.start()

        # Create tick data event
        event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal("50000.123"),
            volume=1000,
            timestamp=1704110400.0,  # 2024-01-01T12:00:00Z
        )

        # Mock symbol service
        mock_symbol = SymbolFactory.create_btc_usdc_spot_hyperliquid()
        with patch("cyberdelta.symbols.global_service.get_symbol_service") as mock_get_service:
            mock_symbol_service = AsyncMock()
            mock_symbol_service.get_symbol.return_value = mock_symbol
            mock_get_service.return_value = mock_symbol_service

            # Process event through public interface
            await handler.handle_event(event)

            # Verify processing occurred through metrics (public interface)
            metrics = handler.get_processing_metrics()
            assert metrics["ticks_processed"] == 1

            # Verify market service was called
            mock_market_service.update_ticker.assert_called_once()

    @pytest.mark.anyio
    async def test_filtering_behavior_through_metrics(
        self,
        handler: MarketDataEventHandler,
        app_config: AppSettings,
        freezer: FreezerProtocol,
    ) -> None:
        """Test event filtering behavior through public metrics interface."""
        freezer.move_to("2024-01-01T12:00:00Z")

        # Start handler
        await handler.start()

        # Create stale event (older than configured max age)
        max_age = app_config.monitoring.market_data.aggregation.max_age_difference_seconds
        stale_timestamp = 1704110400.0 - (max_age + 1.0)

        event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal(50000),
            timestamp=stale_timestamp,
        )

        # Process stale event
        await handler.handle_event(event)

        # Verify filtering through public metrics
        metrics = handler.get_processing_metrics()
        assert metrics["filtered_events"] == 1
        assert metrics["ticks_processed"] == 0

    @pytest.mark.anyio
    async def test_null_price_filtering_through_metrics(
        self,
        handler: MarketDataEventHandler,
    ) -> None:
        """Test null price filtering through public metrics interface."""
        # Start handler
        await handler.start()

        # Create event with null price
        event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=None,  # Invalid - tick data requires price
            timestamp=1704110400.0,
        )

        # Process invalid event
        await handler.handle_event(event)

        # Verify filtering through public metrics
        metrics = handler.get_processing_metrics()
        assert metrics["filtered_events"] == 1
        assert metrics["ticks_processed"] == 0

    @pytest.mark.anyio
    async def test_symbol_caching_through_metrics(
        self,
        handler: MarketDataEventHandler,
        mock_market_service: AsyncMock,
    ) -> None:
        """Test symbol caching through public metrics interface."""
        # Start handler
        await handler.start()

        # Create two events for same symbol
        event1 = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal(50000),
            timestamp=1704110400.0,
        )
        event2 = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal(50001),
            timestamp=1704110401.0,
        )

        # Mock symbol service
        mock_symbol = SymbolFactory.create_btc_usdc_spot_hyperliquid()
        with patch("cyberdelta.symbols.global_service.get_symbol_service") as mock_get_service:
            mock_symbol_service = AsyncMock()
            mock_symbol_service.get_symbol.return_value = mock_symbol
            mock_get_service.return_value = mock_symbol_service

            # Process both events
            await handler.handle_event(event1)
            await handler.handle_event(event2)

            # Verify caching through public metrics
            metrics = handler.get_processing_metrics()
            assert metrics["cache_hits"] == 1
            assert metrics["cache_misses"] == 1

            # Verify symbol service called only once
            assert mock_symbol_service.get_symbol.call_count == 1


class TestMarketOrderbookEventHandlerCompliant:
    """Test suite for MarketOrderbookEventHandler using ONLY public behavior."""

    @pytest.fixture
    def mock_market_service(self) -> AsyncMock:
        """Create mock market data service.

        Returns:
            AsyncMock: Mock market data service for testing
        """
        service = AsyncMock()
        service.update_orderbook = AsyncMock()
        return service

    @pytest.fixture
    def mock_event_bus(self) -> MagicMock:
        """Create mock event bus.

        Returns:
            MagicMock: Mock event bus for testing
        """
        bus = MagicMock(spec=EventBus)
        bus.subscribe = MagicMock()
        bus.unsubscribe = MagicMock()
        return bus

    @pytest.fixture
    def handler_config(self) -> EventHandlerConfig:
        """Create handler configuration.

        Returns:
            EventHandlerConfig: Handler configuration for testing
        """
        return EventHandlerConfig()

    @pytest.fixture
    def app_config(self) -> AppSettings:
        """Create application configuration.

        Returns:
            AppSettings: Application configuration for testing
        """
        return AppSettings()

    @pytest.fixture
    def handler(
        self,
        mock_event_bus: MagicMock,
        handler_config: EventHandlerConfig,
        app_config: AppSettings,
        mock_market_service: AsyncMock,
    ) -> MarketOrderbookEventHandler:
        """Create orderbook event handler.

        Returns:
            MarketOrderbookEventHandler: Handler instance for testing
        """
        return MarketOrderbookEventHandler(
            handler_id="test-orderbook-handler",
            event_bus=mock_event_bus,
            config=handler_config,
            app_config=app_config,
            market_service=mock_market_service,
        )

    def test_handler_initialization(
        self,
        handler: MarketOrderbookEventHandler,
        app_config: AppSettings,
        mock_market_service: AsyncMock,
    ) -> None:
        """Test handler initializes correctly with configuration."""
        assert handler.handler_id == "test-orderbook-handler"
        # Test public behavior only - private field access violates CLAUDE.md Line 72
        metrics = handler.get_orderbook_metrics()
        assert metrics["updates_processed"] == 0

    @pytest.mark.anyio
    async def test_handler_lifecycle(
        self,
        handler: MarketOrderbookEventHandler,
        mock_event_bus: MagicMock,
    ) -> None:
        """Test handler lifecycle through public start/stop methods."""
        # Start handler
        await handler.start()
        assert mock_event_bus.subscribe.called

        # Stop handler
        await handler.stop()
        assert mock_event_bus.unsubscribe.called

    @pytest.mark.anyio
    async def test_orderbook_filtering_through_metrics(
        self,
        handler: MarketOrderbookEventHandler,
        mock_market_service: AsyncMock,
        freezer: FreezerProtocol,
    ) -> None:
        """Test orderbook handler only processes orderbook events."""
        freezer.move_to("2024-01-01T12:00:00Z")

        # Start handler
        await handler.start()

        mock_symbol = SymbolFactory.create_btc_usdc_spot_hyperliquid()

        # Create non-orderbook event
        tick_event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.TICK,
            price=Decimal(50000),
            timestamp=1704110400.0,
        )

        # Create orderbook event
        bids = [(Decimal("49999.00"), Decimal("1.5"))]
        asks = [(Decimal("50001.00"), Decimal("1.2"))]

        orderbook_event = MarketData(
            symbol="BTC-USDC",
            exchange=ExchangeName.HYPERLIQUID,
            data_type=MarketDataType.ORDERBOOK,
            bids=bids,
            asks=asks,
            timestamp=1704110400.0,
        )

        with patch("cyberdelta.symbols.global_service.get_symbol_service") as mock_get_service:
            mock_symbol_service = AsyncMock()
            mock_symbol_service.get_symbol.return_value = mock_symbol
            mock_get_service.return_value = mock_symbol_service

            # Process tick event - should be ignored
            await handler.handle_event(tick_event)
            metrics_after_tick = handler.get_orderbook_metrics()
            assert metrics_after_tick["updates_processed"] == 0

            # Process orderbook event - should be processed
            await handler.handle_event(orderbook_event)
            metrics_after_orderbook = handler.get_orderbook_metrics()
            assert metrics_after_orderbook["updates_processed"] == 1

            # Verify market service called only for orderbook
            mock_market_service.update_orderbook.assert_called_once()
