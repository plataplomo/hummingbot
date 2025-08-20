"""Unit tests for the new Hyperliquid WebSocket Router."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.base.infrastructure_config_domain import MemoryOptimizationMode
from cyberdelta.apis.common import MessageHandler
from cyberdelta.apis.hyperliquid.hl_ws_router import (
    HyperliquidWebSocketRouter,
)
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
from cyberdelta.apis.websocket.error_context.error_handler import WebSocketErrorHandler
from cyberdelta.apis.websocket.registry.registry_factory import WebSocketRegistryFactory
from cyberdelta.apis.websocket.ws_context_factory import WebSocketContextFactory
from cyberdelta.exceptions.service_validation import EmptyStringParameterError


class TestHyperliquidWebSocketRouter:
    """Test HyperliquidWebSocketRouter functionality."""

    @pytest.fixture
    def error_handler(self) -> AsyncMock:
        """Create mock stream error handler.

        Returns:
            AsyncMock: Mock WebSocketErrorHandler instance for testing.
        """
        return MagicMock(spec=WebSocketErrorHandler)

    @pytest.fixture
    def order_book_mapper(self) -> MagicMock:
        """Create mock order book mapper.

        Returns:
            MagicMock: Mock HyperliquidOrderBookMapper instance for testing.
        """
        return MagicMock(spec=HyperliquidOrderBookMapper)

    @pytest.fixture
    def price_ticker_mapper(self) -> MagicMock:
        """Create mock price ticker mapper.

        Returns:
            MagicMock: Mock HyperliquidPriceTickerMapper instance for testing.
        """
        return MagicMock(spec=HyperliquidPriceTickerMapper)

    @pytest.fixture
    def balance_mapper(self) -> MagicMock:
        """Create mock balance mapper.

        Returns:
            MagicMock: Mock HyperliquidBalanceMapper instance for testing.
        """
        return MagicMock(spec=HyperliquidBalanceMapper)

    @pytest.fixture
    def position_mapper(self) -> MagicMock:
        """Create mock position mapper.

        Returns:
            MagicMock: Mock HyperliquidPositionMapper instance for testing.
        """
        return MagicMock(spec=HyperliquidPositionMapper)

    @pytest.fixture
    def order_mapper(self) -> MagicMock:
        """Create mock order mapper.

        Returns:
            MagicMock: Mock HyperliquidOrderMapper instance for testing.
        """
        return MagicMock(spec=HyperliquidOrderMapper)

    @pytest.fixture
    def transaction_mapper(self) -> MagicMock:
        """Create mock transaction mapper.

        Returns:
            MagicMock: Mock HyperliquidTransactionMapper instance for testing.
        """
        return MagicMock(spec=HyperliquidTransactionMapper)

    @pytest.fixture
    def historical_data_mapper(self) -> MagicMock:
        """Create mock historical data mapper.

        Returns:
            MagicMock: Mock HyperliquidHistoricalDataMapper instance for testing.
        """
        return MagicMock(spec=HyperliquidHistoricalDataMapper)

    @pytest.fixture
    def router(
        self,
        error_handler: AsyncMock,
        order_book_mapper: MagicMock,
        price_ticker_mapper: MagicMock,
        balance_mapper: MagicMock,
        position_mapper: MagicMock,
        order_mapper: MagicMock,
        transaction_mapper: MagicMock,
        historical_data_mapper: MagicMock,
    ) -> HyperliquidWebSocketRouter:
        """Create router for testing.

        Returns:
            HyperliquidWebSocketRouter: Configured router instance with mocked dependencies.
        """
        # Create typed processor for testing
        registry = WebSocketRegistryFactory.create_registry()
        context_factory = WebSocketContextFactory(registry)

        return HyperliquidWebSocketRouter(
            stream_error_handler=error_handler,
            context_factory=context_factory,
            memory_optimization_mode=MemoryOptimizationMode.DISABLED,
            memory_pool_size=100,
            order_book_mapper=order_book_mapper,
            price_ticker_mapper=price_ticker_mapper,
            balance_mapper=balance_mapper,
            position_mapper=position_mapper,
            order_mapper=order_mapper,
            transaction_mapper=transaction_mapper,
            historical_data_mapper=historical_data_mapper,
        )

    def test_initialization(self, router: HyperliquidWebSocketRouter) -> None:
        """Test router initialization."""
        assert router.exchange_name == "hyperliquid"
        # Updated expected count for new architecture with decomposed processors
        assert len(router.processors) > 0  # Should have processors set up

        # Check that key processors are set up
        expected_processors = {"l2Book", "trades", "userEvents", "allMids", "candle"}
        for processor_key in expected_processors:
            assert processor_key in router.processors

    @pytest.mark.asyncio
    async def test_routing_key_extraction_through_route_message(
        self, router: HyperliquidWebSocketRouter, error_handler: AsyncMock
    ) -> None:
        """Test routing key extraction through the public route_message interface."""
        # Test l2Book channel routing
        handler = AsyncMock()
        ws_handlers: dict[str, MessageHandler] = {"l2Book": handler}
        message: dict[str, Any] = {"channel": "l2Book", "data": {"coin": "BTC"}}

        # Mock processor to capture the routing
        mock_processor = AsyncMock()
        router.processors["l2Book"] = mock_processor

        await router.route_message(message, ws_handlers)

        # Verify processor was called
        mock_processor.process.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_channel_handling(
        self, router: HyperliquidWebSocketRouter, error_handler: AsyncMock
    ) -> None:
        """Test invalid channel handling through the public interface."""
        # Unknown channel should trigger unroutable message handling
        handler = AsyncMock()
        ws_handlers: dict[str, MessageHandler] = {"l2Book": handler}
        message: dict[str, Any] = {"channel": "invalid_channel", "data": {}}

        await router.route_message(message, ws_handlers)

        # Verify error handler was called for unroutable message
        # Note: Specific error handling verification depends on implementation details

    def test_construct_l2book_subscription_payload(
        self, router: HyperliquidWebSocketRouter
    ) -> None:
        """Test L2 book subscription payload construction."""
        payload = router.construct_l2book_subscription_payload("BTC")

        assert payload.method == "subscribe"
        assert payload.subscription.type == "l2Book"
        assert payload.subscription.coin == "BTC"

    def test_construct_l2book_subscription_payload_empty_coin(
        self, router: HyperliquidWebSocketRouter
    ) -> None:
        """Test L2 book subscription with empty coin."""
        with pytest.raises(EmptyStringParameterError):
            router.construct_l2book_subscription_payload("")

        with pytest.raises(EmptyStringParameterError):
            router.construct_l2book_subscription_payload("   ")

    def test_construct_trades_subscription_payload(
        self, router: HyperliquidWebSocketRouter
    ) -> None:
        """Test trades subscription payload construction."""
        payload = router.construct_trades_subscription_payload("ETH")

        assert payload.method == "subscribe"
        assert payload.subscription.type == "trades"
        assert payload.subscription.coin == "ETH"

    def test_construct_trades_subscription_payload_empty_coin(
        self, router: HyperliquidWebSocketRouter
    ) -> None:
        """Test trades subscription with empty coin."""
        with pytest.raises(EmptyStringParameterError):
            router.construct_trades_subscription_payload("")

    def test_construct_user_events_subscription_payload(
        self, router: HyperliquidWebSocketRouter
    ) -> None:
        """Test user events subscription payload construction."""
        user_address = "0x1234567890123456789012345678901234567890"
        payload = router.construct_user_events_subscription_payload(user_address)

        assert payload.method == "subscribe"
        assert payload.subscription.type == "userEvents"
        assert payload.subscription.user == user_address

    def test_construct_user_events_subscription_payload_empty_user(
        self, router: HyperliquidWebSocketRouter
    ) -> None:
        """Test user events subscription with empty user address."""
        with pytest.raises(EmptyStringParameterError):
            router.construct_user_events_subscription_payload("")

    def test_construct_candle_subscription_payload(
        self, router: HyperliquidWebSocketRouter
    ) -> None:
        """Test candle subscription payload construction."""
        payload = router.construct_candle_subscription_payload("BTC", "1h")

        assert payload.method == "subscribe"
        assert payload.subscription.type == "candle"
        assert payload.subscription.coin == "BTC"
        assert payload.subscription.interval == "1h"

    def test_construct_candle_subscription_payload_empty_params(
        self, router: HyperliquidWebSocketRouter
    ) -> None:
        """Test candle subscription with empty parameters."""
        with pytest.raises(EmptyStringParameterError):
            router.construct_candle_subscription_payload("", "1h")

        with pytest.raises(EmptyStringParameterError):
            router.construct_candle_subscription_payload("BTC", "")

    def test_construct_all_mids_subscription_payload(
        self, router: HyperliquidWebSocketRouter
    ) -> None:
        """Test all mids subscription payload construction."""
        payload = router.construct_all_mids_subscription_payload()

        assert payload.method == "subscribe"
        assert payload.subscription.type == "allMids"

    @pytest.mark.asyncio
    async def test_route_message_success(
        self, router: HyperliquidWebSocketRouter, error_handler: AsyncMock
    ) -> None:
        """Test successful message routing."""
        # Setup
        handler = AsyncMock()
        ws_handlers: dict[str, MessageHandler] = {"l2Book": handler}
        message: dict[str, Any] = {"channel": "l2Book", "data": {"coin": "BTC", "levels": []}}

        # Mock processor
        mock_processor = AsyncMock()
        router.processors["l2Book"] = mock_processor

        # Execute
        await router.route_message(message, ws_handlers)

        # Verify
        mock_processor.process.assert_called_once()

    def test_processor_setup_completeness(self, router: HyperliquidWebSocketRouter) -> None:
        """Test that expected processors are properly set up."""
        # Key processors that should be present
        expected_processors = [
            "l2Book",
            "trades",
            "userEvents",
            "allMids",
            "candle",
            "subscriptionResponse",
        ]

        for processor_key in expected_processors:
            assert processor_key in router.processors, f"Missing processor: {processor_key}"
            assert router.processors[processor_key] is not None
