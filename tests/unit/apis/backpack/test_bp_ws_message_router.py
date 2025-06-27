"""Unit tests for BackpackWsMessageRouter.

Tests the WebSocket message routing logic in isolation with mocked dependencies.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from cyberdelta.apis.backpack.bp_ws_message_router import BackpackWsMessageRouter
from cyberdelta.apis.backpack.bp_ws_raw_message_handler import BackpackWsRawMessageHandler
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
from cyberdelta.apis.base.exchange_api import MessageHandler
from cyberdelta.apis.models.api_error import APIError, TransformationError


class TestBackpackWsMessageRouter:
    """Test suite for BackpackWsMessageRouter."""

    @pytest.fixture
    def mock_market_data_mapper(self) -> Mock:
        """Create a mock market data mapper.

        Returns:
            Mock BackpackMarketDataMapper for testing.
        """
        mapper = Mock(spec=BackpackMarketDataMapper)
        mapper.transform_ws_depth_event_to_internal = Mock(return_value=Mock())
        mapper.transform_ws_ticker_event_to_internal = Mock(return_value=Mock())
        return mapper

    @pytest.fixture
    def mock_account_data_mapper(self) -> Mock:
        """Create a mock account data mapper.

        Returns:
            Mock BackpackAccountDataMapper for testing.
        """
        mapper = Mock(spec=BackpackAccountDataMapper)
        mapper.transform_ws_fill_event_to_internal_trade = Mock(return_value=Mock())
        mapper.transform_ws_position_update_to_internal_position = Mock(return_value=Mock())
        return mapper

    @pytest.fixture
    def mock_trading_data_mapper(self) -> Mock:
        """Create a mock trading data mapper.

        Returns:
            Mock BackpackTradingDataMapper for testing.
        """
        mapper = Mock(spec=BackpackTradingDataMapper)
        mapper.transform_ws_order_update_to_internal_order = Mock(return_value=Mock())
        return mapper

    @pytest.fixture
    def mock_raw_ws_handler(self) -> Mock:
        """Create a mock raw WebSocket message handler.

        Returns:
            Mock BackpackWsRawMessageHandler for testing.
        """
        handler = Mock(spec=BackpackWsRawMessageHandler)
        handler.handle_depth_payload = Mock(return_value={"mock": "depth_data"})
        handler.handle_ticker_payload = Mock(return_value={"mock": "ticker_data"})
        handler.handle_trade_event_payload = Mock(return_value={"mock": "trade_data"})
        handler.handle_order_update_payload = Mock(return_value={"mock": "order_data"})
        handler.handle_position_update_payload = Mock(return_value={"mock": "position_data"})
        return handler

    @pytest.fixture
    def router(
        self,
        mock_market_data_mapper: Mock,
        mock_account_data_mapper: Mock,
        mock_trading_data_mapper: Mock,
        mock_raw_ws_handler: Mock,
    ) -> BackpackWsMessageRouter:
        """Create a BackpackWsMessageRouter instance with mocked dependencies.

        Returns:
            BackpackWsMessageRouter instance with mock dependencies for testing.
        """
        return BackpackWsMessageRouter(
            market_data_mapper=mock_market_data_mapper,
            account_data_mapper=mock_account_data_mapper,
            trading_data_mapper=mock_trading_data_mapper,
            raw_ws_handler=mock_raw_ws_handler,
            exchange_name="Backpack",
        )

    @pytest.fixture
    def mock_app_handler(self) -> AsyncMock:
        """Create a mock application handler with proper typing.

        Returns:
            Mock MessageHandler instance for testing.
        """
        return AsyncMock(spec=MessageHandler)

    def test_init(self, router: BackpackWsMessageRouter) -> None:
        """Test router initialization."""
        # Test that router was created successfully
        assert router is not None
        assert hasattr(router, "logger")

    def test_construct_subscription_payload_basic_topic(
        self,
        router: BackpackWsMessageRouter,
    ) -> None:
        """Test subscription payload construction for basic topics."""
        result = router.construct_subscription_payload("depth.SOL_USDC")

        # Verify the result is a BackpackRawWsSubscriptionRequest
        from cyberdelta.apis.backpack.models.bp_ws_payloads import BackpackRawWsSubscriptionRequest

        assert isinstance(result, BackpackRawWsSubscriptionRequest)
        assert result.method == "SUBSCRIBE"
        assert result.params == ["depth.SOL_USDC"]
        assert result.signature is None

    def test_construct_subscription_payload_various_topics(
        self,
        router: BackpackWsMessageRouter,
    ) -> None:
        """Test subscription payload construction for various topic types."""
        from cyberdelta.apis.backpack.models.bp_ws_payloads import BackpackRawWsSubscriptionRequest

        test_cases = [
            "ticker.BTC_USDC",
            "fills",
            "orders",
            "positionUpdate",
        ]

        for topic in test_cases:
            result = router.construct_subscription_payload(topic)
            assert isinstance(result, BackpackRawWsSubscriptionRequest)
            assert result.method == "SUBSCRIBE"
            assert result.params == [topic]
            assert result.signature is None

    @pytest.mark.asyncio
    async def test_route_message_depth_topic(
        self,
        router: BackpackWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_market_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing depth messages."""
        message: dict[str, Any] = {
            "topic": "depth.SOL_USDC",
            "data": {"bids": [], "asks": []},
        }
        ws_handlers: dict[str, MessageHandler] = {"depth.SOL_USDC": mock_app_handler}

        await router.route_message(message, ws_handlers)

        # Verify raw handler was called
        mock_raw_ws_handler.handle_depth_payload.assert_called_once_with({"bids": [], "asks": []})

        # Verify mapper was called with correct symbol
        mock_market_data_mapper.transform_ws_depth_event_to_internal.assert_called_once_with(
            "SOL_USDC",
            {"mock": "depth_data"},
        )

        # Verify app handler was called
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_ticker_topic(
        self,
        router: BackpackWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_market_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing ticker messages."""
        message = {
            "topic": "ticker.BTC_USDC",
            "data": {"price": "50000", "volume": "100"},
        }
        ws_handlers: dict[str, MessageHandler] = {"ticker.BTC_USDC": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_ticker_payload.assert_called_once_with(
            {"price": "50000", "volume": "100"},
        )
        mock_market_data_mapper.transform_ws_ticker_event_to_internal.assert_called_once_with(
            {"mock": "ticker_data"},
        )
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_fills_topic(
        self,
        router: BackpackWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_account_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing fills messages."""
        message = {
            "type": "fills",
            "data": {"id": "123", "price": "50000", "quantity": "1.0"},
        }
        ws_handlers: dict[str, MessageHandler] = {"fills": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_trade_event_payload.assert_called_once_with(
            {"id": "123", "price": "50000", "quantity": "1.0"},
        )
        mock_account_data_mapper.transform_ws_fill_event_to_internal_trade.assert_called_once_with(
            {"mock": "trade_data"},
        )
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_orders_topic(
        self,
        router: BackpackWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_trading_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing orders messages."""
        message = {
            "type": "orders",
            "data": {"id": "order123", "status": "filled"},
        }
        ws_handlers: dict[str, MessageHandler] = {"orders": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_order_update_payload.assert_called_once_with(
            {"id": "order123", "status": "filled"},
        )
        mock_trading_data_mapper.transform_ws_order_update_to_internal_order.assert_called_once_with(
            {"mock": "order_data"},
        )
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_position_update_topic(
        self,
        router: BackpackWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_account_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing position update messages."""
        message = {
            "type": "positionUpdate",
            "data": {"symbol": "SOL_USDC", "size": "10.0"},
        }
        ws_handlers: dict[str, MessageHandler] = {"positionUpdate": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_position_update_payload.assert_called_once_with(
            {"symbol": "SOL_USDC", "size": "10.0"},
        )
        mock_account_data_mapper.transform_ws_position_update_to_internal_position.assert_called_once_with(
            {"mock": "position_data"},
        )
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_base_topic_fallback(
        self,
        router: BackpackWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_market_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing with base topic fallback when specific handler not found."""
        message: dict[str, Any] = {
            "topic": "depth.ETH_USDC",
            "data": {"bids": [], "asks": []},
        }
        # Only register base topic handler
        ws_handlers: dict[str, MessageHandler] = {"depth": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_raw_ws_handler.handle_depth_payload.assert_called_once()
        mock_market_data_mapper.transform_ws_depth_event_to_internal.assert_called_once()
        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_no_topic_or_type(
        self,
        router: BackpackWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing message with no topic or type - should return early."""
        message = {"data": {"some": "data"}}
        ws_handlers: dict[str, MessageHandler] = {"depth": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_no_data_payload(
        self,
        router: BackpackWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing message with no data payload - should return early."""
        message = {"topic": "depth.SOL_USDC"}
        ws_handlers: dict[str, MessageHandler] = {"depth.SOL_USDC": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_no_handler_registered(
        self,
        router: BackpackWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing message with no registered handler - should return early."""
        message = {
            "topic": "unknown.topic",
            "data": {"some": "data"},
        }
        ws_handlers: dict[str, MessageHandler] = {"depth": mock_app_handler}

        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_unknown_topic_passes_raw_data(
        self,
        router: BackpackWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test routing unknown topic passes raw data to handler."""
        message = {
            "topic": "unknown_topic",
            "data": {"raw": "data"},
        }
        ws_handlers: dict[str, MessageHandler] = {"unknown_topic": mock_app_handler}

        await router.route_message(message, ws_handlers)

        # Should call handler with raw data, not transformed
        mock_app_handler.assert_called_once_with({"raw": "data"}, message)

    @pytest.mark.asyncio
    async def test_route_message_api_error_in_raw_handler(
        self,
        router: BackpackWsMessageRouter,
        mock_raw_ws_handler: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test handling APIError from raw message handler."""
        mock_raw_ws_handler.handle_depth_payload.side_effect = APIError(
            "Invalid depth data",
            code="INVALID_DATA",
        )

        message = {
            "topic": "depth.SOL_USDC",
            "data": {"invalid": "data"},
        }
        ws_handlers: dict[str, MessageHandler] = {"depth.SOL_USDC": mock_app_handler}

        await router.route_message(message, ws_handlers)

        # Handler should not be called due to error
        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_transformation_error_in_mapper(
        self,
        router: BackpackWsMessageRouter,
        mock_market_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test handling TransformationError from mapper."""
        mock_market_data_mapper.transform_ws_depth_event_to_internal.side_effect = (
            TransformationError("Failed to transform depth data")
        )

        message: dict[str, Any] = {
            "topic": "depth.SOL_USDC",
            "data": {"bids": [], "asks": []},
        }
        ws_handlers: dict[str, MessageHandler] = {"depth.SOL_USDC": mock_app_handler}

        await router.route_message(message, ws_handlers)

        # Handler should not be called due to transformation error
        mock_app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_message_exception_in_app_handler(
        self,
        router: BackpackWsMessageRouter,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test handling exception in application handler."""
        mock_app_handler.side_effect = Exception("Handler failed")

        message: dict[str, Any] = {
            "topic": "depth.SOL_USDC",
            "data": {"bids": [], "asks": []},
        }
        ws_handlers: dict[str, MessageHandler] = {"depth.SOL_USDC": mock_app_handler}

        # Should not raise exception, just log it
        await router.route_message(message, ws_handlers)

        mock_app_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_message_symbol_extraction_from_topic(
        self,
        router: BackpackWsMessageRouter,
        mock_market_data_mapper: Mock,
        mock_app_handler: AsyncMock,
    ) -> None:
        """Test correct symbol extraction from topic for depth messages."""
        test_cases = [
            ("depth.BTC_USDC", "BTC_USDC"),
            ("depth.ETH_USDT", "ETH_USDT"),
            ("depth.SOL_USDC", "SOL_USDC"),
            ("depth", "UNKNOWN"),  # No symbol in topic
        ]

        for topic, expected_symbol in test_cases:
            mock_market_data_mapper.reset_mock()

            message: dict[str, Any] = {
                "topic": topic,
                "data": {"bids": [], "asks": []},
            }
            ws_handlers: dict[str, MessageHandler] = {topic: mock_app_handler}

            await router.route_message(message, ws_handlers)

            mock_market_data_mapper.transform_ws_depth_event_to_internal.assert_called_once_with(
                expected_symbol,
                {"mock": "depth_data"},
            )
