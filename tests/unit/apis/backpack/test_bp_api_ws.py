"""
Tests for BackpackAPI WebSocket Message Handling
-----------------------------------------------

This module tests the WebSocket message handling functionality in BackpackAPI,
specifically focusing on the integration between raw message validation,
data transformation using the new consolidated DataMappers, and application
handler invocation.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderUpdate
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPositionUpdate
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTradeEvent
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import DerivativePosition, Order, OrderBook, Ticker, Trade


@pytest.fixture
def mock_api_config() -> dict[str, Any]:
    """Mock API configuration."""
    return {
        "base_url": "https://api.backpack.exchange",
        "timeout": 30,
        "ws_endpoint": "wss://ws.backpack.exchange",
    }


@pytest.fixture
def mock_secrets() -> dict[str, str | None]:
    """Mock secrets configuration."""
    return {
        "BACKPACK_API_KEY": "test_key",
        "BACKPACK_API_SECRET": "test_secret",
    }


@pytest.fixture
def bp_api(mock_api_config: dict[str, Any], mock_secrets: dict[str, str | None]) -> BackpackAPI:
    """Create BackpackAPI instance with mocked dependencies."""
    with patch("cyberdelta.apis.backpack.bp_api.BackpackHmacAuthenticator"):
        with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                    api = BackpackAPI(api_config=mock_api_config, secrets=mock_secrets)
                    return api


class TestBackpackAPIWebSocketMessageRouting:
    """Test WebSocket message routing with new DataMapper integration."""

    @pytest.mark.asyncio
    async def test_route_ws_message_depth_update_success(self, bp_api: BackpackAPI) -> None:
        """Test successful routing of depth update message through new mapper."""
        # Mock the raw message handler
        mock_raw_depth = MagicMock(spec=BackpackRawDepthUpdateEvent)

        # Mock the market data mapper
        mock_internal_orderbook = MagicMock(spec=OrderBook)

        # Mock application handler
        app_handler = AsyncMock()
        bp_api._ws_handlers["depth.SOL_USDC"] = app_handler

        # Test message
        message = {
            "topic": "depth.SOL_USDC",
            "data": {"bids": [["100.0", "10.0"]], "asks": [["101.0", "5.0"]]},
        }

        with patch(
            "cyberdelta.apis.backpack.bp_api.BackpackWsRawMessageHandler.handle_depth_payload",
            return_value=mock_raw_depth,
        ):
            with patch.object(
                bp_api._bp_market_data_mapper,
                "transform_ws_depth_event_to_internal",
                return_value=mock_internal_orderbook,
            ):
                await bp_api._route_ws_message(message)

        # Verify the application handler was called with the transformed data
        app_handler.assert_called_once_with(mock_internal_orderbook, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_ticker_update_success(self, bp_api: BackpackAPI) -> None:
        """Test successful routing of ticker update message through new mapper."""
        # Mock the raw message handler
        mock_raw_ticker = MagicMock(spec=BackpackRawTickerEvent)

        # Mock the market data mapper
        mock_internal_ticker = MagicMock(spec=Ticker)

        # Mock application handler
        app_handler = AsyncMock()
        bp_api._ws_handlers["ticker.SOL_USDC"] = app_handler

        # Test message
        message = {"topic": "ticker.SOL_USDC", "data": {"symbol": "SOL_USDC", "price": "100.50"}}

        with patch(
            "cyberdelta.apis.backpack.bp_api.BackpackWsRawMessageHandler.handle_ticker_payload",
            return_value=mock_raw_ticker,
        ):
            with patch.object(
                bp_api._bp_market_data_mapper,
                "transform_ws_ticker_event_to_internal",
                return_value=mock_internal_ticker,
            ):
                await bp_api._route_ws_message(message)

        # Verify the transformation chain was called correctly
        app_handler.assert_called_once_with(mock_internal_ticker, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_fills_update_success(self, bp_api: BackpackAPI) -> None:
        """Test successful routing of fills update message through new mapper."""
        # Mock the raw message handler
        mock_raw_trade = MagicMock(spec=BackpackRawTradeEvent)

        # Mock the account data mapper
        mock_internal_trade = MagicMock(spec=Trade)

        # Mock application handler
        app_handler = AsyncMock()
        bp_api._ws_handlers["fills"] = app_handler

        # Test message
        message = {
            "type": "fills",
            "data": {"symbol": "SOL_USDC", "price": "100.50", "quantity": "10.0"},
        }

        with patch(
            "cyberdelta.apis.backpack.bp_api.BackpackWsRawMessageHandler.handle_trade_event_payload",
            return_value=mock_raw_trade,
        ):
            with patch.object(
                bp_api._bp_account_data_mapper,
                "transform_ws_fill_event_to_internal_trade",
                return_value=mock_internal_trade,
            ):
                await bp_api._route_ws_message(message)

        # Verify the transformation chain was called correctly
        app_handler.assert_called_once_with(mock_internal_trade, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_order_update_success(self, bp_api: BackpackAPI) -> None:
        """Test successful routing of order update message through new mapper."""
        # Mock the raw message handler
        mock_raw_order_update = MagicMock(spec=BackpackRawOrderUpdate)

        # Mock the trading data mapper
        mock_internal_order = MagicMock(spec=Order)

        # Mock application handler
        app_handler = AsyncMock()
        bp_api._ws_handlers["orders"] = app_handler

        # Test message
        message = {"type": "orders", "data": {"symbol": "SOL_USDC", "side": "Buy", "status": "NEW"}}

        with patch(
            "cyberdelta.apis.backpack.bp_api.BackpackWsRawMessageHandler.handle_order_update_payload",
            return_value=mock_raw_order_update,
        ):
            with patch.object(
                bp_api._bp_trading_data_mapper,
                "transform_ws_order_update_to_internal_order",
                return_value=mock_internal_order,
            ):
                await bp_api._route_ws_message(message)

        # Verify the transformation chain was called correctly
        app_handler.assert_called_once_with(mock_internal_order, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_position_update_success(self, bp_api: BackpackAPI) -> None:
        """Test successful routing of position update message through new mapper."""
        # Mock the raw message handler
        mock_raw_position_update = MagicMock(spec=BackpackRawPositionUpdate)

        # Mock the account data mapper
        mock_internal_position = MagicMock(spec=DerivativePosition)

        # Mock application handler
        app_handler = AsyncMock()
        bp_api._ws_handlers["positionUpdate"] = app_handler

        # Test message
        message = {"type": "positionUpdate", "data": {"symbol": "SOL_USDC", "quantity": "10.0"}}

        with patch(
            "cyberdelta.apis.backpack.bp_api.BackpackWsRawMessageHandler.handle_position_update_payload",
            return_value=mock_raw_position_update,
        ):
            with patch.object(
                bp_api._bp_account_data_mapper,
                "transform_ws_position_update_to_internal_position",
                return_value=mock_internal_position,
            ):
                await bp_api._route_ws_message(message)

        # Verify the transformation chain was called correctly
        app_handler.assert_called_once_with(mock_internal_position, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_api_error_handling(self, bp_api: BackpackAPI) -> None:
        """Test handling of APIError during raw message validation."""
        # Mock application handler
        app_handler = AsyncMock()
        bp_api._ws_handlers["depth.SOL_USDC"] = app_handler

        # Test message
        message = {"topic": "depth.SOL_USDC", "data": {"invalid": "data"}}

        # Mock APIError from raw message handler
        with patch(
            "cyberdelta.apis.backpack.bp_api.BackpackWsRawMessageHandler.handle_depth_payload",
            side_effect=APIError("Invalid payload", APIErrorCode.INVALID_RESPONSE.value),
        ):
            with patch("cyberdelta.apis.backpack.bp_api.logger") as mock_logger:
                await bp_api._route_ws_message(message)

        # Verify error was logged and handler was not called
        mock_logger.error.assert_called_once()
        app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_ws_message_transformation_error_handling(
        self, bp_api: BackpackAPI
    ) -> None:
        """Test handling of TransformationError during data mapping."""
        # Mock the raw message handler
        mock_raw_depth = MagicMock(spec=BackpackRawDepthUpdateEvent)

        # Mock application handler
        app_handler = AsyncMock()
        bp_api._ws_handlers["depth.SOL_USDC"] = app_handler

        # Test message
        message = {
            "topic": "depth.SOL_USDC",
            "data": {"bids": [["100.0", "10.0"]], "asks": [["101.0", "5.0"]]},
        }

        with patch(
            "cyberdelta.apis.backpack.bp_api.BackpackWsRawMessageHandler.handle_depth_payload",
            return_value=mock_raw_depth,
        ):
            with patch.object(
                bp_api._bp_market_data_mapper,
                "transform_ws_depth_event_to_internal",
                side_effect=TransformationError("Failed to transform depth data"),
            ):
                with patch("cyberdelta.apis.backpack.bp_api.logger") as mock_logger:
                    await bp_api._route_ws_message(message)

        # Verify error was logged and handler was not called
        mock_logger.error.assert_called_once()
        app_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_ws_message_no_handler_registered(self, bp_api: BackpackAPI) -> None:
        """Test handling when no application handler is registered for topic."""
        # Test message
        message = {"topic": "unknown.topic", "data": {"some": "data"}}

        with patch("cyberdelta.apis.backpack.bp_api.logger") as mock_logger:
            await bp_api._route_ws_message(message)

        # Verify debug message was logged
        mock_logger.debug.assert_called()

    @pytest.mark.asyncio
    async def test_route_ws_message_missing_topic_and_data(self, bp_api: BackpackAPI) -> None:
        """Test handling of malformed messages missing topic and data."""
        # Test message without topic or type
        message = {"some": "data"}

        with patch("cyberdelta.apis.backpack.bp_api.logger") as mock_logger:
            await bp_api._route_ws_message(message)

        # Verify debug message was logged
        mock_logger.debug.assert_called()

    @pytest.mark.asyncio
    async def test_route_ws_message_missing_data_payload(self, bp_api: BackpackAPI) -> None:
        """Test handling of messages with topic but no data payload."""
        # Test message without data
        message = {"topic": "depth.SOL_USDC"}

        with patch("cyberdelta.apis.backpack.bp_api.logger") as mock_logger:
            await bp_api._route_ws_message(message)

        # Verify debug message was logged
        mock_logger.debug.assert_called()

    @pytest.mark.asyncio
    async def test_route_ws_message_application_handler_exception(
        self, bp_api: BackpackAPI
    ) -> None:
        """Test handling of exceptions in application handler."""
        # Mock the raw message handler
        mock_raw_ticker = MagicMock(spec=BackpackRawTickerEvent)

        # Mock the market data mapper
        mock_internal_ticker = MagicMock(spec=Ticker)

        # Mock application handler that raises exception
        app_handler = AsyncMock(side_effect=Exception("Handler error"))
        bp_api._ws_handlers["ticker.SOL_USDC"] = app_handler

        # Test message
        message = {"topic": "ticker.SOL_USDC", "data": {"symbol": "SOL_USDC", "price": "100.50"}}

        with patch(
            "cyberdelta.apis.backpack.bp_api.BackpackWsRawMessageHandler.handle_ticker_payload",
            return_value=mock_raw_ticker,
        ):
            with patch.object(
                bp_api._bp_market_data_mapper,
                "transform_ws_ticker_event_to_internal",
                return_value=mock_internal_ticker,
            ):
                with patch("cyberdelta.apis.backpack.bp_api.logger") as mock_logger:
                    await bp_api._route_ws_message(message)

        # Verify error was logged
        mock_logger.error.assert_called()
        # Verify handler was called despite the error
        app_handler.assert_called_once_with(mock_internal_ticker, message)


class TestBackpackAPIWebSocketHandleMessage:
    """Test the _handle_websocket_message method."""

    @pytest.mark.asyncio
    async def test_handle_websocket_message_delegates_to_route(self, bp_api: BackpackAPI) -> None:
        """Test that _handle_websocket_message delegates to _route_ws_message."""
        message: dict[str, Any] = {"topic": "test", "data": {}}

        with patch.object(bp_api, "_route_ws_message", new_callable=AsyncMock) as mock_route:
            await bp_api._handle_websocket_message(message)

        mock_route.assert_called_once_with(message)
