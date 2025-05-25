"""
Unit tests for HyperliquidAPI WebSocket message handling.

This module tests the WebSocket message routing and transformation logic
in HyperliquidAPI, ensuring proper integration with the new consolidated
mapper architecture.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsPositionUpdateEvent,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.core.models import DerivativePosition, Order, Trade
from cyberdelta.core.models.market import OrderBook


@pytest.fixture
def hyperliquid_config() -> dict[str, Any]:
    """Basic configuration for HyperliquidAPI tests."""
    return {
        "rest_endpoint": "https://api.hyperliquid.xyz",
        "ws_endpoint": "wss://api.hyperliquid.xyz/ws",
        "rate_limits": {},
        "request_timeout": 30.0,
    }


@pytest.fixture
def hyperliquid_secrets() -> dict[str, str]:
    """Basic secrets for HyperliquidAPI tests."""
    return {
        "wallet_address": "0x0000000000000000000000000000000000000000",
        "private_key": "0x" + "0" * 64,  # Dummy private key
    }


@pytest.fixture
def hl_api(
    hyperliquid_config: dict[str, Any], hyperliquid_secrets: dict[str, str]
) -> HyperliquidAPI:
    """Create a HyperliquidAPI instance with mocked dependencies for WebSocket testing."""
    with (
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
    ):
        # Convert secrets to the expected type
        secrets_with_none: dict[str, str | None] = {k: v for k, v in hyperliquid_secrets.items()}

        api = HyperliquidAPI(
            api_config=hyperliquid_config,
            secrets=secrets_with_none,
        )

        # Mock the mappers for testing
        api._hl_market_data_mapper = MagicMock()
        api._hl_account_data_mapper = MagicMock()
        api._hl_trading_data_mapper = MagicMock()

        return api


class TestHyperliquidAPIWebSocketRouting:
    """Test WebSocket message routing and transformation in HyperliquidAPI."""

    @pytest.mark.asyncio
    async def test_route_ws_message_l2book_update_success(self, hl_api: HyperliquidAPI) -> None:
        """Test successful l2Book update message routing and transformation."""
        # Mock the handler
        mock_handler = AsyncMock()
        hl_api._ws_handlers["l2Book:ETH"] = mock_handler

        # Mock the raw message handler
        mock_validated_book = MagicMock(spec=HyperliquidRawWsBookUpdate)

        # Mock the market data mapper transformation
        mock_internal_orderbook = MagicMock(spec=OrderBook)

        message = {
            "channel": "l2Book",
            "data": {
                "coin": "ETH",
                "levels": [["100.5", "10"], ["100.4", "5"]],
                "time": 1234567890,
            },
        }

        with (
            patch(
                "cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler.HyperliquidWsRawMessageHandler.handle_l2book_payload",
                return_value=mock_validated_book,
            ) as mock_raw_handler,
            patch.object(
                hl_api._hl_market_data_mapper,
                "transform_ws_book_update_to_internal",
                return_value=mock_internal_orderbook,
            ) as mock_transform,
        ):
            await hl_api._route_ws_message(message)

            # Verify raw handler was called
            mock_raw_handler.assert_called_once_with(message["data"])

            # Verify transformation was called
            mock_transform.assert_called_once_with(mock_validated_book)

            # Verify app handler was called with internal model
            mock_handler.assert_called_once_with(mock_internal_orderbook, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_trades_update_success(self, hl_api: HyperliquidAPI) -> None:
        """Test successful trades update message routing and transformation."""
        # Mock the handler
        mock_handler = AsyncMock()
        hl_api._ws_handlers["trades:ETH"] = mock_handler

        # Mock validated trade models
        mock_validated_trade1 = MagicMock(spec=HyperliquidRawWsTradeEvent)
        mock_validated_trade2 = MagicMock(spec=HyperliquidRawWsTradeEvent)

        # Mock internal trade models
        mock_internal_trade1 = MagicMock(spec=Trade)
        mock_internal_trade2 = MagicMock(spec=Trade)

        message = {
            "channel": "trades",
            "data": [
                {"coin": "ETH", "px": "100.5", "sz": "1.0", "time": 1234567890, "side": "B"},
                {"coin": "ETH", "px": "100.4", "sz": "0.5", "time": 1234567891, "side": "A"},
            ],
        }

        with (
            patch(
                "cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler.HyperliquidWsRawMessageHandler.handle_public_trades_payload",
                return_value=[mock_validated_trade1, mock_validated_trade2],
            ) as mock_raw_handler,
            patch.object(
                hl_api._hl_market_data_mapper,
                "transform_ws_trade_event_to_internal",
                side_effect=[mock_internal_trade1, mock_internal_trade2],
            ) as mock_transform,
        ):
            await hl_api._route_ws_message(message)

            # Verify raw handler was called
            mock_raw_handler.assert_called_once_with(message["data"])

            # Verify transformation was called for each trade
            assert mock_transform.call_count == 2
            mock_transform.assert_any_call(mock_validated_trade1)
            mock_transform.assert_any_call(mock_validated_trade2)

            # Verify app handler was called for each internal trade
            assert mock_handler.call_count == 2
            mock_handler.assert_any_call(mock_internal_trade1, message)
            mock_handler.assert_any_call(mock_internal_trade2, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_user_fill_event_success(self, hl_api: HyperliquidAPI) -> None:
        """Test successful userEvents fill event routing and transformation."""
        # Mock the handler
        mock_handler = AsyncMock()
        hl_api._ws_handlers["userEvents"] = mock_handler

        # Mock validated fill event
        mock_validated_fill = MagicMock(spec=HyperliquidRawWsFillEvent)

        # Mock internal trade model
        mock_internal_trade = MagicMock(spec=Trade)

        message = {
            "channel": "userEvents",
            "data": [
                {
                    "type": "fill",
                    "coin": "ETH",
                    "px": "100.5",
                    "sz": "1.0",
                    "side": "B",
                    "time": 1234567890,
                    "hash": "0xabc123",
                    "oid": 12345,
                    "cloid": "client123",
                    "is_maker": True,
                }
            ],
        }

        with (
            patch(
                "cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler.HyperliquidWsRawMessageHandler.handle_user_fill_event_payload",
                return_value=mock_validated_fill,
            ) as mock_raw_handler,
            patch.object(
                hl_api._hl_account_data_mapper,
                "transform_ws_fill_event_to_internal",
                return_value=mock_internal_trade,
            ) as mock_transform,
        ):
            await hl_api._route_ws_message(message)

            # Verify raw handler was called
            mock_raw_handler.assert_called_once_with(message["data"][0])

            # Verify transformation was called
            mock_transform.assert_called_once_with(mock_validated_fill)

            # Verify app handler was called with internal model
            mock_handler.assert_called_once_with(mock_internal_trade, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_user_order_event_success(self, hl_api: HyperliquidAPI) -> None:
        """Test successful userEvents order event routing and transformation."""
        # Mock the handler
        mock_handler = AsyncMock()
        hl_api._ws_handlers["userEvents"] = mock_handler

        # Mock validated order models
        mock_order_wrapper = MagicMock()
        mock_order_wrapper.data = {"oid": 12345, "asset": "ETH", "side": "B"}
        mock_validated_order = MagicMock(spec=HyperliquidRawOrder)

        # Mock internal order model
        mock_internal_order = MagicMock(spec=Order)

        message = {
            "channel": "userEvents",
            "data": [
                {
                    "type": "order",
                    "data": {
                        "oid": 12345,
                        "asset": "ETH",
                        "side": "B",
                        "sz": "1.0",
                        "limit_px": "100.5",
                        "status": "open",
                        "timestamp": 1234567890,
                    },
                }
            ],
        }

        with (
            patch(
                "cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler.HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload",
                return_value=mock_order_wrapper,
            ) as mock_wrapper_handler,
            patch(
                "cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler.HyperliquidWsRawMessageHandler.handle_user_order_event_payload",
                return_value=mock_validated_order,
            ) as mock_order_handler,
            patch.object(
                hl_api._hl_trading_data_mapper,
                "transform_ws_order_update_to_internal_order",
                return_value=mock_internal_order,
            ) as mock_transform,
        ):
            await hl_api._route_ws_message(message)

            # Verify raw handlers were called
            mock_wrapper_handler.assert_called_once_with(message["data"][0])
            mock_order_handler.assert_called_once_with(mock_order_wrapper.data)

            # Verify transformation was called
            mock_transform.assert_called_once_with(mock_validated_order)

            # Verify app handler was called with internal model
            mock_handler.assert_called_once_with(mock_internal_order, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_user_position_event_success(
        self, hl_api: HyperliquidAPI
    ) -> None:
        """Test successful userEvents position update event routing and transformation."""
        # Mock the handler
        mock_handler = AsyncMock()
        hl_api._ws_handlers["userEvents"] = mock_handler

        # Mock validated position update
        mock_validated_position = MagicMock(spec=HyperliquidRawWsPositionUpdateEvent)

        # Mock internal position model
        mock_internal_position = MagicMock(spec=DerivativePosition)

        message = {
            "channel": "userEvents",
            "data": [
                {
                    "type": "positionUpdate",
                    "asset": "ETH",
                    "position": {"szi": "1.5", "entry_px": "100.0", "unrealized_pnl": "5.0"},
                    "time": 1234567890,
                }
            ],
        }

        with (
            patch(
                "cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler.HyperliquidWsRawMessageHandler.handle_user_position_update_event_payload",
                return_value=mock_validated_position,
            ) as mock_raw_handler,
            patch.object(
                hl_api._hl_account_data_mapper,
                "transform_ws_position_update_to_internal_position",
                return_value=mock_internal_position,
            ) as mock_transform,
        ):
            await hl_api._route_ws_message(message)

            # Verify raw handler was called
            mock_raw_handler.assert_called_once_with(message["data"][0])

            # Verify transformation was called
            mock_transform.assert_called_once_with(mock_validated_position)

            # Verify app handler was called with internal model
            mock_handler.assert_called_once_with(mock_internal_position, message)

    @pytest.mark.asyncio
    async def test_route_ws_message_api_error_handling(self, hl_api: HyperliquidAPI) -> None:
        """Test APIError handling during raw message validation."""
        # Mock the handler
        mock_handler = AsyncMock()
        hl_api._ws_handlers["l2Book:ETH"] = mock_handler

        message = {"channel": "l2Book", "data": {"coin": "ETH", "invalid": "data"}}

        with patch(
            "cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler.HyperliquidWsRawMessageHandler.handle_l2book_payload",
            side_effect=APIError("Invalid l2Book data", code="INVALID_DATA"),
        ) as mock_raw_handler:
            # Should not raise, but should log error
            await hl_api._route_ws_message(message)

            # Verify raw handler was called
            mock_raw_handler.assert_called_once_with(message["data"])

            # Verify app handler was NOT called
            mock_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_ws_message_transformation_error_handling(
        self, hl_api: HyperliquidAPI
    ) -> None:
        """Test TransformationError handling during mapper transformation."""
        # Mock the handler
        mock_handler = AsyncMock()
        hl_api._ws_handlers["trades:ETH"] = mock_handler

        # Mock validated trade model
        mock_validated_trade = MagicMock(spec=HyperliquidRawWsTradeEvent)

        message = {
            "channel": "trades",
            "data": [{"coin": "ETH", "px": "100.5", "sz": "1.0", "time": 1234567890, "side": "B"}],
        }

        with (
            patch(
                "cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler.HyperliquidWsRawMessageHandler.handle_public_trades_payload",
                return_value=[mock_validated_trade],
            ) as mock_raw_handler,
            patch.object(
                hl_api._hl_market_data_mapper,
                "transform_ws_trade_event_to_internal",
                side_effect=TransformationError("Failed to transform trade"),
            ) as mock_transform,
        ):
            # Should not raise, but should log error
            await hl_api._route_ws_message(message)

            # Verify raw handler was called
            mock_raw_handler.assert_called_once_with(message["data"])

            # Verify transformation was attempted
            mock_transform.assert_called_once_with(mock_validated_trade)

            # Verify app handler was NOT called
            mock_handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_ws_message_no_handler_registered(self, hl_api: HyperliquidAPI) -> None:
        """Test behavior when no handler is registered for a topic."""
        message = {
            "channel": "l2Book",
            "data": {"coin": "ETH", "levels": [["100.5", "10"]], "time": 1234567890},
        }

        # Should not raise, just log debug message
        await hl_api._route_ws_message(message)

    @pytest.mark.asyncio
    async def test_route_ws_message_missing_channel(self, hl_api: HyperliquidAPI) -> None:
        """Test behavior when message has no channel."""
        message = {"data": {"some": "data"}}

        # Should not raise, just return early
        await hl_api._route_ws_message(message)

    @pytest.mark.asyncio
    async def test_route_ws_message_missing_data_payload(self, hl_api: HyperliquidAPI) -> None:
        """Test behavior when message has no data payload."""
        message = {"channel": "l2Book"}

        # Should not raise, just log warning and return
        await hl_api._route_ws_message(message)

    @pytest.mark.asyncio
    async def test_route_ws_message_application_handler_exception(
        self, hl_api: HyperliquidAPI
    ) -> None:
        """Test handling of exceptions in application handler."""
        # Mock the handler to raise an exception
        mock_handler = AsyncMock(side_effect=Exception("Handler error"))
        hl_api._ws_handlers["l2Book:ETH"] = mock_handler

        # Mock the raw message handler and transformation
        mock_validated_book = MagicMock(spec=HyperliquidRawWsBookUpdate)
        mock_internal_orderbook = MagicMock(spec=OrderBook)

        message = {
            "channel": "l2Book",
            "data": {"coin": "ETH", "levels": [["100.5", "10"]], "time": 1234567890},
        }

        with (
            patch(
                "cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler.HyperliquidWsRawMessageHandler.handle_l2book_payload",
                return_value=mock_validated_book,
            ),
            patch.object(
                hl_api._hl_market_data_mapper,
                "transform_ws_book_update_to_internal",
                return_value=mock_internal_orderbook,
            ),
        ):
            # Should not raise, but should log error
            await hl_api._route_ws_message(message)

            # Verify handler was called
            mock_handler.assert_called_once_with(mock_internal_orderbook, message)

    @pytest.mark.asyncio
    async def test_handle_websocket_message_delegates_to_route(
        self, hl_api: HyperliquidAPI
    ) -> None:
        """Test that _handle_websocket_message delegates to _route_ws_message."""
        message = {"channel": "test", "data": {"test": "data"}}

        with patch.object(hl_api, "_route_ws_message", new_callable=AsyncMock) as mock_route:
            await hl_api._handle_websocket_message(message)
            mock_route.assert_called_once_with(message)
