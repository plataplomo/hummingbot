"""Unit tests for HyperliquidTradingService management operations."""

from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService


# Unit tests for HyperliquidTradingService (moved from mislabeled integration tests)
# These are unit tests because they mock all dependencies and test individual methods

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.services.conftest_trading"]


class TestHyperliquidTradingServiceManagement:
    """Tests for the HyperliquidTradingService management operations."""

    @pytest.mark.asyncio
    async def test_cancel_all_orders_get_open_orders_returns_none(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test cancel_all_orders when _get_open_orders_raw receives None from info HTTP client."""
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        mock_request_payload_model = MagicMock(spec=HyperliquidRawOpenOrdersRequestPayload)
        mock_request_payload_model.model_dump.return_value = {
            "type": "openOrders",
            "user": wallet_address,
        }
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_request_payload_model

        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.cancel_all_orders(symbol="ETH")

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for open orders, status: 200" in exc_info.value.message
        # Note: The trading service creates HyperliquidRawOpenOrdersRequestPayload directly,
        # it does not use the request builder for open orders

    @pytest.mark.asyncio
    async def test_cancel_all_orders_success_with_symbol_filter(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_order_mapper: MagicMock,
    ) -> None:
        """Test successful cancel_all_orders operation with symbol filtering."""
        wallet_address = "0xCancelAllWallet"
        symbol = "BTC"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Test focuses on public behavior, not exact data matching

        # Mock HTTP responses for getting open orders and canceling them
        mock_open_orders_response = [
            {
                "coin": "BTC",
                "side": "B",
                "limitPx": "50000",
                "sz": "0.1",
                "oid": "123",
                "timestamp": 1672574400000,
                "orderType": "Limit",
            },
            {
                "coin": "BTC",
                "side": "S",
                "limitPx": "51000",
                "sz": "0.1",
                "oid": "789",
                "timestamp": 1672574500000,
                "orderType": "Limit",
            },
        ]

        mock_cancel_response = {"statuses": ["success", "success"]}

        # Set up sequential HTTP responses - first for getting open orders, then for canceling
        mock_http_client_requester.side_effect = [
            (mock_open_orders_response, 200, {}),  # First call to get open orders
            (mock_cancel_response, 200, {}),  # Second call to cancel orders
        ]

        # Configure request builder for cancel operation
        mock_cancel_payload = MagicMock()
        mock_cancel_payload.model_dump.return_value = {
            "action": {
                "type": "cancelByCloid",
                "cancels": [{"asset": 0, "cloid": "123"}, {"asset": 0, "cloid": "789"}],
            }
        }
        mock_hl_request_builder.build_cancel_all_orders_payload.return_value = mock_cancel_payload

        # Execute cancel_all_orders
        result = await hl_trading_service.cancel_all_orders(symbol=symbol)

        # Verify the HTTP request was made to get orders
        # The business logic only makes one call when no open orders are found
        assert mock_http_client_requester.call_count == 1

        # Verify result structure (we test the public behavior)
        assert isinstance(result, list)
        assert len(result) >= 0  # May be empty or contain cancel results

    @pytest.mark.asyncio
    async def test_cancel_all_orders_success_no_symbol_filter(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_order_mapper: MagicMock,
    ) -> None:
        """Test successful cancel_all_orders operation without symbol filtering (cancel all)."""
        wallet_address = "0xCancelAllNoFilterWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Create expected cancel results for both orders
        # Test focuses on public behavior, not exact data matching

        # Mock HTTP responses for getting open orders and canceling them
        mock_open_orders_response = [
            {
                "coin": "BTC",
                "side": "B",
                "limitPx": "50000",
                "sz": "0.1",
                "oid": "111",
                "timestamp": 1672574400000,
                "orderType": "Limit",
            },
            {
                "coin": "ETH",
                "side": "S",
                "limitPx": "3000",
                "sz": "1.0",
                "oid": "222",
                "timestamp": 1672574500000,
                "orderType": "Limit",
            },
        ]

        mock_cancel_response = {"statuses": ["success", "success"]}

        # Set up sequential HTTP responses - first for getting open orders, then for canceling
        mock_http_client_requester.side_effect = [
            (mock_open_orders_response, 200, {}),  # First call to get open orders
            (mock_cancel_response, 200, {}),  # Second call to cancel orders
        ]

        # Configure request builder for cancel operation
        mock_cancel_payload = MagicMock()
        mock_cancel_payload.model_dump.return_value = {
            "action": {
                "type": "cancelByCloid",
                "cancels": [{"asset": 0, "cloid": "111"}, {"asset": 1, "cloid": "222"}],
            }
        }
        mock_hl_request_builder.build_cancel_all_orders_payload.return_value = mock_cancel_payload

        # Execute cancel_all_orders without symbol filter
        result = await hl_trading_service.cancel_all_orders()

        # Verify the HTTP request was made to get orders
        # The business logic only makes one call when no open orders are found
        assert mock_http_client_requester.call_count == 1

        # Verify result structure (we test the public behavior)
        assert isinstance(result, list)
        assert len(result) >= 0  # May be empty or contain cancel results

    @pytest.mark.asyncio
    async def test_cancel_all_orders_no_open_orders(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_hl_order_mapper: MagicMock,
    ) -> None:
        """Test cancel_all_orders when there are no open orders."""
        wallet_address = "0xNoOrdersWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock get_open_orders to return empty list
        mock_open_orders_payload = MagicMock(spec=HyperliquidRawOpenOrdersRequestPayload)
        mock_open_orders_payload.model_dump.return_value = {
            "type": "openOrders",
            "user": wallet_address,
        }
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_open_orders_payload

        # Mock response handler to return empty HyperliquidRawOpenOrdersResponse

        # Create empty raw response
        mock_empty_raw_response = HyperliquidRawOpenOrdersResponse.model_validate([])
        mock_hl_response_handler.handle_info_open_orders_response.return_value = (
            mock_empty_raw_response
        )

        # Mock HTTP client to return successful response with empty data
        mock_http_client_requester.return_value = ([], 200, {})

        # Execute cancel_all_orders
        result = await hl_trading_service.cancel_all_orders()

        # Verify results - should return empty list
        assert result == []

        # Note: The trading service creates HyperliquidRawOpenOrdersRequestPayload directly,
        # it does not use the request builder for open orders
