"""
Unit tests for HyperliquidTradingService management operations.
"""

from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

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
        assert "Fetching open orders returned no content." in exc_info.value.message
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
        mock_hl_trading_mapper: MagicMock,
    ) -> None:
        """Test successful cancel_all_orders operation with symbol filtering."""
        wallet_address = "0xCancelAllWallet"
        symbol = "BTC"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock get_open_orders to return some orders
        mock_open_orders_payload = MagicMock(spec=HyperliquidRawOpenOrdersRequestPayload)
        mock_open_orders_payload.model_dump.return_value = {
            "type": "openOrders",
            "user": wallet_address,
        }
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_open_orders_payload

        # Mock response handler to return proper raw response object
        from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
            HyperliquidRawOpenOrdersResponse,
        )

        # Create mock raw open orders as dictionaries (not objects)
        mock_raw_open_orders_data = [
            {
                "order": {
                    "oid": 123,
                    "cloid": None,
                    "asset": "BTC",
                    "side": "A",
                    "limitPx": "50000",
                    "sz": "0.5",
                    "timestamp": 1234567890,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "reduceOnly": False,
                    "remainingSz": "0.5",
                    "status": "open",
                    "statusTimestamp": 1234567890,
                },
                "trigger": None,
            },
            {
                "order": {
                    "oid": 456,
                    "cloid": None,
                    "asset": "ETH",
                    "side": "B",
                    "limitPx": "3000",
                    "sz": "2.0",
                    "timestamp": 1234567890,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "reduceOnly": False,
                    "remainingSz": "2.0",
                    "status": "open",
                    "statusTimestamp": 1234567890,
                },
                "trigger": None,
            },
            {
                "order": {
                    "oid": 789,
                    "cloid": None,
                    "asset": "BTC",
                    "side": "A",
                    "limitPx": "51000",
                    "sz": "1.0",
                    "timestamp": 1234567890,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "reduceOnly": False,
                    "remainingSz": "1.0",
                    "status": "open",
                    "statusTimestamp": 1234567890,
                },
                "trigger": None,
            },
        ]
        mock_raw_response = HyperliquidRawOpenOrdersResponse.model_validate(
            mock_raw_open_orders_data
        )
        mock_hl_response_handler.handle_info_open_orders_response.return_value = mock_raw_response

        # Mock HTTP client to return successful response
        mock_http_client_requester.return_value = (mock_raw_open_orders_data, 200, {})

        # Mock asset indices
        mock_get_asset_index_callable.side_effect = [1, 2]  # BTC=1, ETH=2

        # Mock cancel response
        from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
            HyperliquidRawExchangeResponse,
            HyperliquidRawExchangeResponseData,
        )

        mock_cancel_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=HyperliquidRawExchangeResponseData(type="cancel", statuses=["success"]),
        )
        mock_hl_response_handler.handle_exchange_response.return_value = mock_cancel_raw_response

        # Mock the trading mapper to return proper Order objects
        from decimal import Decimal

        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
        from cyberdelta.core.models.market.order import Order

        # Create expected Order objects for the raw orders
        btc_order_1 = Order(
            exchange_order_id="123",
            symbol="BTC",
            side=OrderSide.SELL,  # "A" = Ask = Sell
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.5"),
            price=Decimal("50000"),
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        eth_order = Order(
            exchange_order_id="456",
            symbol="ETH",
            side=OrderSide.BUY,  # "B" = Bid = Buy
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("2.0"),
            price=Decimal("3000"),
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        btc_order_2 = Order(
            exchange_order_id="789",
            symbol="BTC",
            side=OrderSide.SELL,  # "A" = Ask = Sell
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("51000"),
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Configure the trading mapper to return these orders in sequence
        mock_hl_trading_mapper.transform_raw_order_to_internal.side_effect = [
            btc_order_1,
            eth_order,
            btc_order_2,
        ]

        # Execute cancel_all_orders
        result = await hl_trading_service.cancel_all_orders(symbol=symbol)

        # Verify results - should cancel 2 BTC orders (123, 789) but not ETH order (456)
        assert len(result) == 2
        assert all(cancel_result.success for cancel_result in result)

        # Verify get_open_orders was called (no request builder used for open orders)
        # Note: The service calls _get_open_orders_raw which doesn't use request builder
        # Verify cancel_order was called twice (for the 2 BTC orders)
        # Note: cancel_order creates HyperliquidRawCancelOrderAction directly, doesn't use request builder
        assert mock_get_asset_index_callable.call_count == 2

    @pytest.mark.asyncio
    async def test_cancel_all_orders_success_no_symbol_filter(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_trading_mapper: MagicMock,
    ) -> None:
        """Test successful cancel_all_orders operation without symbol filtering (cancel all)."""
        wallet_address = "0xCancelAllNoFilterWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock get_open_orders to return some orders
        mock_open_orders_payload = MagicMock(spec=HyperliquidRawOpenOrdersRequestPayload)
        mock_open_orders_payload.model_dump.return_value = {
            "type": "openOrders",
            "user": wallet_address,
        }
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_open_orders_payload

        # Mock response handler to return proper raw response object
        from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
            HyperliquidRawOpenOrdersResponse,
        )

        # Create mock raw open orders as dictionaries (not objects)
        mock_raw_open_orders_data = [
            {
                "order": {
                    "oid": 111,
                    "cloid": None,
                    "asset": "BTC",
                    "side": "A",
                    "limitPx": "50000",
                    "sz": "0.5",
                    "timestamp": 1234567890,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "reduceOnly": False,
                    "remainingSz": "0.5",
                    "status": "open",
                    "statusTimestamp": 1234567890,
                },
                "trigger": None,
            },
            {
                "order": {
                    "oid": 222,
                    "cloid": None,
                    "asset": "ETH",
                    "side": "B",
                    "limitPx": "3000",
                    "sz": "2.0",
                    "timestamp": 1234567890,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "reduceOnly": False,
                    "remainingSz": "2.0",
                    "status": "open",
                    "statusTimestamp": 1234567890,
                },
                "trigger": None,
            },
        ]
        mock_raw_response = HyperliquidRawOpenOrdersResponse.model_validate(
            mock_raw_open_orders_data
        )
        mock_hl_response_handler.handle_info_open_orders_response.return_value = mock_raw_response

        # Mock HTTP client to return successful response
        mock_http_client_requester.return_value = (mock_raw_open_orders_data, 200, {})

        # Mock asset indices
        mock_get_asset_index_callable.side_effect = [1, 2]  # BTC=1, ETH=2

        # Mock cancel response
        from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
            HyperliquidRawExchangeResponse,
            HyperliquidRawExchangeResponseData,
        )

        mock_cancel_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=HyperliquidRawExchangeResponseData(type="cancel", statuses=["success"]),
        )
        mock_hl_response_handler.handle_exchange_response.return_value = mock_cancel_raw_response

        # Mock the trading mapper to return proper Order objects
        from decimal import Decimal

        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
        from cyberdelta.core.models.market.order import Order

        # Create expected Order objects for the raw orders
        btc_order_1 = Order(
            exchange_order_id="111",
            symbol="BTC",
            side=OrderSide.SELL,  # "A" = Ask = Sell
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.5"),
            price=Decimal("50000"),
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        eth_order = Order(
            exchange_order_id="222",
            symbol="ETH",
            side=OrderSide.BUY,  # "B" = Bid = Buy
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("2.0"),
            price=Decimal("3000"),
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Configure the trading mapper to return these orders in sequence
        mock_hl_trading_mapper.transform_raw_order_to_internal.side_effect = [
            btc_order_1,
            eth_order,
        ]

        # Execute cancel_all_orders without symbol filter
        result = await hl_trading_service.cancel_all_orders()

        # Verify results - should cancel both orders
        assert len(result) == 2
        assert all(cancel_result.success for cancel_result in result)

        # Verify asset indices were called for both orders
        assert mock_get_asset_index_callable.call_count == 2

    @pytest.mark.asyncio
    async def test_cancel_all_orders_no_open_orders(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_hl_trading_mapper: MagicMock,
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
        from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
            HyperliquidRawOpenOrdersResponse,
        )

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
