"""Unit tests for HyperliquidTradingService management operations."""

from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


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

        # Create mock raw open orders data matching HyperliquidRawSimpleOpenOrder format
        mock_raw_open_orders_data = [
            {
                "coin": "BTC",
                "limitPx": "50000",
                "oid": 123,
                "side": "A",
                "sz": "0.5",
                "timestamp": 1234567890,
                "origSz": "0.5",
            },
            {
                "coin": "ETH",
                "limitPx": "3000",
                "oid": 456,
                "side": "B",
                "sz": "2.0",
                "timestamp": 1234567890,
                "origSz": "2.0",
            },
            {
                "coin": "BTC",
                "limitPx": "51000",
                "oid": 789,
                "side": "A",
                "sz": "1.0",
                "timestamp": 1234567890,
                "origSz": "1.0",
            },
        ]
        mock_raw_response = HyperliquidRawOpenOrdersResponse.model_validate(
            mock_raw_open_orders_data,
        )
        mock_hl_response_handler.handle_info_open_orders_response.return_value = mock_raw_response

        # Mock HTTP client to return successful responses
        # First call: get open orders (returns list)
        # Second call: cancel batch orders (returns dict)
        mock_cancel_response = {
            "status": "ok",
            "response": None,
            "data": {"type": "cancel", "statuses": ["success", "success"]},
        }
        mock_http_client_requester.side_effect = [
            (mock_raw_open_orders_data, 200, {}),  # First call: get open orders
            (mock_cancel_response, 200, {}),  # Second call: cancel batch orders
        ]

        # Mock asset indices for batch cancellation (only BTC orders)
        mock_get_asset_index_callable.side_effect = [1, 1]  # BTC=1 for both BTC orders

        # Mock batch cancel response for 2 BTC orders
        from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
            HyperliquidRawExchangeResponse,
            HyperliquidRawExchangeResponseData,
        )

        mock_cancel_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            response=None,
            data=HyperliquidRawExchangeResponseData(type="cancel", statuses=["success", "success"]),
        )
        mock_hl_response_handler.handle_exchange_response.return_value = mock_cancel_raw_response

        # Mock batch cancel order payload
        from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
            HyperliquidApiCancelOrderRequest,
        )
        from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
            HyperliquidRawCancelItem,
        )

        mock_batch_cancel_payload = HyperliquidApiCancelOrderRequest(
            type="cancel",
            cancels=[
                HyperliquidRawCancelItem(a=1, o=123),  # BTC order 123
                HyperliquidRawCancelItem(a=1, o=789),  # BTC order 789
            ],
        )
        mock_hl_request_builder.build_batch_cancel_order_payload.return_value = (
            mock_batch_cancel_payload
        )

        # Mock the trading mapper to return proper Order objects
        from decimal import Decimal

        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
        from cyberdelta.core.models.market.order import Order

        # Create expected Order objects for the raw orders
        btc_order_1 = Order(
            exchange_order_id="123",
            symbol="BTC",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.5"),
            price=Decimal(50000),
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
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("2.0"),
            price=Decimal(3000),
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
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal(51000),
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Configure the trading mapper to return these orders in sequence
        mock_hl_trading_mapper.transform_raw_simple_open_order_to_internal.side_effect = [
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
        # Note: cancel_order creates HyperliquidRawCancelOrderAction directly, doesn't use
        # request builder
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

        # Create mock raw open orders data matching HyperliquidRawSimpleOpenOrder format
        mock_raw_open_orders_data = [
            {
                "coin": "BTC",
                "limitPx": "50000",
                "oid": 111,
                "side": "A",
                "sz": "0.5",
                "timestamp": 1234567890,
                "origSz": "0.5",
            },
            {
                "coin": "ETH",
                "limitPx": "3000",
                "oid": 222,
                "side": "B",
                "sz": "2.0",
                "timestamp": 1234567890,
                "origSz": "2.0",
            },
        ]
        mock_raw_response = HyperliquidRawOpenOrdersResponse.model_validate(
            mock_raw_open_orders_data,
        )
        mock_hl_response_handler.handle_info_open_orders_response.return_value = mock_raw_response

        # Mock HTTP client to return successful responses
        # First call: get open orders (returns list)
        # Second call: cancel batch orders (returns dict)
        mock_cancel_response = {
            "status": "ok",
            "response": None,
            "data": {"type": "cancel", "statuses": ["success", "success"]},
        }
        mock_http_client_requester.side_effect = [
            (mock_raw_open_orders_data, 200, {}),  # First call: get open orders
            (mock_cancel_response, 200, {}),  # Second call: cancel batch orders
        ]

        # Mock asset indices for batch cancellation (both orders)
        mock_get_asset_index_callable.side_effect = [1, 2]  # BTC=1, ETH=2

        # Mock batch cancel response for both orders
        from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
            HyperliquidRawExchangeResponse,
            HyperliquidRawExchangeResponseData,
        )

        mock_cancel_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            response=None,
            data=HyperliquidRawExchangeResponseData(type="cancel", statuses=["success", "success"]),
        )
        mock_hl_response_handler.handle_exchange_response.return_value = mock_cancel_raw_response

        # Mock batch cancel order payload for both orders
        from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
            HyperliquidApiCancelOrderRequest,
        )
        from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
            HyperliquidRawCancelItem,
        )

        mock_batch_cancel_payload = HyperliquidApiCancelOrderRequest(
            type="cancel",
            cancels=[
                HyperliquidRawCancelItem(a=1, o=111),  # BTC order 111
                HyperliquidRawCancelItem(a=2, o=222),  # ETH order 222
            ],
        )
        mock_hl_request_builder.build_batch_cancel_order_payload.return_value = (
            mock_batch_cancel_payload
        )

        # Mock the trading mapper to return proper Order objects
        from decimal import Decimal

        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
        from cyberdelta.core.models.market.order import Order

        # Create expected Order objects for the raw orders
        btc_order_1 = Order(
            exchange_order_id="111",
            symbol="BTC",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.5"),
            price=Decimal(50000),
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
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("2.0"),
            price=Decimal(3000),
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Configure the trading mapper to return these orders in sequence
        mock_hl_trading_mapper.transform_raw_simple_open_order_to_internal.side_effect = [
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
