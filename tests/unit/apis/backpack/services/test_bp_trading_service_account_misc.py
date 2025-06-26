"""Unit tests for BackpackTradingService account and miscellaneous functionality."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetOpenOrdersParams,
    BackpackRawGetOrderParams,
)
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models.enums import CancelOrderResultStatus, OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.market.order import CancelOrderResult


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.services.conftest_trading"]


class TestBackpackTradingServiceAccountMisc:
    """Tests for the BackpackTradingService account and miscellaneous functionality."""

    @pytest.mark.asyncio
    async def test_service_with_custom_mapper_transforms_data_correctly(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test that service with custom mapper uses it to transform data."""
        service = BackpackTradingService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="test_exchange",
            mapper=mock_order_mapper,
        )

        # Set up mocks for a get_open_orders call
        symbol = "SOL_USDC"
        mock_raw_order = BackpackRawOrder(
            id="123",
            clientId="client_123",
            relatedOrderId="rel_123",
            symbol=symbol,
            side="Bid",
            orderType="LIMIT",
            quantity="10.0",
            price="100.0",
            executedQuantity="0",
            executedQuoteQuantity="0",
            triggerPrice="0",
            avgFillPrice="0",
            status="NEW",
            timeInForce="GTC",
            triggerBy="last",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention="cn",
            createdAt=1678886400000,
            updatedAt=1678886400000,
            triggeredAt=None,
            expiryReason=None,
            origin="API",
        )
        mock_custom_result = "custom_mapper_result"

        mock_request_builder.build_get_open_orders_params.return_value = (
            BackpackRawGetOpenOrdersParams(symbol=symbol)
        )
        mock_http_client_requester.return_value = ([{"id": "123"}], 200, {})
        mock_response_handler.handle_get_open_orders_response.return_value = [mock_raw_order]
        mock_order_mapper.transform_raw_order_to_internal.return_value = mock_custom_result

        # Test that the service uses the custom mapper
        result = await service.get_open_orders(symbol=symbol)

        # Verify the custom mapper was called and returned our custom result
        mock_order_mapper.transform_raw_order_to_internal.assert_called_once_with(mock_raw_order)
        assert len(result) == 1
        # The result is whatever the custom mapper returned
        assert result is not None

    @pytest.mark.asyncio
    async def test_service_with_default_mapper_transforms_data_correctly(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test that service with default mapper creates and uses a real mapper."""
        service = BackpackTradingService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="test_exchange",
            mapper=None,
        )

        # Set up mocks for a get_open_orders call
        symbol = "SOL_USDC"
        mock_raw_order = BackpackRawOrder(
            id="123",
            symbol=symbol,
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity="10.0",
            price="100.0",
            createdAt="2024-01-15T10:30:00Z",
            clientId=None,
            executedQuantity="0.0",
            executedQuoteQuantity="0.0",
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            relatedOrderId=None,
            avgFillPrice=None,
            triggerPrice=None,
            triggerBy=None,
            updatedAt=None,
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        mock_request_builder.build_get_open_orders_params.return_value = (
            BackpackRawGetOpenOrdersParams(symbol=symbol)
        )
        mock_http_client_requester.return_value = ([{"id": "123"}], 200, {})
        mock_response_handler.handle_get_open_orders_response.return_value = [mock_raw_order]

        # Test that the service can successfully transform data (indicating a working mapper)
        result = await service.get_open_orders(symbol=symbol)

        # Verify that we got a result, indicating the default mapper worked
        assert result is not None
        assert len(result) == 1
        # The result should be an internal order object, not the raw order
        assert type(result[0]).__name__ != "BackpackRawOrder"

    @pytest.mark.asyncio
    async def test_service_handles_authenticated_requests_correctly(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that the service properly handles authenticated trading requests."""
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        mock_payload = {"symbol": symbol, "side": side.value}
        # Convert internal OrderSide to Backpack side format
        # bp_side = "Bid" if side == OrderSide.BUY else "Ask"
        mock_raw_order = BackpackRawOrder(
            id="123",
            symbol=symbol,
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity=str(quantity),
            price=str(price),
            createdAt="2024-01-15T10:30:00Z",
            clientId=None,
            executedQuantity="0.0",
            executedQuoteQuantity="0.0",
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            relatedOrderId=None,
            avgFillPrice=None,
            triggerPrice=None,
            triggerBy=None,
            updatedAt=None,
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = ({"id": "123"}, 200, {})
        mock_response_handler.handle_place_order_response.return_value = mock_raw_order

        # Test that the service can place orders (requires authentication)
        place_order_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=time_in_force,
            price=price,
        )
        result = await bp_trading_service.place_order(args=place_order_args)

        # Verify the authenticated request was made correctly
        mock_http_client_requester.assert_called_once()
        call_args = mock_http_client_requester.call_args
        assert call_args[1]["is_signed"] is True
        assert call_args[1]["method"] == "POST"
        assert call_args[1]["endpoint"] == "/api/v1/order"
        assert result is not None

    @pytest.mark.asyncio
    async def test_service_handles_query_operations_correctly(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that the service properly handles query operations like getting orders."""
        symbol = "SOL_USDC"
        order_id = "12345"

        mock_raw_order = BackpackRawOrder(
            id=order_id,
            symbol=symbol,
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity="10.0",
            price="100.0",
            createdAt="2024-01-15T10:30:00Z",
            clientId=None,
            executedQuantity="0.0",
            executedQuoteQuantity="0.0",
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            relatedOrderId=None,
            avgFillPrice=None,
            triggerPrice=None,
            triggerBy=None,
            updatedAt=None,
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        mock_request_builder.build_get_order_params.return_value = BackpackRawGetOrderParams(
            symbol=symbol,
        )
        mock_http_client_requester.return_value = ({"id": order_id}, 200, {})
        mock_response_handler.handle_get_order_status_response.return_value = mock_raw_order

        # Test that the service can query order status
        result = await bp_trading_service.get_order_status(
            args=GetOrderArgs(order_id=order_id, symbol=symbol),
        )

        # Verify the query request was made correctly
        mock_http_client_requester.assert_called_once()
        call_args = mock_http_client_requester.call_args
        assert call_args[1]["is_signed"] is True
        assert call_args[1]["method"] == "GET"
        assert call_args[1]["endpoint"] == f"/api/v1/order/{order_id}"
        assert result is not None

    @pytest.mark.asyncio
    async def test_service_handles_cancellation_operations_correctly(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that the service properly handles order cancellation operations."""
        symbol = "SOL_USDC"
        order_id = "12345"

        mock_request_builder.build_cancel_order_payload.return_value = {
            "symbol": symbol,
            "orderId": order_id,
        }
        mock_http_client_requester.return_value = (
            {"orderId": order_id, "status": "CANCELLED"},
            200,
            {},
        )
        mock_response_handler.handle_cancel_order_response.return_value = CancelOrderResult(
            symbol=symbol,
            order_id=order_id,
            client_order_id=None,
            success=True,
            message=None,
            status=CancelOrderResultStatus.SUCCESS,
            raw_response=None,
        )

        # Test that the service can cancel orders
        result = await bp_trading_service.cancel_order(
            args=CancelOrderArgs(order_id=order_id, symbol=symbol),
        )

        # Verify the cancellation request was made correctly
        mock_http_client_requester.assert_called_once()
        call_args = mock_http_client_requester.call_args
        assert call_args[1]["is_signed"] is True
        assert call_args[1]["method"] == "DELETE"
        assert call_args[1]["endpoint"] == "/api/v1/order"
        assert result.success is True

    @pytest.mark.asyncio
    async def test_service_handles_bulk_operations_correctly(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that the service properly handles bulk operations like getting all open orders."""
        mock_raw_orders = [
            BackpackRawOrder(
                id="order_1",
                symbol="SOL_USDC",
                side="Buy",
                orderType="LIMIT",
                status="NEW",
                quantity="10.0",
                price="20.0",
                createdAt="2024-01-15T10:30:00Z",
                clientId=None,
                executedQuantity="0.0",
                executedQuoteQuantity="0.0",
                timeInForce="GTC",
                reduceOnly=False,
                postOnly=False,
                selfTradePrevention=None,
                relatedOrderId=None,
                avgFillPrice=None,
                triggerPrice=None,
                triggerBy=None,
                updatedAt=None,
                triggeredAt=None,
                expiryReason=None,
                origin=None,
            ),
            BackpackRawOrder(
                id="order_2",
                symbol="ETH_USDC",
                side="Sell",
                orderType="MARKET",
                status="NEW",
                quantity="5.0",
                price=None,
                createdAt="2024-01-15T10:30:00Z",
                clientId=None,
                executedQuantity="0.0",
                executedQuoteQuantity="0.0",
                timeInForce="GTC",
                reduceOnly=False,
                postOnly=False,
                selfTradePrevention=None,
                relatedOrderId=None,
                avgFillPrice=None,
                triggerPrice=None,
                triggerBy=None,
                updatedAt=None,
                triggeredAt=None,
                expiryReason=None,
                origin=None,
            ),
        ]

        mock_request_builder.build_get_open_orders_params.return_value = (
            BackpackRawGetOpenOrdersParams(symbol=None)
        )
        mock_http_client_requester.return_value = ([{"id": "order_1"}, {"id": "order_2"}], 200, {})
        mock_response_handler.handle_get_open_orders_response.return_value = mock_raw_orders

        # Test that the service can get all open orders
        result = await bp_trading_service.get_all_open_orders(args=GetAllOpenOrdersArgs())

        # Verify the bulk query request was made correctly
        mock_http_client_requester.assert_called_once()
        call_args = mock_http_client_requester.call_args
        assert call_args[1]["is_signed"] is True
        assert call_args[1]["method"] == "GET"
        assert call_args[1]["endpoint"] == "/api/v1/orders"
        assert result is not None
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_service_properly_configured_for_trading_operations(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that the service is properly configured to handle trading operations end-to-end."""
        # Test multiple operations to ensure the service is properly set up
        symbol = "SOL_USDC"

        # 1. Test getting open orders
        mock_request_builder.build_get_open_orders_params.return_value = (
            BackpackRawGetOpenOrdersParams(symbol=symbol)
        )
        mock_http_client_requester.return_value = ([], 200, {})
        mock_response_handler.handle_get_open_orders_response.return_value = []

        open_orders = await bp_trading_service.get_open_orders(symbol=symbol)
        assert open_orders == []

        # 2. Test placing an order
        mock_request_builder.build_place_order_payload.return_value = {"symbol": symbol}
        mock_raw_order = BackpackRawOrder(
            id="123",
            symbol=symbol,
            side="Buy",
            orderType="LIMIT",
            status="NEW",
            quantity="10.0",
            price="100.0",
            createdAt="2024-01-15T10:30:00Z",
            clientId=None,
            executedQuantity="0.0",
            executedQuoteQuantity="0.0",
            timeInForce="GTC",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention=None,
            relatedOrderId=None,
            avgFillPrice=None,
            triggerPrice=None,
            triggerBy=None,
            updatedAt=None,
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )
        mock_http_client_requester.return_value = ({"id": "123"}, 200, {})
        mock_response_handler.handle_place_order_response.return_value = mock_raw_order

        place_order_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("100.0"),
        )
        placed_order = await bp_trading_service.place_order(args=place_order_args)
        assert placed_order is not None

        # 3. Test cancelling an order
        mock_request_builder.build_cancel_order_payload.return_value = {
            "symbol": symbol,
            "orderId": "123",
        }
        mock_http_client_requester.return_value = ({"status": "CANCELLED"}, 200, {})
        mock_response_handler.handle_cancel_order_response.return_value = CancelOrderResult(
            symbol=symbol,
            order_id="123",
            client_order_id=None,
            success=True,
            message=None,
            status=CancelOrderResultStatus.SUCCESS,
            raw_response=None,
        )

        cancelled = await bp_trading_service.cancel_order(
            args=CancelOrderArgs(order_id="123", symbol=symbol),
        )
        assert cancelled.success is True

        # If we reach here, the service is properly configured for all trading operations
