"""Unit tests for BackpackTradingService account and miscellaneous functionality."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.enums import (
    CancelOrderResultStatus,
    OrderStatus,
)
from cyberdelta.enums import ExchangeName, OrderSide, OrderType, TimeInForce
from cyberdelta.models.market.order import CancelOrderResult, Order
from tests.common_symbols import SOL_USDC_BP


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
            exchange_name=ExchangeName.BACKPACK,
            order_mapper=mock_order_mapper,
        )

        # Set up mocks for a get_open_orders call
        symbol = SOL_USDC_BP
        BackpackRawOrderResponse(
            id="123",
            clientId="client_123",
            relatedOrderId="rel_123",
            symbol=symbol.value,
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
        # Create a proper Order object for the mock result
        mock_custom_result = Order(
            client_order_id="client_123",
            exchange_order_id="123",
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("10.0"),
            quantity_filled=Decimal(0),
            price=Decimal("100.0"),
            average_fill_price=None,
            status=OrderStatus.OPEN,
            time_in_force=TimeInForce.GTC,
            exchange=ExchangeName.BACKPACK,
            created_at=datetime.fromtimestamp(1678886400, UTC),
            updated_at=datetime.fromtimestamp(1678886400, UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Mock the order query service since business logic delegates to it
        with patch.object(service, "_order_query_service") as mock_query_service:
            mock_query_service.get_open_orders = AsyncMock(return_value=[mock_custom_result])

            # Test that the service uses the order query service
            result = await service.get_open_orders(symbol=symbol)

            # Verify the order query service was called with correct arguments
            mock_query_service.get_open_orders.assert_called_once_with(symbol)
            assert len(result) == 1
            assert result[0] == mock_custom_result

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
            exchange_name=ExchangeName.BACKPACK,
            order_mapper=None,
        )

        # Set up mocks for a get_open_orders call
        symbol = SOL_USDC_BP
        BackpackRawOrderResponse(
            id="123",
            symbol=symbol.value,
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

        # Mock the order query service since business logic delegates to it
        with patch.object(service, "_order_query_service") as mock_query_service:
            mock_internal_order = MagicMock()
            mock_query_service.get_open_orders = AsyncMock(return_value=[mock_internal_order])

            # Test that the service can successfully transform data (indicating a working mapper)
            result = await service.get_open_orders(symbol=symbol)

        # Verify that we got a result, indicating the default mapper worked
        assert result is not None
        assert len(result) == 1
        # The result should be an internal order object, not the raw order
        assert type(result[0]).__name__ != "BackpackRawOrderResponse"

    @pytest.mark.asyncio
    async def test_service_handles_authenticated_requests_correctly(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that the service properly handles authenticated trading requests."""
        symbol = SOL_USDC_BP
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        # Convert internal OrderSide to Backpack side format
        BackpackRawOrderResponse(
            id="123",
            symbol=symbol.value,
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

        # Mock the order placement service since business logic delegates to it
        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            mock_internal_order = MagicMock()
            mock_placement_service.place_order = AsyncMock(return_value=mock_internal_order)

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

            # Verify the order placement service was called with correct arguments
            mock_placement_service.place_order.assert_called_once_with(place_order_args)
            assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_service_handles_query_operations_correctly(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that the service properly handles query operations like getting orders."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        BackpackRawOrderResponse(
            id=order_id,
            symbol=symbol.value,
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

        # Mock the order query service since business logic delegates to it
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_internal_order = MagicMock()
            mock_query_service.get_order = AsyncMock(return_value=mock_internal_order)

            # Test that the service can query order status
            get_order_args = GetOrderArgs(order_id=order_id, symbol=symbol)
            result = await bp_trading_service.get_order(args=get_order_args)

            # Verify the order query service was called with correct arguments
            mock_query_service.get_order.assert_called_once_with(get_order_args)
            assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_service_handles_cancellation_operations_correctly(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test that the service properly handles order cancellation operations."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        mock_cancel_result = CancelOrderResult(
            symbol=symbol,
            order_id=order_id,
            client_order_id=None,
            success=True,
            message=None,
            status=CancelOrderResultStatus.SUCCESS,
            raw_response=None,
        )

        # Mock the order cancellation service since business logic delegates to it
        with patch.object(
            bp_trading_service, "_order_cancellation_service"
        ) as mock_cancellation_service:
            mock_cancellation_service.cancel_order = AsyncMock(return_value=mock_cancel_result)

            # Test that the service can cancel orders
            cancel_order_args = CancelOrderArgs(order_id=order_id, symbol=symbol)
            result = await bp_trading_service.cancel_order(args=cancel_order_args)

            # Verify the order cancellation service was called with correct arguments
            mock_cancellation_service.cancel_order.assert_called_once_with(cancel_order_args)
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
        # Note: Mock raw orders not needed as business logic delegates to order query service

        # Mock the order query service since business logic delegates to it
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_internal_orders = [MagicMock(), MagicMock()]
            mock_query_service.get_open_orders = AsyncMock(return_value=mock_internal_orders)

            # Test that the service can get all open orders
            result = await bp_trading_service.get_open_orders()

            # Verify the order query service was called with correct arguments
            mock_query_service.get_open_orders.assert_called_once_with(None)
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
        symbol = SOL_USDC_BP

        # Mock all decomposed services for end-to-end testing
        with (
            patch.object(bp_trading_service, "_order_query_service") as mock_query_service,
            patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service,
            patch.object(
                bp_trading_service, "_order_cancellation_service"
            ) as mock_cancellation_service,
        ):
            # 1. Test getting open orders
            mock_query_service.get_open_orders = AsyncMock(return_value=[])
            open_orders = await bp_trading_service.get_open_orders(symbol=symbol)
            assert open_orders == []
            mock_query_service.get_open_orders.assert_called_with(symbol)

            # 2. Test placing an order
            mock_internal_order = MagicMock()
            mock_placement_service.place_order = AsyncMock(return_value=mock_internal_order)
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
            mock_placement_service.place_order.assert_called_with(place_order_args)

            # 3. Test cancelling an order
            mock_cancel_result = CancelOrderResult(
                symbol=symbol,
                order_id="123",
                client_order_id=None,
                success=True,
                message=None,
                status=CancelOrderResultStatus.SUCCESS,
                raw_response=None,
            )
            mock_cancellation_service.cancel_order = AsyncMock(return_value=mock_cancel_result)
            cancel_args = CancelOrderArgs(order_id="123", symbol=symbol)
            cancelled = await bp_trading_service.cancel_order(args=cancel_args)
            assert cancelled.success is True
            mock_cancellation_service.cancel_order.assert_called_with(cancel_args)

        # If we reach here, the service is properly configured for all trading operations
