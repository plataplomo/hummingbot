"""Unit tests for BackpackTradingService order management functionality."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import CancelOrderArgs, GetOrderArgs, PlaceOrderArgs
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.services.conftest_trading"]


class TestBackpackTradingServiceOrderManagement:
    """Tests for the BackpackTradingService order management functionality."""

    # =============================================================================
    # INPUT VALIDATION TESTS (NEW - ITERATION 2)
    # =============================================================================

    @pytest.mark.asyncio
    async def test_place_order_empty_symbol_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test place_order raises ValidationError for empty symbol (now from PlaceOrderArgs)."""
        with pytest.raises(ValidationError) as exc_info:
            args = PlaceOrderArgs(
                symbol="",  # Empty symbol should be rejected
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
            )
            await bp_trading_service.place_order(args=args)

        assert "String cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_place_order_invalid_quantity_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test place_order raises ValueError for invalid quantity values."""
        # Test zero quantity
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.0"),  # Invalid: zero quantity
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
            )
            await bp_trading_service.place_order(args=args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test negative quantity
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("-5.0"),  # Invalid: negative quantity
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
            )
            await bp_trading_service.place_order(args=args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test infinite quantity
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("inf"),  # Invalid: infinite quantity
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
            )
            await bp_trading_service.place_order(args=args)
        assert "must be a finite decimal" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_place_order_invalid_price_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test place_order raises ValueError for invalid price values when provided."""
        # Test zero price
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("0.0"),  # Invalid: zero price
            )
            await bp_trading_service.place_order(args=args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test negative price
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("-50.0"),  # Invalid: negative price
            )
            await bp_trading_service.place_order(args=args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test infinite price
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("inf"),  # Invalid: infinite price
            )
            await bp_trading_service.place_order(args=args)
        assert "must be a finite decimal" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_place_order_invalid_stop_price_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test place_order raises ValueError for invalid stop_price values when provided."""
        # Test negative stop_price
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.STOP_LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
                stop_price=Decimal("-10.0"),  # Invalid: negative stop_price
            )
            await bp_trading_service.place_order(args=args)
        assert "Input should be greater than 0" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_empty_order_id_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test cancel_order raises ValidationError for empty order_id."""
        with pytest.raises(ValidationError) as exc_info:
            args = CancelOrderArgs(
                order_id="",  # Empty order_id should be rejected
                symbol="SOL_USDC",
            )
            await bp_trading_service.cancel_order(args=args)

        assert "order_id" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_empty_symbol_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test cancel_order raises ValidationError for empty symbol."""
        with pytest.raises(ValidationError) as exc_info:
            args = CancelOrderArgs(
                order_id="12345",
                symbol="",  # Empty symbol should be rejected
            )
            await bp_trading_service.cancel_order(args=args)

        assert "symbol" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_empty_order_id_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test get_order raises ValueError for empty order_id."""
        with pytest.raises(ValueError) as exc_info:
            await bp_trading_service.get_order(args=GetOrderArgs(order_id="", symbol="SOL_USDC"))

        assert "String cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_none_symbol_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test get_order raises ValueError for None symbol."""
        with pytest.raises(ValueError) as exc_info:
            await bp_trading_service.get_order(args=GetOrderArgs(order_id="12345", symbol=None))

        assert "'symbol' parameter is required" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_empty_symbol_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test get_order raises ValueError for empty symbol."""
        with pytest.raises(ValueError) as exc_info:
            await bp_trading_service.get_order(args=GetOrderArgs(order_id="12345", symbol=""))

        assert "String cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_status_none_symbol_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test get_order_status raises ValueError for None symbol."""
        with pytest.raises(ValueError) as exc_info:
            await bp_trading_service.get_order_status(
                args=GetOrderArgs(order_id="12345", symbol=None),  # None symbol should be rejected
            )

        assert "'symbol' parameter is required" in str(exc_info.value)

    # =============================================================================
    # EXISTING FUNCTIONALITY TESTS
    # =============================================================================

    @pytest.mark.asyncio
    async def test_place_order_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test place_order successfully places an order."""
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC
        client_order_id = "client_order_123"

        mock_endpoint_path = "/api/v1/order"
        mock_payload = {
            "symbol": symbol,
            "side": side.value,
            "orderType": order_type.value,
            "quantity": str(quantity),
            "price": str(price),
            "clientOrderId": client_order_id,
            "timeInForce": time_in_force.value,
        }
        mock_raw_response_content = {
            "id": "12345",
            "clientId": client_order_id,
            "relatedOrderId": "order_123",
            "symbol": symbol,
            "side": side.value,
            "orderType": order_type.value,
            "quantity": str(quantity),
            "price": str(price),
            "executedQuantity": "0",
            "executedQuoteQuantity": "0",
            "triggerPrice": "0",
            "avgFillPrice": "0",
            "status": "New",
            "timeInForce": time_in_force.value,
            "triggerBy": "last",
            "reduceOnly": False,
            "postOnly": False,
            "selfTradePrevention": "cn",
            "createdAt": 1678886400000,
            "updatedAt": 1678886400000,
            "triggeredAt": None,
            "expiryReason": None,
            "origin": "API",
        }
        mock_status_code = 200
        mock_headers_from_client = MagicMock()

        mock_raw_order = BackpackRawOrder(
            id="12345",
            clientId=client_order_id,
            relatedOrderId="order_123",
            symbol=symbol,
            side="Bid",
            orderType=order_type.value,
            quantity=str(quantity),
            price=str(price),
            executedQuantity="0",
            executedQuoteQuantity="0",
            triggerPrice="0",
            avgFillPrice="0",
            status="NEW",
            timeInForce=time_in_force.value,
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

        # Mock a simple Order object result (the actual return type)
        mock_order_result = MagicMock()

        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,
        )
        mock_response_handler.handle_place_order_response.return_value = mock_raw_order

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_order_to_internal.return_value = mock_order_result

            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                client_order_id="123456",  # Use numeric string instead of alphanumeric
            )
            result = await bp_trading_service.place_order(args=args)

            mock_request_builder.build_place_order_payload.assert_called_once_with(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                client_order_id="123456",
                post_only=False,
                trigger_price=None,
            )
            mock_http_client_requester.assert_called_once_with(
                method="POST",
                endpoint=mock_endpoint_path,
                data=mock_payload,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )
            mock_response_handler.handle_place_order_response.assert_called_once_with(
                mock_raw_response_content,
            )
            mock_mapper.transform_raw_order_to_internal.assert_called_once_with(mock_raw_order)
            assert result == mock_order_result

    @pytest.mark.asyncio
    async def test_place_order_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test place_order when HTTP client returns None content."""
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        mock_endpoint_path = "/api/v1/order"
        mock_payload = {
            "symbol": symbol,
            "side": side.value,
            "orderType": order_type.value,
            "quantity": str(quantity),
            "price": str(price),
            "timeInForce": time_in_force.value,
        }

        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                args = PlaceOrderArgs(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    time_in_force=time_in_force,
                    price=price,
                )
                await bp_trading_service.place_order(args=args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                f"Place order for {symbol} returned invalid data (status: 200)"
                in exc_info.value.message
            )

            mock_request_builder.build_place_order_payload.assert_called_once_with(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                client_order_id=None,
                post_only=False,
                trigger_price=None,
            )
            mock_http_client_requester.assert_called_once_with(
                method="POST",
                endpoint=mock_endpoint_path,
                data=mock_payload,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
            )
            mock_response_handler.handle_place_order_response.assert_not_called()
            mock_mapper.transform_raw_order_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_place_order_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test place_order handles validation error from response handler."""
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        mock_payload = {"symbol": symbol, "side": side.value}
        mock_raw_response = {"invalid": "order_data"}

        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawOrder.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_place_order_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
            )
            await bp_trading_service.place_order(args=args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_place_order_unexpected_exception(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test place_order handles unexpected exception."""
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        mock_payload = {"symbol": symbol, "side": side.value}
        mock_raw_response = {"id": "123", "symbol": symbol}

        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_place_order_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
            )
            await bp_trading_service.place_order(args=args)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected service failure" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_cancel_order_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_order successfully cancels an order."""
        symbol = "SOL_USDC"
        order_id = "12345"

        mock_endpoint_path = "/api/v1/order"
        mock_payload = {
            "symbol": symbol,
            "orderId": order_id,
        }
        mock_raw_response_content = {
            "orderId": order_id,
            "symbol": symbol,
            "status": "CANCELLED",
        }
        mock_status_code = 200
        mock_headers_from_client = MagicMock()

        # The service returns a bool, not a complex result
        mock_cancel_result = True

        mock_request_builder.build_cancel_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,
        )
        mock_response_handler.handle_cancel_order_response.return_value = mock_cancel_result

        result = await bp_trading_service.cancel_order(
            args=CancelOrderArgs(order_id=order_id, symbol=symbol),
        )

        mock_request_builder.build_cancel_order_payload.assert_called_once_with(
            symbol=symbol,
            order_id=order_id,
        )
        mock_http_client_requester.assert_called_once_with(
            method="DELETE",
            endpoint=mock_endpoint_path,
            data=mock_payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        mock_response_handler.handle_cancel_order_response.assert_called_once_with(
            raw_response_content=mock_raw_response_content,
            order_id=order_id,
            symbol=symbol,
        )
        assert result == mock_cancel_result

    @pytest.mark.asyncio
    async def test_cancel_order_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_order when HTTP client returns None content."""
        symbol = "SOL_USDC"
        order_id = "12345"

        mock_endpoint_path = "/api/v1/order"
        mock_payload = {"symbol": symbol, "orderId": order_id}

        mock_request_builder.build_cancel_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.cancel_order(
                args=CancelOrderArgs(order_id=order_id, symbol=symbol),
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"No data received when cancelling order {order_id} ({symbol}), status: 200"
            in exc_info.value.message
        )

        mock_request_builder.build_cancel_order_payload.assert_called_once_with(
            symbol=symbol,
            order_id=order_id,
        )
        mock_http_client_requester.assert_called_once_with(
            method="DELETE",
            endpoint=mock_endpoint_path,
            data=mock_payload,
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )
        mock_response_handler.handle_cancel_order_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancel_order_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_order handles validation error from response handler."""
        symbol = "SOL_USDC"
        order_id = "12345"

        mock_payload = {"symbol": symbol, "orderId": order_id}
        mock_raw_response = {"invalid": "cancel_data"}

        mock_request_builder.build_cancel_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a simple ValidationError
        validation_error = ValidationError.from_exception_data(
            title="ValidationError",
            line_errors=[],
        )
        mock_response_handler.handle_cancel_order_response.side_effect = validation_error

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.cancel_order(
                args=CancelOrderArgs(order_id=order_id, symbol=symbol),
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed." in exc_info.value.message

    @pytest.mark.asyncio
    async def test_cancel_order_unexpected_exception(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_order handles unexpected exception."""
        symbol = "SOL_USDC"
        order_id = "12345"

        mock_payload = {"symbol": symbol, "orderId": order_id}
        mock_raw_response = {"orderId": order_id, "status": "CANCELLED"}

        mock_request_builder.build_cancel_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_cancel_order_response.side_effect = Exception(
            "Unexpected error",
        )

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.cancel_order(
                args=CancelOrderArgs(order_id=order_id, symbol=symbol),
            )

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected service failure" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_cancel_all_orders_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_all_orders successfully cancels orders for a given symbol."""
        symbol = "SOL_USDC"
        mock_payload = {"symbol": symbol}
        mock_raw_response_list = ["order1", "order2"]

        mock_request_builder.build_cancel_all_orders_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response_list, 200, {})

        result = await bp_trading_service.cancel_all_orders(symbol=symbol)

        mock_request_builder.build_cancel_all_orders_payload.assert_called_once_with(symbol=symbol)
        mock_http_client_requester.assert_called_once_with(
            method="DELETE",
            endpoint="/api/v1/orders",
            data={"symbol": "SOL_USDC"},
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

        # Verify the service builds CancelOrderResult objects correctly
        assert len(result) == 2
        assert all(cancel_result.success for cancel_result in result)
        assert result[0].order_id == "order1"
        assert result[1].order_id == "order2"
        assert all(cancel_result.symbol == symbol for cancel_result in result)

    @pytest.mark.asyncio
    async def test_cancel_all_orders_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_all_orders when HTTP client returns None content."""
        symbol = "SOL_USDC"
        mock_payload = {"symbol": symbol}

        mock_request_builder.build_cancel_all_orders_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        result = await bp_trading_service.cancel_all_orders(symbol=symbol)

        mock_request_builder.build_cancel_all_orders_payload.assert_called_once_with(symbol=symbol)
        mock_http_client_requester.assert_called_once_with(
            method="DELETE",
            endpoint="/api/v1/orders",
            data={"symbol": "SOL_USDC"},
            is_signed=True,
            endpoint_group="private",
            request_weight=1,
        )

        # Service returns empty list when no data received
        assert result == []

    @pytest.mark.asyncio
    async def test_place_order_with_optional_parameters(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test place_order with optional parameters like stop_price and post_only."""
        symbol = "SOL_USDC"
        side = OrderSide.SELL
        order_type = OrderType.LIMIT
        quantity = Decimal("5.0")
        price = Decimal("105.0")
        time_in_force = TimeInForce.IOC
        stop_price = Decimal("110.0")
        post_only = True

        mock_payload = {
            "symbol": symbol,
            "side": side.value,
            "orderType": order_type.value,
            "quantity": str(quantity),
            "price": str(price),
            "timeInForce": time_in_force.value,
            "triggerPrice": str(stop_price),
            "postOnly": post_only,
        }
        mock_raw_response_content = {
            "id": "67890",
            "clientId": None,
            "relatedOrderId": "order_456",
            "symbol": symbol,
            "side": side.value,
            "orderType": order_type.value,
            "quantity": str(quantity),
            "price": str(price),
            "executedQuantity": "0",
            "executedQuoteQuantity": "0",
            "triggerPrice": str(stop_price),
            "avgFillPrice": "0",
            "status": "NEW",
            "timeInForce": time_in_force.value,
            "triggerBy": "last",
            "reduceOnly": False,
            "postOnly": post_only,
            "selfTradePrevention": "cn",
            "createdAt": 1678886400000,
            "updatedAt": 1678886400000,
            "triggeredAt": None,
            "expiryReason": None,
            "origin": "API",
        }

        mock_raw_order = BackpackRawOrder(
            id="67890",
            clientId=None,
            relatedOrderId="order_456",
            symbol=symbol,
            side="Ask",
            orderType=order_type.value,
            quantity=str(quantity),
            price=str(price),
            executedQuantity="0",
            executedQuoteQuantity="0",
            triggerPrice=str(stop_price),
            avgFillPrice="0",
            status="NEW",
            timeInForce=time_in_force.value,
            triggerBy="last",
            reduceOnly=False,
            postOnly=post_only,
            selfTradePrevention="cn",
            createdAt=1678886400000,
            updatedAt=1678886400000,
            triggeredAt=None,
            expiryReason=None,
            origin="API",
        )
        mock_order_result = MagicMock()

        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response_content, 200, {})
        mock_response_handler.handle_place_order_response.return_value = mock_raw_order

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_order_to_internal.return_value = mock_order_result

            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                stop_price=stop_price,
                post_only=post_only,
            )
            result = await bp_trading_service.place_order(args=args)

            mock_request_builder.build_place_order_payload.assert_called_once_with(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                client_order_id=None,
                post_only=post_only,
                trigger_price=stop_price,
            )
            assert result == mock_order_result

    @pytest.mark.asyncio
    async def test_place_order_non_dict_response(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test place_order handles non-dict response."""
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        mock_payload = {"symbol": symbol, "side": side.value}
        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = ("invalid_string_response", 200, {})

        with pytest.raises(APIError) as exc_info:
            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
            )
            await bp_trading_service.place_order(args=args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Place order for {symbol} returned invalid data (status: 200)"
            in exc_info.value.message
        )

    @pytest.mark.asyncio
    async def test_cancel_all_orders_no_data(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test cancel_all_orders when no data is returned."""
        symbol = "SOL_USDC"

        mock_payload = {"symbol": symbol}
        mock_request_builder.build_cancel_all_orders_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (None, 200, {})

        # Based on service implementation, it returns empty list when no data received
        result = await bp_trading_service.cancel_all_orders(symbol=symbol)

        assert result == []
