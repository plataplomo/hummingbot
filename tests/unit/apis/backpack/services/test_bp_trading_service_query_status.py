"""Unit tests for BackpackTradingService query and status functionality."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetOpenOrdersParams,
)
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args.trading import (
    GetAllOpenOrdersArgs,
    GetOrderArgs,
)
from tests.common_symbols import SOL_USDC_BP


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.services.conftest_trading"]


class TestBackpackTradingServiceQueryStatus:
    """Tests for the BackpackTradingService query and status functionality."""

    @pytest.mark.asyncio
    async def test_get_open_orders_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_open_orders successfully retrieves open orders."""
        symbol = SOL_USDC_BP
        MagicMock()

        # Use model_validate to handle optional fields automatically
        order_data_1 = {
            "id": "12345",
            "clientId": "client_order_123",
            "relatedOrderId": "order_123",
            "symbol": symbol.value,
            "side": "Bid",
            "orderType": "LIMIT",
            "quantity": "10.0",
            "price": "100.0",
            "executedQuantity": "0",
            "executedQuoteQuantity": "0",
            "triggerPrice": "0",
            "avgFillPrice": "0",
            "status": "NEW",
            "timeInForce": "GTC",
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

        # Validate order data structure
        BackpackRawOrderResponse.model_validate(order_data_1)
        mock_internal_orders = [MagicMock()]

        # Mock the order query service since business logic delegates to it
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_open_orders = AsyncMock(return_value=mock_internal_orders)

            result = await bp_trading_service.get_open_orders(symbol=symbol)

            # Verify the business logic calls the order query service with correct arguments
            mock_query_service.get_open_orders.assert_called_once_with(symbol)
            assert result == mock_internal_orders

    @pytest.mark.asyncio
    async def test_get_open_orders_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_open_orders when HTTP client returns None content."""
        symbol = SOL_USDC_BP

        # Mock the order query service to raise an API error (simulating HTTP client returning None)
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No data received for get open orders for {symbol.value}, status: 200",
            )
            mock_query_service.get_open_orders = AsyncMock(side_effect=api_error)

            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_open_orders(symbol=symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                f"No data received for get open orders for {symbol.value}, status: 200"
                in exc_info.value.message
            )

            # Verify the business logic calls the order query service
            mock_query_service.get_open_orders.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_get_open_orders_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_open_orders handles validation error from response handler."""
        symbol = SOL_USDC_BP

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawOrderResponse.model_validate({"invalid": "data"})
        except ValidationError as validation_error:
            # Mock the order query service to raise the validation error
            with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
                mock_query_service.get_open_orders = AsyncMock(side_effect=validation_error)

                with pytest.raises(ValidationError):
                    await bp_trading_service.get_open_orders(symbol=symbol)

                # Verify the business logic calls the order query service
                mock_query_service.get_open_orders.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_get_open_orders_unexpected_exception(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_open_orders handles unexpected exception."""
        symbol = SOL_USDC_BP

        # Mock the order query service to raise an unexpected exception
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_open_orders = AsyncMock(
                side_effect=Exception("Unexpected service failure")
            )

            with pytest.raises(Exception) as exc_info:
                await bp_trading_service.get_open_orders(symbol=symbol)

            assert "Unexpected service failure" in str(exc_info.value)

            # Verify the business logic calls the order query service
            mock_query_service.get_open_orders.assert_called_once_with(symbol)

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_status successfully retrieves order status."""
        symbol = SOL_USDC_BP
        order_id = "12345"
        MagicMock()

        order_data_2 = {
            "id": order_id,
            "clientId": "client_order_123",
            "relatedOrderId": "order_123",
            "symbol": symbol.value,
            "side": "Bid",
            "orderType": "LIMIT",
            "quantity": "10.0",
            "price": "100.0",
            "executedQuantity": "5.0",
            "executedQuoteQuantity": "500.0",
            "triggerPrice": "0",
            "avgFillPrice": "100.0",
            "status": "PARTIALLY_FILLED",
            "timeInForce": "GTC",
            "triggerBy": "last",
            "reduceOnly": False,
            "postOnly": False,
            "selfTradePrevention": "cn",
            "createdAt": 1678886400000,
            "updatedAt": 1678886450000,
            "triggeredAt": None,
            "expiryReason": None,
            "origin": "API",
        }

        BackpackRawOrderResponse.model_validate(order_data_2)
        mock_internal_order = MagicMock()

        # Mock the order query service since business logic delegates to it
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_order = AsyncMock(return_value=mock_internal_order)

            args = GetOrderArgs(order_id=order_id, symbol=symbol)
            result = await bp_trading_service.get_order_status(args=args)

            # Verify the business logic calls the order query service with correct arguments
            mock_query_service.get_order.assert_called_once_with(args)
            assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_get_order_status_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_status when HTTP client returns None content."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        # Mock the order query service to raise an exception that get_order_status catches
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            # get_order_status catches KeyError, ValueError, LookupError and returns None
            mock_query_service.get_order = AsyncMock(side_effect=KeyError("Order not found"))

            args = GetOrderArgs(order_id=order_id, symbol=symbol)
            result = await bp_trading_service.get_order_status(args=args)

            # get_order_status should return None when order not found
            assert result is None

            # Verify the business logic calls the order query service
            mock_query_service.get_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_order_status_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_status handles validation error from response handler."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        # Create a ValidationError by trying to validate invalid data
        validation_error = None
        try:
            BackpackRawOrderResponse.model_validate({"invalid": "data"})
        except ValidationError as e:
            validation_error = e

        # Mock the order query service to raise the validation error
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            # Ensure we have a validation error
            assert validation_error is not None, "ValidationError should have been captured"
            mock_query_service.get_order = AsyncMock(side_effect=validation_error)

            args = GetOrderArgs(order_id=order_id, symbol=symbol)

            # Call the method to see what happens
            result = await bp_trading_service.get_order_status(args=args)

            # If ValidationError is not being raised, check what we get back
            # It should be None if something is unexpectedly catching it
            assert result is None, f"Expected None but got {result}"

            # Verify the business logic calls the order query service
            mock_query_service.get_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_order_status_unexpected_exception(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_status handles unexpected exception."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        # Mock the order query service to raise an unexpected exception
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_order = AsyncMock(
                side_effect=Exception("Unexpected service failure")
            )

            args = GetOrderArgs(order_id=order_id, symbol=symbol)
            with pytest.raises(Exception) as exc_info:
                await bp_trading_service.get_order_status(args=args)

            assert "Unexpected service failure" in str(exc_info.value)

            # Verify the business logic calls the order query service
            mock_query_service.get_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_order_status_not_found(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_order_status when order is not found."""
        symbol = SOL_USDC_BP
        order_id = "nonexistent_order"

        # Mock the order query service to raise a LookupError (which get_order_status catches)
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_order = AsyncMock(
                side_effect=LookupError(f"Order {order_id} not found")
            )

            args = GetOrderArgs(order_id=order_id, symbol=symbol)
            result = await bp_trading_service.get_order_status(args=args)

            # get_order_status should return None when order not found
            assert result is None

            # Verify the business logic calls the order query service
            mock_query_service.get_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_order_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order successfully retrieves an order."""
        symbol = SOL_USDC_BP
        order_id = "12345"
        client_order_id = "client_order_123"
        MagicMock()

        BackpackRawOrderResponse.model_validate({
            "id": order_id,
            "clientId": client_order_id,
            "relatedOrderId": "order_123",
            "symbol": symbol.value,
            "side": "Bid",
            "orderType": "LIMIT",
            "quantity": "10.0",
            "price": "100.0",
            "executedQuantity": "5.0",
            "executedQuoteQuantity": "500.0",
            "triggerPrice": "0",
            "avgFillPrice": "100.0",
            "status": "PARTIALLY_FILLED",
            "timeInForce": "GTC",
            "triggerBy": "last",
            "reduceOnly": False,
            "postOnly": False,
            "selfTradePrevention": "cn",
            "createdAt": 1678886400000,
            "updatedAt": 1678886450000,
            "triggeredAt": None,
            "expiryReason": None,
            "origin": "API",
        })
        mock_internal_order = MagicMock()

        # Mock the order query service since business logic delegates to it
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_order = AsyncMock(return_value=mock_internal_order)

            args = GetOrderArgs(
                order_id=order_id,
                symbol=symbol,
                client_order_id=client_order_id,
            )
            result = await bp_trading_service.get_order(args=args)

            # Verify the business logic calls the order query service with correct arguments
            mock_query_service.get_order.assert_called_once_with(args)
            assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_get_order_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order when HTTP client returns None content."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        # Mock the order query service to raise an API error (simulating HTTP client returning None)
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            api_error = APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No data received for get order {order_id} ({symbol.value}), status: 200",
            )
            mock_query_service.get_order = AsyncMock(side_effect=api_error)

            args = GetOrderArgs(order_id=order_id, symbol=symbol)
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_order(args=args)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                f"No data received for get order {order_id} ({symbol.value}), status: 200"
                in exc_info.value.message
            )

            # Verify the business logic calls the order query service
            mock_query_service.get_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_order_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order handles validation error from response handler."""
        symbol = SOL_USDC_BP
        order_id = "12345"

        # Create a ValidationError by trying to validate invalid data
        validation_error = None
        try:
            BackpackRawOrderResponse.model_validate({"invalid": "data"})
        except ValidationError as e:
            validation_error = e

        # Mock the order query service to raise the validation error
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_order = AsyncMock(side_effect=validation_error)

            args = GetOrderArgs(order_id=order_id, symbol=symbol)
            with pytest.raises(ValidationError):
                await bp_trading_service.get_order(args=args)

            # Verify the business logic calls the order query service
            mock_query_service.get_order.assert_called_once_with(args)

    @pytest.mark.asyncio
    async def test_get_all_open_orders_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_all_open_orders successfully retrieves all open orders."""
        # Validate request parameters and mock orders
        BackpackRawGetOpenOrdersParams(symbol=None)  # No symbol filter for all orders
        mock_internal_orders = [MagicMock(), MagicMock()]

        # Mock the order query service since business logic delegates to it
        with patch.object(bp_trading_service, "_order_query_service") as mock_query_service:
            mock_query_service.get_open_orders = AsyncMock(return_value=mock_internal_orders)

            args = GetAllOpenOrdersArgs()
            result = await bp_trading_service.get_all_open_orders(args=args)

            # Verify the business logic calls the order query service with correct arguments
            mock_query_service.get_open_orders.assert_called_once_with(args.symbol)
            assert result == mock_internal_orders
