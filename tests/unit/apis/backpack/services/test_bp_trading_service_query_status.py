"""
Unit tests for BackpackTradingService query and status functionality.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

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
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_open_orders successfully retrieves open orders."""
        symbol = "SOL_USDC"
        mock_endpoint_path = "/api/v1/orders"
        mock_params = {"symbol": symbol}
        mock_raw_response_content = [
            {
                "id": "12345",
                "clientId": "client_order_123",
                "relatedOrderId": "order_123",
                "symbol": symbol,
                "side": "Bid",
                "orderType": "Limit",
                "quantity": "10.0",
                "price": "100.0",
                "executedQuantity": "0",
                "executedQuoteQuantity": "0",
                "triggerPrice": "0",
                "avgFillPrice": "0",
                "status": "New",
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
        ]
        mock_status_code = 200
        mock_headers_from_client = MagicMock()

        mock_raw_orders = [
            BackpackRawOrder(
                id="12345",
                clientId="client_order_123",
                relatedOrderId="order_123",
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
        ]
        mock_internal_orders = [MagicMock()]

        mock_request_builder.build_get_open_orders_params.return_value = mock_params
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_open_orders_response.return_value = mock_raw_orders

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_order_to_internal.side_effect = mock_internal_orders

            result = await bp_trading_service.get_open_orders(symbol=symbol)

            mock_request_builder.build_get_open_orders_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
                rate_limiter_service=mock_rate_limiter_service,
            )
            mock_response_handler.handle_get_open_orders_response.assert_called_once_with(
                mock_raw_response_content, symbol
            )
            assert mock_mapper.transform_raw_order_to_internal.call_count == len(mock_raw_orders)
            assert result == mock_internal_orders

    @pytest.mark.asyncio
    async def test_get_open_orders_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_open_orders when HTTP client returns None content."""
        symbol = "SOL_USDC"
        mock_endpoint_path = "/api/v1/orders"
        mock_params = {"symbol": symbol}

        mock_request_builder.build_get_open_orders_params.return_value = mock_params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_open_orders(symbol=symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_msg_part = f"Get open orders for {symbol} returned invalid data"
            assert expected_msg_part in exc_info.value.message

            mock_request_builder.build_get_open_orders_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
                rate_limiter_service=mock_rate_limiter_service,
            )
            mock_response_handler.handle_get_open_orders_response.assert_not_called()
            mock_mapper.transform_raw_order_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_open_orders_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_open_orders handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol}
        mock_raw_response = [{"invalid": "order_data"}]

        mock_request_builder.build_get_open_orders_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawOrder.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_open_orders_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.get_open_orders(symbol=symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error getting open orders" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_open_orders_unexpected_exception(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_open_orders handles unexpected exception."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol}
        mock_raw_response = [{"id": "123", "symbol": symbol}]

        mock_request_builder.build_get_open_orders_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_open_orders_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.get_open_orders(symbol=symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error getting open orders" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_order_status successfully retrieves order status."""
        symbol = "SOL_USDC"
        order_id = "12345"
        mock_endpoint_path = "/api/v1/order"
        mock_params = {"symbol": symbol, "orderId": order_id}
        mock_raw_response_content = {
            "id": order_id,
            "clientId": "client_order_123",
            "relatedOrderId": "order_123",
            "symbol": symbol,
            "side": "Bid",
            "orderType": "Limit",
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
        mock_status_code = 200
        mock_headers_from_client = MagicMock()

        mock_raw_order = BackpackRawOrder(
            id=order_id,
            clientId="client_order_123",
            relatedOrderId="order_123",
            symbol=symbol,
            side="Bid",
            orderType="LIMIT",
            quantity="10.0",
            price="100.0",
            executedQuantity="5.0",
            executedQuoteQuantity="500.0",
            triggerPrice="0",
            avgFillPrice="100.0",
            status="PARTIALLY_FILLED",
            timeInForce="GTC",
            triggerBy="last",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention="cn",
            createdAt=1678886400000,
            updatedAt=1678886450000,
            triggeredAt=None,
            expiryReason=None,
            origin="API",
        )
        mock_internal_order = MagicMock()

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_order_status_response.return_value = mock_raw_order

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_order_to_internal.return_value = mock_internal_order

            result = await bp_trading_service.get_order_status(order_id=order_id, symbol=symbol)

            mock_request_builder.build_get_order_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=f"{mock_endpoint_path}/{order_id}",
                params=mock_params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
                rate_limiter_service=mock_rate_limiter_service,
            )
            mock_response_handler.handle_get_order_status_response.assert_called_once_with(
                mock_raw_response_content, order_id
            )
            mock_mapper.transform_raw_order_to_internal.assert_called_once_with(mock_raw_order)
            assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_get_order_status_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_order_status when HTTP client returns None content."""
        symbol = "SOL_USDC"
        order_id = "12345"
        mock_endpoint_path = "/api/v1/order"
        mock_params = {"symbol": symbol, "orderId": order_id}

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_order_status(symbol=symbol, order_id=order_id)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_msg_part = f"Get order {order_id} ({symbol}) returned invalid data"
            assert expected_msg_part in exc_info.value.message

            mock_request_builder.build_get_order_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=f"{mock_endpoint_path}/{order_id}",
                params=mock_params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
                rate_limiter_service=mock_rate_limiter_service,
            )
            mock_response_handler.handle_get_order_status_response.assert_not_called()
            mock_mapper.transform_raw_order_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_order_status_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_order_status handles validation error from response handler."""
        symbol = "SOL_USDC"
        order_id = "12345"
        mock_params = {"symbol": symbol, "orderId": order_id}
        mock_raw_response = {"invalid": "order_data"}

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawOrder.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_order_status_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.get_order_status(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert f"Unexpected error getting order {order_id} ({symbol})" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_status_unexpected_exception(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_order_status handles unexpected exception."""
        symbol = "SOL_USDC"
        order_id = "12345"
        mock_params = {"symbol": symbol, "orderId": order_id}
        mock_raw_response = {"id": order_id, "symbol": symbol}

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_order_status_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.get_order_status(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert f"Unexpected error getting order {order_id} ({symbol})" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_status_not_found(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_order_status when order is not found."""
        symbol = "SOL_USDC"
        order_id = "nonexistent_order"

        mock_params = {"symbol": symbol, "orderId": order_id}
        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (None, 404, {})

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.get_order_status(order_id=order_id, symbol=symbol)

        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert f"Order {order_id} for symbol {symbol} not found" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_order successfully retrieves an order."""
        symbol = "SOL_USDC"
        order_id = "12345"
        client_order_id = "client_order_123"
        mock_endpoint_path = "/api/v1/order"
        mock_params = {"symbol": symbol}
        mock_raw_response_content = {
            "id": order_id,
            "clientId": client_order_id,
            "relatedOrderId": "order_123",
            "symbol": symbol,
            "side": "Bid",
            "orderType": "Limit",
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
        mock_status_code = 200
        mock_headers_from_client = MagicMock()

        mock_raw_order = BackpackRawOrder(
            id=order_id,
            clientId=client_order_id,
            relatedOrderId="order_123",
            symbol=symbol,
            side="Bid",
            orderType="LIMIT",
            quantity="10.0",
            price="100.0",
            executedQuantity="5.0",
            executedQuoteQuantity="500.0",
            triggerPrice="0",
            avgFillPrice="100.0",
            status="PARTIALLY_FILLED",
            timeInForce="GTC",
            triggerBy="last",
            reduceOnly=False,
            postOnly=False,
            selfTradePrevention="cn",
            createdAt=1678886400000,
            updatedAt=1678886450000,
            triggeredAt=None,
            expiryReason=None,
            origin="API",
        )
        mock_internal_order = MagicMock()

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_order_status_response.return_value = mock_raw_order

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_order_to_internal.return_value = mock_internal_order

            result = await bp_trading_service.get_order(
                order_id=order_id, symbol=symbol, client_order_id=client_order_id
            )

            mock_request_builder.build_get_order_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=f"{mock_endpoint_path}/{order_id}",
                params=mock_params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
                rate_limiter_service=mock_rate_limiter_service,
            )
            mock_response_handler.handle_get_order_status_response.assert_called_once_with(
                mock_raw_response_content, order_id
            )
            mock_mapper.transform_raw_order_to_internal.assert_called_once_with(mock_raw_order)
            assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_get_order_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_order when HTTP client returns None content."""
        symbol = "SOL_USDC"
        order_id = "12345"
        mock_endpoint_path = "/api/v1/order"
        mock_params = {"symbol": symbol}

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_order(order_id=order_id, symbol=symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_msg_part = f"Get order {order_id} ({symbol}) returned invalid data"
            assert expected_msg_part in exc_info.value.message

            mock_request_builder.build_get_order_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=f"{mock_endpoint_path}/{order_id}",
                params=mock_params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
                rate_limiter_service=mock_rate_limiter_service,
            )
            mock_response_handler.handle_get_order_status_response.assert_not_called()
            mock_mapper.transform_raw_order_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_order_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_order handles validation error from response handler."""
        symbol = "SOL_USDC"
        order_id = "12345"
        mock_params = {"symbol": symbol}
        mock_raw_response = {"invalid": "order_data"}

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawOrder.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_order_status_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.get_order(order_id=order_id, symbol=symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error getting order" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_all_open_orders_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_all_open_orders successfully retrieves all open orders."""
        mock_endpoint_path = "/api/v1/orders"
        mock_params = None  # No symbol filter for all orders
        mock_raw_response_content = [
            {
                "id": "order_1",
                "clientId": "client_1",
                "relatedOrderId": "rel_1",
                "symbol": "SOL_USDC",
                "side": "Bid",
                "orderType": "Limit",
                "quantity": "10.0",
                "price": "20.0",
                "executedQuantity": "0",
                "executedQuoteQuantity": "0",
                "triggerPrice": "0",
                "avgFillPrice": "0",
                "status": "New",
                "timeInForce": "GTC",
                "triggerBy": "last",
                "reduceOnly": False,
                "postOnly": False,
                "selfTradePrevention": "cn",
                "createdAt": 1234567890000,
                "updatedAt": 1234567890000,
                "triggeredAt": None,
                "expiryReason": None,
                "origin": "API",
            },
            {
                "id": "order_2",
                "clientId": "client_2",
                "relatedOrderId": "rel_2",
                "symbol": "ETH_USDC",
                "side": "Ask",
                "orderType": "Market",
                "quantity": "5.0",
                "price": "0",
                "executedQuantity": "0",
                "executedQuoteQuantity": "0",
                "triggerPrice": "0",
                "avgFillPrice": "0",
                "status": "New",
                "timeInForce": "GTC",
                "triggerBy": "last",
                "reduceOnly": False,
                "postOnly": False,
                "selfTradePrevention": "cn",
                "createdAt": 1234567890000,
                "updatedAt": 1234567890000,
                "triggeredAt": None,
                "expiryReason": None,
                "origin": "API",
            },
        ]
        mock_status_code = 200
        mock_headers_from_client = MagicMock()

        mock_raw_orders = [
            BackpackRawOrder(
                id="order_1",
                clientId="client_1",
                relatedOrderId="rel_1",
                symbol="SOL_USDC",
                side="Bid",
                orderType="LIMIT",
                quantity="10.0",
                price="20.0",
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
                createdAt=1234567890000,
                updatedAt=1234567890000,
                triggeredAt=None,
                expiryReason=None,
                origin="API",
            ),
            BackpackRawOrder(
                id="order_2",
                clientId="client_2",
                relatedOrderId="rel_2",
                symbol="ETH_USDC",
                side="Ask",
                orderType="MARKET",
                quantity="5.0",
                price="0",
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
                createdAt=1234567890000,
                updatedAt=1234567890000,
                triggeredAt=None,
                expiryReason=None,
                origin="API",
            ),
        ]
        mock_internal_orders = [MagicMock(), MagicMock()]

        mock_request_builder.build_get_open_orders_params.return_value = mock_params
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,
        )
        mock_response_handler.handle_get_open_orders_response.return_value = mock_raw_orders

        with patch.object(bp_trading_service, "_trading_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_order_to_internal.side_effect = mock_internal_orders

            result = await bp_trading_service.get_all_open_orders()

            mock_request_builder.build_get_open_orders_params.assert_called_once_with(symbol=None)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=True,
                endpoint_group="private",
                request_weight=1,
                rate_limiter_service=mock_rate_limiter_service,
            )
            mock_response_handler.handle_get_open_orders_response.assert_called_once_with(
                mock_raw_response_content, None
            )
            assert mock_mapper.transform_raw_order_to_internal.call_count == len(mock_raw_orders)
            assert result == mock_internal_orders
