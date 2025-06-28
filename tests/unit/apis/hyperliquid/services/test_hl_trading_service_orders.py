"""Unit tests for HyperliquidTradingService order operations."""

from collections.abc import Callable
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeResponseData,
    HyperliquidRawExchangeStatusObject,
    HyperliquidRawExchangeStatusResting,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
    HyperliquidRawHistoricalOrderData,
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOpenOrdersArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order


# Unit tests for HyperliquidTradingService (moved from mislabeled integration tests)
# These are unit tests because they mock all dependencies and test individual methods

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.services.conftest_trading"]


class TestHyperliquidTradingServiceOrders:
    """Tests for the HyperliquidTradingService order operations."""

    # =============================================================================
    # INPUT VALIDATION TESTS (NEW - ITERATION 2)
    # =============================================================================

    @pytest.mark.asyncio
    async def test_place_order_empty_symbol_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test place_order raises ValueError for empty symbol."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="",  # Empty symbol should be rejected
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("100.0"),
                time_in_force=TimeInForce.GTC,
            )
            await hl_trading_service.place_order(args)

        assert "String cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_place_order_invalid_quantity_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test place_order raises ValueError for invalid quantity values."""
        hl_trading_service = make_hl_trading_service()

        # Test zero quantity
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="ETH",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.0"),  # Invalid: zero quantity
                price=Decimal("100.0"),
                time_in_force=TimeInForce.GTC,
            )
            await hl_trading_service.place_order(args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test negative quantity
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="ETH",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("-5.0"),  # Invalid: negative quantity
                price=Decimal("100.0"),
                time_in_force=TimeInForce.GTC,
            )
            await hl_trading_service.place_order(args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test infinite quantity
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="ETH",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("inf"),  # Invalid: infinite quantity
                price=Decimal("100.0"),
                time_in_force=TimeInForce.GTC,
            )
            await hl_trading_service.place_order(args)
        assert "Field 'quantity' must be a finite decimal" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_place_order_invalid_price_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
    ) -> None:
        """Test place_order raises ValueError for invalid price values."""
        hl_trading_service = make_hl_trading_service()

        # Mock the asset index lookup to return a valid index
        mock_get_asset_index_callable.return_value = 1

        # Mock the HTTP client to return a valid response (though we shouldn't reach this)
        mock_http_client_requester.return_value = (
            {"status": "ok", "response": {"type": "order", "data": {"statuses": []}}},
            200,
            {},
        )

        # Test zero price for LIMIT order - should be rejected
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="ETH",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("0.0"),  # Invalid: zero price for LIMIT order
                time_in_force=TimeInForce.GTC,
            )
            await hl_trading_service.place_order(args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test negative price
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="ETH",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("-50.0"),  # Invalid: negative price
                time_in_force=TimeInForce.GTC,
            )
            await hl_trading_service.place_order(args)
        assert "Input should be greater than 0" in str(exc_info.value)

        # Test infinite price
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="ETH",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("inf"),  # Invalid: infinite price
                time_in_force=TimeInForce.GTC,
            )
            await hl_trading_service.place_order(args)
        assert "Field 'price' must be a finite decimal" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_place_order_invalid_stop_price_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test place_order raises ValueError for invalid stop_price values when provided."""
        hl_trading_service = make_hl_trading_service()

        # Test negative stop_price
        with pytest.raises(ValueError) as exc_info:
            args = PlaceOrderArgs(
                symbol="ETH",
                side=OrderSide.BUY,
                order_type=OrderType.STOP_LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("100.0"),
                time_in_force=TimeInForce.GTC,
                stop_price=Decimal("-10.0"),  # Invalid: negative stop_price
            )
            await hl_trading_service.place_order(args)
        assert "Input should be greater than 0" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_empty_order_id_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test get_order raises ValueError for empty order_id."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            await hl_trading_service.get_order(args=GetOrderArgs(symbol="ETH", order_id=""))

        assert "String cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_invalid_string_order_id_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test get_order raises ValueError for invalid string order_id.

        Input validation errors are re-raised as ValueError, not wrapped in APIError.
        """
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            await hl_trading_service.get_order(
                args=GetOrderArgs(symbol="ETH", order_id="not_a_number"),
            )

        # The service re-raises input validation errors as ValueError
        assert "'order_id' must be a valid integer" in str(exc_info.value)
        assert "not_a_number" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_empty_symbol_when_provided_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test get_order raises ValueError for empty symbol when provided."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            await hl_trading_service.get_order(args=GetOrderArgs(symbol="", order_id="12345"))

        assert "String cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_open_orders_empty_symbol_when_provided_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test get_open_orders raises ValueError for empty symbol when provided."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            await hl_trading_service.get_open_orders(
                symbol="",  # Empty symbol should be rejected when provided
            )

        assert "'symbol' must be a non-empty string when provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_none_symbol_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test cancel_order raises ValueError for None symbol."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            args = CancelOrderArgs(
                order_id="12345",
                symbol=None,  # None symbol should be rejected
            )
            await hl_trading_service.cancel_order(args)

        assert "'symbol' parameter is required" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_empty_symbol_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test cancel_order raises ValueError for empty symbol."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            args = CancelOrderArgs(
                order_id="12345",
                symbol="",  # Empty symbol should be rejected
            )
            await hl_trading_service.cancel_order(args)

        assert "String cannot be empty" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_invalid_string_order_id_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test cancel_order raises ValueError for invalid string order_id."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            args = CancelOrderArgs(
                order_id="not_a_number",  # Invalid string order_id
                symbol="ETH",
            )
            await hl_trading_service.cancel_order(args)

        assert "'order_id' must be a valid integer" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_zero_order_id_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test cancel_order raises ValueError for zero order_id."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            args = CancelOrderArgs(
                order_id="0",  # Zero order_id should be rejected
                symbol="ETH",
            )
            await hl_trading_service.cancel_order(args)

        assert "'order_id' must be positive" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_negative_order_id_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test cancel_order raises ValueError for negative order_id."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValueError) as exc_info:
            args = CancelOrderArgs(
                order_id="-12345",  # Negative order_id should be rejected
                symbol="ETH",
            )
            await hl_trading_service.cancel_order(args)

        assert "'order_id' must be positive" in str(exc_info.value)

    # =============================================================================
    # EXISTING FUNCTIONALITY TESTS
    # =============================================================================

    @pytest.mark.asyncio
    async def test_place_order_http_client_returns_none_in_exchange_action(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test place_order when the exchange HTTP client returns None content."""
        symbol = "ETH"
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)
        mock_get_asset_index_callable.return_value = 0

        # Configure the mock to return a payload with the correct type attribute
        mock_payload = MagicMock()
        mock_payload.type = "order"  # Set the expected type value
        mock_hl_request_builder.build_place_order_payload.return_value = mock_payload

        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal(1),
                price=Decimal(100),
                time_in_force=TimeInForce.GTC,
            )
            await hl_trading_service.place_order(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Exchange action (order) returned invalid content" in exc_info.value.message
        mock_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_order_success(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test successful place_order operation."""
        symbol = "BTC"
        wallet_address = "0xSuccessWallet"
        quantity = Decimal("0.5")
        price = Decimal(50000)
        asset_index = 1

        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)
        mock_get_asset_index_callable.return_value = asset_index

        # Mock payload building
        mock_payload = MagicMock()
        mock_payload.type = "order"
        mock_payload_dict = {
            "action": {
                "type": "order",
                "orders": [
                    {
                        "a": asset_index,
                        "b": True,  # Buy
                        "p": str(price),
                        "s": str(quantity),
                        "r": False,  # Not reduce-only
                        "t": {"limit": {"tif": "Gtc"}},
                        "c": None,  # No client order ID
                    },
                ],
                "grouping": "na",
            },
            "nonce": 1234567890,
            "signature": {
                "r": "0xr_value",
                "s": "0xs_value",
                "v": 27,
            },
        }
        mock_payload.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_place_order_payload.return_value = mock_payload

        # Mock successful exchange response
        mock_response_content = {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {"statuses": [{"resting": {"oid": 123456}}]},
            },
        }
        mock_http_client_requester.return_value = (
            mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler to return raw Pydantic model (not internal Order)
        mock_raw_response = HyperliquidRawExchangeResponse(
            response=None,
            status="ok",
            data=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    HyperliquidRawExchangeStatusObject(
                        resting=HyperliquidRawExchangeStatusResting(oid=123456),
                        filled=None,
                        error=None,
                    ),
                ],
            ),
        )
        mock_hl_response_handler.handle_exchange_response.return_value = mock_raw_response

        # Mock the get_order method that gets called after successful place_order
        expected_order = Order(
            exchange_order_id="123456",
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=quantity,
            price=price,
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )

        # Mock the get_order method that place_order calls internally
        with patch.object(
            hl_trading_service,
            "get_order",
            return_value=expected_order,
        ) as mock_get_order:
            args = PlaceOrderArgs(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                price=price,
                time_in_force=TimeInForce.GTC,
            )
            result = await hl_trading_service.place_order(args)

            assert result == expected_order
            mock_get_asset_index_callable.assert_called_once_with(symbol)
            mock_hl_request_builder.build_place_order_payload.assert_called_once()
            mock_http_client_requester.assert_called_once_with(
                method="POST",
                endpoint="/exchange",
                data=mock_payload,
                is_signed=True,
                serialize_none_as_null=True,
            )
            # Fix the method call signature - handle_exchange_response takes
            mock_hl_response_handler.handle_exchange_response.assert_called_once_with(
                mock_response_content,
                action_type="order",
                status_code=200,
            )
            # Verify get_order was called with the returned OID
            mock_get_order.assert_called_once_with(GetOrderArgs(symbol=symbol, order_id="123456"))

    @pytest.mark.asyncio
    async def test_get_order_http_client_returns_none_in_info_request(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test get_order when the info HTTP client returns None content."""
        symbol = "ETH"
        order_id = "12345"
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Configure the mock to return a payload for order status

        mock_payload = HyperliquidRawOrderStatusRequestPayload(
            type="orderStatus",
            user=wallet_address,
            oid=int(order_id),
        )
        mock_hl_request_builder.build_order_status_payload.return_value = mock_payload

        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.get_order(args=GetOrderArgs(symbol=symbol, order_id=order_id))

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "No data received for order status for OID 12345, status: 200" in exc_info.value.message
        )
        mock_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_order_success(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_hl_trading_mapper: MagicMock,
    ) -> None:
        """Test successful get_order operation."""
        symbol = "BTC"
        order_id = "123456"
        wallet_address = "0xSuccessWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock payload building for order status
        mock_payload_model = HyperliquidRawOrderStatusRequestPayload(
            type="orderStatus",
            user=wallet_address,
            oid=int(order_id),
        )
        mock_hl_request_builder.build_order_status_payload.return_value = mock_payload_model

        # Mock successful order status response
        mock_response_content = {
            "order": {
                "oid": int(order_id),
                "cloid": None,
                "coin": "BTC",
                "side": "B",
                "limitPx": "50000.0",
                "sz": "0.5",
                "timestamp": 1234567890000,
                "orderType": "limit",
                "reduceOnly": False,
                "origSz": "0.5",
                "tif": "Gtc",
            },
            "status": "open",
            "statusTimestamp": 1234567890000,
        }
        mock_http_client_requester.return_value = (
            mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler to return historical order response

        mock_historical_order_data = HyperliquidRawHistoricalOrderData.model_validate(
            mock_response_content["order"],
        )
        mock_historical_order_response = HyperliquidRawHistoricalOrderResponse(
            order=mock_historical_order_data,
            status=str(mock_response_content["status"]),
            statusTimestamp=int(str(mock_response_content["statusTimestamp"])),
        )

        # For the mapper, create a full HyperliquidRawHistoricalOrder with all fields

        mock_historical_order = HyperliquidRawHistoricalOrder.model_validate({
            **cast("dict[str, Any]", mock_response_content["order"]),
            "status": mock_response_content["status"],
            "statusTimestamp": mock_response_content["statusTimestamp"],
        })
        mock_hl_response_handler.handle_info_order_status_response.return_value = (
            mock_historical_order_response
        )

        # Mock the mapper to return internal Order
        expected_order = Order(
            exchange_order_id=str(order_id),
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.5"),
            price=Decimal("50000.0"),
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_hl_trading_mapper.transform_raw_historical_order_to_internal.return_value = (
            expected_order
        )

        result = await hl_trading_service.get_order(
            args=GetOrderArgs(symbol=symbol, order_id=order_id),
        )

        assert result == expected_order
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data={"type": "orderStatus", "user": "0xSuccessWallet", "oid": 123456},
            is_signed=False,
        )
        mock_hl_response_handler.handle_info_order_status_response.assert_called_once_with(
            mock_response_content,
            user_address=wallet_address,
            order_id=int(order_id),
        )
        mock_hl_trading_mapper.transform_raw_historical_order_to_internal.assert_called_once_with(
            raw_historical_order=mock_historical_order,
            trigger=None,
        )

    @pytest.mark.asyncio
    async def test_get_open_orders_http_client_returns_none_in_info_request(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test get_open_orders when the info HTTP client returns None content."""
        symbol = "ETH"
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Configure the mock to return a payload for open orders

        mock_payload = HyperliquidRawOpenOrdersRequestPayload(
            type="openOrders",
            user=wallet_address,
        )
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_payload

        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.get_open_orders(symbol=symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for open orders, status: 200" in exc_info.value.message
        mock_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_open_orders_success(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_hl_trading_mapper: MagicMock,
    ) -> None:
        """Test successful get_open_orders operation."""
        symbol = "BTC"
        wallet_address = "0xSuccessWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock successful info response with multiple orders
        mock_response_content = [
            {
                "order": {
                    "oid": 123456,
                    "cloid": None,
                    "asset": "BTC",
                    "side": "B",
                    "limitPx": "50000.0",
                    "sz": "0.5",
                    "timestamp": 1234567890000,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "reduceOnly": False,
                    "remainingSz": "0.5",
                    "status": "open",
                    "statusTimestamp": 1234567890000,
                },
                "trigger": None,
            },
            {
                "order": {
                    "oid": 123457,
                    "cloid": None,
                    "asset": "BTC",
                    "side": "A",
                    "limitPx": "51000.0",
                    "sz": "0.3",
                    "timestamp": 1234567891000,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "reduceOnly": False,
                    "remainingSz": "0.3",
                    "status": "open",
                    "statusTimestamp": 1234567891000,
                },
                "trigger": None,
            },
        ]
        mock_http_client_requester.return_value = (
            mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock request builder to return expected payload

        mock_payload = HyperliquidRawOpenOrdersRequestPayload(
            type="openOrders",
            user=wallet_address,
        )
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_payload

        # Update mock response to use simple order format
        simple_mock_response_content = [
            {
                "coin": "BTC",
                "limitPx": "50000.0",
                "oid": 123456,
                "side": "B",
                "sz": "0.5",
                "timestamp": 1234567890000,
                "origSz": "0.5",
            },
            {
                "coin": "BTC",
                "limitPx": "51000.0",
                "oid": 123457,
                "side": "A",
                "sz": "0.3",
                "timestamp": 1234567891000,
                "origSz": "0.3",
            },
        ]
        mock_http_client_requester.return_value = (
            simple_mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler to return raw Pydantic model
        mock_raw_response = HyperliquidRawOpenOrdersResponse.model_validate(
            simple_mock_response_content,
        )
        mock_hl_response_handler.handle_info_open_orders_response.return_value = mock_raw_response

        # Mock the mapper to return internal Orders
        expected_orders = [
            Order(
                exchange_order_id="123456",
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity_requested=Decimal("0.5"),
                price=Decimal("50000.0"),
                exchange="hyperliquid_test_trading",
                time_in_force=TimeInForce.GTC,
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            ),
            Order(
                exchange_order_id="123457",
                symbol="BTC",
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity_requested=Decimal("0.3"),
                price=Decimal("51000.0"),
                exchange="hyperliquid_test_trading",
                time_in_force=TimeInForce.GTC,
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            ),
        ]
        mock_hl_trading_mapper.transform_raw_simple_open_order_to_internal.side_effect = (
            expected_orders
        )

        result = await hl_trading_service.get_open_orders(symbol=symbol)

        assert result == expected_orders
        assert len(result) == 2
        # Verify request builder was called correctly

        mock_hl_request_builder.build_open_orders_payload.assert_called_once_with(
            GetOpenOrdersArgs(wallet_address=wallet_address),
        )
        # Verify HTTP request was made with the mock payload
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_payload.model_dump(by_alias=True),
            is_signed=False,
        )
        mock_hl_response_handler.handle_info_open_orders_response.assert_called_once_with(
            simple_mock_response_content,
            user_address=wallet_address,
            status_code=200,
        )
        mock_hl_trading_mapper.transform_raw_simple_open_order_to_internal.assert_has_calls(
            [
                call(raw_simple_order=mock_raw_response.root[0]),
                call(raw_simple_order=mock_raw_response.root[1]),
            ],
            any_order=False,
        )

    @pytest.mark.asyncio
    async def test_cancel_order_http_client_returns_none_in_exchange_action(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test cancel_order when the exchange HTTP client returns None content."""
        symbol = "ETH"
        order_id = 111222
        wallet_address = "0xCancelWallet"
        asset_index = 2

        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)
        mock_get_asset_index_callable.return_value = asset_index

        # Configure the mock to return a cancel request with proper type attribute
        mock_cancel_request = MagicMock()
        mock_cancel_request.type = "cancel"
        mock_hl_request_builder.build_cancel_order_payload.return_value = mock_cancel_request

        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            args = CancelOrderArgs(order_id=str(order_id), symbol=symbol)
            await hl_trading_service.cancel_order(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Exchange action (cancel) returned invalid content" in exc_info.value.message
        mock_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancel_order_success(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test successful cancel_order operation."""
        symbol = "ETH"
        order_id = 111222
        wallet_address = "0xCancelSuccessWallet"
        asset_index = 2

        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)
        mock_get_asset_index_callable.return_value = asset_index

        # Configure the mock to return a cancel request with proper type attribute
        mock_cancel_request = MagicMock()
        mock_cancel_request.type = "cancel"
        mock_hl_request_builder.build_cancel_order_payload.return_value = mock_cancel_request

        # Mock successful exchange response
        mock_response_content = {
            "status": "ok",
            "response": {"type": "cancel", "data": {"statuses": ["success"]}},
        }
        mock_http_client_requester.return_value = (
            mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler to return raw Pydantic model (not boolean)
        mock_raw_response = HyperliquidRawExchangeResponse(
            response=None,
            status="ok",
            data=HyperliquidRawExchangeResponseData(type="cancel", statuses=["success"]),
        )
        mock_hl_response_handler.handle_exchange_response.return_value = mock_raw_response

        result = await hl_trading_service.cancel_order(
            args=CancelOrderArgs(order_id=str(order_id), symbol=symbol),
        )

        # cancel_order now returns CancelOrderResult, not boolean
        assert result.success is True
        assert result.order_id == str(order_id)
        assert result.symbol == symbol
        mock_get_asset_index_callable.assert_called_once_with(symbol)
        # Note: cancel_order creates HyperliquidRawCancelOrderAction directly,
        # doesn't use request builder
        mock_http_client_requester.assert_called_once()
        # The response handler is called with the mock's type attribute
        mock_hl_response_handler.handle_exchange_response.assert_called_once_with(
            mock_response_content,
            action_type=mock_cancel_request.type,
            status_code=200,
        )
