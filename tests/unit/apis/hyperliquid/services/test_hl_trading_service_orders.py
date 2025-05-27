"""
Unit tests for HyperliquidTradingService order operations.
"""

from collections.abc import Callable
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeResponseData,
    HyperliquidRawExchangeStatusObject,
    HyperliquidRawExchangeStatusResting,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.market.order import Order

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.services.conftest_trading"]


class TestHyperliquidTradingServiceOrders:
    """Tests for the HyperliquidTradingService order operations."""

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
            await hl_trading_service.place_order(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1"),
                price=Decimal("100"),
                time_in_force=TimeInForce.GTC,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Exchange action (order) returned no content" in exc_info.value.message
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
        price = Decimal("50000")
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
                    }
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
            status="ok",
            data=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    HyperliquidRawExchangeStatusObject(
                        resting=HyperliquidRawExchangeStatusResting(oid=123456),
                        filled=None,
                        error=None,
                    )
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
            hl_trading_service, "get_order", return_value=expected_order
        ) as mock_get_order:
            result = await hl_trading_service.place_order(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                price=price,
                time_in_force=TimeInForce.GTC,
            )

            assert result == expected_order
            mock_get_asset_index_callable.assert_called_once_with(symbol)
            mock_hl_request_builder.build_place_order_payload.assert_called_once()
            mock_http_client_requester.assert_called_once_with(
                method="POST",
                endpoint="/exchange",
                data=mock_payload_dict,
                is_signed=True,
                serialize_none_as_null=True,
            )
            # Fix the method call signature - handle_exchange_response takes (content, action_type)
            mock_hl_response_handler.handle_exchange_response.assert_called_once_with(
                mock_response_content, action_type="order"
            )
            # Verify get_order was called with the returned OID
            mock_get_order.assert_called_once_with(symbol=symbol, order_id=123456)

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
        order_id = 12345
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Configure the mock to return a payload
        mock_payload = {"type": "openOrders", "user": wallet_address}
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_payload

        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.get_order(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for order status for OID 12345." in exc_info.value.message
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
        order_id = 123456
        wallet_address = "0xSuccessWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock payload building for order status
        mock_payload_model = HyperliquidRawOrderStatusRequestPayload(
            type="orderStatus", user=wallet_address, oid=order_id
        )
        mock_hl_request_builder.build_order_status_payload.return_value = mock_payload_model

        # Mock successful order status response
        mock_response_content = {
            "order": {
                "oid": order_id,
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
            }
        }
        mock_http_client_requester.return_value = (
            mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler to return historical order response
        from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
            HyperliquidRawHistoricalOrder,
            HyperliquidRawHistoricalOrderResponse,
        )

        mock_historical_order = HyperliquidRawHistoricalOrder.model_validate(
            mock_response_content["order"]
        )
        mock_historical_order_response = HyperliquidRawHistoricalOrderResponse(
            order=mock_historical_order
        )
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

        result = await hl_trading_service.get_order(symbol=symbol, order_id=order_id)

        assert result == expected_order
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data=mock_payload_model.model_dump(by_alias=True),
            is_signed=True,
            is_public_info_endpoint=True,
        )
        mock_hl_response_handler.handle_info_order_status_response.assert_called_once_with(
            mock_response_content, user_address=wallet_address, order_id=order_id
        )
        mock_hl_trading_mapper.transform_raw_historical_order_to_internal.assert_called_once_with(
            raw_historical_order=mock_historical_order, trigger=None
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

        # Configure the mock to return a payload
        mock_payload = {"type": "openOrders", "user": wallet_address}
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_payload

        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.get_open_orders(symbol=symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Fetching open orders returned no content." in exc_info.value.message
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

        # Mock response handler to return raw Pydantic model
        mock_raw_response = HyperliquidRawOpenOrdersResponse.model_validate(mock_response_content)
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
        mock_hl_trading_mapper.transform_raw_order_to_internal.side_effect = expected_orders

        result = await hl_trading_service.get_open_orders(symbol=symbol)

        assert result == expected_orders
        assert len(result) == 2
        mock_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint="/info",
            data={"type": "openOrders", "user": wallet_address},
            is_signed=True,
            is_public_info_endpoint=True,
        )
        mock_hl_response_handler.handle_info_open_orders_response.assert_called_once_with(
            mock_response_content, user_address=wallet_address
        )
        mock_hl_trading_mapper.transform_raw_order_to_internal.assert_has_calls(
            [
                call(
                    raw_order=mock_raw_response.root[0].order,
                    trigger=mock_raw_response.root[0].trigger,
                ),
                call(
                    raw_order=mock_raw_response.root[1].order,
                    trigger=mock_raw_response.root[1].trigger,
                ),
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

        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.cancel_order(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Exchange action (cancel) returned no content" in exc_info.value.message
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
            status="ok",
            data=HyperliquidRawExchangeResponseData(type="cancel", statuses=["success"]),
        )
        mock_hl_response_handler.handle_exchange_response.return_value = mock_raw_response

        result = await hl_trading_service.cancel_order(
            symbol=symbol,
            order_id=order_id,
        )

        assert result is True  # cancel_order returns boolean, not raw response
        mock_get_asset_index_callable.assert_called_once_with(symbol)
        # Note: cancel_order creates HyperliquidRawCancelOrderAction directly,
        # doesn't use request builder
        mock_http_client_requester.assert_called_once()
        mock_hl_response_handler.handle_exchange_response.assert_called_once_with(
            mock_response_content, action_type="cancel"
        )
