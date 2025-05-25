"""
Unit tests for HyperliquidTradingService order operations.
"""

from collections.abc import Callable
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
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
        mock_exchange_http_client_requester: AsyncMock,
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

        mock_exchange_http_client_requester.return_value = (None, 200, MagicMock())

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
        mock_exchange_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_order_success(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_exchange_http_client_requester: AsyncMock,
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
        mock_exchange_http_client_requester.return_value = (
            mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler
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
        mock_hl_response_handler.handle_place_order_response.return_value = expected_order

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
        mock_exchange_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/exchange",
            data=mock_payload_dict,
            authenticator=mock_authenticator,
            rate_limiter_service=None,
            is_signed=True,
        )
        mock_hl_response_handler.handle_place_order_response.assert_called_once_with(
            mock_response_content, 200, {"content-type": "application/json"}
        )

    @pytest.mark.asyncio
    async def test_get_order_http_client_returns_none_in_info_request(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test get_order when the info HTTP client returns None content."""
        symbol = "ETH"
        order_id = 12345
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        mock_request_payload_model = MagicMock()
        mock_request_payload_dict = {"type": "orderStatus", "user": wallet_address, "oid": order_id}
        mock_hl_request_builder.build_order_status_payload.return_value = mock_request_payload_model
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_info_http_client_requester.return_value = None

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.get_order(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"No data received for order status for OID {order_id}." in exc_info.value.message
        mock_hl_request_builder.build_order_status_payload.assert_called_once_with(
            wallet_address=wallet_address, order_id=order_id
        )
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_model.model_dump.return_value,
            authenticator=mock_authenticator,
            rate_limiter_service=None,
            is_signed=True,
        )

    @pytest.mark.asyncio
    async def test_get_order_success(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test successful get_order operation."""
        symbol = "BTC"
        order_id = 789012
        wallet_address = "0xOrderWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock request building
        mock_request_payload_model = MagicMock()
        mock_request_payload_dict = {"type": "orderStatus", "user": wallet_address, "oid": order_id}
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_hl_request_builder.build_order_status_payload.return_value = mock_request_payload_model

        # Mock successful info response
        mock_response_content = {
            "status": "ok",
            "response": {"order": {"oid": order_id, "coin": symbol, "side": "A", "sz": "1.0"}},
        }
        mock_info_http_client_requester.return_value = (
            mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler
        expected_order = Order(
            exchange_order_id=str(order_id),
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal("45000"),
            exchange="hyperliquid_test_trading",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_hl_response_handler.handle_get_order_response.return_value = expected_order

        result = await hl_trading_service.get_order(symbol=symbol, order_id=order_id)

        assert result == expected_order
        mock_hl_request_builder.build_order_status_payload.assert_called_once_with(
            wallet_address=wallet_address, order_id=order_id
        )
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            authenticator=mock_authenticator,
            rate_limiter_service=None,
            is_signed=True,
        )
        mock_hl_response_handler.handle_get_order_response.assert_called_once_with(
            mock_response_content, 200, {"content-type": "application/json"}
        )

    @pytest.mark.asyncio
    async def test_get_open_orders_http_client_returns_none_in_info_request(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test get_open_orders when the info HTTP client returns None content."""
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        mock_request_payload_model = MagicMock(spec=HyperliquidRawOpenOrdersRequestPayload)
        mock_request_payload_model.model_dump.return_value = {
            "type": "openOrders",
            "user": wallet_address,
        }
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_request_payload_model
        mock_info_http_client_requester.return_value = None

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.get_open_orders()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"No data received for open orders for wallet {wallet_address}."
            in exc_info.value.message
        )
        mock_hl_request_builder.build_open_orders_payload.assert_called_once_with(wallet_address)
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_model.model_dump.return_value,
            authenticator=mock_authenticator,
            rate_limiter_service=None,
            is_signed=True,
        )

    @pytest.mark.asyncio
    async def test_get_open_orders_success(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test successful get_open_orders operation."""
        wallet_address = "0xOpenOrdersWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock request building
        mock_request_payload_model = MagicMock(spec=HyperliquidRawOpenOrdersRequestPayload)
        mock_request_payload_dict = {"type": "openOrders", "user": wallet_address}
        mock_request_payload_model.model_dump.return_value = mock_request_payload_dict
        mock_hl_request_builder.build_open_orders_payload.return_value = mock_request_payload_model

        # Mock successful info response
        mock_response_content = {
            "status": "ok",
            "response": [
                {"oid": 123, "coin": "BTC", "side": "A", "sz": "0.5"},
                {"oid": 456, "coin": "ETH", "side": "B", "sz": "2.0"},
            ],
        }
        mock_info_http_client_requester.return_value = (
            mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler
        expected_orders = [
            Order(
                exchange_order_id="123",
                symbol="BTC",
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity_requested=Decimal("0.5"),
                price=Decimal("50000"),
                exchange="hyperliquid_test_trading",
                time_in_force=TimeInForce.GTC,
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            ),
            Order(
                exchange_order_id="456",
                symbol="ETH",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity_requested=Decimal("2.0"),
                price=Decimal("3000"),
                exchange="hyperliquid_test_trading",
                time_in_force=TimeInForce.GTC,
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            ),
        ]
        mock_hl_response_handler.handle_get_open_orders_response.return_value = expected_orders

        result = await hl_trading_service.get_open_orders()

        assert result == expected_orders
        mock_hl_request_builder.build_open_orders_payload.assert_called_once_with(wallet_address)
        mock_info_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=mock_request_payload_dict,
            authenticator=mock_authenticator,
            rate_limiter_service=None,
            is_signed=True,
        )
        mock_hl_response_handler.handle_get_open_orders_response.assert_called_once_with(
            mock_response_content, 200, {"content-type": "application/json"}
        )

    @pytest.mark.asyncio
    async def test_cancel_order_http_client_returns_none_in_exchange_action(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_exchange_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test cancel_order when the exchange HTTP client returns None content."""
        symbol = "BTC"
        order_id = 987654
        wallet_address = "0xCancelWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)
        mock_get_asset_index_callable.return_value = 1

        # Configure the mock to return a payload with the correct type attribute
        mock_payload = MagicMock()
        mock_payload.type = "cancel"  # Set the expected type value for cancel
        mock_hl_request_builder.build_cancel_order_payload.return_value = mock_payload

        mock_exchange_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.cancel_order(symbol=symbol, order_id=order_id)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Exchange action (cancel) returned no content" in exc_info.value.message
        mock_exchange_http_client_requester.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancel_order_success(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_exchange_http_client_requester: AsyncMock,
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

        # Mock payload building
        mock_payload = MagicMock()
        mock_payload.type = "cancel"
        mock_payload_dict = {
            "action": {
                "type": "cancel",
                "cancels": [{"a": asset_index, "o": order_id}],
            },
            "nonce": 1234567890,
            "signature": {
                "r": "0xr_value",
                "s": "0xs_value",
                "v": 27,
            },
        }
        mock_payload.model_dump.return_value = mock_payload_dict
        mock_hl_request_builder.build_cancel_order_payload.return_value = mock_payload

        # Mock successful exchange response
        mock_response_content = {
            "status": "ok",
            "response": {"type": "cancel", "data": {"statuses": ["success"]}},
        }
        mock_exchange_http_client_requester.return_value = (
            mock_response_content,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler
        expected_success = True
        mock_hl_response_handler.handle_cancel_order_response.return_value = expected_success

        result = await hl_trading_service.cancel_order(symbol=symbol, order_id=order_id)

        assert result is expected_success
        mock_get_asset_index_callable.assert_called_once_with(symbol)
        mock_hl_request_builder.build_cancel_order_payload.assert_called_once()
        mock_exchange_http_client_requester.assert_called_once_with(
            method="POST",
            endpoint_path="/exchange",
            data=mock_payload_dict,
            authenticator=mock_authenticator,
            rate_limiter_service=None,
            is_signed=True,
        )
        mock_hl_response_handler.handle_cancel_order_response.assert_called_once_with(
            mock_response_content, 200, {"content-type": "application/json"}
        )
