"""
Unit tests for HyperliquidTradingService management operations.
"""

from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.services.conftest_trading"]


class TestHyperliquidTradingServiceManagement:
    """Tests for the HyperliquidTradingService management operations."""

    @pytest.mark.asyncio
    async def test_cancel_all_orders_get_open_orders_returns_none(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
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

        mock_info_http_client_requester.return_value = None

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.cancel_all_orders(symbol="ETH")

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
    async def test_cancel_all_orders_success_with_symbol_filter(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_exchange_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_get_asset_index_callable: AsyncMock,
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

        # Mock open orders response
        mock_open_orders_response = {
            "status": "ok",
            "response": [
                {"oid": 123, "coin": "BTC", "side": "A", "sz": "0.5"},
                {"oid": 456, "coin": "ETH", "side": "B", "sz": "2.0"},
                {"oid": 789, "coin": "BTC", "side": "A", "sz": "1.0"},
            ],
        }
        mock_info_http_client_requester.return_value = (
            mock_open_orders_response,
            200,
            {"content-type": "application/json"},
        )

        # Mock asset index for BTC
        mock_get_asset_index_callable.return_value = 1

        # Mock cancel order payloads
        mock_cancel_payload_1 = MagicMock()
        mock_cancel_payload_1.type = "cancel"
        mock_cancel_payload_1.model_dump.return_value = {
            "action": {"type": "cancel", "cancels": [{"a": 1, "o": 123}]},
            "nonce": 1234567890,
            "signature": {"r": "0xr1", "s": "0xs1", "v": 27},
        }

        mock_cancel_payload_2 = MagicMock()
        mock_cancel_payload_2.type = "cancel"
        mock_cancel_payload_2.model_dump.return_value = {
            "action": {"type": "cancel", "cancels": [{"a": 1, "o": 789}]},
            "nonce": 1234567891,
            "signature": {"r": "0xr2", "s": "0xs2", "v": 27},
        }

        mock_hl_request_builder.build_cancel_order_payload.side_effect = [
            mock_cancel_payload_1,
            mock_cancel_payload_2,
        ]

        # Mock successful cancel responses
        mock_cancel_response = {
            "status": "ok",
            "response": {"type": "cancel", "data": {"statuses": ["success"]}},
        }
        mock_exchange_http_client_requester.return_value = (
            mock_cancel_response,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler for open orders and cancel operations
        from decimal import Decimal

        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
        from cyberdelta.core.models.market.order import Order

        mock_open_orders = [
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
            Order(
                exchange_order_id="789",
                symbol="BTC",
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity_requested=Decimal("1.0"),
                price=Decimal("51000"),
                exchange="hyperliquid_test_trading",
                time_in_force=TimeInForce.GTC,
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            ),
        ]
        mock_hl_response_handler.handle_info_open_orders_response.return_value = mock_open_orders
        mock_hl_response_handler.handle_exchange_response.return_value = True

        # Execute cancel_all_orders
        result = await hl_trading_service.cancel_all_orders(symbol=symbol)

        # Verify results - should cancel 2 BTC orders (123, 789) but not ETH order (456)
        assert len(result) == 2
        assert all(cancel_result.success for cancel_result in result)

        # Verify get_open_orders was called
        mock_hl_request_builder.build_open_orders_payload.assert_called_once_with(wallet_address)
        mock_info_http_client_requester.assert_called_once()

        # Verify cancel_order was called twice (for the 2 BTC orders)
        assert mock_hl_request_builder.build_cancel_order_payload.call_count == 2
        assert mock_exchange_http_client_requester.call_count == 2
        assert mock_get_asset_index_callable.call_count == 2

    @pytest.mark.asyncio
    async def test_cancel_all_orders_success_no_symbol_filter(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_exchange_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_get_asset_index_callable: AsyncMock,
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

        # Mock open orders response with 2 different symbols
        mock_open_orders_response = {
            "status": "ok",
            "response": [
                {"oid": 111, "coin": "BTC", "side": "A", "sz": "0.5"},
                {"oid": 222, "coin": "ETH", "side": "B", "sz": "2.0"},
            ],
        }
        mock_info_http_client_requester.return_value = (
            mock_open_orders_response,
            200,
            {"content-type": "application/json"},
        )

        # Mock asset indices
        mock_get_asset_index_callable.side_effect = [1, 2]  # BTC=1, ETH=2

        # Mock cancel order payloads
        mock_cancel_payload_1 = MagicMock()
        mock_cancel_payload_1.type = "cancel"
        mock_cancel_payload_2 = MagicMock()
        mock_cancel_payload_2.type = "cancel"
        mock_hl_request_builder.build_cancel_order_payload.side_effect = [
            mock_cancel_payload_1,
            mock_cancel_payload_2,
        ]

        # Mock successful cancel responses
        mock_cancel_response = {
            "status": "ok",
            "response": {"type": "cancel", "data": {"statuses": ["success"]}},
        }
        mock_exchange_http_client_requester.return_value = (
            mock_cancel_response,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler
        from decimal import Decimal

        from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
        from cyberdelta.core.models.market.order import Order

        mock_open_orders = [
            Order(
                exchange_order_id="111",
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
                exchange_order_id="222",
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
        mock_hl_response_handler.handle_info_open_orders_response.return_value = mock_open_orders
        mock_hl_response_handler.handle_exchange_response.return_value = True

        # Execute cancel_all_orders without symbol filter
        result = await hl_trading_service.cancel_all_orders()

        # Verify results - should cancel both orders
        assert len(result) == 2
        assert all(cancel_result.success for cancel_result in result)

        # Verify all orders were cancelled
        assert mock_hl_request_builder.build_cancel_order_payload.call_count == 2
        assert mock_exchange_http_client_requester.call_count == 2
        assert mock_get_asset_index_callable.call_count == 2

    @pytest.mark.asyncio
    async def test_cancel_all_orders_no_open_orders(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_info_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_hl_response_handler: MagicMock,
        mock_authenticator: MagicMock,
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

        # Mock empty open orders response
        mock_open_orders_response: dict[str, Any] = {
            "status": "ok",
            "response": [],
        }
        mock_info_http_client_requester.return_value = (
            mock_open_orders_response,
            200,
            {"content-type": "application/json"},
        )

        # Mock response handler to return empty list
        mock_hl_response_handler.handle_info_open_orders_response.return_value = []

        # Execute cancel_all_orders
        result = await hl_trading_service.cancel_all_orders()

        # Verify results - should return empty list
        assert result == []

        # Verify get_open_orders was called but no cancel operations
        mock_hl_request_builder.build_open_orders_payload.assert_called_once_with(wallet_address)
        mock_info_http_client_requester.assert_called_once()

        # No cancel operations should have been attempted
        assert mock_hl_request_builder.build_cancel_order_payload.call_count == 0
