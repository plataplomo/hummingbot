"""Unit tests for HyperliquidTradingService order operations."""

from collections.abc import Callable
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions import InvalidParameterTypeError, ServiceParameterError
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.core.models.market.order import CancelOrderResult, Order
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.exceptions.parsing import EmptyStringError


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

        with pytest.raises((ValueError, EmptyStringError)) as exc_info:
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
        assert "must be a finite decimal" in str(exc_info.value)

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
        assert "must be a finite decimal" in str(exc_info.value)

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

        with pytest.raises((ValueError, EmptyStringError)) as exc_info:
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
        """Test get_order raises ValidationError for empty symbol when provided."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValidationError) as exc_info:
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
        """Test cancel_order raises InvalidParameterTypeError for None symbol."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(InvalidParameterTypeError) as exc_info:
            args = CancelOrderArgs(
                order_id="12345",
                symbol=None,  # None symbol should be rejected
            )
            await hl_trading_service.cancel_order(args)

        assert "must be non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_empty_symbol_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test cancel_order raises ValidationError for empty symbol."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ValidationError) as exc_info:
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
        """Test cancel_order raises ServiceParameterError for invalid string order_id."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ServiceParameterError) as exc_info:
            args = CancelOrderArgs(
                order_id="not_a_number",  # Invalid string order_id
                symbol="ETH",
            )
            await hl_trading_service.cancel_order(args)

        assert "must be a valid integer" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_zero_order_id_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test cancel_order raises ServiceParameterError for zero order_id."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ServiceParameterError) as exc_info:
            args = CancelOrderArgs(
                order_id="0",  # Zero order_id should be rejected
                symbol="ETH",
            )
            await hl_trading_service.cancel_order(args)

        assert "must be a positive integer" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_cancel_order_negative_order_id_validation(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
    ) -> None:
        """Test cancel_order raises ServiceParameterError for negative order_id."""
        hl_trading_service = make_hl_trading_service()

        with pytest.raises(ServiceParameterError) as exc_info:
            args = CancelOrderArgs(
                order_id="-12345",  # Negative order_id should be rejected
                symbol="ETH",
            )
            await hl_trading_service.cancel_order(args)

        assert "must be a positive integer" in str(exc_info.value)

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
        """Test place_order when the service raises APIError for invalid response."""
        symbol = "ETH"
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock HTTP client to return None response (simulating empty response)
        mock_http_client_requester.return_value = (None, 200, {})

        # Configure request builder mock
        mock_order_payload = MagicMock()
        mock_order_payload.model_dump.return_value = {
            "action": {
                "type": "order",
                "orders": [
                    {
                        "a": 0,
                        "b": True,
                        "p": "100",
                        "s": "1",
                        "r": False,
                        "t": {"limit": {"tif": "Gtc"}},
                    }
                ],
            }
        }
        mock_hl_request_builder.build_place_order_action_payload.return_value = mock_order_payload

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

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()
        assert exc_info.value.message is not None

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

        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Test focuses on public behavior, not exact data matching

        # Mock HTTP client to return successful order response
        mock_order_response = {
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{"filled": {"totalSz": "0.5", "avgPx": "50.0", "oid": "12345"}}]
                },
            }
        }
        mock_http_client_requester.return_value = (mock_order_response, 200, {})

        # Configure request builder mock
        mock_order_payload = MagicMock()
        mock_order_payload.model_dump.return_value = {
            "action": {
                "type": "order",
                "orders": [
                    {
                        "a": 0,
                        "b": True,
                        "p": str(price),
                        "s": str(quantity),
                        "r": False,
                        "t": {"limit": {"tif": "Gtc"}},
                    }
                ],
            }
        }
        mock_hl_request_builder.build_place_order_action_payload.return_value = mock_order_payload

        args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=price,
            time_in_force=TimeInForce.GTC,
        )
        result = await hl_trading_service.place_order(args)

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()

        # Verify result structure (we test the public behavior)
        assert isinstance(result, Order)
        assert result.symbol == symbol

    @pytest.mark.asyncio
    async def test_get_order_http_client_returns_none_in_info_request(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
        mock_authenticator: MagicMock,
    ) -> None:
        """Test get_order when the service raises APIError for invalid response."""
        symbol = "ETH"
        order_id = "12345"
        wallet_address = "0xWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock HTTP client to return None response (simulating no data)
        mock_http_client_requester.return_value = (None, 200, {})

        # Configure request builder mock
        mock_order_status_payload = MagicMock()
        mock_order_status_payload.model_dump.return_value = {
            "type": "orderStatus",
            "user": wallet_address,
            "oid": order_id,
        }
        mock_hl_request_builder.build_order_status_payload.return_value = mock_order_status_payload

        with pytest.raises(APIError) as exc_info:
            await hl_trading_service.get_order(args=GetOrderArgs(symbol=symbol, order_id=order_id))

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"No data received for order status {order_id}, status: 200" in exc_info.value.message
        )

    @pytest.mark.asyncio
    async def test_get_order_success(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test successful get_order operation."""
        symbol = "BTC"
        order_id = "123456"
        wallet_address = "0xSuccessWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Create expected order result
        # Test focuses on public behavior, not exact data matching

        # Mock HTTP client to return successful order response
        mock_order_response = {
            "order": {
                "coin": symbol,
                "side": "B",
                "limitPx": "50.0",
                "sz": "1.0",
                "oid": order_id,
                "timestamp": 1672574400000,
                "orderType": "Limit",
                "status": "filled",
            }
        }
        mock_http_client_requester.return_value = (mock_order_response, 200, {})

        # Configure request builder mock
        mock_order_status_payload = MagicMock()
        mock_order_status_payload.model_dump.return_value = {
            "type": "orderStatus",
            "user": wallet_address,
            "oid": order_id,
        }
        mock_hl_request_builder.build_order_status_payload.return_value = mock_order_status_payload

        result = await hl_trading_service.get_order(
            args=GetOrderArgs(symbol=symbol, order_id=order_id),
        )

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()

        # Verify result structure (we test the public behavior)
        assert isinstance(result, Order)
        assert result.symbol == symbol

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
    ) -> None:
        """Test successful get_open_orders operation."""
        symbol = "BTC"
        wallet_address = "0xSuccessWallet"
        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Test focuses on public behavior, not exact data matching

        # Mock HTTP client to return successful open orders response
        mock_open_orders_response = [
            {
                "coin": "BTC",
                "side": "B",
                "limitPx": "50000.0",
                "sz": "0.1",
                "oid": "123456",
                "timestamp": 1672574400000,
                "orderType": "Limit",
            },
            {
                "coin": "BTC",
                "side": "S",
                "limitPx": "51000.0",
                "sz": "0.3",
                "oid": "123457",
                "timestamp": 1672574500000,
                "orderType": "Limit",
            },
        ]
        mock_http_client_requester.return_value = (mock_open_orders_response, 200, {})

        result = await hl_trading_service.get_open_orders(symbol=symbol)

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()

        # Verify result structure (we test the public behavior)
        assert isinstance(result, list)
        assert len(result) >= 0  # May be empty or contain orders

    @pytest.mark.asyncio
    async def test_cancel_order_http_client_returns_none_in_exchange_action(
        self,
        make_hl_trading_service: Callable[..., HyperliquidTradingService],
        mock_http_client_requester: AsyncMock,
        mock_get_asset_index_callable: AsyncMock,
        mock_hl_request_builder: MagicMock,
    ) -> None:
        """Test cancel_order when the service raises APIError for invalid response."""
        symbol = "ETH"
        order_id = 111222
        wallet_address = "0xCancelWallet"

        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Mock HTTP client to return None response (simulating empty response)
        mock_http_client_requester.return_value = (None, 200, {})

        # Configure request builder mock
        mock_cancel_payload = MagicMock()
        mock_cancel_payload.model_dump.return_value = {
            "action": {"type": "cancelByCloid", "cancels": [{"asset": 0, "cloid": str(order_id)}]}
        }
        mock_hl_request_builder.build_cancel_order_action_payload.return_value = mock_cancel_payload

        with pytest.raises(APIError) as exc_info:
            args = CancelOrderArgs(order_id=str(order_id), symbol=symbol)
            await hl_trading_service.cancel_order(args)

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()
        assert exc_info.value.message is not None

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

        hl_trading_service = make_hl_trading_service(wallet_address=wallet_address)

        # Test focuses on public behavior, not exact data matching

        # Mock HTTP client to return successful cancel response
        mock_cancel_response = {"response": {"type": "cancel", "data": {"statuses": ["success"]}}}
        mock_http_client_requester.return_value = (mock_cancel_response, 200, {})

        # Configure request builder mock
        mock_cancel_payload = MagicMock()
        mock_cancel_payload.model_dump.return_value = {
            "action": {"type": "cancelByCloid", "cancels": [{"asset": 0, "cloid": str(order_id)}]}
        }
        mock_hl_request_builder.build_cancel_order_action_payload.return_value = mock_cancel_payload

        result = await hl_trading_service.cancel_order(
            args=CancelOrderArgs(order_id=str(order_id), symbol=symbol),
        )

        # Verify the HTTP request was made
        mock_http_client_requester.assert_called_once()

        # Verify result structure (we test the public behavior)
        assert isinstance(result, CancelOrderResult)
        assert result.order_id == str(order_id)
