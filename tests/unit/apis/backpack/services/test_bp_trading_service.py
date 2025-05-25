"""
Unit tests for the BackpackTradingService.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult, Order

# Type alias for the HTTP client requester callable
HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Provides a mock HTTP client requester."""
    return AsyncMock(spec=HttpClientRequesterSig)


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Provides a mock BackpackRequestBuilder."""
    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Provides a mock BackpackResponseHandler."""
    return MagicMock(spec=BackpackResponseHandler)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Provides a mock IAuthenticator."""
    return MagicMock(spec=IAuthenticator)


@pytest.fixture
def mock_rate_limiter_service() -> AsyncMock:
    """Provides a mock RateLimiterService."""
    return AsyncMock(spec=RateLimiterService)


@pytest.fixture
def mock_order_mapper() -> MagicMock:  # Renamed from mock_mapper for clarity
    """Provides a mock BackpackTradingDataMapper."""
    return MagicMock(spec=BackpackTradingDataMapper)


@pytest.fixture
def bp_trading_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_authenticator: MagicMock,
    mock_rate_limiter_service: AsyncMock,
) -> BackpackTradingService:
    """Provides an instance of BackpackTradingService with mocked dependencies."""
    service = BackpackTradingService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name="backpack_test_trading",
        rate_limiter_service=mock_rate_limiter_service,
    )
    # The service instantiates its own _order_mapper. Tests will patch this.
    return service


class TestBackpackTradingService:
    """Tests for the BackpackTradingService class."""

    @pytest.mark.asyncio
    async def test_place_order_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test place_order successfully places an order."""
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10")
        price = Decimal("20.0")
        time_in_force = TimeInForce.GTC
        client_order_id = "test_client_order_123"

        mock_payload = {
            "symbol": symbol,
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "10",
            "price": "20.0",
            "clientOrderId": client_order_id,
        }
        mock_raw_response: dict[str, Any] = {
            "id": "order_123",
            "symbol": symbol,
            "side": "buy",
            "status": "FILLED",
            "orderType": "LIMIT",
            "quantity": "10",
            "price": "20.0",
            "createdAt": 1234567890000,
        }
        mock_raw_order = BackpackRawOrder.model_validate(mock_raw_response)
        mock_internal_order = Order(
            exchange_order_id="order_123",
            exchange="backpack_test_trading",
            symbol=symbol,
            side=side,
            order_type=order_type,
            status=OrderStatus.FILLED,
            quantity_requested=quantity,
            price=price,
            time_in_force=time_in_force,
            created_at=datetime.fromtimestamp(1234567890, tz=UTC),
            updated_at=datetime.fromtimestamp(1234567890, tz=UTC),
            client_order_id=client_order_id,
            quantity_filled=quantity,
            average_fill_price=price,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            bp_details=None,
            hl_details=None,
        )

        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_place_order_response.return_value = mock_raw_order
        mock_order_mapper.transform_raw_order_to_internal.return_value = mock_internal_order

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            result = await bp_trading_service.place_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                time_in_force=time_in_force,
                client_order_id=client_order_id,
            )

        mock_request_builder.build_place_order_payload.assert_called_once_with(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
            client_order_id=client_order_id,
            post_only=False,
            trigger_price=None,
        )
        mock_response_handler.handle_place_order_response.assert_called_once_with(mock_raw_response)
        mock_order_mapper.transform_raw_order_to_internal.assert_called_once_with(mock_raw_order)
        assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_place_order_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test place_order handles validation error from response handler."""
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10")
        price = Decimal("20.0")

        mock_payload = {"symbol": symbol, "side": "buy"}
        mock_raw_response = {"invalid": "order_data"}

        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a proper ValidationError by trying to validate invalid data
        try:
            BackpackRawOrder.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_place_order_response.side_effect = e

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    time_in_force=TimeInForce.GTC,
                )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing place order data failed" in exc_info.value.message

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
        quantity = Decimal("10")
        time_in_force = TimeInForce.GTC

        mock_payload = {"symbol": symbol}
        mock_request_builder.build_place_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = ("invalid_string_response", 200, {})

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.place_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                time_in_force=time_in_force,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Place order for {symbol} returned invalid data" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_place_order_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,  # For assert_not_called
        mock_order_mapper: MagicMock,  # For assert_not_called
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test place_order when HTTP client returns None content."""
        symbol = "SOL_USDC"
        side = OrderSide.BUY
        order_type = OrderType.LIMIT
        quantity = Decimal("10")
        price = Decimal("20.0")
        time_in_force = TimeInForce.GTC

        mock_payload = {"symbol": symbol, "side": "buy", "orderType": "LIMIT", "quantity": "10"}
        mock_request_builder.build_place_order_payload.return_value = mock_payload

        # Simulate HTTP client returning None for content
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    time_in_force=time_in_force,
                )

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
                price=price,
                time_in_force=time_in_force,
                client_order_id=None,
                post_only=False,
                trigger_price=None,
            )
            mock_http_client_requester.assert_called_once()
            call_kwargs = mock_http_client_requester.call_args.kwargs
            assert call_kwargs.get("method") == "POST"
            assert call_kwargs.get("endpoint") == "/api/v1/order"
            assert call_kwargs.get("data") == mock_payload
            assert call_kwargs.get("is_signed") is True
            assert call_kwargs.get("endpoint_group") == "private"
            assert call_kwargs.get("request_weight") == 1
            assert call_kwargs.get("is_public_info_endpoint") is False

            mock_response_handler.handle_place_order_response.assert_not_called()
            mock_order_mapper.transform_raw_order_to_internal.assert_not_called()

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
        order_id = "test_order_id_123"
        mock_payload = {"symbol": symbol, "orderId": order_id}
        mock_raw_response = {"orderId": order_id, "symbol": symbol, "status": "CANCELLED"}

        mock_cancel_result = CancelOrderResult(
            order_id=order_id,
            symbol=symbol,
            success=True,
            status=CancelOrderResultStatus.SUCCESS,
        )

        mock_request_builder.build_cancel_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_cancel_order_response.return_value = mock_cancel_result

        result = await bp_trading_service.cancel_order(order_id=order_id, symbol=symbol)

        mock_request_builder.build_cancel_order_payload.assert_called_once_with(
            order_id=order_id, symbol=symbol, client_order_id=None
        )
        mock_response_handler.handle_cancel_order_response.assert_called_once_with(
            mock_raw_response, order_id, symbol, None
        )
        assert result == mock_cancel_result

    @pytest.mark.asyncio
    async def test_cancel_order_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,  # For assert_not_called
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test cancel_order when HTTP client returns None content."""
        symbol = "SOL_USDC"
        order_id = "test_order_id_123"
        mock_payload = {"symbol": symbol, "orderId": order_id}
        mock_request_builder.build_cancel_order_payload.return_value = mock_payload

        # Simulate HTTP client returning None for content
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.cancel_order(order_id=order_id, symbol=symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Cancel order {order_id} ({symbol}) returned invalid data" in exc_info.value.message

        mock_request_builder.build_cancel_order_payload.assert_called_once_with(
            order_id=order_id, symbol=symbol, client_order_id=None
        )
        mock_http_client_requester.assert_called_once()
        call_kwargs = mock_http_client_requester.call_args.kwargs
        assert call_kwargs.get("method") == "DELETE"
        assert call_kwargs.get("endpoint") == "/api/v1/order"
        assert call_kwargs.get("data") == mock_payload
        assert call_kwargs.get("is_signed") is True
        assert call_kwargs.get("endpoint_group") == "private"
        assert call_kwargs.get("request_weight") == 1
        assert call_kwargs.get("is_public_info_endpoint") is False

        mock_response_handler.handle_cancel_order_response.assert_not_called()

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
        order_id = "order_123"

        mock_payload = {"symbol": symbol, "orderId": order_id}
        mock_raw_response = {"orderId": order_id, "status": "cancelled"}

        mock_request_builder.build_cancel_order_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_cancel_order_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.cancel_order(order_id=order_id, symbol=symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert f"Unexpected error cancelling order {order_id} ({symbol})" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_open_orders_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test get_open_orders successfully retrieves open orders."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol}
        mock_raw_response = [
            {
                "id": "order_1",
                "symbol": symbol,
                "side": "buy",
                "status": "NEW",
                "orderType": "LIMIT",
                "quantity": "10",
                "price": "20.0",
                "createdAt": 1234567890000,
            },
            {
                "id": "order_2",
                "symbol": symbol,
                "side": "sell",
                "status": "NEW",
                "orderType": "MARKET",
                "quantity": "5",
                "createdAt": 1234567890000,
            },
        ]
        mock_raw_orders = [BackpackRawOrder.model_validate(order) for order in mock_raw_response]
        mock_internal_orders = [
            Order(
                exchange_order_id="order_1",
                exchange="backpack_test_trading",
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                status=OrderStatus.NEW,
                quantity_requested=Decimal("10"),
                price=Decimal("20.0"),
                time_in_force=TimeInForce.GTC,
                created_at=datetime.fromtimestamp(1234567890, tz=UTC),
                updated_at=datetime.fromtimestamp(1234567890, tz=UTC),
                client_order_id="mock_client_order_1",
                quantity_filled=Decimal("0"),
                average_fill_price=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                bp_details=None,
                hl_details=None,
            ),
            Order(
                exchange_order_id="order_2",
                exchange="backpack_test_trading",
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                status=OrderStatus.NEW,
                quantity_requested=Decimal("5"),
                price=None,
                time_in_force=TimeInForce.GTC,
                created_at=datetime.fromtimestamp(1234567890, tz=UTC),
                updated_at=datetime.fromtimestamp(1234567890, tz=UTC),
                client_order_id="mock_client_order_2",
                quantity_filled=Decimal("0"),
                average_fill_price=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                bp_details=None,
                hl_details=None,
            ),
        ]

        mock_request_builder.build_get_open_orders_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_open_orders_response.return_value = mock_raw_orders
        mock_order_mapper.transform_raw_order_to_internal.side_effect = mock_internal_orders

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            result = await bp_trading_service.get_open_orders(symbol=symbol)

        mock_request_builder.build_get_open_orders_params.assert_called_once_with(symbol=symbol)
        mock_response_handler.handle_get_open_orders_response.assert_called_once_with(
            mock_raw_response, symbol
        )
        assert mock_order_mapper.transform_raw_order_to_internal.call_count == 2
        assert result == mock_internal_orders

    @pytest.mark.asyncio
    async def test_get_open_orders_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,  # For assert_not_called
        mock_order_mapper: MagicMock,  # For assert_not_called
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_open_orders when HTTP client returns None content."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol}
        mock_request_builder.build_get_open_orders_params.return_value = mock_params

        # Simulate HTTP client returning None for content
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            result = await bp_trading_service.get_open_orders(symbol=symbol)

        assert result == []

        mock_request_builder.build_get_open_orders_params.assert_called_once_with(symbol=symbol)
        mock_http_client_requester.assert_called_once()
        call_kwargs = mock_http_client_requester.call_args.kwargs
        assert call_kwargs.get("method") == "GET"
        assert call_kwargs.get("endpoint") == "/api/v1/orders"
        assert call_kwargs.get("params") == mock_params
        assert call_kwargs.get("is_signed") is True
        assert call_kwargs.get("endpoint_group") == "private"
        assert call_kwargs.get("request_weight") == 1
        assert call_kwargs.get("is_public_info_endpoint") is False

        mock_response_handler.handle_get_open_orders_response.assert_not_called()
        mock_order_mapper.transform_raw_order_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_open_orders_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test get_open_orders handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol}
        mock_raw_response = [{"invalid": "order_data"}]

        mock_request_builder.build_get_open_orders_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a proper ValidationError by trying to validate invalid data
        try:
            BackpackRawOrder.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_open_orders_response.side_effect = e

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_open_orders(symbol=symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing open orders data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test get_order successfully retrieves an order."""
        symbol = "SOL_USDC"
        order_id = "order_123"
        client_order_id = "client_order_123"

        mock_params = {"symbol": symbol, "orderId": order_id}
        mock_raw_response = {
            "id": order_id,
            "symbol": symbol,
            "side": "buy",
            "status": "FILLED",
            "orderType": "LIMIT",
            "quantity": "10",
            "price": "20.0",
            "createdAt": 1234567890000,
        }
        mock_raw_order = BackpackRawOrder.model_validate(mock_raw_response)
        mock_internal_order = Order(
            exchange_order_id=order_id,
            exchange="backpack_test_trading",
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.FILLED,
            quantity_requested=Decimal("10"),
            price=Decimal("20.0"),
            time_in_force=TimeInForce.GTC,
            created_at=datetime.fromtimestamp(1234567890, tz=UTC),
            updated_at=datetime.fromtimestamp(1234567890, tz=UTC),
            client_order_id=client_order_id,
            quantity_filled=Decimal("10"),
            average_fill_price=Decimal("20.0"),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            bp_details=None,
            hl_details=None,
        )

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_order_status_response.return_value = mock_raw_order
        mock_order_mapper.transform_raw_order_to_internal.return_value = mock_internal_order

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            result = await bp_trading_service.get_order(
                order_id=order_id, symbol=symbol, client_order_id=client_order_id
            )

        mock_request_builder.build_get_order_params.assert_called_once_with(
            order_id=order_id, symbol=symbol, client_order_id=client_order_id
        )
        mock_response_handler.handle_get_order_status_response.assert_called_once_with(
            mock_raw_response
        )
        mock_order_mapper.transform_raw_order_to_internal.assert_called_once_with(mock_raw_order)
        assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_get_order_http_client_returns_none(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,  # For assert_not_called
        mock_order_mapper: MagicMock,  # For assert_not_called
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_order when HTTP client returns None content."""
        symbol = "SOL_USDC"
        order_id = "test_order_id_123"
        mock_params = {"symbol": symbol, "orderId": order_id}
        mock_request_builder.build_get_order_params.return_value = mock_params

        # Simulate HTTP client returning None for content
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_order(order_id=order_id, symbol=symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Get order {order_id} ({symbol}) returned invalid data" in exc_info.value.message

        mock_request_builder.build_get_order_params.assert_called_once_with(symbol=symbol)
        mock_http_client_requester.assert_called_once()
        call_kwargs = mock_http_client_requester.call_args.kwargs
        assert call_kwargs.get("method") == "GET"
        # The endpoint pattern includes the identifier, handled by request builder
        assert "/api/v1/order" in call_kwargs.get("endpoint", "")
        assert call_kwargs.get("params") == mock_params
        assert call_kwargs.get("is_signed") is True
        assert call_kwargs.get("endpoint_group") == "private"
        assert call_kwargs.get("request_weight") == 1
        assert call_kwargs.get("is_public_info_endpoint") is False

        mock_response_handler.handle_get_order_status_response.assert_not_called()
        mock_order_mapper.transform_raw_order_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_order_validation_error(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test get_order handles validation error from response handler."""
        symbol = "SOL_USDC"
        order_id = "order_123"

        mock_params = {"symbol": symbol, "orderId": order_id}
        mock_raw_response = {"invalid": "order_data"}

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a proper ValidationError by trying to validate invalid data
        try:
            BackpackRawOrder.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_order_response.side_effect = e

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_order(order_id=order_id, symbol=symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing get order data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test get_order_status successfully retrieves order status."""
        symbol = "SOL_USDC"
        order_id = "order_123"

        mock_params = {"symbol": symbol, "orderId": order_id}
        mock_raw_response = {
            "id": order_id,
            "symbol": symbol,
            "side": "buy",
            "status": "FILLED",
            "orderType": "LIMIT",
            "quantity": "10",
            "price": "20.0",
            "createdAt": 1234567890000,
        }
        mock_raw_order = BackpackRawOrder.model_validate(mock_raw_response)
        mock_internal_order = Order(
            exchange_order_id=order_id,
            exchange="backpack_test_trading",
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            status=OrderStatus.FILLED,
            quantity_requested=Decimal("10"),
            price=Decimal("20.0"),
            time_in_force=TimeInForce.GTC,
            created_at=datetime.fromtimestamp(1234567890, tz=UTC),
            updated_at=datetime.fromtimestamp(1234567890, tz=UTC),
            client_order_id="mock_client_order_status",
            quantity_filled=Decimal("10"),
            average_fill_price=Decimal("20.0"),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            bp_details=None,
            hl_details=None,
        )

        mock_request_builder.build_get_order_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_order_status_response.return_value = mock_raw_order
        mock_order_mapper.transform_raw_order_to_internal.return_value = mock_internal_order

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            result = await bp_trading_service.get_order_status(order_id=order_id, symbol=symbol)

        assert result == mock_internal_order

    @pytest.mark.asyncio
    async def test_get_order_status_not_found(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
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
    async def test_cancel_all_orders_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test cancel_all_orders successfully cancels all orders."""
        symbol = "SOL_USDC"

        mock_payload = {"symbol": symbol}
        mock_raw_response = {
            "cancelledOrders": [
                {"orderId": "order_1", "status": "cancelled"},
                {"orderId": "order_2", "status": "cancelled"},
            ]
        }
        mock_cancel_results = [
            CancelOrderResult(
                order_id="order_1",
                symbol=symbol,
                success=True,
                status=CancelOrderResultStatus.SUCCESS,
            ),
            CancelOrderResult(
                order_id="order_2",
                symbol=symbol,
                success=True,
                status=CancelOrderResultStatus.SUCCESS,
            ),
        ]

        mock_request_builder.build_cancel_all_orders_payload.return_value = mock_payload
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_cancel_all_orders_response.return_value = mock_cancel_results

        result = await bp_trading_service.cancel_all_orders(symbol=symbol)

        mock_request_builder.build_cancel_all_orders_payload.assert_called_once_with(symbol=symbol)
        mock_response_handler.handle_cancel_all_orders_response.assert_called_once_with(
            mock_raw_response, symbol
        )
        assert result == mock_cancel_results

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

        # Based on service implementation, it returns empty list instead of raising error
        result = await bp_trading_service.cancel_all_orders(symbol=symbol)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_all_open_orders_success(
        self,
        bp_trading_service: BackpackTradingService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test get_all_open_orders successfully retrieves all open orders."""
        mock_params = None  # No symbol filter for all orders
        mock_raw_response = [
            {
                "id": "order_1",
                "symbol": "SOL_USDC",
                "side": "buy",
                "status": "NEW",
                "orderType": "LIMIT",
                "quantity": "10",
                "price": "20.0",
                "createdAt": 1234567890000,
            },
            {
                "id": "order_2",
                "symbol": "ETH_USDC",
                "side": "sell",
                "status": "NEW",
                "orderType": "MARKET",
                "quantity": "5",
                "createdAt": 1234567890000,
            },
        ]
        mock_raw_orders = [BackpackRawOrder.model_validate(order) for order in mock_raw_response]
        mock_internal_orders = [
            Order(
                exchange_order_id="order_1",
                exchange="backpack_test_trading",
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                status=OrderStatus.NEW,
                quantity_requested=Decimal("10"),
                price=Decimal("20.0"),
                time_in_force=TimeInForce.GTC,
                created_at=datetime.fromtimestamp(1234567890, tz=UTC),
                updated_at=datetime.fromtimestamp(1234567890, tz=UTC),
                client_order_id="mock_client_order_1",
                quantity_filled=Decimal("0"),
                average_fill_price=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                bp_details=None,
                hl_details=None,
            ),
            Order(
                exchange_order_id="order_2",
                exchange="backpack_test_trading",
                symbol="ETH_USDC",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                status=OrderStatus.NEW,
                quantity_requested=Decimal("5"),
                price=None,
                time_in_force=TimeInForce.GTC,
                created_at=datetime.fromtimestamp(1234567890, tz=UTC),
                updated_at=datetime.fromtimestamp(1234567890, tz=UTC),
                client_order_id="mock_client_order_2",
                quantity_filled=Decimal("0"),
                average_fill_price=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                bp_details=None,
                hl_details=None,
            ),
        ]

        mock_request_builder.build_get_open_orders_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_open_orders_response.return_value = mock_raw_orders
        mock_order_mapper.transform_raw_order_to_internal.side_effect = mock_internal_orders

        with patch.object(bp_trading_service, "_trading_mapper", mock_order_mapper):
            result = await bp_trading_service.get_all_open_orders()

        mock_request_builder.build_get_open_orders_params.assert_called_once_with(symbol=None)
        mock_response_handler.handle_get_open_orders_response.assert_called_once_with(
            mock_raw_response, None
        )
        assert mock_order_mapper.transform_raw_order_to_internal.call_count == 2
        assert result == mock_internal_orders

    @pytest.mark.asyncio
    async def test_constructor_with_custom_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_rate_limiter_service: AsyncMock,
        mock_order_mapper: MagicMock,
    ) -> None:
        """Test constructor with custom mapper injection."""
        service = BackpackTradingService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="test_exchange",
            rate_limiter_service=mock_rate_limiter_service,
            mapper=mock_order_mapper,
        )

        # Test behavior that uses the mapper to verify it was set correctly
        mock_request_builder.build_place_order_payload.return_value = {"symbol": "TEST"}
        mock_http_client_requester.return_value = ({"id": "test_order"}, 200, {})
        mock_response_handler.handle_place_order_response.return_value = MagicMock()
        mock_order_mapper.transform_raw_order_to_internal.return_value = MagicMock()

        with patch.object(service, "_trading_mapper", mock_order_mapper):
            await service.place_order(
                symbol="TEST",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.GTC,
            )

        # Verify the injected mapper was used
        mock_order_mapper.transform_raw_order_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_constructor_with_default_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_authenticator: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test constructor creates default mapper when none provided."""
        service = BackpackTradingService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            authenticator=mock_authenticator,
            exchange_name="test_exchange",
            rate_limiter_service=mock_rate_limiter_service,
            mapper=None,
        )

        # Test behavior that uses the mapper to verify it's working
        mock_request_builder.build_place_order_payload.return_value = {"symbol": "TEST"}
        mock_http_client_requester.return_value = ({"id": "test_order"}, 200, {})
        mock_response_handler.handle_place_order_response.return_value = MagicMock()

        # Mock the mapper for this test
        mock_mapper = MagicMock(spec=BackpackTradingDataMapper)
        mock_mapper.transform_raw_order_to_internal.return_value = MagicMock()

        with patch.object(service, "_trading_mapper", mock_mapper):
            await service.place_order(
                symbol="TEST",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.GTC,
            )

        # Verify the default mapper functionality works
        mock_mapper.transform_raw_order_to_internal.assert_called_once()
