"""
Unit tests for the BackpackTradingService.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.backpack.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce

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
    """Provides a mock BackpackOrderMapper."""
    return MagicMock(spec=BackpackOrderMapper)


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

        mock_payload = {"symbol": symbol, "side": "buy", "orderType": "limit", "quantity": "10"}
        mock_request_builder.build_place_order_payload.return_value = mock_payload

        # Simulate HTTP client returning None for content
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_order_mapper", mock_order_mapper):
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

        # No need to patch _order_mapper as it's not used in the error path before response handler
        with pytest.raises(APIError) as exc_info:
            await bp_trading_service.cancel_order(order_id=order_id, symbol=symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"No data received when cancelling order {order_id} ({symbol}), status: 200"
            in exc_info.value.message
        )

        mock_request_builder.build_cancel_order_payload.assert_called_once_with(
            symbol=symbol, order_id=order_id
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

        mock_params = {"symbol": symbol}  # Example params
        mock_request_builder.build_get_open_orders_params.return_value = mock_params

        # Simulate HTTP client returning None for content
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_order_mapper", mock_order_mapper):
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_open_orders(symbol=symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                f"Get open orders for {symbol or 'all'} returned invalid data (status: 200)"
                in exc_info.value.message
            )

            mock_request_builder.build_get_open_orders_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once()
            call_kwargs = mock_http_client_requester.call_args.kwargs
            assert call_kwargs.get("method") == "GET"
            assert call_kwargs.get("endpoint") == "/api/v1/orders"
            assert call_kwargs.get("params") == mock_params
            assert call_kwargs.get("is_signed") is True
            assert call_kwargs.get("rate_limiter_service") is mock_rate_limiter_service
            assert call_kwargs.get("endpoint_group") == "private"
            assert call_kwargs.get("request_weight") == 1
            assert call_kwargs.get("is_public_info_endpoint") is False

            mock_response_handler.handle_get_open_orders_response.assert_not_called()
            mock_order_mapper.transform_raw_order_to_internal.assert_not_called()

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
        order_id = "test_order_id_456"
        identifier = order_id  # Since order_id is provided

        mock_params = {"symbol": symbol}  # Example params
        mock_request_builder.build_get_order_params.return_value = mock_params

        # Simulate HTTP client returning None for content
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(bp_trading_service, "_order_mapper", mock_order_mapper):
            with pytest.raises(APIError) as exc_info:
                await bp_trading_service.get_order(order_id=order_id, symbol=symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert (
                f"Get order {identifier} ({symbol}) returned invalid data (status: 200)"
                in exc_info.value.message
            )

            mock_request_builder.build_get_order_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once()
            call_kwargs = mock_http_client_requester.call_args.kwargs
            assert call_kwargs.get("method") == "GET"
            assert call_kwargs.get("endpoint") == f"/api/v1/order/{identifier}"
            assert call_kwargs.get("params") == mock_params
            assert call_kwargs.get("is_signed") is True
            assert call_kwargs.get("rate_limiter_service") is mock_rate_limiter_service
            assert call_kwargs.get("endpoint_group") == "private"
            assert call_kwargs.get("request_weight") == 1
            assert call_kwargs.get("is_public_info_endpoint") is False

            mock_response_handler.handle_get_order_status_response.assert_not_called()
            mock_order_mapper.transform_raw_order_to_internal.assert_not_called()

    # Add other tests here
