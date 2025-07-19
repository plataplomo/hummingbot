"""Unit tests for Hyperliquid Order Query Service.

Tests cover all methods of the HyperliquidOrderQueryService including:
- Get open orders
- Get order status
- Get order history
- Query validation and processing
- Error handling
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_auth import IAuthenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
    HyperliquidRawOrderStatusResponse,
    HyperliquidRawSimpleOpenOrder,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_trading_request_builder import (
    HyperliquidTradingRequestBuilder,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_trading_response_handler import (
    HyperliquidTradingResponseHandler,
)
from cyberdelta.apis.hyperliquid.services.trading.hl_order_query_service import (
    HyperliquidOrderQueryService,
)
from cyberdelta.apis.models.service_args_models import (
    GetOpenOrdersArgs,
    GetOrderArgs,
)
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.models import Order, Trade
from cyberdelta.enums import OrderSide, OrderType, TimeInForce


HyperliquidResponseHandler = HyperliquidTradingResponseHandler


@pytest.fixture
def mock_http_requester() -> AsyncMock:
    """Create a mock HTTP requester."""
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> Mock:
    """Create a mock request builder."""
    return MagicMock(spec=HyperliquidTradingRequestBuilder)


@pytest.fixture
def mock_response_handler() -> Mock:
    """Create a mock response handler."""
    return MagicMock(spec=HyperliquidTradingResponseHandler)


@pytest.fixture
def mock_mapper() -> Mock:
    """Create a mock data mapper."""
    return MagicMock(spec=HyperliquidOrderMapper)


@pytest.fixture
def order_query_service(
    mock_http_requester: AsyncMock,
    mock_request_builder: Mock,
    mock_response_handler: Mock,
    mock_mapper: Mock,
) -> HyperliquidOrderQueryService:
    """Create an order query service instance with mocks."""
    mock_error_mapper = Mock(spec=HyperliquidErrorMapper)
    mock_authenticator = Mock(spec=IAuthenticator)
    return HyperliquidOrderQueryService(
        http_client_requester=mock_http_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        error_mapper=mock_error_mapper,
        authenticator=mock_authenticator,
        wallet_address="0x123...abc",
        info_endpoint="/info",
        exchange_name="hyperliquid",
    )


@pytest.fixture
def mock_open_order() -> Order:
    """Create a mock open order."""
    return Order(
        exchange_order_id="12345",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("0.1"),
        price=Decimal(50000),
        status=OrderStatus.NEW,
        quantity_filled=Decimal(0),
        created_at=datetime.now(UTC),
        exchange="hyperliquid",
        time_in_force=TimeInForce.GTC,
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )


@pytest.fixture
def mock_filled_order() -> Order:
    """Create a mock filled order."""
    return Order(
        exchange_order_id="67890",
        client_order_id="client_456",
        symbol="ETH-USD",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity_requested=Decimal("1.0"),
        price=None,
        status=OrderStatus.FILLED,
        quantity_filled=Decimal("1.0"),
        average_fill_price=Decimal(3500),
        created_at=datetime.now(UTC) - timedelta(hours=1),
        exchange="hyperliquid",
        time_in_force=TimeInForce.IOC,
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )


@pytest.fixture
def mock_trade() -> Trade:
    """Create a mock trade."""
    return Trade(
        id="trade_123",
        order_id="12345",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        price=Decimal(50000),
        quantity=Decimal("0.1"),
        fee=Decimal("0.05"),
        fee_asset="USDC",
        executed_at=datetime.now(UTC),
        exchange="hyperliquid",
    )


class TestOrderQueryService:
    """Test suite for HyperliquidOrderQueryService."""

    @pytest.mark.asyncio
    async def test_get_open_orders_success(
        self,
        order_query_service: HyperliquidOrderQueryService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_open_order: Order,
    ) -> None:
        """Test successful retrieval of open orders."""
        # Arrange
        args = GetOpenOrdersArgs(wallet_address="0x123...abc")

        mock_request_builder.build_open_orders_payload.return_value = {"type": "openOrders"}

        mock_raw_open_order = HyperliquidRawSimpleOpenOrder(
            coin="BTC-USD",
            side="B",
            limitPx="50000.0",
            sz="0.1",
            oid=12345,
            timestamp=int(datetime.now(UTC).timestamp() * 1000),
            origSz="0.1",
            cloid="client_123",
        )
        mock_raw_response = HyperliquidRawOpenOrdersResponse([mock_raw_open_order])
        mock_response_handler.handle_open_orders_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ([{"coin": "BTC-USD"}], 200, {})
        mock_http_requester.return_value = mock_http_response

        mock_mapper.transform_raw_open_order_to_internal.return_value = mock_open_order

        # Act
        result = await order_query_service.get_open_orders(args.wallet_address)

        # Assert
        assert len(result) == 1
        assert result[0] == mock_open_order
        mock_request_builder.build_open_orders_payload.assert_called_once_with(args)
        mock_http_requester.assert_called_once()
        mock_response_handler.handle_open_orders_response.assert_called_once()
        mock_mapper.transform_raw_open_order_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_open_orders_empty(
        self,
        order_query_service: HyperliquidOrderQueryService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
    ) -> None:
        """Test retrieval of open orders when none exist."""
        # Arrange
        args = GetOpenOrdersArgs(wallet_address="0x123...abc")

        mock_request_builder.build_open_orders_payload.return_value = {"type": "openOrders"}

        mock_raw_response = HyperliquidRawOpenOrdersResponse([])
        mock_response_handler.handle_open_orders_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ([], 200, {})
        mock_http_requester.return_value = mock_http_response

        # Act
        result = await order_query_service.get_open_orders(args.wallet_address)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_open_orders_http_error(
        self,
        order_query_service: HyperliquidOrderQueryService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test open orders retrieval with HTTP error."""
        # Arrange
        args = GetOpenOrdersArgs(wallet_address="0x123...abc")

        mock_request_builder.build_open_orders_payload.return_value = {"type": "openOrders"}
        mock_http_requester.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_query_service.get_open_orders(args.wallet_address)

        assert "Network error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_status_success(
        self,
        order_query_service: HyperliquidOrderQueryService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_filled_order: Order,
    ) -> None:
        """Test successful retrieval of order status."""
        # Arrange
        args = GetOrderArgs(
            order_id="67890",
        )

        mock_request_builder.build_order_status_payload.return_value = {"type": "orderStatus"}

        mock_raw_response = HyperliquidRawOrderStatusResponse(
            status="ok",
            order=MagicMock(),
        )
        mock_response_handler.handle_order_status_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ({"status": "filled"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        mock_mapper.transform_raw_order_info_to_internal.return_value = mock_filled_order

        # Act
        result = await order_query_service.get_order(args)

        # Assert
        assert result == mock_filled_order
        mock_request_builder.build_order_status_payload.assert_called_once_with(args)
        mock_http_requester.assert_called_once()
        mock_response_handler.handle_order_status_response.assert_called_once()
        mock_mapper.transform_raw_order_info_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_order_status_not_found(
        self,
        order_query_service: HyperliquidOrderQueryService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
    ) -> None:
        """Test order status retrieval for non-existent order."""
        # Arrange
        args = GetOrderArgs(
            order_id="99999",
        )

        mock_request_builder.build_order_status_payload.return_value = {"type": "orderStatus"}

        # API returns None for non-existent orders
        mock_http_response: tuple[Any, int, dict[str, str]] = (None, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Act
        result = await order_query_service.get_order(args)

        # Assert
        assert result is None

    # Order history methods moved to HyperliquidAccountService in architecture refactor

    @pytest.mark.asyncio
    async def test_get_open_orders_http_client_returns_none(
        self,
        order_query_service: HyperliquidOrderQueryService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
    ) -> None:
        """Test get_open_orders when HTTP client returns None."""
        # Arrange
        mock_request_builder.build_open_orders_payload.return_value = {"type": "openOrders"}

        mock_http_requester.return_value = (
            None,
            200,
            {},
        )  # HTTP client returns None

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_query_service.get_open_orders()

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data received for open orders" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_open_orders_response_validation_error(
        self,
        order_query_service: HyperliquidOrderQueryService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
    ) -> None:
        """Test open orders retrieval with response validation error."""
        # Arrange
        args = GetOpenOrdersArgs(wallet_address="0x123...abc")

        mock_request_builder.build_open_orders_payload.return_value = {"type": "openOrders"}

        # Response handler raises validation error
        mock_response_handler.handle_open_orders_response.side_effect = (
            ValidationError.from_exception_data(
                "validation_error",
                [{"type": "missing", "loc": ("0", "coin"), "input": {"invalid": "data"}}],
            )
        )

        mock_http_response: tuple[Any, int, dict[str, str]] = ([{"invalid": "data"}], 200, {})
        mock_http_requester.return_value = mock_http_response

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_query_service.get_open_orders(args.wallet_address)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_get_order_status_mapper_error(
        self,
        order_query_service: HyperliquidOrderQueryService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
    ) -> None:
        """Test order status retrieval with mapper transformation error."""
        # Arrange
        args = GetOrderArgs(
            order_id="67890",
        )

        mock_request_builder.build_order_status_payload.return_value = {"type": "orderStatus"}

        mock_raw_response = HyperliquidRawOrderStatusResponse(
            status="ok",
            order=MagicMock(),
        )
        mock_response_handler.handle_order_status_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ({"status": "filled"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mapper raises transformation error
        mock_mapper.transform_raw_order_info_to_internal.side_effect = APIError(
            message="Failed to transform order",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_query_service.get_order(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
