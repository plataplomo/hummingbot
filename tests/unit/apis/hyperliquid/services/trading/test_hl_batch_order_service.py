"""Unit tests for Hyperliquid Batch Order Service.

Tests cover all methods of the HyperliquidBatchOrderService including:
- Batch order placement
- Batch order cancellation
- Batch validation and processing
- Error handling and partial failures
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions import OrderError
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeResponseData,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_trading_request_builder import (
    HyperliquidTradingRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.trading.hl_batch_order_service import (
    HyperliquidBatchOrderService,
)
from cyberdelta.apis.models.service_args_models import CancelOrderArgs, PlaceOrderArgs
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)


@pytest.fixture
def mock_http_requester() -> AsyncMock:
    """Create a mock HTTP requester."""
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> Mock:
    """Create a mock request builder."""
    mock = MagicMock(spec=HyperliquidTradingRequestBuilder)
    mock.build_batch_place_order_payload = MagicMock()
    mock.build_batch_cancel_order_payload = MagicMock()
    return mock


@pytest.fixture
def mock_response_handler() -> Mock:
    """Create a mock response handler."""
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_mapper() -> Mock:
    """Create a mock data mapper."""
    return MagicMock(spec=HyperliquidOrderMapper)


@pytest.fixture
def mock_error_mapper() -> Mock:
    """Create a mock error mapper."""
    return MagicMock(spec=HyperliquidErrorMapper)


@pytest.fixture
def mock_authenticator() -> Mock:
    """Create a mock authenticator."""
    mock = MagicMock()
    mock.sign_transaction = AsyncMock()
    return mock


@pytest.fixture
def batch_order_service(
    mock_http_requester: AsyncMock,
    mock_request_builder: Mock,
    mock_response_handler: Mock,
    mock_mapper: Mock,
    mock_error_mapper: Mock,
    mock_authenticator: Mock,
) -> HyperliquidBatchOrderService:
    """Create a batch order service instance with mocks."""
    return HyperliquidBatchOrderService(
        http_client_requester=mock_http_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        error_mapper=mock_error_mapper,
        authenticator=mock_authenticator,
        action_endpoint="/exchange",
        exchange_name="hyperliquid",
    )


@pytest.fixture
def valid_place_order_args() -> PlaceOrderArgs:
    """Create valid place order arguments."""
    return PlaceOrderArgs(
        symbol="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.1"),
        time_in_force=TimeInForce.GTC,
        price=Decimal(50000),
        reduce_only=False,
        post_only=False,
    )


@pytest.fixture
def mock_order() -> Order:
    """Create a mock order."""
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
        updated_at=None,
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )


@pytest.fixture
def valid_cancel_order_args() -> CancelOrderArgs:
    """Create valid cancel order arguments."""
    return CancelOrderArgs(
        symbol="BTC-USD",
        order_id="12345",
    )


class TestBatchOrderService:
    """Test suite for HyperliquidBatchOrderService."""

    @pytest.mark.asyncio
    async def test_place_batch_orders_success(
        self,
        batch_order_service: HyperliquidBatchOrderService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_authenticator: Mock,
        valid_place_order_args: PlaceOrderArgs,
        mock_order: Order,
    ) -> None:
        """Test successful batch order placement."""
        # Arrange
        batch_args = [
            valid_place_order_args,
            PlaceOrderArgs(
                symbol="ETH-USD",
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal(3500),
            ),
        ]

        mock_request_builder.build_batch_place_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    "success",
                    "success",
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[dict[str, Any] | None, int, dict[str, str]] = (
            {"status": "ok"},
            200,
            {},
        )
        mock_http_requester.return_value = mock_http_response

        # Create different orders for each
        mock_order2 = Order(
            exchange_order_id="67890",
            client_order_id="client_456",
            symbol="ETH-USD",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal(3500),
            status=OrderStatus.NEW,
            quantity_filled=Decimal(0),
            created_at=datetime.now(UTC),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_mapper.build_order_from_response.side_effect = [mock_order, mock_order2]

        # Mock asset index lookup
        with patch.object(
            batch_order_service,
            "_get_asset_indices",
            return_value={
                "BTC-USD": 0,
                "ETH-USD": 1,
            },
        ):
            # Act
            result = await batch_order_service.place_batch_orders(batch_args)

        # Assert
        assert len(result) == 2
        assert result[0] == mock_order
        assert result[1] == mock_order2
        mock_request_builder.build_batch_place_order_payload.assert_called_once()
        mock_authenticator.sign_transaction.assert_called_once()
        mock_http_requester.assert_called_once()
        mock_response_handler.handle_exchange_response.assert_called_once()
        assert mock_mapper.build_order_from_response.call_count == 2

    @pytest.mark.asyncio
    async def test_place_batch_orders_partial_failure(
        self,
        batch_order_service: HyperliquidBatchOrderService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_error_mapper: Mock,
        valid_place_order_args: PlaceOrderArgs,
        mock_order: Order,
    ) -> None:
        """Test batch order placement with partial failures."""
        # Arrange
        batch_args = [valid_place_order_args, valid_place_order_args]

        mock_request_builder.build_batch_place_order_payload.return_value = MagicMock()

        # One success, one failure
        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    "success",
                    "Insufficient balance",
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[dict[str, Any] | None, int, dict[str, str]] = (
            {"status": "ok"},
            200,
            {},
        )
        mock_http_requester.return_value = mock_http_response

        mock_mapper.build_order_from_response.return_value = mock_order
        mock_error_mapper.map_error_to_api_error.return_value = APIError(
            message="Insufficient balance",
            code=APIErrorCode.INSUFFICIENT_FUNDS.value,
        )

        with patch.object(
            batch_order_service,
            "_get_asset_indices",
            return_value={
                "BTC-USD": 0,
            },
        ):
            # Act & Assert
            with pytest.raises(APIError) as exc_info:
                await batch_order_service.place_batch_orders(batch_args)

            # Should raise error for the failed order
            assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value

    @pytest.mark.asyncio
    async def test_place_batch_orders_empty_list(
        self,
        batch_order_service: HyperliquidBatchOrderService,
    ) -> None:
        """Test batch order placement with empty list."""
        # Act & Assert
        with pytest.raises(OrderError) as exc_info:
            await batch_order_service.place_batch_orders([])

        assert "empty" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_place_batch_orders_too_many(
        self,
        batch_order_service: HyperliquidBatchOrderService,
        valid_place_order_args: PlaceOrderArgs,
    ) -> None:
        """Test batch order placement exceeding max batch size."""
        # Arrange
        too_many_orders = [valid_place_order_args] * 101  # Exceeds max of 100

        # Act & Assert
        with pytest.raises(OrderError) as exc_info:
            await batch_order_service.place_batch_orders(too_many_orders)

        assert "exceeds maximum" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_cancel_batch_orders_success(
        self,
        batch_order_service: HyperliquidBatchOrderService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        valid_cancel_order_args: CancelOrderArgs,
    ) -> None:
        """Test successful batch order cancellation."""
        # Arrange
        batch_args = [
            valid_cancel_order_args,
            CancelOrderArgs(symbol="ETH-USD", order_id="67890"),
        ]

        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=[
                    "success",
                    "success",
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[dict[str, Any] | None, int, dict[str, str]] = (
            {"status": "ok"},
            200,
            {},
        )
        mock_http_requester.return_value = mock_http_response

        with patch.object(
            batch_order_service,
            "_get_asset_indices",
            return_value={
                "BTC-USD": 0,
                "ETH-USD": 1,
            },
        ):
            # Act
            result = await batch_order_service.cancel_batch_orders(batch_args)

        # Assert
        assert len(result) == 2
        assert all(r.status == CancelOrderResultStatus.SUCCESS for r in result)
        assert result[0].order_id == "12345"
        assert result[1].order_id == "67890"
        mock_request_builder.build_batch_cancel_order_payload.assert_called_once()
        mock_http_requester.assert_called_once()
        mock_response_handler.handle_exchange_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancel_batch_orders_partial_failure(
        self,
        batch_order_service: HyperliquidBatchOrderService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        valid_cancel_order_args: CancelOrderArgs,
    ) -> None:
        """Test batch cancellation with partial failures."""
        # Arrange
        batch_args = [
            valid_cancel_order_args,
            CancelOrderArgs(symbol="ETH-USD", order_id="99999"),
        ]

        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        # One success, one failure
        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=[
                    "success",
                    "Order not found",
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[dict[str, Any] | None, int, dict[str, str]] = (
            {"status": "ok"},
            200,
            {},
        )
        mock_http_requester.return_value = mock_http_response

        with patch.object(
            batch_order_service,
            "_get_asset_indices",
            return_value={
                "BTC-USD": 0,
                "ETH-USD": 1,
            },
        ):
            # Act
            result = await batch_order_service.cancel_batch_orders(batch_args)

        # Assert
        assert len(result) == 2
        assert result[0].status == CancelOrderResultStatus.SUCCESS
        assert result[0].order_id == "12345"
        assert result[1].status == CancelOrderResultStatus.FAILED
        assert result[1].order_id == "99999"
        assert "Order not found" in (result[1].message or "")

    @pytest.mark.asyncio
    async def test_place_batch_orders_http_error(
        self,
        batch_order_service: HyperliquidBatchOrderService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        valid_place_order_args: PlaceOrderArgs,
    ) -> None:
        """Test batch order placement with HTTP error."""
        # Arrange
        batch_args = [valid_place_order_args]

        mock_request_builder.build_batch_place_order_payload.return_value = MagicMock()
        mock_http_requester.side_effect = Exception("Network error")

        with patch.object(
            batch_order_service,
            "_get_asset_indices",
            return_value={
                "BTC-USD": 0,
            },
        ):
            # Act & Assert
            with pytest.raises(APIError) as exc_info:
                await batch_order_service.place_batch_orders(batch_args)

            assert "Network error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_place_batch_orders_exchange_error(
        self,
        batch_order_service: HyperliquidBatchOrderService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_error_mapper: Mock,
        valid_place_order_args: PlaceOrderArgs,
    ) -> None:
        """Test batch order placement with exchange error response."""
        # Arrange
        batch_args = [valid_place_order_args]

        mock_request_builder.build_batch_place_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="err",
            data=None,
            response="Rate limit exceeded",
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[dict[str, Any] | None, int, dict[str, str]] = (
            {"status": "err"},
            200,
            {},
        )
        mock_http_requester.return_value = mock_http_response

        mock_error_mapper.map_error_to_api_error.return_value = APIError(
            message="Rate limit exceeded",
            code=APIErrorCode.RATE_LIMITED.value,
        )

        with patch.object(
            batch_order_service,
            "_get_asset_indices",
            return_value={
                "BTC-USD": 0,
            },
        ):
            # Act & Assert
            with pytest.raises(APIError) as exc_info:
                await batch_order_service.place_batch_orders(batch_args)

            assert exc_info.value.code == APIErrorCode.RATE_LIMITED.value

    @pytest.mark.asyncio
    async def test_cancel_batch_orders_empty_list(
        self,
        batch_order_service: HyperliquidBatchOrderService,
    ) -> None:
        """Test batch cancellation with empty list."""
        # Act & Assert
        with pytest.raises(OrderError) as exc_info:
            await batch_order_service.cancel_batch_orders([])

        assert "empty" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_place_batch_orders_with_client_ids(
        self,
        batch_order_service: HyperliquidBatchOrderService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_order: Order,
    ) -> None:
        """Test batch order placement with client order IDs."""
        # Arrange
        order1 = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.GTC,
            price=Decimal(50000),
            client_order_id="batch_order_1",
        )
        order2 = PlaceOrderArgs(
            symbol="ETH-USD",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal(3500),
            client_order_id="batch_order_2",
        )
        batch_args = [order1, order2]

        mock_request_builder.build_batch_place_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    "success",
                    "success",
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[dict[str, Any] | None, int, dict[str, str]] = (
            {"status": "ok"},
            200,
            {},
        )
        mock_http_requester.return_value = mock_http_response

        # Create orders with client IDs
        mock_order1 = Order(
            exchange_order_id="12345",
            client_order_id="batch_order_1",
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
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_order2 = Order(
            exchange_order_id="67890",
            client_order_id="batch_order_2",
            symbol="ETH-USD",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal(3500),
            status=OrderStatus.NEW,
            quantity_filled=Decimal(0),
            created_at=datetime.now(UTC),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=None,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
        )
        mock_mapper.build_order_from_response.side_effect = [mock_order1, mock_order2]

        with patch.object(
            batch_order_service,
            "_get_asset_indices",
            return_value={
                "BTC-USD": 0,
                "ETH-USD": 1,
            },
        ):
            # Act
            result = await batch_order_service.place_batch_orders(batch_args)

        # Assert
        assert len(result) == 2
        assert result[0].client_order_id == "batch_order_1"
        assert result[1].client_order_id == "batch_order_2"
