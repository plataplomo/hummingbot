"""Unit tests for Hyperliquid Order Placement Service.

Tests cover all methods of the HyperliquidOrderPlacementService including:
- Single order placement
- Batch order placement
- Order validation and processing
- Error handling and status processing
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions import OrderError
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeResponseData,
    HyperliquidRawExchangeStatusFilled,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_trading_request_builder import (
    HyperliquidTradingRequestBuilder,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_trading_response_handler import (
    HyperliquidTradingResponseHandler,
)
from cyberdelta.apis.hyperliquid.services.trading.hl_order_placement_service import (
    HyperliquidOrderPlacementService,
)
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)


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
def order_placement_service(
    mock_http_requester: AsyncMock,
    mock_request_builder: Mock,
    mock_response_handler: Mock,
    mock_mapper: Mock,
    mock_error_mapper: Mock,
    mock_authenticator: Mock,
) -> HyperliquidOrderPlacementService:
    """Create an order placement service instance with mocks."""
    mock_get_asset_index = AsyncMock(return_value=0)
    return HyperliquidOrderPlacementService(
        http_client_requester=mock_http_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        error_mapper=mock_error_mapper,
        authenticator=mock_authenticator,
        get_asset_index_callable=mock_get_asset_index,
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
        price=Decimal(50000),
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        post_only=False,
    )


@pytest.fixture
def mock_order_response() -> Order:
    """Create a mock successful order response."""
    return Order(
        exchange_order_id="12345",
        client_order_id="client_123",
        symbol="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("0.1"),
        price=Decimal(50000),
        status=OrderStatus.FILLED,
        quantity_filled=Decimal("0.1"),
        created_at=datetime.now(UTC),
        exchange="hyperliquid",
        time_in_force=TimeInForce.GTC,
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )


class TestOrderPlacementService:
    """Test suite for HyperliquidOrderPlacementService."""

    @pytest.mark.asyncio
    async def test_place_order_success(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_authenticator: Mock,
        valid_place_order_args: PlaceOrderArgs,
        mock_order_response: Order,
    ) -> None:
        """Test successful order placement."""
        # Arrange
        mock_request_payload = HyperliquidApiPlaceOrderRequest(
            type="order",
            orders=[],
            grouping="na",
        )
        mock_request_builder.build_place_order_payload.return_value = mock_request_payload

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            response=None,
            data=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    HyperliquidRawExchangeStatusObject(
                        resting=None,
                        error=None,
                        filled=HyperliquidRawExchangeStatusFilled(
                            oid=12345,
                            totalSz="0.1",
                            avgPx="50000.0",
                        ),
                    )
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        mock_mapper.build_order_from_response.return_value = mock_order_response

        # Act
        result = await order_placement_service.place_order(valid_place_order_args)

        # Assert
        assert result == mock_order_response
        mock_request_builder.build_place_order_payload.assert_called_once()
        mock_authenticator.sign_transaction.assert_called_once()
        mock_http_requester.assert_called_once()
        mock_response_handler.handle_exchange_response.assert_called_once()
        mock_mapper.build_order_from_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_order_validation_error(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        valid_place_order_args: PlaceOrderArgs,
    ) -> None:
        """Test order placement with validation error."""
        # Arrange
        invalid_args = PlaceOrderArgs(
            symbol="",  # Invalid empty symbol
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal(50000),
            time_in_force=TimeInForce.GTC,
        )

        # Act & Assert
        with pytest.raises(OrderError) as exc_info:
            await order_placement_service.place_order(invalid_args)

        assert exc_info.value.code == APIErrorCode.INVALID_REQUEST.value

    @pytest.mark.asyncio
    async def test_place_order_http_error(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        valid_place_order_args: PlaceOrderArgs,
    ) -> None:
        """Test order placement with HTTP error."""
        # Arrange
        mock_request_builder.build_place_order_payload.return_value = MagicMock()
        mock_http_requester.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_placement_service.place_order(valid_place_order_args)

        assert "Network error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_place_order_exchange_error_response(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_error_mapper: Mock,
        valid_place_order_args: PlaceOrderArgs,
    ) -> None:
        """Test order placement with exchange error response."""
        # Arrange
        mock_request_builder.build_place_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="err",
            response="Insufficient funds",
            data=None,
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ({"status": "err"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        mock_error_mapper.map_error_to_api_error.return_value = APIError(
            message="Insufficient funds",
            code=APIErrorCode.INSUFFICIENT_FUNDS.value,
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_placement_service.place_order(valid_place_order_args)

        assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        mock_error_mapper.map_error_to_api_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_batch_orders_success(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        valid_place_order_args: PlaceOrderArgs,
        mock_order_response: Order,
    ) -> None:
        """Test successful batch order placement."""
        # Arrange
        batch_args = [valid_place_order_args, valid_place_order_args]

        mock_request_builder.build_batch_place_order_payload = MagicMock()
        mock_request_builder.build_batch_place_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            response=None,
            data=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    HyperliquidRawExchangeStatusObject(
                        resting=None,
                        error=None,
                        filled=HyperliquidRawExchangeStatusFilled(
                            oid=12345,
                            totalSz="0.1",
                            avgPx="50000.0",
                        ),
                    ),
                    HyperliquidRawExchangeStatusObject(
                        resting=None,
                        error=None,
                        filled=HyperliquidRawExchangeStatusFilled(
                            oid=12346,
                            totalSz="0.1",
                            avgPx="50000.0",
                        ),
                    ),
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        mock_mapper.build_order_from_response.return_value = mock_order_response

        # Act
        with patch.object(order_placement_service, "_validate_orders_list"):
            result = await order_placement_service._place_orders_core(batch_args, "batch")

        # Assert
        assert len(result) == 2
        assert all(order == mock_order_response for order in result)

    @pytest.mark.asyncio
    async def test_place_order_market_order_validation(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_order_response: Order,
    ) -> None:
        """Test market order placement with price calculation."""
        # Arrange
        market_order_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            price=None,  # Market orders don't have a price initially
            time_in_force=TimeInForce.IOC,
        )

        # Mock the market order price calculation
        with patch(
            "cyberdelta.apis.hyperliquid.services.utils.order_validation."
            "validate_place_order_params"
        ):
            mock_request_builder.build_place_order_payload.return_value = MagicMock()

            mock_raw_response = HyperliquidRawExchangeResponse(
                status="ok",
                response=None,
                data=HyperliquidRawExchangeResponseData(
                    type="order",
                    statuses=[
                        HyperliquidRawExchangeStatusObject(
                            resting=None,
                            error=None,
                            filled=HyperliquidRawExchangeStatusFilled(
                                oid=12345,
                                totalSz="0.1",
                                avgPx="50000.0",
                            ),
                        )
                    ],
                ),
            )
            mock_response_handler.handle_exchange_response.return_value = mock_raw_response

            mock_http_response: tuple[Any, int, dict[str, str]] = ({"status": "ok"}, 200, {})
            mock_http_requester.return_value = mock_http_response

            mock_mapper.build_order_from_response.return_value = mock_order_response

            # Act
            result = await order_placement_service.place_order(market_order_args)

            # Assert
            assert result == mock_order_response

    @pytest.mark.asyncio
    async def test_place_order_response_validation_error(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        valid_place_order_args: PlaceOrderArgs,
    ) -> None:
        """Test order placement with response validation error."""
        # Arrange
        mock_request_builder.build_place_order_payload.return_value = MagicMock()

        # Response handler raises validation error
        mock_response_handler.handle_exchange_response.side_effect = (
            ValidationError.from_exception_data(
                "validation_error",
                [{"type": "missing", "loc": ("response", "status"), "input": {"response": {}}}],
            )
        )

        mock_http_response: tuple[Any, int, dict[str, str]] = ({"invalid": "response"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_placement_service.place_order(valid_place_order_args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_place_order_mapper_transformation_error(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        valid_place_order_args: PlaceOrderArgs,
    ) -> None:
        """Test order placement with mapper transformation error."""
        # Arrange
        mock_request_builder.build_place_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            response=None,
            data=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    HyperliquidRawExchangeStatusObject(
                        resting=None,
                        error=None,
                        filled=HyperliquidRawExchangeStatusFilled(
                            oid=12345,
                            totalSz="0.1",
                            avgPx="50000.0",
                        ),
                    )
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mapper raises transformation error
        mock_mapper.build_order_from_response.side_effect = APIError(
            message="Failed to transform order",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_placement_service.place_order(valid_place_order_args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    def test_validate_orders_list_empty(
        self, order_placement_service: HyperliquidOrderPlacementService
    ) -> None:
        """Test validation of empty orders list."""
        # Act & Assert
        with pytest.raises(OrderError) as exc_info:
            order_placement_service._validate_orders_list([], "test_method")

        assert "empty list" in str(exc_info.value).lower()

    def test_validate_orders_list_too_many(
        self, order_placement_service: HyperliquidOrderPlacementService
    ) -> None:
        """Test validation of orders list exceeding max batch size."""
        # Arrange
        # Create real PlaceOrderArgs instead of MagicMock
        too_many_orders = [
            PlaceOrderArgs(
                symbol="BTC-USD",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.1"),
                price=Decimal(50000),
                time_in_force=TimeInForce.GTC,
            )
            for _ in range(101)
        ]  # Assuming max is 100

        # Act & Assert
        with pytest.raises(OrderError) as exc_info:
            order_placement_service._validate_orders_list(too_many_orders, "test_method")

        assert "exceeds maximum" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_place_order_with_client_order_id(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_order_response: Order,
    ) -> None:
        """Test order placement with client order ID."""
        # Arrange
        args_with_client_id = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal(50000),
            client_order_id="my_order_123",
            time_in_force=TimeInForce.GTC,
        )

        mock_request_builder.build_place_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            response=None,
            data=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    HyperliquidRawExchangeStatusObject(
                        resting=None,
                        error=None,
                        filled=HyperliquidRawExchangeStatusFilled(
                            oid=12345,
                            totalSz="0.1",
                            avgPx="50000.0",
                        ),
                    )
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        mock_order_response.client_order_id = "my_order_123"
        mock_mapper.build_order_from_response.return_value = mock_order_response

        # Act
        result = await order_placement_service.place_order(args_with_client_id)

        # Assert
        assert result.client_order_id == "my_order_123"
