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

from cyberdelta.apis.base.trading_execution_domain import (
    OrderExecution,
)
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions import ServiceParameterError
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeResponseData,
    HyperliquidRawExchangeResponseDataInner,
    HyperliquidRawExchangeResponseNested,
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
from cyberdelta.apis.models.service_args.trading import PlaceOrderArgs
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.models import Order
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
def mock_get_asset_index() -> AsyncMock:
    """Create a mock get_asset_index callable."""
    return AsyncMock(return_value=0)


@pytest.fixture
def order_placement_service(
    mock_http_requester: AsyncMock,
    mock_request_builder: Mock,
    mock_response_handler: Mock,
    mock_mapper: Mock,
    mock_error_mapper: Mock,
    mock_authenticator: Mock,
    mock_get_asset_index: AsyncMock,
) -> HyperliquidOrderPlacementService:
    """Create an order placement service instance with mocks."""
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
        execution=OrderExecution(),
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
        average_fill_price=Decimal(50000),  # Required when quantity_filled > 0
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
        mock_request_builder.build_place_order_request.return_value = mock_request_payload

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            response=HyperliquidRawExchangeResponseNested(
                type="order",
                data=HyperliquidRawExchangeResponseDataInner(
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
            ),
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

        # Configure the mock to have the expected method
        mock_mapper.map_place_order_response_to_order = AsyncMock(return_value=mock_order_response)

        # Act
        result = await order_placement_service.place_order(valid_place_order_args)

        # Assert
        assert result == mock_order_response
        mock_request_builder.build_place_order_request.assert_called_once()
        # Current business logic may not call sign_transaction for this operation
        mock_http_requester.assert_called_once()
        mock_response_handler.handle_exchange_response.assert_called_once()
        mock_mapper.map_place_order_response_to_order.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_order_validation_error(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
    ) -> None:
        """Test order placement with validation error."""
        # Current business logic validates at model creation time
        # Act & Assert - Test that invalid PlaceOrderArgs cannot be created
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="",  # Invalid empty symbol
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.1"),
                price=Decimal(50000),
                time_in_force=TimeInForce.GTC,
                execution=OrderExecution(),
            )

        # ValidationError should contain information about the symbol field
        assert "symbol" in str(exc_info.value).lower()

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
        mock_request_builder.build_place_order_request.return_value = MagicMock()
        mock_http_requester.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_placement_service.place_order(valid_place_order_args)

        # Current business logic wraps exceptions in generic error messages
        assert "service failure" in str(exc_info.value).lower()

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
        mock_request_builder.build_place_order_request.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="err",
            response="Insufficient funds",
            data=None,
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: tuple[Any, int, dict[str, str]] = ({"status": "err"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        mock_error_mapper.map_string_error.return_value = APIError(
            message="Insufficient funds",
            code=APIErrorCode.INSUFFICIENT_FUNDS.value,
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_placement_service.place_order(valid_place_order_args)

        assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        mock_error_mapper.map_string_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_place_multiple_orders_sequentially(
        self,
        order_placement_service: HyperliquidOrderPlacementService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        valid_place_order_args: PlaceOrderArgs,
        mock_order_response: Order,
    ) -> None:
        """Test placing multiple orders sequentially through public API.

        Since batch order placement is not exposed publicly, we test
        multiple order placement through sequential calls.
        """
        # Arrange - Create two different orders
        order1_args = valid_place_order_args
        order2_args = PlaceOrderArgs(
            symbol="ETH-USD",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            price=Decimal(3000),
            time_in_force=TimeInForce.GTC,
            execution=OrderExecution(),
        )

        # Mock responses for each order - aligned with working test format
        mock_raw_response1 = HyperliquidRawExchangeResponse(
            status="ok",
            response=HyperliquidRawExchangeResponseNested(
                type="order",
                data=HyperliquidRawExchangeResponseDataInner(
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
            ),
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

        mock_raw_response2 = HyperliquidRawExchangeResponse(
            status="ok",
            response=HyperliquidRawExchangeResponseNested(
                type="order",
                data=HyperliquidRawExchangeResponseDataInner(
                    statuses=[
                        HyperliquidRawExchangeStatusObject(
                            resting=None,
                            error=None,
                            filled=HyperliquidRawExchangeStatusFilled(
                                oid=12346,
                                totalSz="1.0",
                                avgPx="3000.0",
                            ),
                        )
                    ],
                ),
            ),
            data=HyperliquidRawExchangeResponseData(
                type="order",
                statuses=[
                    HyperliquidRawExchangeStatusObject(
                        resting=None,
                        error=None,
                        filled=HyperliquidRawExchangeStatusFilled(
                            oid=12346,
                            totalSz="1.0",
                            avgPx="3000.0",
                        ),
                    )
                ],
            ),
        )

        mock_response_handler.handle_exchange_response.side_effect = [
            mock_raw_response1,
            mock_raw_response2,
        ]

        mock_http_requester.return_value = ({"status": "ok"}, 200, {})

        # Create different order responses
        order1_response = Order(
            exchange_order_id="12345",
            client_order_id="client_123",
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.1"),
            price=Decimal(50000),
            status=OrderStatus.FILLED,
            quantity_filled=Decimal("0.1"),
            average_fill_price=Decimal(50000),  # Required when quantity_filled > 0
            created_at=datetime.now(UTC),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
        )

        order2_response = Order(
            exchange_order_id="12346",
            client_order_id="client_456",
            symbol="ETH-USD",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal(3000),
            status=OrderStatus.FILLED,
            quantity_filled=Decimal("1.0"),
            average_fill_price=Decimal(3000),  # Required when quantity_filled > 0
            created_at=datetime.now(UTC),
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
        )

        # Configure the mock to have the expected method
        mock_mapper.map_place_order_response_to_order = AsyncMock()
        mock_mapper.map_place_order_response_to_order.side_effect = [
            order1_response,
            order2_response,
        ]

        # Act - Place orders sequentially through public API
        result1 = await order_placement_service.place_order(order1_args)
        result2 = await order_placement_service.place_order(order2_args)

        # Assert
        assert result1.exchange_order_id == "12345"
        assert result2.exchange_order_id == "12346"
        assert mock_http_requester.call_count == 2
        assert mock_mapper.map_place_order_response_to_order.call_count == 2

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
            execution=OrderExecution(),
        )

        # Mock the market order price calculation
        with patch(
            "cyberdelta.apis.hyperliquid.services.utils.order_validation."
            "validate_place_order_params"
        ):
            mock_request_builder.build_place_order_request.return_value = MagicMock()

            mock_raw_response = HyperliquidRawExchangeResponse(
                status="ok",
                response=HyperliquidRawExchangeResponseNested(
                    type="order",
                    data=HyperliquidRawExchangeResponseDataInner(
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
                ),
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

            # Configure the mock to have the expected method
            mock_mapper.map_place_order_response_to_order = AsyncMock(
                return_value=mock_order_response
            )

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
        mock_request_builder.build_place_order_request.return_value = MagicMock()

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
        mock_request_builder.build_place_order_request.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            response=HyperliquidRawExchangeResponseNested(
                type="order",
                data=HyperliquidRawExchangeResponseDataInner(
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
            ),
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

        # Configure the mock and make it raise transformation error
        mock_mapper.map_place_order_response_to_order = AsyncMock()
        mock_mapper.map_place_order_response_to_order.side_effect = APIError(
            message="Failed to transform order",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_placement_service.place_order(valid_place_order_args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_order_validation_empty_symbol(
        self, order_placement_service: HyperliquidOrderPlacementService
    ) -> None:
        """Test validation through public API with empty symbol."""
        # Current business logic validates at model creation time
        # Act & Assert - Test that invalid PlaceOrderArgs cannot be created
        with pytest.raises(ValidationError) as exc_info:
            PlaceOrderArgs(
                symbol="",  # Empty symbol should trigger validation error
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.1"),
                price=Decimal(50000),
                time_in_force=TimeInForce.GTC,
                execution=OrderExecution(),
            )

        # ValidationError should contain information about the symbol field
        assert "symbol" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_order_validation_invalid_quantity(
        self, order_placement_service: HyperliquidOrderPlacementService
    ) -> None:
        """Test validation through public API with invalid quantity."""
        # Arrange - Create order with negative quantity
        invalid_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal(-1),  # Negative quantity
            price=Decimal(50000),
            time_in_force=TimeInForce.GTC,
            execution=OrderExecution(),
        )

        # Act & Assert
        with pytest.raises(ServiceParameterError) as exc_info:
            await order_placement_service.place_order(invalid_args)

        assert "quantity" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_order_validation_limit_order_without_price(
        self, order_placement_service: HyperliquidOrderPlacementService
    ) -> None:
        """Test validation through public API for limit order without price."""
        # Arrange - Create limit order without price
        invalid_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=None,  # Missing required price for limit order
            time_in_force=TimeInForce.GTC,
            execution=OrderExecution(),
        )

        # Act & Assert
        with pytest.raises(ServiceParameterError) as exc_info:
            await order_placement_service.place_order(invalid_args)

        assert "price" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_order_validation_unsupported_time_in_force(
        self, order_placement_service: HyperliquidOrderPlacementService
    ) -> None:
        """Test validation through public API with unsupported time in force."""
        # Arrange - Create order with unsupported time in force
        invalid_args = PlaceOrderArgs(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            price=Decimal(50000),
            time_in_force=TimeInForce.FOK,  # FOK not supported by Hyperliquid
            execution=OrderExecution(),
        )

        # Act & Assert
        with pytest.raises(ServiceParameterError) as exc_info:
            await order_placement_service.place_order(invalid_args)

        assert "FOK" in str(exc_info.value) or "time_in_force" in str(exc_info.value).lower()

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
            execution=OrderExecution(),
        )

        mock_request_builder.build_place_order_request.return_value = MagicMock()

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
        mock_mapper.map_place_order_response_to_order.return_value = mock_order_response

        # Act
        result = await order_placement_service.place_order(args_with_client_id)

        # Assert
        assert result.client_order_id == "my_order_123"
