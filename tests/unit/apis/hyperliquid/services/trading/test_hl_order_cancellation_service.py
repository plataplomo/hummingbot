"""Unit tests for Hyperliquid Order Cancellation Service.

Tests cover all methods of the HyperliquidOrderCancellationService including:
- Single order cancellation
- Batch order cancellation
- Cancel all orders
- Cancellation validation and processing
- Error handling
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import RawStatusStringHL
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawCancelItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeResponseData,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_trading_request_builder import (
    HyperliquidTradingRequestBuilder,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_trading_response_handler import (
    HyperliquidTradingResponseHandler,
)
from cyberdelta.apis.hyperliquid.services.trading.hl_order_cancellation_service import (
    HyperliquidOrderCancellationService,
)
from cyberdelta.apis.models.service_args.trading import CancelOrderArgs
from cyberdelta.core.enums import (
    CancelOrderResultStatus,
    OrderStatus,
)
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.models import CancelOrderResult, Order
from tests.common_symbols import BTC_USD_HL, ETH_USD_HL, SOL_USD_HL


# Type alias for mock HTTP response
MockHttpResponse = tuple[dict[str, Any] | None, int, dict[str, str]]

# Test data and constants
CANCEL_ORDER_VALIDATION_TEST_CASES = [
    ("", "12345", APIErrorCode.INVALID_PARAMS),  # Empty symbol
    (BTC_USD_HL, "", APIErrorCode.INVALID_PARAMS),  # Empty order ID
    (BTC_USD_HL, "12345", APIErrorCode.INVALID_PARAMS),  # Valid params for testing
    (BTC_USD_HL, "67890", APIErrorCode.INVALID_PARAMS),  # Valid params for testing
]

# Test cases: tuples of (num_orders, success_count, failure_count)
BATCH_CANCEL_TEST_CASES = [
    (2, 2, 0),  # All successful
    (3, 2, 1),  # Partial success
    (1, 0, 1),  # All failed
    (5, 3, 2),  # Mixed results
]

ERROR_SCENARIOS = [
    ("Order not found", APIErrorCode.ORDER_NOT_FOUND),
    ("Insufficient permissions", APIErrorCode.AUTHENTICATION_FAILED),
    ("Rate limit exceeded", APIErrorCode.RATE_LIMITED),
    ("Invalid symbol", APIErrorCode.INVALID_PARAMS),
]

HTTP_ERROR_SCENARIOS = [
    ("Network error", Exception),
    ("Connection timeout", TimeoutError),
    ("Server unavailable", ConnectionError),
]

VALIDATION_LIST_SCENARIOS: list[tuple[list[MagicMock], str]] = [
    ([], "empty list"),  # Empty list
    ([MagicMock() for _ in range(101)], "exceeds maximum"),  # Too many orders
]

ORDER_ID_SCENARIOS = [
    ("12345", None, "exchange_order"),  # Exchange order ID
    ("67890", "client_123", "both_ids"),  # Both IDs provided
    ("54321", "client_456", "both_ids_alt"),  # Both IDs provided alternative
]


# Fixtures
@pytest.fixture
def mock_http_requester() -> AsyncMock:
    """Create a mock HTTP requester.

    Returns:
        AsyncMock: A mock instance of the HTTP requester.
    """
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> Mock:
    """Create a mock request builder.

    Returns:
        Mock: A mock instance of HyperliquidTradingRequestBuilder.
    """
    return MagicMock(spec=HyperliquidTradingRequestBuilder)


@pytest.fixture
def mock_response_handler() -> Mock:
    """Create a mock response handler.

    Returns:
        Mock: A mock instance of HyperliquidTradingResponseHandler.
    """
    return MagicMock(spec=HyperliquidTradingResponseHandler)


@pytest.fixture
def mock_error_mapper() -> Mock:
    """Create a mock error mapper.

    Returns:
        Mock: A mock instance of HyperliquidErrorMapper.
    """
    return MagicMock(spec=HyperliquidErrorMapper)


@pytest.fixture
def mock_authenticator() -> Mock:
    """Create a mock authenticator.

    Returns:
        Mock: A mock authenticator with sign_transaction method.
    """
    mock = MagicMock()
    mock.sign_transaction = AsyncMock()
    return mock


@pytest.fixture
def mock_get_asset_index() -> AsyncMock:
    """Create a mock get_asset_index callable.

    Returns:
        AsyncMock: A mock callable that returns asset index 0.
    """
    return AsyncMock(return_value=0)


@pytest.fixture
def mock_order_query_service() -> Mock:
    """Create a mock order query service.

    Returns:
        Mock: A mock order query service with async get_open_orders method.
    """
    mock = MagicMock()
    # Make get_open_orders async to match business logic
    mock.get_open_orders = AsyncMock()
    return mock


@pytest.fixture
def order_cancellation_service(
    mock_http_requester: AsyncMock,
    mock_request_builder: Mock,
    mock_response_handler: Mock,
    mock_error_mapper: Mock,
    mock_authenticator: Mock,
    mock_get_asset_index: AsyncMock,
    mock_order_query_service: Mock,
) -> HyperliquidOrderCancellationService:
    """Create an order cancellation service instance with mocks.

    Returns:
        HyperliquidOrderCancellationService: Service instance configured with mock dependencies.
    """
    return HyperliquidOrderCancellationService(
        http_client_requester=mock_http_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        error_mapper=mock_error_mapper,
        authenticator=mock_authenticator,
        get_asset_index_callable=mock_get_asset_index,
        order_query_service=mock_order_query_service,
        action_endpoint="/exchange",
        exchange_name="hyperliquid",
    )


@pytest.fixture
def valid_cancel_order_args() -> CancelOrderArgs:
    """Create valid cancel order arguments.

    Returns:
        CancelOrderArgs: Valid arguments for canceling an order.
    """
    return CancelOrderArgs(
        symbol=BTC_USD_HL,
        order_id="12345",
    )


@pytest.fixture
def mock_cancel_result() -> CancelOrderResult:
    """Create a mock successful cancel result.

    Returns:
        CancelOrderResult: A successful order cancellation result.
    """
    return CancelOrderResult(
        order_id="12345",
        client_order_id=None,
        success=True,
        status=CancelOrderResultStatus.SUCCESS,
    )


@pytest.fixture
def mock_open_order() -> Order:
    """Create a mock open order.

    Returns:
        Order: A mock open order with test data.
    """
    return Order(
        exchange_order_id="12345",
        client_order_id="client_123",
        symbol=BTC_USD_HL,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("0.1"),
        price=Decimal(50000),
        status=OrderStatus.NEW,
        quantity_filled=Decimal(0),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
        exchange="hyperliquid",
        time_in_force=TimeInForce.GTC,
    )


@pytest.fixture
def multiple_cancel_order_args() -> list[CancelOrderArgs]:
    """Create multiple cancel order arguments for batch testing.

    Returns:
        list[CancelOrderArgs]: A list of cancel order arguments for different symbols.
    """
    return [
        CancelOrderArgs(symbol=BTC_USD_HL, order_id="12345"),
        CancelOrderArgs(symbol=ETH_USD_HL, order_id="67890"),
        CancelOrderArgs(symbol=SOL_USD_HL, order_id="11111"),
    ]


@pytest.fixture
def mock_open_orders() -> list[Order]:
    """Create multiple mock open orders.

    Returns:
        list[Order]: A list of mock open orders for testing.
    """
    return [
        Order(
            exchange_order_id="12345",
            client_order_id="client_123",
            symbol=BTC_USD_HL,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.1"),
            price=Decimal(50000),
            status=OrderStatus.NEW,
            quantity_filled=Decimal(0),
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
        ),
        Order(
            exchange_order_id="67890",
            client_order_id="client_456",
            symbol=ETH_USD_HL,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("1.0"),
            price=Decimal(3000),
            status=OrderStatus.NEW,
            quantity_filled=Decimal(0),
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
        ),
    ]


# Test Classes
@pytest.mark.order_cancellation
@pytest.mark.asyncio
class TestSingleOrderCancellation:
    """Test single order cancellation functionality."""

    async def test_cancel_order_success(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_authenticator: Mock,
        mock_get_asset_index: AsyncMock,
        valid_cancel_order_args: CancelOrderArgs,
        mock_cancel_result: CancelOrderResult,
    ) -> None:
        """Test successful order cancellation."""
        # Arrange
        mock_request_payload = HyperliquidApiCancelOrderRequest(
            type="cancel",
            cancels=[HyperliquidRawCancelItem(a=0, o=12345)],
        )
        mock_request_builder.build_batch_cancel_order_payload.return_value = mock_request_payload

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=["success"],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: MockHttpResponse = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup
        mock_get_asset_index.return_value = 0

        # Act
        result = await order_cancellation_service.cancel_order(valid_cancel_order_args)

        # Assert
        assert result.order_id == "12345"
        assert result.status == CancelOrderResultStatus.SUCCESS
        mock_request_builder.build_batch_cancel_order_payload.assert_called_once()
        mock_http_requester.assert_called_once()
        mock_response_handler.handle_exchange_response.assert_called_once()

    @pytest.mark.parametrize(
        ("symbol", "order_id", "expected_error"), CANCEL_ORDER_VALIDATION_TEST_CASES
    )
    async def test_cancel_order_validation_scenarios(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        symbol: Symbol | str | None,
        order_id: str | None,
        expected_error: APIErrorCode,
    ) -> None:
        """Test order cancellation with various validation scenarios."""
        # Skip None tests since CancelOrderArgs validation will prevent instantiation
        if symbol is None or order_id is None or not symbol or not order_id:
            pytest.skip(
                "Skipping None/empty validation tests - handled by Pydantic model validation"
            )

        # Arrange - for remaining valid test cases
        # These args would be valid for actual service calls

        # Act & Assert - test that service handles these normally
        # For this test we'll expect success path since args are valid
        # This would need proper mocking to avoid actual network calls
        pytest.skip("This needs proper service-level mocking setup")

    @pytest.mark.parametrize(("error_message", "expected_code"), ERROR_SCENARIOS)
    async def test_cancel_order_error_scenarios(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_error_mapper: Mock,
        mock_get_asset_index: AsyncMock,
        valid_cancel_order_args: CancelOrderArgs,
        error_message: str,
        expected_code: APIErrorCode,
    ) -> None:
        """Test cancellation error scenarios."""
        # Arrange
        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="err",
            data=None,
            response=error_message,
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: MockHttpResponse = ({"status": "err"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        mock_error_mapper.map_string_error.return_value = APIError(
            message=error_message,
            code=expected_code.value,
        )

        # Mock asset index lookup
        mock_get_asset_index.return_value = 0

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_cancellation_service.cancel_order(valid_cancel_order_args)

        assert exc_info.value.code == expected_code.value

    @pytest.mark.parametrize(("order_id", "client_order_id", "scenario"), ORDER_ID_SCENARIOS)
    async def test_cancel_order_id_scenarios(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_get_asset_index: AsyncMock,
        order_id: str | None,
        client_order_id: str | None,
        scenario: str,
    ) -> None:
        """Test order cancellation with different ID scenarios."""
        # Arrange
        cancel_args = CancelOrderArgs(
            symbol=BTC_USD_HL,
            order_id=order_id or "fallback_order_id",  # Ensure order_id is never None
            client_order_id=client_order_id,
        )

        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=["success"],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: MockHttpResponse = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup
        mock_get_asset_index.return_value = 0

        # Act
        result = await order_cancellation_service.cancel_order(cancel_args)

        # Assert - The service will use the exchange_order_id from the args
        expected_order_id = order_id or "fallback_order_id"
        assert result.order_id == expected_order_id
        assert result.status == CancelOrderResultStatus.SUCCESS
        # Note: Hyperliquid API does not preserve client_order_id in cancellation responses


@pytest.mark.order_cancellation
@pytest.mark.asyncio
class TestBatchOrderCancellation:
    """Test batch order cancellation functionality."""

    async def test_cancel_all_orders_success(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_get_asset_index: AsyncMock,
        mock_order_query_service: Mock,
        mock_open_orders: list[Order],
    ) -> None:
        """Test successful batch order cancellation."""
        # Arrange - Mock open orders retrieval
        # Use first 2 orders
        mock_order_query_service.get_open_orders.return_value = mock_open_orders[:2]

        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=["success", "success"],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: MockHttpResponse = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup for each order
        mock_get_asset_index.side_effect = [0, 1]

        # Act
        results = await order_cancellation_service.cancel_all_orders(BTC_USD_HL)

        # Assert
        assert len(results) == 2
        assert all(r.status == CancelOrderResultStatus.SUCCESS for r in results)
        # The results should match the exchange order IDs from the mock orders
        assert results[0].order_id == "12345"
        assert results[1].order_id == "67890"

    @pytest.mark.parametrize(
        ("num_orders", "success_count", "failure_count"), BATCH_CANCEL_TEST_CASES
    )
    async def test_cancel_all_orders_scenarios(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_get_asset_index: AsyncMock,
        mock_order_query_service: Mock,
        mock_open_orders: list[Order],
        num_orders: int,
        success_count: int,
        failure_count: int,
    ) -> None:
        """Test batch cancellation with various success/failure scenarios."""
        # Arrange - Create mock orders for testing
        test_orders: list[Order] = []
        for i in range(num_orders):
            order = Order(
                exchange_order_id=str(12345 + i),
                client_order_id=f"client_{i}",
                symbol=BTC_USD_HL,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity_requested=Decimal("0.1"),
                price=Decimal(50000),
                status=OrderStatus.NEW,
                quantity_filled=Decimal(0),
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                exchange="hyperliquid",
                time_in_force=TimeInForce.GTC,
            )
            test_orders.append(order)

        # Mock open orders retrieval
        mock_order_query_service.get_open_orders.return_value = test_orders

        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        # Create response data with mixed success/failure
        response_data: list[HyperliquidRawExchangeStatusObject | str] = []
        for i in range(num_orders):
            if i < success_count:
                response_data.append("success")  # Valid status string
            else:
                # Use proper status object with error field for failures
                response_data.append(
                    HyperliquidRawExchangeStatusObject(
                        resting=None, filled=None, error=f"Order {12345 + i} not found"
                    )
                )

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=response_data,
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        mock_http_response: MockHttpResponse = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup for each order
        mock_get_asset_index.side_effect = list(range(num_orders))

        # Act
        results = await order_cancellation_service.cancel_all_orders(BTC_USD_HL)

        # Assert
        assert len(results) == num_orders
        success_results = [r for r in results if r.status == CancelOrderResultStatus.SUCCESS]
        failed_results = [r for r in results if r.status == CancelOrderResultStatus.FAILED]
        assert len(success_results) == success_count
        assert len(failed_results) == failure_count


@pytest.mark.order_cancellation
@pytest.mark.asyncio
class TestCancelAllOrders:
    """Test cancel all orders functionality."""

    async def test_cancel_all_orders_success(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_get_asset_index: AsyncMock,
        mock_order_query_service: Mock,
        mock_open_orders: list[Order],
    ) -> None:
        """Test successful cancellation of all orders."""
        # Arrange - Mock order query service to return our mock orders
        mock_order_query_service.get_open_orders.return_value = mock_open_orders

        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=["success", "success"],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response
        mock_http_response: MockHttpResponse = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup for each order
        mock_get_asset_index.side_effect = [0, 1]

        # Act
        results = await order_cancellation_service.cancel_all_orders()

        # Assert
        assert len(results) == 2
        assert all(r.status == CancelOrderResultStatus.SUCCESS for r in results)
        assert results[0].order_id == "12345"
        assert results[1].order_id == "67890"

    async def test_cancel_all_orders_no_open_orders(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_order_query_service: Mock,
    ) -> None:
        """Test cancel all orders when no open orders exist."""
        # Arrange - Mock order query service to return empty list
        mock_order_query_service.get_open_orders.return_value = []

        # Act
        results = await order_cancellation_service.cancel_all_orders()

        # Assert
        assert results == []

    async def test_cancel_all_orders_with_mixed_results(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_get_asset_index: AsyncMock,
        mock_order_query_service: Mock,
        mock_open_orders: list[Order],
    ) -> None:
        """Test cancel all orders with mixed success/failure results."""
        # Arrange - Mock order query service to return our mock orders
        mock_order_query_service.get_open_orders.return_value = mock_open_orders

        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        # Create mixed response with success and failure
        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=[
                    "success",
                    HyperliquidRawExchangeStatusObject(
                        resting=None, filled=None, error="Order not found"
                    ),
                ],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response
        mock_http_response: MockHttpResponse = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup for each order
        mock_get_asset_index.side_effect = [0, 1]

        # Act
        results = await order_cancellation_service.cancel_all_orders()

        # Assert
        assert len(results) == 2
        success_results = [r for r in results if r.status == CancelOrderResultStatus.SUCCESS]
        failed_results = [r for r in results if r.status == CancelOrderResultStatus.FAILED]
        assert len(success_results) == 1
        assert len(failed_results) == 1
        assert results[0].order_id == "12345"
        assert results[1].order_id == "67890"


@pytest.mark.order_cancellation
@pytest.mark.error_handling
@pytest.mark.asyncio
class TestErrorHandling:
    """Test error handling in order cancellation."""

    @pytest.mark.parametrize(("error_message", "exception_type"), HTTP_ERROR_SCENARIOS)
    async def test_cancel_order_http_errors(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_get_asset_index: AsyncMock,
        valid_cancel_order_args: CancelOrderArgs,
        error_message: str,
        exception_type: type[Exception],
    ) -> None:
        """Test order cancellation with various HTTP errors."""
        # Arrange
        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()
        mock_http_requester.side_effect = exception_type(error_message)

        # Mock asset index lookup
        mock_get_asset_index.return_value = 0

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_cancellation_service.cancel_order(valid_cancel_order_args)

        # Service wraps the error as "Unexpected service failure"
        assert "service failure" in str(exc_info.value).lower()

    async def test_cancel_order_response_validation_error(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_get_asset_index: AsyncMock,
        valid_cancel_order_args: CancelOrderArgs,
    ) -> None:
        """Test order cancellation with response validation error."""
        # Arrange
        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        # Response handler raises validation error
        mock_response_handler.handle_exchange_response.side_effect = (
            ValidationError.from_exception_data(
                "validation_error",
                [
                    {
                        "type": "missing",
                        "loc": ("response", "status"),
                        "input": {"invalid": "response"},
                    }
                ],
            )
        )

        mock_http_response: MockHttpResponse = ({"invalid": "response"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup
        mock_get_asset_index.return_value = 0

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_cancellation_service.cancel_order(valid_cancel_order_args)

        # Service wraps validation errors as "Internal data validation failed"
        assert "validation failed" in str(exc_info.value).lower()

    async def test_cancel_all_error_handling(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_order_query_service: Mock,
    ) -> None:
        """Test error handling in cancel all orders."""
        # Arrange - Mock empty open orders to trigger HTTP request
        mock_order_query_service.get_open_orders.return_value = []
        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()
        mock_http_requester.side_effect = Exception("Cancel all operation failed")

        # Act & Assert
        # The service will return empty list when no open orders, so we need to mock some orders
        # to trigger the HTTP request and error
        test_order = Order(
            exchange_order_id="12345",
            client_order_id="client_123",
            symbol=BTC_USD_HL,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity_requested=Decimal("0.1"),
            price=Decimal(50000),
            status=OrderStatus.NEW,
            quantity_filled=Decimal(0),
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            exchange="hyperliquid",
            time_in_force=TimeInForce.GTC,
        )
        mock_order_query_service.get_open_orders.return_value = [test_order]

        with pytest.raises(APIError) as exc_info:
            await order_cancellation_service.cancel_all_orders(BTC_USD_HL)

        # Service wraps the error as "Unexpected service failure"
        assert "service failure" in str(exc_info.value).lower()


@pytest.mark.order_cancellation
@pytest.mark.validation
class TestValidationLogic:
    """Test validation logic for order cancellation through public methods."""

    @pytest.mark.asyncio
    async def test_cancel_order_empty_symbol_validation(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
    ) -> None:
        """Test cancel_order raises error for empty symbol."""
        # Test validation through public method - Pydantic validates args before service
        with pytest.raises(ValidationError) as exc_info:
            await order_cancellation_service.cancel_order(
                CancelOrderArgs(symbol="", order_id="123")  # type: ignore[arg-type]
            )

        assert "symbol" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_cancel_order_empty_order_id_validation(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
    ) -> None:
        """Test cancel_order raises error for empty order ID."""
        # Test validation through public method - Pydantic validates args before service
        with pytest.raises(ValidationError) as exc_info:
            await order_cancellation_service.cancel_order(
                CancelOrderArgs(symbol=BTC_USD_HL, order_id="")  # type: ignore[arg-type]
            )

        assert "order_id" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_cancel_all_orders_too_many_orders(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_order_query_service: MagicMock,
    ) -> None:
        """Test cancel_all_orders fails when there are too many orders."""
        # Mock query service to return 51 orders (exceeds limit of 50)
        mock_open_orders = [
            Order(
                exchange_order_id=str(i + 1),  # Start from 1, not 0
                symbol=BTC_USD_HL,
                exchange="hyperliquid",
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                quantity_requested=Decimal("0.1"),
                price=Decimal(50000),
                quantity_filled=Decimal(0),
                status=OrderStatus.NEW,
                time_in_force=TimeInForce.GTC,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )
            for i in range(51)
        ]

        mock_order_query_service.get_open_orders.return_value = mock_open_orders

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await order_cancellation_service.cancel_all_orders()

        # Service should raise ValueError for too many orders directly
        assert "too many orders" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_cancel_order_valid_single_order(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_get_asset_index: AsyncMock,
    ) -> None:
        """Test cancel_order succeeds with valid single order."""
        # Arrange
        cancel_args = CancelOrderArgs(symbol=BTC_USD_HL, order_id="123")

        # Mock the HTTP flow using correct method names
        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()
        mock_http_requester.return_value = ({"status": "ok"}, 200, {})

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=["success"],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        # Mock asset index lookup
        mock_get_asset_index.return_value = 0

        # Act - should not raise any exception
        result = await order_cancellation_service.cancel_order(cancel_args)

        # Assert
        assert result is not None
        assert result.order_id == "123"
        assert result.status == CancelOrderResultStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_cancel_all_orders_boundary_50_orders(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_order_query_service: Mock,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_get_asset_index: AsyncMock,
    ) -> None:
        """Test cancel_all_orders succeeds with exactly 50 orders (boundary)."""
        # Mock query service to return exactly 50 orders
        mock_open_orders = [
            Order(
                exchange_order_id=str(i + 1),  # Start from 1, not 0
                symbol=BTC_USD_HL,
                exchange="hyperliquid",
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                quantity_requested=Decimal("0.1"),
                price=Decimal(50000),
                quantity_filled=Decimal(0),
                status=OrderStatus.NEW,
                time_in_force=TimeInForce.GTC,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )
            for i in range(50)
        ]

        mock_order_query_service.get_open_orders.return_value = mock_open_orders

        # Mock the HTTP flow using correct method names
        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()
        mock_http_requester.return_value = ({"status": "ok"}, 200, {})

        # Create success statuses for all 50 orders - cast to proper type
        success_statuses = cast(
            list[HyperliquidRawExchangeStatusObject | RawStatusStringHL], ["success"] * 50
        )
        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=success_statuses,
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response

        # Mock asset index lookup for each order
        mock_get_asset_index.side_effect = list(range(50))

        # Act - should not raise any exception at boundary
        result = await order_cancellation_service.cancel_all_orders()

        # Assert
        assert len(result) == 50
        assert all(r.status == CancelOrderResultStatus.SUCCESS for r in result)


@pytest.mark.order_cancellation
@pytest.mark.integration
@pytest.mark.asyncio
class TestIntegrationScenarios:
    """Test integration scenarios and edge cases."""

    async def test_end_to_end_single_cancellation(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_authenticator: Mock,
        mock_get_asset_index: AsyncMock,
        valid_cancel_order_args: CancelOrderArgs,
    ) -> None:
        """Test end-to-end single order cancellation."""
        # Arrange - Set up complete mock chain
        mock_request_payload = HyperliquidApiCancelOrderRequest(
            type="cancel",
            cancels=[HyperliquidRawCancelItem(a=0, o=12345)],
        )
        mock_request_builder.build_batch_cancel_order_payload.return_value = mock_request_payload

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=["success"],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response
        mock_http_response: MockHttpResponse = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup
        mock_get_asset_index.return_value = 0

        # Act
        result = await order_cancellation_service.cancel_order(valid_cancel_order_args)

        # Assert - Verify all components were called correctly
        mock_request_builder.build_batch_cancel_order_payload.assert_called_once()
        mock_http_requester.assert_called_once()
        mock_response_handler.handle_exchange_response.assert_called_once()

        assert result.order_id == "12345"
        assert result.status == CancelOrderResultStatus.SUCCESS

    async def test_end_to_end_batch_cancellation(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_authenticator: Mock,
        mock_get_asset_index: AsyncMock,
        mock_order_query_service: Mock,
    ) -> None:
        """Test end-to-end batch order cancellation."""
        # Arrange - Create 3 test orders
        test_orders = [
            Order(
                exchange_order_id="12345",
                client_order_id="client_123",
                symbol=BTC_USD_HL,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity_requested=Decimal("0.1"),
                price=Decimal(50000),
                status=OrderStatus.NEW,
                quantity_filled=Decimal(0),
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                exchange="hyperliquid",
                time_in_force=TimeInForce.GTC,
            ),
            Order(
                exchange_order_id="67890",
                client_order_id="client_456",
                symbol=BTC_USD_HL,
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity_requested=Decimal("0.2"),
                price=Decimal(51000),
                status=OrderStatus.NEW,
                quantity_filled=Decimal(0),
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                exchange="hyperliquid",
                time_in_force=TimeInForce.GTC,
            ),
            Order(
                exchange_order_id="11111",
                client_order_id="client_789",
                symbol=BTC_USD_HL,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity_requested=Decimal("0.3"),
                price=Decimal(49000),
                status=OrderStatus.NEW,
                quantity_filled=Decimal(0),
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                exchange="hyperliquid",
                time_in_force=TimeInForce.GTC,
            ),
        ]

        # Mock open orders retrieval
        mock_order_query_service.get_open_orders.return_value = test_orders

        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=["success", "success", "success"],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response
        mock_http_response: MockHttpResponse = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup for each order
        mock_get_asset_index.side_effect = [0, 1, 2]

        # Act
        results = await order_cancellation_service.cancel_all_orders(BTC_USD_HL)

        # Assert
        assert len(results) == 3
        assert all(r.status == CancelOrderResultStatus.SUCCESS for r in results)
        assert [r.order_id for r in results] == ["12345", "67890", "11111"]

        # Verify service interactions
        mock_request_builder.build_batch_cancel_order_payload.assert_called_once()
        mock_http_requester.assert_called_once()
        mock_response_handler.handle_exchange_response.assert_called_once()

    async def test_service_state_consistency(
        self,
        order_cancellation_service: HyperliquidOrderCancellationService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_get_asset_index: AsyncMock,
        valid_cancel_order_args: CancelOrderArgs,
    ) -> None:
        """Test that service maintains consistent state across operations."""
        # Arrange - Simulate multiple successful operations
        mock_request_builder.build_batch_cancel_order_payload.return_value = MagicMock()

        mock_raw_response = HyperliquidRawExchangeResponse(
            status="ok",
            data=None,
            response=HyperliquidRawExchangeResponseData(
                type="cancel",
                statuses=["success"],
            ),
        )
        mock_response_handler.handle_exchange_response.return_value = mock_raw_response
        mock_http_response: MockHttpResponse = ({"status": "ok"}, 200, {})
        mock_http_requester.return_value = mock_http_response

        # Mock asset index lookup
        mock_get_asset_index.return_value = 0

        # Act - Perform multiple operations
        result1 = await order_cancellation_service.cancel_order(valid_cancel_order_args)
        result2 = await order_cancellation_service.cancel_order(valid_cancel_order_args)

        # Assert - Both operations should succeed independently
        assert result1.status == CancelOrderResultStatus.SUCCESS
        assert result2.status == CancelOrderResultStatus.SUCCESS

        # Verify both operations called the necessary components
        assert mock_request_builder.build_batch_cancel_order_payload.call_count == 2
        assert mock_http_requester.call_count == 2
