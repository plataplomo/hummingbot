"""Unit tests for Backpack Transaction History Service.

Tests cover all methods of the BackpackTransactionHistoryService including:
- Order history retrieval with filtering
- Trade history (fills) retrieval
- Time-based and symbol-based filtering
- Error handling and data validation
- Response transformation and selective skipping
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper import BackpackTransactionMapper
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.backpack.request_builders.bp_trading_request_builder import (
    BackpackTradingRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_trading_response_handler import (
    BackpackTradingResponseHandler,
)
from cyberdelta.apis.backpack.services.account.bp_transaction_history_service import (
    BackpackTransactionHistoryService,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.models.service_args.trading import GetOrderHistoryArgs, GetTradeHistoryArgs
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.models import Order, Trade
from cyberdelta.enums import OrderSide, OrderType, TimeInForce


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Create a mock HTTP client requester.
    
    Returns:
        AsyncMock instance configured for HTTP client testing.
    """
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Create a mock request builder.
    
    Returns:
        MagicMock instance configured as BackpackTradingRequestBuilder.
    """
    return MagicMock(spec=BackpackTradingRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Create a mock response handler.
    
    Returns:
        MagicMock instance configured as BackpackTradingResponseHandler.
    """
    return MagicMock(spec=BackpackTradingResponseHandler)


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Create a mock data mapper.
    
    Returns:
        MagicMock instance configured as BackpackTransactionMapper.
    """
    return MagicMock(spec=BackpackTransactionMapper)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Create a mock authenticator.
    
    Returns:
        MagicMock instance for authentication testing.
    """
    return MagicMock()


@pytest.fixture
def transaction_history_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_mapper: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackTransactionHistoryService:
    """Create a transaction history service instance with mocks.
    
    Args:
        mock_http_client: Mock HTTP client for API requests.
        mock_request_builder: Mock request builder for constructing requests.
        mock_response_handler: Mock response handler for processing responses.
        mock_mapper: Mock mapper for data transformation.
        mock_authenticator: Mock authenticator for authentication.
        
    Returns:
        BackpackTransactionHistoryService instance configured with all mocks.
    """
    return BackpackTransactionHistoryService(
        http_client_requester=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        authenticator=mock_authenticator,
        exchange_name="backpack",
    )


@pytest.fixture
def mock_raw_order() -> BackpackRawOrderResponse:
    """Create a mock raw order.
    
    Returns:
        BackpackRawOrderResponse instance with sample order data
    """
    return BackpackRawOrderResponse(
        id="order_123",
        symbol="BTC-USDC",
        side="Buy",
        orderType="Limit",
        status="Filled",
        createdAt=1705312200,
        clientId="client_123",
        relatedOrderId=None,
        price="50000.00",
        quantity="0.1",
        triggerPrice=None,
        avgFillPrice="50000.00",
        triggerBy=None,
        timeInForce="GTC",
        postOnly=False,
        reduceOnly=False,
        selfTradePrevention=None,
        executedQuantity="0.1",
        executedQuoteQuantity="5000.00",
        updatedAt=None,
        triggeredAt=None,
        expiryReason=None,
        origin=None,
    )


@pytest.fixture
def mock_order() -> Order:
    """Create a mock order.
    
    Returns:
        Order instance with sample data matching the mock_raw_order.
    """
    return Order(
        exchange_order_id="order_123",
        client_order_id="client_123",
        symbol="BTC-USDC",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity_requested=Decimal("0.1"),
        price=Decimal("50000.00"),
        status=OrderStatus.FILLED,
        quantity_filled=Decimal("0.1"),
        average_fill_price=Decimal("50000.00"),  # Required when quantity_filled > 0
        created_at=datetime(2024, 1, 15, 10, 30, tzinfo=UTC),
        updated_at=None,
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
        exchange="backpack",
    )


@pytest.fixture
def mock_raw_fill() -> BackpackRawFillResponse:
    """Create a mock raw fill.
    
    Returns:
        BackpackRawFillResponse instance with sample fill/trade data
    """
    return BackpackRawFillResponse(
        tradeId=456,
        orderId="order_123",
        symbol="BTC-USDC",
        side="Bid",  # Business logic expects "Bid" or "Ask", not "Buy"
        price="50000.00",
        quantity="0.05",
        fee="2.50",
        feeSymbol="USDC",
        isMaker=False,
        timestamp="2024-01-15T10:31:00Z",
        clientId=None,
        systemOrderType=None,
    )


@pytest.fixture
def mock_trade() -> Trade:
    """Create a mock trade.
    
    Returns:
        Trade instance with sample data matching the mock_raw_fill.
    """
    return Trade(
        id="trade_456",
        order_id="order_123",
        symbol="BTC-USDC",
        side=OrderSide.BUY,
        price=Decimal("50000.00"),
        quantity=Decimal("0.05"),
        fee=Decimal("2.50"),
        fee_asset="USDC",
        executed_at=datetime(2024, 1, 15, 10, 31, tzinfo=UTC),
        exchange="backpack",
    )


class TestBackpackTransactionHistoryService:
    """Test suite for BackpackTransactionHistoryService."""

    @pytest.mark.asyncio
    async def test_get_order_history_success(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_order: BackpackRawOrderResponse,
        mock_order: Order,
    ) -> None:
        """Test successful order history retrieval."""
        # Arrange
        args = GetOrderHistoryArgs(
            symbol="BTC-USDC",
            limit=100,
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 31, tzinfo=UTC),
        )

        raw_orders_response = [mock_raw_order.model_dump()]
        mock_http_client.return_value = (raw_orders_response, 200, {})

        mock_response_handler.handle_get_order_history_response.return_value = [mock_raw_order]
        mock_mapper.transform_raw_order_to_internal.return_value = mock_order

        # Act
        result = await transaction_history_service.get_order_history(args)

        # Assert
        assert len(result) == 1
        assert result[0] == mock_order
        mock_http_client.assert_called_once()
        mock_response_handler.handle_get_order_history_response.assert_called_once()
        mock_mapper.transform_raw_order_to_internal.assert_called_once_with(mock_raw_order)

    @pytest.mark.asyncio
    async def test_get_order_history_with_order_id_filter(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_order: BackpackRawOrderResponse,
        mock_order: Order,
    ) -> None:
        """Test order history retrieval with specific order ID."""
        # Arrange
        args = GetOrderHistoryArgs(
            order_id="order_123",
        )

        raw_orders_response = [mock_raw_order.model_dump()]
        mock_http_client.return_value = (raw_orders_response, 200, {})

        mock_response_handler.handle_get_order_history_response.return_value = [mock_raw_order]
        mock_mapper.transform_raw_order_to_internal.return_value = mock_order

        # Act
        result = await transaction_history_service.get_order_history(args)

        # Assert
        assert len(result) == 1
        assert result[0].exchange_order_id == "order_123"

    @pytest.mark.asyncio
    async def test_get_order_history_with_client_order_id_filter(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_order: BackpackRawOrderResponse,
        mock_order: Order,
    ) -> None:
        """Test order history retrieval with client order ID."""
        # Arrange
        args = GetOrderHistoryArgs(
            client_order_id="client_123",
        )

        raw_orders_response = [mock_raw_order.model_dump()]
        mock_http_client.return_value = (raw_orders_response, 200, {})

        mock_response_handler.handle_get_order_history_response.return_value = [mock_raw_order]
        mock_mapper.transform_raw_order_to_internal.return_value = mock_order

        # Act
        result = await transaction_history_service.get_order_history(args)

        # Assert
        assert len(result) == 1
        assert result[0].client_order_id == "client_123"

    @pytest.mark.asyncio
    async def test_get_order_history_empty_response(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test order history with empty response."""
        # Arrange
        args = GetOrderHistoryArgs(symbol="NONEXISTENT-USDC")

        mock_http_client.return_value = ([], 200, {})
        mock_response_handler.handle_get_order_history_response.return_value = []

        # Act
        result = await transaction_history_service.get_order_history(args)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_order_history_partial_mapping_failure(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_order: BackpackRawOrderResponse,
        mock_order: Order,
    ) -> None:
        """Test order history with some orders failing to map."""
        # Arrange
        args = GetOrderHistoryArgs(limit=3)

        # Create multiple raw orders - business logic requires valid decimal values
        raw_order2 = BackpackRawOrderResponse(
            id="order_invalid",
            clientId="client_invalid",
            symbol="INVALID",
            side="Buy",
            price="60000.00",  # Valid decimal value
            quantity="0.1",
            orderType="Limit",
            status="New",
            createdAt="2024-01-15T10:30:00Z",
            relatedOrderId=None,
            executedQuantity=None,
            executedQuoteQuantity=None,
            triggerPrice=None,
            avgFillPrice=None,
            triggerBy=None,
            timeInForce=None,
            reduceOnly=None,
            postOnly=None,
            selfTradePrevention=None,
            updatedAt=None,
            triggeredAt=None,
            expiryReason=None,
            origin=None,
        )

        raw_orders_response = [
            mock_raw_order.model_dump(),
            raw_order2.model_dump(),
        ]
        mock_http_client.return_value = (raw_orders_response, 200, {})

        mock_response_handler.handle_get_order_history_response.return_value = [
            mock_raw_order,
            raw_order2,
        ]

        # First succeeds, second fails
        mock_mapper.transform_raw_order_to_internal.side_effect = [
            mock_order,
            TransformationError(
                message="Failed to transform order", source_data={"order_id": "order_invalid"}
            ),
        ]

        # Act
        result = await transaction_history_service.get_order_history(args)

        # Assert - Only successful mapping returned
        assert len(result) == 1
        assert result[0] == mock_order

    @pytest.mark.asyncio
    async def test_get_order_history_http_error(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
    ) -> None:
        """Test order history with HTTP error."""
        # Arrange
        args = GetOrderHistoryArgs()
        # Business logic catches and wraps exceptions from HTTP client
        mock_http_client.side_effect = Exception("Network error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await transaction_history_service.get_order_history(args)

        # Business logic wraps the exception in APIError with message "Unexpected service failure."
        # and the original exception is accessible via original_exception
        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected service failure" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_history_transformation_error(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test order history with transformation error."""
        # Arrange
        args = GetOrderHistoryArgs()

        mock_http_client.return_value = ([{"invalid": "data"}], 200, {})
        mock_response_handler.handle_get_order_history_response.side_effect = TransformationError(
            "Invalid order format"
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await transaction_history_service.get_order_history(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_get_trade_history_success(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_fill: BackpackRawFillResponse,
        mock_trade: Trade,
    ) -> None:
        """Test successful trade history retrieval."""
        # Arrange
        args = GetTradeHistoryArgs(
            symbol="BTC-USDC",
            limit=50,
        )

        raw_fills_response = [mock_raw_fill.model_dump()]
        mock_http_client.return_value = (raw_fills_response, 200, {})

        mock_response_handler.handle_get_fills_response.return_value = [mock_raw_fill]
        mock_mapper.transform_raw_fill_to_internal.return_value = mock_trade

        # Act
        result = await transaction_history_service.get_trade_history(args)

        # Assert
        assert len(result) == 1
        assert result[0] == mock_trade
        mock_http_client.assert_called_once()

        # Verify fills endpoint was called
        call_args = mock_http_client.call_args
        assert call_args.kwargs["endpoint"] == "/wapi/v1/history/fills"

    @pytest.mark.asyncio
    async def test_get_trade_history_empty_response(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test trade history with empty response."""
        # Arrange
        args = GetTradeHistoryArgs(symbol="NONEXISTENT-USDC")

        mock_http_client.return_value = ([], 200, {})
        mock_response_handler.handle_get_fills_response.return_value = []

        # Act
        result = await transaction_history_service.get_trade_history(args)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_trade_history_partial_mapping_failure(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_fill: BackpackRawFillResponse,
        mock_trade: Trade,
    ) -> None:
        """Test trade history with some fills failing to map."""
        # Arrange
        args = GetTradeHistoryArgs(limit=2)

        # Create multiple raw fills
        raw_fill2 = BackpackRawFillResponse.model_validate({
            "tradeId": 123457,
            "orderId": "order_invalid",
            "symbol": "INVALID",
            "side": "Ask",  # Business logic expects "Bid" or "Ask" for fills
            "price": "100.00",
            "quantity": "0.1",
            "fee": "0.1",
            "feeSymbol": "USDC",
            "timestamp": "2024-01-15T10:31:00Z",
            "isMaker": False,
            "clientId": None,
            "systemOrderType": "Market",
        })

        raw_fills_response = [
            mock_raw_fill.model_dump(),
            raw_fill2.model_dump(),
        ]
        mock_http_client.return_value = (raw_fills_response, 200, {})

        mock_response_handler.handle_get_fills_response.return_value = [
            mock_raw_fill,
            raw_fill2,
        ]

        # First succeeds, second fails
        mock_mapper.transform_raw_fill_to_internal.side_effect = [
            mock_trade,
            ValidationError.from_exception_data(
                "validation_error",
                [{"type": "missing", "loc": ("price",), "input": {}}],
            ),
        ]

        # Act
        result = await transaction_history_service.get_trade_history(args)

        # Assert - Only successful mapping returned
        assert len(result) == 1
        assert result[0] == mock_trade

    @pytest.mark.asyncio
    async def test_get_trade_history_mapper_returns_none(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
        mock_raw_fill: BackpackRawFillResponse,
        mock_trade: Trade,
    ) -> None:
        """Test trade history when mapper returns None for some fills."""
        # Arrange
        args = GetTradeHistoryArgs()

        raw_fills_response = [
            mock_raw_fill.model_dump(),
            mock_raw_fill.model_dump(),
        ]
        mock_http_client.return_value = (raw_fills_response, 200, {})

        mock_response_handler.handle_get_fills_response.return_value = [
            mock_raw_fill,
            mock_raw_fill,
        ]

        # First returns trade, second returns None
        mock_mapper.transform_raw_fill_to_internal.side_effect = [
            mock_trade,
            None,
        ]

        # Act
        result = await transaction_history_service.get_trade_history(args)

        # Assert - Only non-None trades returned
        assert len(result) == 1
        assert result[0] == mock_trade

    @pytest.mark.asyncio
    async def test_get_trade_history_http_error(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
    ) -> None:
        """Test trade history with HTTP error."""
        # Arrange
        args = GetTradeHistoryArgs()
        # Business logic catches and wraps exceptions from HTTP client
        mock_http_client.side_effect = Exception("Connection timeout")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await transaction_history_service.get_trade_history(args)

        # Business logic wraps the exception in APIError with message "Unexpected service failure."
        # and the original exception is accessible via original_exception
        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected service failure" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_trade_history_validation_error(
        self,
        transaction_history_service: BackpackTransactionHistoryService,
        mock_http_client: AsyncMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test trade history with validation error."""
        # Arrange
        args = GetTradeHistoryArgs()

        mock_http_client.return_value = ([{"invalid": "data"}], 200, {})
        mock_response_handler.handle_get_fills_response.side_effect = (
            ValidationError.from_exception_data(
                "validation_error",
                [{"type": "missing", "loc": ("tradeId",), "input": {}}],
            )
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await transaction_history_service.get_trade_history(args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_get_order_history_without_authenticator(
        self,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test order history when authenticator is None."""
        # Arrange
        service = BackpackTransactionHistoryService(
            http_client_requester=mock_http_client,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            mapper=mock_mapper,
            authenticator=None,  # No authenticator
            exchange_name="backpack",
        )

        args = GetOrderHistoryArgs()
        mock_http_client.return_value = ([], 200, {})
        mock_response_handler.handle_get_order_history_response.return_value = []

        # Act
        result = await service.get_order_history(args)

        # Assert - Should still work, authenticator is optional
        assert result == []
