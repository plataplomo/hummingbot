"""Unit tests for Hyperliquid Order Book Service.

Tests cover all methods of the HyperliquidOrderBookService including:
- L2 order book data retrieval
- Recent public trades retrieval
- Response validation and transformation
- Error handling and edge cases
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.response_validation import EmptyResponseError
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_order_book_mapper import (
    HyperliquidOrderBookMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawBookLevel,
    HyperliquidRawL2Book,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.market_data.hl_order_book_service import (
    HyperliquidOrderBookService,
)
from cyberdelta.apis.models.service_args.market_data import GetL2BookArgs, GetRecentTradesArgs
from cyberdelta.enums import OrderSide
from cyberdelta.models import OrderBook, Trade


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
        Mock: A mock instance of HyperliquidMarketDataRequestBuilder.
    """
    return MagicMock(spec=HyperliquidMarketDataRequestBuilder)


@pytest.fixture
def mock_response_handler() -> Mock:
    """Create a mock response handler.

    Returns:
        Mock: A mock instance of HyperliquidResponseHandler.
    """
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_mapper() -> Mock:
    """Create a mock data mapper.

    Returns:
        Mock: A mock instance of HyperliquidOrderBookMapper.
    """
    return MagicMock(spec=HyperliquidOrderBookMapper)


@pytest.fixture
def order_book_service(
    mock_http_requester: AsyncMock,
    mock_request_builder: Mock,
    mock_response_handler: Mock,
    mock_mapper: Mock,
) -> HyperliquidOrderBookService:
    """Create an order book service instance with mocks.

    Returns:
        HyperliquidOrderBookService: Service instance configured with mock dependencies.
    """
    return HyperliquidOrderBookService(
        http_client_requester=mock_http_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        mapper=mock_mapper,
        exchange_name="hyperliquid",
    )


@pytest.fixture
def mock_raw_l2_book() -> HyperliquidRawL2Book:
    """Create a mock raw L2 order book.

    Returns:
        HyperliquidRawL2Book: A mock L2 order book for ETH.
    """
    return HyperliquidRawL2Book(
        coin="ETH",
        levels=[
            [
                HyperliquidRawBookLevel(px="3499.50", sz="10.5", n=3),
                HyperliquidRawBookLevel(px="3499.00", sz="20.0", n=5),
            ],  # Bids
            [
                HyperliquidRawBookLevel(px="3500.50", sz="15.0", n=4),
                HyperliquidRawBookLevel(px="3501.00", sz="25.0", n=6),
            ],  # Asks
        ],
        time=1704067200000,  # 2024-01-01 00:00:00
    )


@pytest.fixture
def mock_order_book() -> OrderBook:
    """Create a mock order book.

    Returns:
        OrderBook: A mock order book with bids and asks for ETH.
    """
    return OrderBook(
        symbol="ETH",
        bids=[
            (Decimal("3499.50"), Decimal("10.5")),
            (Decimal("3499.00"), Decimal("20.0")),
        ],
        asks=[
            (Decimal("3500.50"), Decimal("15.0")),
            (Decimal("3501.00"), Decimal("25.0")),
        ],
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )


@pytest.fixture
def mock_raw_public_trade() -> HyperliquidRawPublicTrade:
    """Create a mock raw public trade.

    Returns:
        HyperliquidRawPublicTrade: A mock public trade for ETH.
    """
    return HyperliquidRawPublicTrade(
        coin="ETH",
        side="B",  # Buy
        px="3500.00",
        sz="0.5",
        hash="0xabc123",
        time=1704067200000,
        tid=123,
        users=["user1", "user2"],
    )


@pytest.fixture
def mock_trade() -> Trade:
    """Create a mock trade.

    Returns:
        Trade: A mock trade for ETH.
    """
    return Trade(
        id="trade_123",
        order_id="order_123",
        symbol="ETH",
        side=OrderSide.BUY,
        price=Decimal("3500.00"),
        quantity=Decimal("0.5"),
        executed_at=datetime(2024, 1, 1, tzinfo=UTC),
        exchange="hyperliquid",
    )


class TestHyperliquidOrderBookService:
    """Test suite for HyperliquidOrderBookService."""

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_raw_l2_book: HyperliquidRawL2Book,
        mock_order_book: OrderBook,
    ) -> None:
        """Test successful order book retrieval."""
        # Arrange
        symbol = "ETH"

        mock_request_payload = MagicMock()
        mock_request_builder.build_l2_book_request_payload.return_value = mock_request_payload

        raw_response: dict[str, str | list[list[list[str]]]] = {"coin": "ETH", "levels": [[[], []]]}
        mock_http_requester.return_value = (raw_response, 200, {})

        mock_response_handler.handle_info_l2_book_response.return_value = mock_raw_l2_book
        mock_mapper.transform_raw_order_book_to_internal.return_value = mock_order_book

        # Act
        result = await order_book_service.get_order_book(symbol)

        # Assert
        assert result == mock_order_book
        mock_request_builder.build_l2_book_request_payload.assert_called_once()
        call_args = mock_request_builder.build_l2_book_request_payload.call_args
        assert isinstance(call_args[0][0], GetL2BookArgs)
        assert call_args[0][0].symbol == symbol
        mock_http_requester.assert_called_once()
        assert mock_http_requester.call_args.kwargs["method"] == "POST"
        assert mock_http_requester.call_args.kwargs["endpoint"] == "/info"

    @pytest.mark.asyncio
    async def test_get_order_book_invalid_symbol(
        self,
        order_book_service: HyperliquidOrderBookService,
    ) -> None:
        """Test order book retrieval with invalid symbol."""
        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await order_book_service.get_order_book("")

        assert "'symbol' must be a non-empty string" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            await order_book_service.get_order_book("   ")

        assert "'symbol' cannot be empty or whitespace only" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_book_empty_response(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test order book retrieval with None response."""
        # Arrange
        symbol = "ETH"
        mock_request_builder.build_l2_book_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = (None, 200, {})

        # Act & Assert
        with pytest.raises(EmptyResponseError) as exc_info:
            await order_book_service.get_order_book(symbol)

        assert "l2Book data" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_order_book_request_builder_error(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_request_builder: Mock,
    ) -> None:
        """Test order book retrieval when request builder fails."""
        # Arrange
        symbol = "ETH"
        mock_request_builder.build_l2_book_request_payload.side_effect = Exception("Builder error")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_book_service.get_order_book(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Failed to build l2Book request" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_book_transformation_error(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_raw_l2_book: HyperliquidRawL2Book,
    ) -> None:
        """Test order book retrieval with transformation error."""
        # Arrange
        symbol = "ETH"

        mock_request_builder.build_l2_book_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = ({"coin": "ETH"}, 200, {})
        mock_response_handler.handle_info_l2_book_response.return_value = mock_raw_l2_book

        mock_mapper.transform_raw_order_book_to_internal.side_effect = TransformationError(
            "Invalid order book data"
        )

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_book_service.get_order_book(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to process/transform" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_raw_public_trade: HyperliquidRawPublicTrade,
        mock_trade: Trade,
    ) -> None:
        """Test successful recent trades retrieval."""
        # Arrange
        symbol = "ETH"

        mock_request_payload = MagicMock()
        mock_request_builder.build_recent_trades_request_payload.return_value = mock_request_payload

        raw_response = [{"coin": "ETH", "side": "B", "px": "3500.00", "sz": "0.5"}]
        mock_http_requester.return_value = (raw_response, 200, {})

        mock_response_handler.handle_info_recent_trades_response.return_value = [
            mock_raw_public_trade
        ]
        mock_mapper.transform_raw_public_trade_to_internal.return_value = mock_trade

        # Act
        result = await order_book_service.get_recent_trades(symbol)

        # Assert
        assert len(result) == 1
        assert result[0] == mock_trade
        mock_request_builder.build_recent_trades_request_payload.assert_called_once()
        call_args = mock_request_builder.build_recent_trades_request_payload.call_args
        assert isinstance(call_args[0][0], GetRecentTradesArgs)
        assert call_args[0][0].symbol == symbol

    @pytest.mark.asyncio
    async def test_get_recent_trades_empty_list(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
    ) -> None:
        """Test recent trades retrieval with empty response."""
        # Arrange
        symbol = "ETH"

        mock_request_builder.build_recent_trades_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = ([], 200, {})
        mock_response_handler.handle_info_recent_trades_response.return_value = []

        # Act
        result = await order_book_service.get_recent_trades(symbol)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_recent_trades_invalid_symbol(
        self,
        order_book_service: HyperliquidOrderBookService,
    ) -> None:
        """Test recent trades retrieval with invalid symbol."""
        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            await order_book_service.get_recent_trades("")

        assert "'symbol' must be a non-empty string" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_recent_trades_partial_mapping_failure(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_raw_public_trade: HyperliquidRawPublicTrade,
        mock_trade: Trade,
    ) -> None:
        """Test recent trades with some trades failing to map."""
        # Arrange
        symbol = "ETH"

        mock_request_builder.build_recent_trades_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = ([{}, {}], 200, {})

        # Create two raw trades
        raw_trade2 = HyperliquidRawPublicTrade(
            coin="ETH",
            side="A",  # Use "A" for Ask/Sell instead of "S"
            px="3501.00",
            sz="1.0",
            hash="0xdef456",
            time=1704067201000,
            tid=124,
            users=["user3"],
        )

        mock_response_handler.handle_info_recent_trades_response.return_value = [
            mock_raw_public_trade,
            raw_trade2,
        ]

        # First succeeds, second fails
        validation_error = ValidationError.from_exception_data(
            "validation_error",
            [
                {
                    "type": "value_error",
                    "loc": ("sz",),
                    "input": "Invalid size",
                    "ctx": {"error": "Invalid size value"},
                }
            ],
        )
        mock_mapper.transform_raw_public_trade_to_internal.side_effect = [
            mock_trade,
            validation_error,
        ]

        # Act
        result = await order_book_service.get_recent_trades(symbol)

        # Assert - Only successful mapping returned
        assert len(result) == 1
        assert result[0] == mock_trade

    @pytest.mark.asyncio
    async def test_get_recent_trades_none_response(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test recent trades retrieval with None response."""
        # Arrange
        symbol = "ETH"
        mock_request_builder.build_recent_trades_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = (None, 200, {})

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_book_service.get_recent_trades(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No content received" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_recent_trades_validation_error(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
    ) -> None:
        """Test recent trades with validation error."""
        # Arrange
        symbol = "ETH"

        mock_request_builder.build_recent_trades_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = ([{"invalid": "data"}], 200, {})

        validation_error = ValidationError.from_exception_data(
            "validation_error",
            [
                {
                    "type": "missing",
                    "loc": ("coin",),
                    "input": "Field required",
                    "ctx": {"error": "Field required"},
                }
            ],
        )
        mock_response_handler.handle_info_recent_trades_response.side_effect = validation_error

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_book_service.get_recent_trades(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Internal data validation failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_recent_trades_mapper_returns_none(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
        mock_response_handler: Mock,
        mock_mapper: Mock,
        mock_raw_public_trade: HyperliquidRawPublicTrade,
    ) -> None:
        """Test recent trades when mapper returns None."""
        # Arrange
        symbol = "ETH"

        mock_request_builder.build_recent_trades_request_payload.return_value = MagicMock()
        mock_http_requester.return_value = ([{}], 200, {})
        mock_response_handler.handle_info_recent_trades_response.return_value = [
            mock_raw_public_trade
        ]

        # Mapper returns None (trade filtered out)
        mock_mapper.transform_raw_public_trade_to_internal.return_value = None

        # Act
        result = await order_book_service.get_recent_trades(symbol)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_get_order_book_http_error(
        self,
        order_book_service: HyperliquidOrderBookService,
        mock_http_requester: AsyncMock,
        mock_request_builder: Mock,
    ) -> None:
        """Test order book retrieval with HTTP error."""
        # Arrange
        symbol = "ETH"
        mock_request_builder.build_l2_book_request_payload.return_value = MagicMock()
        mock_http_requester.side_effect = Exception("Network timeout")

        # Act & Assert
        with pytest.raises(APIError) as exc_info:
            await order_book_service.get_order_book(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected service failure" in str(exc_info.value)
