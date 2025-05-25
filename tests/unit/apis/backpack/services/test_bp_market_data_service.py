"""
Unit tests for the BackpackMarketDataService.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import (  # Example Raw Models
    BackpackRawOrderBook,
    BackpackRawTicker,
)
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Import internal models for assertions
from cyberdelta.core.models.market import Candle, FundingRate, OrderBook, Ticker, Trade
from cyberdelta.core.models.market.funding_rate import BackpackFundingDetails


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    return MagicMock(spec=BackpackResponseHandler)


@pytest.fixture
def mock_rate_limiter_service() -> MagicMock:
    """Provides a mock RateLimiterService."""
    mock_service = MagicMock(spec=RateLimiterService)
    # Mock get_limiter to return an AsyncMock for the runtime limiter
    limiter_runtime_mock = AsyncMock()  # This will have .acquire()
    limiter_runtime_mock.acquire = AsyncMock()  # Ensure acquire is an AsyncMock
    mock_service.get_limiter.return_value = limiter_runtime_mock
    return mock_service


@pytest.fixture
def backpack_market_data_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_rate_limiter_service: AsyncMock,
) -> BackpackMarketDataService:
    """Provides an instance of BackpackMarketDataService with mocked dependencies."""
    return BackpackMarketDataService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        exchange_name="backpack_test_market_data",
        rate_limiter_service=mock_rate_limiter_service,
    )


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Provides a mock BackpackMarketDataMapper."""
    return MagicMock(spec=BackpackMarketDataMapper)


class TestBackpackMarketDataService:
    """Tests for the BackpackMarketDataService class."""

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker successfully retrieves and processes ticker data."""
        symbol = "SOL_USDC"
        mock_timestamp_int = 1678886400  # Example timestamp
        mock_timestamp_dt = datetime.fromtimestamp(mock_timestamp_int, tz=UTC)

        mock_endpoint_path = "/api/v1/ticker"
        mock_params = {"symbol": symbol}
        mock_raw_response_content = {
            "symbol": symbol,
            "price": "100.0",
            "volume": "1000.0",
            "bid": "99.9",
            "ask": "100.1",
            "time": mock_timestamp_int,
        }
        mock_status_code = 200
        mock_headers_from_client = MagicMock()  # Consistent mock instance

        mock_raw_ticker = BackpackRawTicker(
            symbol=symbol,
            price="100.0",
            volume="1000.0",
            bid="99.9",
            ask="100.1",
            time=mock_timestamp_int,
        )
        mock_internal_ticker = Ticker(
            symbol=symbol,
            price=Decimal("100.0"),
            volume=Decimal("1000.0"),
            bid=Decimal("99.9"),
            ask=Decimal("100.1"),
            timestamp=mock_timestamp_dt,
        )

        mock_request_builder.build_get_ticker_params.return_value = mock_params  # Only params
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,  # Use consistent mock
        )
        mock_response_handler.handle_get_ticker_response.return_value = mock_raw_ticker

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_ticker_to_internal.return_value = mock_internal_ticker

            result_ticker = await backpack_market_data_service.get_ticker(symbol)

            mock_request_builder.build_get_ticker_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_mapper.transform_raw_ticker_to_internal.assert_called_once_with(
                mock_raw_ticker, symbol_override=symbol
            )
            mock_response_handler.handle_get_ticker_response.assert_called_once_with(
                mock_raw_response_content,
                symbol,
                mock_status_code,
                mock_headers_from_client,  # Use consistent mock
            )
            assert result_ticker == mock_internal_ticker

    @pytest.mark.asyncio
    async def test_get_ticker_api_error_from_requester(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_ticker handles APIError from http_client."""
        symbol = "SOL_USDC"
        mock_endpoint_path = f"/api/v1/ticker?symbol={symbol}"
        mock_params = {"symbol": symbol}

        mock_request_builder.build_get_ticker_params.return_value = (
            mock_endpoint_path,
            mock_params,
        )
        mock_http_client_requester.side_effect = APIError(
            message="Network error", code=APIErrorCode.SERVER_ERROR.value
        )

        with pytest.raises(APIError) as excinfo:
            await backpack_market_data_service.get_ticker(symbol)

        assert excinfo.value.code == APIErrorCode.SERVER_ERROR.value

    @pytest.mark.asyncio
    async def test_get_ticker_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker when HTTP client returns None content."""
        symbol = "SOL_USDC"
        mock_endpoint_path = "/api/v1/ticker"
        mock_params = {"symbol": symbol}

        mock_request_builder.build_get_ticker_params.return_value = mock_params  # Only params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_ticker(symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_msg_part = f"No data for ticker {symbol}, status: 200"
            assert expected_msg_part in exc_info.value.message

            mock_request_builder.build_get_ticker_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_ticker_response.assert_not_called()
            mock_mapper.transform_raw_ticker_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_ticker_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol}
        mock_raw_response = {"invalid": "ticker_data"}

        mock_request_builder.build_get_ticker_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawTicker.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_ticker_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_ticker(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing ticker data failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_ticker_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_ticker handles unexpected exception."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol}
        mock_raw_response = {"symbol": symbol, "price": "100.0"}

        mock_request_builder.build_get_ticker_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_ticker_response.side_effect = Exception("Unexpected error")

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_ticker(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for ticker" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book successfully retrieves and processes order book data."""
        symbol = "SOL_USDC"
        depth = 50
        mock_endpoint_path = "/api/v1/depth"
        mock_params = {"symbol": symbol, "limit": depth}
        mock_raw_response_content = {
            "bids": [["100.0", "10"]],
            "asks": [["100.1", "12"]],
            "lastUpdateId": "12345",
            "timestamp": 1678886400000,
        }
        mock_validated_book = BackpackRawOrderBook(
            bids=[("100.0", "10")],
            asks=[("100.1", "12")],
            lastUpdateId="12345",
            timestamp=1678886400000,
        )
        mock_headers_from_client = MagicMock()  # Consistent mock

        mock_request_builder.build_get_order_book_params.return_value = mock_params  # Only params
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            mock_headers_from_client,  # Use consistent mock
        )
        mock_response_handler.handle_get_order_book_response.return_value = mock_validated_book

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_order_book = MagicMock(spec=OrderBook)  # Assume mapper returns this
            mock_mapper.transform_raw_order_book_to_internal.return_value = mock_internal_order_book

            result = await backpack_market_data_service.get_order_book(symbol, limit=depth)

            mock_request_builder.build_get_order_book_params.assert_called_once_with(
                symbol=symbol, limit=depth
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_order_book_response.assert_called_once_with(
                mock_raw_response_content,
                symbol,
                200,
                mock_headers_from_client,  # Use consistent mock
            )
            mock_mapper.transform_raw_order_book_to_internal.assert_called_once_with(
                symbol, mock_validated_book
            )  # Add symbol to call
            assert result == mock_internal_order_book

    @pytest.mark.asyncio
    async def test_get_order_book_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book when HTTP client returns None content."""
        symbol = "SOL_USDC"
        depth = 100
        mock_endpoint_path = "/api/v1/depth"
        mock_params = {"symbol": symbol, "limit": depth}

        mock_request_builder.build_get_order_book_params.return_value = mock_params  # Only params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_order_book(symbol, limit=depth)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_msg_part = f"No data for order_book {symbol}, status: 200"
            assert expected_msg_part in exc_info.value.message

            mock_request_builder.build_get_order_book_params.assert_called_once_with(
                symbol=symbol, limit=depth
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_order_book_response.assert_not_called()
            mock_mapper.transform_raw_order_book_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_order_book_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol, "limit": 20}
        mock_raw_response: dict[str, Any] = {"invalid": "order_book_data"}

        mock_request_builder.build_get_order_book_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawOrderBook.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_order_book_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_order_book(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing order_book failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_book_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_order_book handles unexpected exception."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol, "limit": 20}
        mock_raw_response: dict[str, list[Any]] = {"bids": [], "asks": []}

        mock_request_builder.build_get_order_book_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_order_book_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_order_book(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for order_book" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades successfully retrieves and processes trade data."""
        symbol = "SOL_USDC"
        limit = 50
        mock_endpoint_path = "/api/v1/trades"
        mock_params = {"symbol": symbol, "limit": limit}
        mock_raw_response_content = [
            {
                "tradeId": 12345,
                "orderId": "o1",
                "symbol": symbol,
                "price": "2000.0",
                "qty": "1.0",
                "time": 1678886400100,
            },
            {
                "tradeId": 12346,
                "orderId": "o2",
                "symbol": symbol,
                "price": "2000.1",
                "qty": "0.5",
                "time": 1678886400200,
            },
        ]
        mock_headers_from_client = MagicMock()  # Consistent mock

        mock_request_builder.build_get_recent_trades_params.return_value = (
            mock_params  # Only params
        )
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            mock_headers_from_client,  # Use consistent mock
        )
        mock_response_handler.handle_get_recent_trades_response.return_value = (
            mock_raw_response_content
        )

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_trades = [MagicMock(spec=Trade), MagicMock(spec=Trade)]
            mock_mapper.transform_raw_trade_to_internal.side_effect = mock_internal_trades

            result = await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            mock_request_builder.build_get_recent_trades_params.assert_called_once_with(
                symbol=symbol, limit=limit
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_recent_trades_response.assert_called_once_with(
                mock_raw_response_content,
                symbol,
                200,
                mock_headers_from_client,  # Use consistent mock
            )
            assert mock_mapper.transform_raw_trade_to_internal.call_count == len(
                mock_raw_response_content
            )
            assert result == mock_internal_trades

    @pytest.mark.asyncio
    async def test_get_recent_trades_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades when HTTP client returns None content."""
        symbol = "ETH_USDC"
        limit = 5
        mock_endpoint_path = "/api/v1/trades"
        mock_params = {"symbol": symbol, "limit": limit}

        mock_request_builder.build_get_recent_trades_params.return_value = (
            mock_params  # Only params
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert f"No data for recent_trades {symbol}, status: 200" in exc_info.value.message

            mock_request_builder.build_get_recent_trades_params.assert_called_once_with(
                symbol=symbol, limit=limit
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_recent_trades_response.assert_not_called()
            mock_mapper.transform_raw_trade_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_recent_trades_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades handles validation error from response handler."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol, "limit": 100}
        mock_raw_response: list[dict[str, str | int]] = [{"invalid": "trade_data"}]

        mock_request_builder.build_get_recent_trades_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            # This will cause a validation error since it doesn't match expected schema
            from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade

            BackpackRawTrade.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_recent_trades_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_recent_trades(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing recent_trades failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_recent_trades_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_recent_trades handles unexpected exception."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol, "limit": 100}
        mock_raw_response = [{"tradeId": 123}]

        mock_request_builder.build_get_recent_trades_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_recent_trades_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_recent_trades(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for recent_trades" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate successfully retrieves and processes funding rate data."""
        symbol = "SOL-PERP"
        mock_endpoint_path = "/api/v1/funding"
        mock_params = {"symbol": symbol}
        raw_time_str = "2023-10-27T10:00:00Z"
        mock_raw_response_content = {
            "symbol": symbol,
            "rate": "0.0001",
            "markPrice": "100.0",
            "indexPrice": "99.0",
            "time": raw_time_str,
        }
        mock_status_code = 200
        mock_headers_from_client = MagicMock()  # Consistent mock

        mock_raw_funding_rate = BackpackRawFundingRate(
            symbol=symbol,
            rate="0.0001",
            markPrice="100.0",
            indexPrice="99.0",
            time=raw_time_str,
        )
        expected_internal_funding_rate = FundingRate(
            symbol=symbol,
            timestamp=datetime.fromisoformat(raw_time_str.replace("Z", "+00:00")),
            funding_rate=Decimal("0.0001"),
            mark_price=Decimal("100.0"),
            index_price=Decimal("99.0"),
            next_funding_time=datetime.fromisoformat(raw_time_str.replace("Z", "+00:00")),
            bp_details=BackpackFundingDetails(),
        )

        mock_request_builder.build_get_funding_rate_params.return_value = mock_params  # Only params
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers_from_client,  # Use consistent mock
        )
        mock_response_handler.handle_get_funding_rate_response.return_value = mock_raw_funding_rate

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_mapper.transform_raw_funding_rate_to_internal.return_value = (
                expected_internal_funding_rate
            )
            result_funding_rate = await backpack_market_data_service.get_funding_rate(symbol)

            mock_request_builder.build_get_funding_rate_params.assert_called_once_with(
                symbol=symbol
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_funding_rate_response.assert_called_once_with(
                mock_raw_response_content,
                symbol,
                mock_status_code,
                mock_headers_from_client,  # Use consistent mock
            )
            mock_mapper.transform_raw_funding_rate_to_internal.assert_called_once_with(
                mock_raw_funding_rate
            )
            assert result_funding_rate == expected_internal_funding_rate

    @pytest.mark.asyncio
    async def test_get_funding_rate_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate when HTTP client returns None content."""
        symbol = "SOL-PERP"
        mock_endpoint_path = "/api/v1/funding"
        mock_params = {"symbol": symbol}

        mock_request_builder.build_get_funding_rate_params.return_value = mock_params  # Only params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_funding_rate(symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert f"No data for funding_rate {symbol}, status: 200" in exc_info.value.message

            mock_request_builder.build_get_funding_rate_params.assert_called_once_with(
                symbol=symbol
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_funding_rate_response.assert_not_called()
            mock_mapper.transform_raw_funding_rate_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_funding_rate_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate handles validation error from response handler."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol}
        mock_raw_response = {"invalid": "funding_rate_data"}

        mock_request_builder.build_get_funding_rate_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawFundingRate.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_funding_rate_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_funding_rate(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing funding_rate failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_funding_rate_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_funding_rate handles unexpected exception."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol}
        mock_raw_response = {"symbol": symbol, "rate": "0.001"}

        mock_request_builder.build_get_funding_rate_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_funding_rate_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_funding_rate(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for funding_rate" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data successfully retrieves and processes kline data."""
        symbol = "SOL_USDC"
        timeframe = "1m"
        limit = 2
        mock_endpoint_path = "/api/v1/klines"
        mock_params = {"symbol": symbol, "interval": timeframe, "limit": limit}
        mock_raw_kline_data = [
            [
                1678886400,  # int
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                "1000.0",
                1678886400,  # int
                "100000.0",
                50,  # int
                "60.0",
                "180000.0",
                "0",
            ],
            [
                1678886460,  # int
                "100.5",
                "101.5",
                "99.5",
                "101.0",
                "1200.0",
                1678886460,  # int
                "120000.0",
                60,  # int
                "70.0",
                "210000.0",
                "0",
            ],
        ]
        mock_validated_klines_raw = [
            BackpackRawKline.model_validate(kline) for kline in mock_raw_kline_data
        ]
        mock_headers_from_client = MagicMock()  # Consistent mock

        mock_request_builder.build_get_market_data_params.return_value = mock_params  # Only params

        mock_http_client_requester.return_value = (
            mock_raw_kline_data,  # Raw list of lists
            200,
            mock_headers_from_client,  # Use consistent mock
        )
        mock_response_handler.handle_get_market_data_response.return_value = (
            mock_validated_klines_raw
        )

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_candles = [MagicMock(spec=Candle), MagicMock(spec=Candle)]
            mock_mapper.transform_raw_kline_to_internal.side_effect = mock_internal_candles

            result = await backpack_market_data_service.get_market_data(
                symbol=symbol, timeframe=timeframe, limit=limit
            )

            mock_request_builder.build_get_market_data_params.assert_called_once_with(
                symbol=symbol,
                timeframe_str=timeframe,
                limit=limit,
                start_time_ms=None,
                end_time_ms=None,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_market_data_response.assert_called_once_with(
                mock_raw_kline_data,
                symbol,
                timeframe,
                200,
                mock_headers_from_client,  # Use consistent mock
            )
            assert mock_mapper.transform_raw_kline_to_internal.call_count == len(
                mock_validated_klines_raw
            )
            assert result == mock_internal_candles

    @pytest.mark.asyncio
    async def test_get_market_data_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data when HTTP client returns None content."""
        symbol = "SOL_USDC"
        timeframe = "1h"
        limit = 100
        mock_endpoint_path = "/api/v1/klines"
        mock_params = {"symbol": symbol, "interval": timeframe, "limit": limit}

        mock_request_builder.build_get_market_data_params.return_value = mock_params  # Only params
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(
            backpack_market_data_service, "_mapper", autospec=True
        ) as mock_mapper:  # Keep mapper patch for consistency
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_market_data(
                    symbol=symbol, timeframe=timeframe, limit=limit
                )

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_error_msg = f"No data for klines {symbol}@{timeframe}, status: 200"
            assert exc_info.value.message == expected_error_msg

            mock_request_builder.build_get_market_data_params.assert_called_once_with(
                symbol=symbol,
                timeframe_str=timeframe,
                limit=limit,
                start_time_ms=None,
                end_time_ms=None,
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_market_data_response.assert_not_called()
            # Ensure mapper not called
            mock_mapper.transform_raw_kline_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_data_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data handles validation error from response handler."""
        symbol = "SOL_USDC"
        timeframe = "1m"
        mock_params = {"symbol": symbol, "interval": timeframe, "limit": 100}
        mock_raw_response = [["invalid", "kline_data"]]

        mock_request_builder.build_get_market_data_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawKline.model_validate(["invalid", "data"])
        except ValidationError as e:
            mock_response_handler.handle_get_market_data_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_market_data(symbol, timeframe)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing klines failed" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_market_data_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data handles unexpected exception."""
        symbol = "SOL_USDC"
        timeframe = "1m"
        mock_params = {"symbol": symbol, "interval": timeframe, "limit": 100}
        mock_raw_response = [[1678886400, "100.0"]]

        mock_request_builder.build_get_market_data_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_market_data_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_market_data(symbol, timeframe)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert "Unexpected error for klines" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates success."""
        symbol = "SOL-PERP"
        start_time_ms = 1678880000000
        end_time_ms = 1678886400000
        limit = 10
        mock_endpoint_path = "/api/v1/funding/history"
        mock_params = {
            "symbol": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
            "limit": limit,
        }
        raw_time_1 = "2023-10-27T10:00:00Z"
        raw_time_2 = "2023-10-28T10:00:00Z"
        mock_raw_response_content_list = [
            {
                "symbol": symbol,
                "rate": "0.0001",
                "markPrice": "100.0",
                "indexPrice": "99.0",
                "time": raw_time_1,
            },
            {
                "symbol": symbol,
                "rate": "0.0002",
                "markPrice": "101.0",
                "indexPrice": "100.0",
                "time": raw_time_2,
            },
        ]
        mock_validated_funding_rates_raw = mock_raw_response_content_list
        mock_headers_from_client = MagicMock()  # Consistent mock

        mock_request_builder.build_get_historical_funding_rates_params.return_value = (
            mock_params  # Only params
        )
        mock_http_client_requester.return_value = (
            mock_raw_response_content_list,
            200,
            mock_headers_from_client,  # Use consistent mock
        )
        mock_response_handler.handle_get_historical_funding_rates_response.return_value = (
            mock_validated_funding_rates_raw
        )

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_funding_rates = [MagicMock(spec=FundingRate), MagicMock(spec=FundingRate)]
            mock_mapper.transform_raw_funding_interval_rate_to_internal.side_effect = (
                mock_internal_funding_rates
            )

            result = await backpack_market_data_service.get_historical_funding_rates(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms, limit=limit
            )

            mock_request_builder.build_get_historical_funding_rates_params.assert_called_once_with(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms, limit=limit
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )

            # Assert call and then check args/kwargs separately due to persistent assertion issues
            mock_response_handler.handle_get_historical_funding_rates_response.assert_called_once()
            call_args_tuple = (
                mock_response_handler.handle_get_historical_funding_rates_response.call_args
            )

            # Check positional arguments
            assert call_args_tuple.args[0] == mock_raw_response_content_list
            assert call_args_tuple.args[1] == symbol
            assert call_args_tuple.args[2] == 200
            assert call_args_tuple.args[3] is mock_headers_from_client  # Check instance for mock

            # Check keyword arguments
            assert not call_args_tuple.kwargs  # Ensure no unexpected kwargs were passed

            # Assert calls to mapper
            assert mock_mapper.transform_raw_funding_interval_rate_to_internal.call_count == len(
                mock_validated_funding_rates_raw
            )
            for _i, _raw_item in enumerate(mock_validated_funding_rates_raw):
                pass  # Placeholder for more specific arg checking once call_count is fixed

            assert len(result) == len(mock_internal_funding_rates)

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates when HTTP client returns None content."""
        symbol = "SOL-PERP"
        start_time_ms = 1678880000000
        limit = 5
        mock_endpoint_path = "/api/v1/funding/history"  # Corrected from /api/v1/fundingRates
        mock_params = {"symbol": symbol, "startTime": start_time_ms, "limit": limit}

        mock_request_builder.build_get_historical_funding_rates_params.return_value = (
            mock_params  # Only params
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_historical_funding_rates(
                    symbol=symbol, start_time_ms=start_time_ms, limit=limit
                )

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            expected_msg = f"No data for historical funding rates {symbol}, status: 200"
            assert exc_info.value.message == expected_msg

            mock_request_builder.build_get_historical_funding_rates_params.assert_called_once_with(
                symbol=symbol,
                start_time_ms=start_time_ms,
                end_time_ms=None,
                limit=limit,  # end_time_ms is None
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_signed=False,
                is_public_info_endpoint=True,
                endpoint_group="public",
                request_weight=1,
            )
            mock_response_handler.handle_get_historical_funding_rates_response.assert_not_called()
            mock_mapper.transform_raw_funding_interval_rate_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_validation_error(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates handles validation error from response handler."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol, "limit": 10}
        mock_raw_response = [{"invalid": "funding_rate_data"}]

        mock_request_builder.build_get_historical_funding_rates_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})

        # Create a ValidationError by trying to validate invalid data
        try:
            BackpackRawFundingRate.model_validate({"invalid": "data"})
        except ValidationError as e:
            mock_response_handler.handle_get_historical_funding_rates_response.side_effect = e

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_historical_funding_rates(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Processing historical funding rates data for" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_unexpected_exception(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates handles unexpected exception."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol, "limit": 10}
        mock_raw_response = [{"symbol": symbol, "rate": "0.001"}]

        mock_request_builder.build_get_historical_funding_rates_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_historical_funding_rates_response.side_effect = Exception(
            "Unexpected error"
        )

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_historical_funding_rates(symbol)

        assert exc_info.value.code == APIErrorCode.UNKNOWN.value
        assert (
            "Unhandled error during historical funding rates fetch for SOL-PERP: Unexpected error"
            in exc_info.value.message
        )

    @pytest.mark.asyncio
    async def test_constructor_with_custom_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
        mock_mapper: MagicMock,
    ) -> None:
        """Test constructor with custom mapper injection."""
        service = BackpackMarketDataService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            exchange_name="test_exchange",
            rate_limiter_service=mock_rate_limiter_service,
            mapper=mock_mapper,
        )

        # Test behavior that uses the mapper to verify it was set correctly
        mock_request_builder.build_get_ticker_params.return_value = {"symbol": "TEST"}
        mock_http_client_requester.return_value = ({"symbol": "TEST", "price": "100.0"}, 200, {})
        mock_response_handler.handle_get_ticker_response.return_value = MagicMock()
        mock_mapper.transform_raw_ticker_to_internal.return_value = MagicMock()

        with patch.object(service, "_mapper", mock_mapper):
            await service.get_ticker("TEST")

        # Verify the injected mapper was used
        mock_mapper.transform_raw_ticker_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_constructor_with_default_mapper(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test constructor creates default mapper when none provided."""
        service = BackpackMarketDataService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            exchange_name="test_exchange",
            rate_limiter_service=mock_rate_limiter_service,
            mapper=None,
        )

        # Test behavior that uses the mapper to verify it's working
        mock_request_builder.build_get_ticker_params.return_value = {"symbol": "TEST"}
        mock_http_client_requester.return_value = ({"symbol": "TEST", "price": "100.0"}, 200, {})
        mock_response_handler.handle_get_ticker_response.return_value = MagicMock()

        # Mock the mapper for this test
        mock_mapper = MagicMock(spec=BackpackMarketDataMapper)
        mock_mapper.transform_raw_ticker_to_internal.return_value = MagicMock()

        with patch.object(service, "_mapper", mock_mapper):
            await service.get_ticker("TEST")

        # Verify the default mapper functionality works
        mock_mapper.transform_raw_ticker_to_internal.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_market_data_with_time_parameters(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_market_data with start and end time parameters."""
        symbol = "SOL_USDC"
        timeframe = "5m"
        limit = 10
        start_time_ms = 1678880000000
        end_time_ms = 1678886400000

        mock_params = {
            "symbol": symbol,
            "interval": timeframe,
            "limit": limit,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_raw_kline_data = [
            [
                1678886400,
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                "1000.0",
                1678886400,
                "100000.0",
                50,
                "60.0",
                "180000.0",
                "0",
            ]
        ]
        mock_validated_klines_raw = [BackpackRawKline.model_validate(mock_raw_kline_data[0])]

        mock_request_builder.build_get_market_data_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_kline_data, 200, {})
        mock_response_handler.handle_get_market_data_response.return_value = (
            mock_validated_klines_raw
        )

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_candles = [MagicMock(spec=Candle)]
            mock_mapper.transform_raw_kline_to_internal.side_effect = mock_internal_candles

            result = await backpack_market_data_service.get_market_data(
                symbol=symbol,
                timeframe=timeframe,
                limit=limit,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )

            mock_request_builder.build_get_market_data_params.assert_called_once_with(
                symbol=symbol,
                timeframe_str=timeframe,
                limit=limit,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            assert result == mock_internal_candles

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_with_optional_parameters(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates with only symbol parameter."""
        symbol = "SOL-PERP"
        mock_params = {"symbol": symbol}
        mock_raw_response = [
            {
                "symbol": symbol,
                "rate": "0.0001",
                "markPrice": "100.0",
                "indexPrice": "99.0",
                "time": "2023-10-27T10:00:00Z",
            }
        ]

        mock_request_builder.build_get_historical_funding_rates_params.return_value = mock_params
        mock_http_client_requester.return_value = (mock_raw_response, 200, {})
        mock_response_handler.handle_get_historical_funding_rates_response.return_value = (
            mock_raw_response
        )

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            mock_internal_funding_rates = [MagicMock(spec=FundingRate)]
            mock_mapper.transform_raw_funding_interval_rate_to_internal.side_effect = (
                mock_internal_funding_rates
            )

            result = await backpack_market_data_service.get_historical_funding_rates(symbol=symbol)

            mock_request_builder.build_get_historical_funding_rates_params.assert_called_once_with(
                symbol=symbol,
                start_time_ms=None,
                end_time_ms=None,
                limit=None,
            )
            assert result == mock_internal_funding_rates

    @pytest.mark.asyncio
    async def test_get_all_tickers_not_implemented(
        self, backpack_market_data_service: BackpackMarketDataService
    ) -> None:
        """Test that get_all_tickers raises NotImplementedError."""
        with pytest.raises(NotImplementedError) as exc_info:
            await backpack_market_data_service.get_all_tickers()

        assert "get_all_tickers is not implemented for BackpackMarketDataService" in str(
            exc_info.value
        )

    @pytest.mark.asyncio
    async def test_get_ticker_none_response(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_ticker handles None response from http client."""
        symbol = "BTC_USDC"
        mock_request_builder.build_get_ticker_params.return_value = {"symbol": symbol}
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_ticker(symbol)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"No data for ticker {symbol}" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_book_none_response(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_order_book handles None response from http client."""
        symbol = "BTC_USDC"
        limit = 20
        mock_request_builder.build_get_order_book_params.return_value = {
            "symbol": symbol,
            "limit": limit,
        }
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_order_book(symbol, limit)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"No data for order_book {symbol}" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_order_book_non_dict_response(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_order_book handles non-dict response."""
        symbol = "BTC_USDC"
        limit = 20
        mock_request_builder.build_get_order_book_params.return_value = {
            "symbol": symbol,
            "limit": limit,
        }
        mock_http_client_requester.return_value = (["invalid_list"], 200, {})

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_order_book(symbol, limit)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Order_book data for {symbol} is not a dict" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_recent_trades_none_response(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_recent_trades handles None response from http client."""
        symbol = "BTC_USDC"
        limit = 100
        mock_request_builder.build_get_recent_trades_params.return_value = {
            "symbol": symbol,
            "limit": limit,
        }
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_recent_trades(symbol, limit)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"No data for recent_trades {symbol}" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_recent_trades_non_list_response(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_recent_trades handles non-list response."""
        symbol = "BTC_USDC"
        limit = 100
        mock_request_builder.build_get_recent_trades_params.return_value = {
            "symbol": symbol,
            "limit": limit,
        }
        mock_http_client_requester.return_value = ({"invalid": "dict"}, 200, {})

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_recent_trades(symbol, limit)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Recent_trades data for {symbol} is not a list" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_funding_rates_none_response(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_funding_rates handles None response from http client."""
        symbols = ["BTC_USDC", "ETH_USDC"]
        mock_request_builder.build_get_funding_rate_params.return_value = {"symbols": symbols}
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_funding_rates(symbols)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to get funding rate for BTC_USDC" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_none_response(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_historical_funding_rates handles None response from http client."""
        symbol = "BTC_USDC"
        start_time_ms = 1640995200000  # 2022-01-01
        end_time_ms = 1641081600000  # 2022-01-02
        limit = 100

        mock_request_builder.build_get_historical_funding_rates_params.return_value = {
            "symbol": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
            "limit": limit,
        }
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_historical_funding_rates(
                symbol, start_time_ms, end_time_ms, limit
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "No data for historical funding rates" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_get_market_data_none_response(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
    ) -> None:
        """Test get_market_data handles None response from http client."""
        symbol = "BTC_USDC"
        timeframe = "1h"
        limit = 100
        start_time_ms = 1640995200000
        end_time_ms = 1641081600000

        mock_request_builder.build_get_market_data_params.return_value = {
            "symbol": symbol,
            "interval": timeframe,
            "limit": limit,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        mock_http_client_requester.return_value = (None, 200, {})

        with pytest.raises(APIError) as exc_info:
            await backpack_market_data_service.get_market_data(
                symbol, timeframe, limit, start_time_ms, end_time_ms
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"No data for klines {symbol}@{timeframe}" in exc_info.value.message
