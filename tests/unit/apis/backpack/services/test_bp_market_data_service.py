"""
Unit tests for the BackpackMarketDataService.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.backpack.bp_order_mapper import (
    BackpackOrderMapper,
)  # For creating expected internal models
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import (  # Example Raw Models
    BackpackRawOrderBook,
    BackpackRawTicker,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
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


class TestBackpackMarketDataService:
    """Tests for the BackpackMarketDataService class."""

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_ticker successfully retrieves and processes ticker data."""
        symbol = "SOL_USDC"
        mock_timestamp_int = 1678886400  # Example timestamp
        mock_timestamp_dt = datetime.fromtimestamp(mock_timestamp_int, tz=UTC)

        mock_endpoint_path = f"/api/v1/ticker?symbol={symbol}"
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
        mock_headers: dict[Any, Any] = {}

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

        mock_request_builder.build_get_ticker_params.return_value = (
            mock_endpoint_path,
            mock_params,
        )
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers,
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
                is_public_info_endpoint=True,
                rate_limiter_service=mock_rate_limiter_service,
                endpoint_group="public_info",
                request_weight=1,
            )
            mock_response_handler.handle_get_ticker_response.assert_called_once_with(
                mock_raw_response_content, symbol, mock_status_code, mock_headers
            )
            mock_mapper.transform_raw_ticker_to_internal.assert_called_once_with(
                mock_raw_ticker, symbol_override=symbol
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
        mock_response_handler: MagicMock,  # For assert_not_called
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_ticker when HTTP client returns None content."""
        symbol = "SOL_USDC"
        mock_endpoint_path = f"/api/v1/ticker?symbol={symbol}"
        mock_params = {"symbol": symbol}

        mock_request_builder.build_get_ticker_params.return_value = (
            mock_endpoint_path,
            mock_params,
        )
        # Simulate HTTP client returning None for content
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_ticker(symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            # Corrected assertion to match the actual error message from the service
            expected_msg_part = f"No data for ticker {symbol}, status: 200"
            assert expected_msg_part in exc_info.value.message

            mock_request_builder.build_get_ticker_params.assert_called_once_with(symbol=symbol)
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_public_info_endpoint=True,
                rate_limiter_service=mock_rate_limiter_service,
                endpoint_group="public_info",
                request_weight=1,
            )
            mock_response_handler.handle_get_ticker_response.assert_not_called()
            mock_mapper.transform_raw_ticker_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_order_book successfully retrieves and processes order book data."""
        symbol = "SOL_USDC"
        depth = 50
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

        mock_headers_obj = MagicMock()  # Create a specific mock object for headers
        mock_request_builder.build_get_order_book_params.return_value = mock_params
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            200,
            mock_headers_obj,
        )  # Use it here
        mock_response_handler.handle_get_order_book_response.return_value = mock_validated_book

        result = await backpack_market_data_service.get_order_book(symbol, limit=depth)

        mock_request_builder.build_get_order_book_params.assert_called_once_with(
            symbol=symbol, limit=depth
        )
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint="/api/v1/depth",
            params=mock_params,
            is_public_info_endpoint=True,
            rate_limiter_service=mock_rate_limiter_service,
            endpoint_group="public_info",
            request_weight=1,
        )
        mock_response_handler.handle_get_order_book_response.assert_called_once_with(
            mock_raw_response_content,
            symbol,
            200,
            mock_headers_obj,  # Assert with the same object
        )

        assert isinstance(result, OrderBook)

    @pytest.mark.asyncio
    async def test_get_order_book_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_order_book when HTTP client returns None content."""
        symbol = "SOL_USDC"
        depth = 100  # Default depth if not specified, or a common test value

        # mock_endpoint_path = f"/api/v1/depth?symbol={symbol}" # Not needed if builder returns only params
        mock_base_endpoint_path = "/api/v1/depth"  # The service uses the base path
        mock_params = {"symbol": symbol, "limit": depth}

        mock_request_builder.build_get_order_book_params.return_value = (
            mock_params  # Builder for get_order_book should return only params dict
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_order_book(symbol, limit=depth)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            # Corrected assertion to match the actual error message from the service
            expected_msg_part = f"No data for order_book {symbol}, status: 200"
            assert expected_msg_part in exc_info.value.message

            mock_request_builder.build_get_order_book_params.assert_called_once_with(
                symbol=symbol, limit=depth
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_base_endpoint_path,  # Assert with the base path
                params=mock_params,
                is_public_info_endpoint=True,
                rate_limiter_service=mock_rate_limiter_service,
                endpoint_group="public_info",
                request_weight=1,
            )
            mock_response_handler.handle_get_order_book_response.assert_not_called()
            mock_mapper.transform_raw_orderbook_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_recent_trades successfully retrieves and processes trade data."""
        symbol = "ETH_USDC"
        limit = 2
        mock_params = {"symbol": symbol, "limit": limit}
        mock_raw_trades_data = [
            {
                "id": "1",
                "orderId": "o1",
                "symbol": symbol,
                "price": "2000.0",
                "qty": "1.0",
                "time": 1678886400100,
            },
            {
                "id": "2",
                "orderId": "o2",
                "symbol": symbol,
                "price": "2000.1",
                "qty": "0.5",
                "time": 1678886400200,
            },
        ]
        mock_validated_trades = [
            BackpackRawTrade(
                id="1",
                orderId="o1",
                symbol=symbol,
                price="2000.0",
                qty="1.0",
                time=1678886400100,
            ),
            BackpackRawTrade(
                id="2",
                orderId="o2",
                symbol=symbol,
                price="2000.1",
                qty="0.5",
                time=1678886400200,
            ),
        ]

        mock_base_endpoint_path = "/api/v1/trades"
        mock_request_builder.build_get_recent_trades_params.return_value = (
            mock_base_endpoint_path,
            mock_params,  # Builder returns tuple (endpoint, params)
        )
        mock_headers_obj = MagicMock()  # Specific instance for headers
        mock_http_client_requester.return_value = (mock_raw_trades_data, 200, mock_headers_obj)
        mock_response_handler.handle_get_recent_trades_response.return_value = mock_validated_trades

        result = await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

        mock_request_builder.build_get_recent_trades_params.assert_called_once_with(
            symbol=symbol, limit=limit
        )
        mock_http_client_requester.assert_called_once_with(
            method="GET",
            endpoint=mock_base_endpoint_path,  # Assert with the base path provided by builder
            params=mock_params,
            is_public_info_endpoint=True,
            rate_limiter_service=mock_rate_limiter_service,
            endpoint_group="public_info",
            request_weight=1,
        )
        mock_response_handler.handle_get_recent_trades_response.assert_called_once_with(
            mock_raw_trades_data,
            symbol,
            200,
            mock_headers_obj,  # Use the same instance
        )
        mapper = BackpackOrderMapper()
        expected_internal_trades: list[Trade] = []
        for raw_model in mock_validated_trades:
            trade = mapper.transform_raw_trade_to_internal(raw_model)
            if trade:
                expected_internal_trades.append(trade)
        assert result == expected_internal_trades

    @pytest.mark.asyncio
    async def test_get_recent_trades_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,  # For assert_not_called
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_recent_trades when HTTP client returns None content."""
        symbol = "ETH_USDC"
        limit = 5
        mock_endpoint_path = "/api/v1/trades"  # As per get_recent_trades_success
        mock_params = {"symbol": symbol, "limit": limit}

        mock_request_builder.build_get_recent_trades_params.return_value = (
            mock_endpoint_path,  # builder returns tuple (endpoint, params)
            mock_params,
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_recent_trades(symbol, limit=limit)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert f"No data for recent_trades {symbol}, status: 200" in exc_info.value.message
            # Or, if the service method is updated for more specific None checks:
            # assert f"No content received from HTTP client for GET {mock_endpoint_path}" in \
            # exc_info.value.message

            mock_request_builder.build_get_recent_trades_params.assert_called_once_with(
                symbol=symbol, limit=limit
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_public_info_endpoint=True,
                rate_limiter_service=mock_rate_limiter_service,
                endpoint_group="public_info",
                request_weight=1,
            )
            mock_response_handler.handle_get_recent_trades_response.assert_not_called()
            mock_mapper.transform_raw_trade_to_internal.assert_not_called()
            # If a list transformation method is used by the mapper, assert that too
            if hasattr(mock_mapper, "transform_raw_trades_to_internal_list"):  # Fictitious example
                mock_mapper.transform_raw_trades_to_internal_list.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_funding_rate successfully retrieves and processes funding rate data."""
        symbol = "SOL-PERP"
        mock_timestamp_str = "2023-10-27T10:00:00Z"
        mock_timestamp_dt = datetime.fromisoformat(mock_timestamp_str.replace("Z", "+00:00"))

        mock_endpoint_path = f"/api/v1/funding?symbol={symbol}"
        mock_params = {"symbol": symbol}
        mock_raw_response_content = {
            "symbol": symbol,
            "rate": "0.0001",
            "markPrice": "100.0",
            "indexPrice": "99.0",
            "time": mock_timestamp_str,
        }
        mock_status_code = 200
        mock_headers: dict[Any, Any] = {}

        mock_raw_funding_rate = BackpackRawFundingRate(
            symbol=symbol,
            rate="0.0001",
            markPrice="100.0",
            indexPrice="99.0",
            time=mock_timestamp_str,
        )
        expected_internal_funding_rate = FundingRate(
            symbol=symbol,
            timestamp=mock_timestamp_dt,
            funding_rate=Decimal("0.0001"),
            mark_price=Decimal(mock_raw_funding_rate.mark_price),
            index_price=Decimal(mock_raw_funding_rate.index_price),
            next_funding_time=mock_timestamp_dt,
            bp_details=BackpackFundingDetails(),
        )

        mock_request_builder.build_get_funding_rate_params.return_value = (
            mock_endpoint_path,
            mock_params,
        )
        mock_http_client_requester.return_value = (
            mock_raw_response_content,
            mock_status_code,
            mock_headers,
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
                is_public_info_endpoint=True,
                rate_limiter_service=mock_rate_limiter_service,
                endpoint_group="public_info",
                request_weight=1,
            )
            mock_response_handler.handle_get_funding_rate_response.assert_called_once_with(
                mock_raw_response_content, symbol, mock_status_code, mock_headers
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
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_funding_rate when HTTP client returns None content."""
        symbol = "SOL-PERP"
        mock_endpoint_path = f"/api/v1/funding?symbol={symbol}"
        mock_params = {"symbol": symbol}

        mock_request_builder.build_get_funding_rate_params.return_value = (
            mock_endpoint_path,
            mock_params,
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_funding_rate(symbol)

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert f"No data for funding_rate {symbol}" in exc_info.value.message

            mock_request_builder.build_get_funding_rate_params.assert_called_once_with(
                symbol=symbol
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_public_info_endpoint=True,
                rate_limiter_service=mock_rate_limiter_service,
                endpoint_group="public_info",
                request_weight=1,
            )
            mock_response_handler.handle_get_funding_rate_response.assert_not_called()
            mock_mapper.transform_raw_funding_rate_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_market_data (klines) successfully retrieves and processes kline data."""
        symbol = "BTC_USDC"
        timeframe = "1h"
        limit = 2
        mock_params_returned_by_builder = {"symbol": symbol, "interval": timeframe, "limit": limit}

        mock_raw_kline_data = [
            [
                1678882800000,
                "30000.0",
                "30100.0",
                "29900.0",
                "30050.0",
                "100.0",
                1678886399999,
                "3000000.0",
                50,
                "60.0",
                "1800000.0",
                "0",
            ],
            [
                1678886400000,
                "30050.0",
                "30150.0",
                "30000.0",
                "30100.0",
                "120.0",
                1678889999999,
                "3600000.0",
                60,
                "70.0",
                "2100000.0",
                "0",
            ],
        ]
        mock_validated_klines = [
            BackpackRawKline.model_validate(
                [
                    1678882800000,
                    "30000.0",
                    "30100.0",
                    "29900.0",
                    "30050.0",
                    "100.0",
                    1678886399999,
                    "3000000.0",
                    50,
                    "60.0",
                    "1800000.0",
                    "0",
                ]
            ),
            BackpackRawKline.model_validate(
                [
                    1678886400000,
                    "30050.0",
                    "30150.0",
                    "30000.0",
                    "30100.0",
                    "120.0",
                    1678889999999,
                    "3600000.0",
                    60,
                    "70.0",
                    "2100000.0",
                    "0",
                ]
            ),
        ]

        expected_endpoint_path = "/api/v1/klines"
        mock_request_builder.build_get_market_data_params.return_value = (
            expected_endpoint_path,
            mock_params_returned_by_builder,
        )
        mock_http_client_requester.return_value = (mock_raw_kline_data, 200, MagicMock())
        mock_response_handler.handle_get_market_data_response.return_value = mock_validated_klines

        result = await backpack_market_data_service.get_market_data(
            symbol, interval=timeframe, limit=limit
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
            endpoint=expected_endpoint_path,
            params=mock_params_returned_by_builder,
            is_public_info_endpoint=True,
            rate_limiter_service=mock_rate_limiter_service,
            endpoint_group="public_info",
            request_weight=1,
        )
        mock_response_handler.handle_get_market_data_response.assert_called_once_with(
            mock_raw_kline_data, symbol, timeframe, 200, MagicMock()
        )
        mapper = BackpackOrderMapper()
        expected_internal_candles: list[Candle] = []
        for raw_model in mock_validated_klines:
            candle = mapper.transform_raw_kline_to_internal(symbol, timeframe, raw_model)
            expected_internal_candles.append(candle)
        assert result == expected_internal_candles

    @pytest.mark.asyncio
    async def test_get_market_data_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_market_data (klines) when HTTP client returns None content."""
        symbol = "BTC_USDC"
        timeframe = "1h"
        limit = 10
        mock_endpoint_path = "/api/v1/klines"
        mock_params = {"symbol": symbol, "interval": timeframe, "limit": limit}

        mock_request_builder.build_get_market_data_params.return_value = (
            mock_endpoint_path,
            mock_params,  # builder returns tuple now
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_market_data(
                    symbol, interval=timeframe, limit=limit
                )

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert f"No data for klines {symbol} interval {timeframe}" in exc_info.value.message

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
                is_public_info_endpoint=True,
                rate_limiter_service=mock_rate_limiter_service,
                endpoint_group="public_info",
                request_weight=1,
            )
            mock_response_handler.handle_get_market_data_response.assert_not_called()
            mock_mapper.transform_raw_kline_to_internal.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_historical_funding_rates_http_client_returns_none(
        self,
        backpack_market_data_service: BackpackMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: AsyncMock,
    ) -> None:
        """Test get_historical_funding_rates when HTTP client returns None content."""
        symbol = "SOL-PERP"
        start_time_ms = 1678880000000
        end_time_ms = 1678886400000
        limit = 10

        mock_endpoint_path = "/api/v1/fundingRates"
        mock_params = {
            "symbol": symbol,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
            "limit": limit,
        }

        mock_request_builder.build_get_historical_funding_rates_params.return_value = (
            mock_endpoint_path,
            mock_params,
        )
        mock_http_client_requester.return_value = (None, 200, MagicMock())

        with patch.object(backpack_market_data_service, "_mapper", autospec=True) as mock_mapper:
            with pytest.raises(APIError) as exc_info:
                await backpack_market_data_service.get_historical_funding_rates(
                    symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms, limit=limit
                )

            assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
            assert f"No data for historical_funding_rates {symbol}" in exc_info.value.message

            mock_request_builder.build_get_historical_funding_rates_params.assert_called_once_with(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms, limit=limit
            )
            mock_http_client_requester.assert_called_once_with(
                method="GET",
                endpoint=mock_endpoint_path,
                params=mock_params,
                is_public_info_endpoint=True,
                rate_limiter_service=mock_rate_limiter_service,
                endpoint_group="public_info",
                request_weight=1,
            )
            mock_response_handler.handle_get_historical_funding_rates_response.assert_not_called()
            # Assuming mapper method for a single item, or a list method might be called
            if hasattr(mock_mapper, "transform_raw_funding_interval_rate_to_internal"):  # Example
                mock_mapper.transform_raw_funding_interval_rate_to_internal.assert_not_called()
            if hasattr(
                mock_mapper, "transform_raw_funding_interval_rates_to_internal_list"
            ):  # Example
                mock_mapper.transform_raw_funding_interval_rates_to_internal_list.assert_not_called()
