"""
Unit tests for the BackpackMarketDataService.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

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
from cyberdelta.apis.connectivity.http_client import HttpClient
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Provides a mock HttpClient."""
    client = AsyncMock(spec=HttpClient)
    # Explicitly set .request to be an AsyncMock. This new mock won't use
    # HttpClient.request spec for its own call validation during assertions.
    # It will accept any kwargs. The spec on 'client' handles attribute errors.
    return client


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Provides a mock BackpackRequestBuilder."""
    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Provides a mock BackpackResponseHandler."""
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
def bp_market_data_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_rate_limiter_service: MagicMock,
) -> BackpackMarketDataService:
    """Provides an instance of BackpackMarketDataService with mocked dependencies."""
    return BackpackMarketDataService(
        http_client=mock_http_client,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        rate_limiter_service=mock_rate_limiter_service,
        exchange_name="backpack_test",
    )


class TestBackpackMarketDataService:
    """Tests for the BackpackMarketDataService class."""

    @pytest.mark.asyncio
    async def test_get_ticker_success(
        self,
        bp_market_data_service: BackpackMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_ticker successfully retrieves and processes ticker data."""
        symbol = "SOL_USDC"
        endpoint = "/api/v1/ticker"
        mock_params = {"symbol": symbol}
        mock_raw_response_content = {
            "symbol": symbol,
            "price": "100.0",
            "volume": "1000",
            "bid": "99.9",
            "ask": "100.1",
            "time": 1234567890,
        }
        mock_validated_ticker = BackpackRawTicker(
            symbol=symbol, price="100.0", bid="99.9", ask="100.1", volume="1000", time=1234567890
        )

        # Get the mock for the limiter runtime instance
        mock_limiter_runtime = mock_rate_limiter_service.get_limiter.return_value

        mock_request_builder.build_get_ticker_params.return_value = mock_params
        mock_http_client.request.return_value = (mock_raw_response_content, 200, MagicMock())
        mock_response_handler.handle_get_ticker_response.return_value = mock_validated_ticker

        result = await bp_market_data_service.get_ticker(symbol)

        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", endpoint)
        mock_limiter_runtime.acquire.assert_awaited_once()

        mock_request_builder.build_get_ticker_params.assert_called_once_with(symbol=symbol)
        mock_http_client.request.assert_called_once_with(
            method="GET",
            endpoint_path="/api/v1/ticker",
            params=mock_params,
            rate_limiter_service=mock_rate_limiter_service,
        )
        mock_response_handler.handle_get_ticker_response.assert_called_once_with(
            mock_raw_response_content, symbol
        )
        assert result == mock_validated_ticker

    @pytest.mark.asyncio
    async def test_get_ticker_api_error_from_client(
        self,
        bp_market_data_service: BackpackMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_ticker handles APIError from http_client."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol}
        api_error_instance = APIError("Client error", code=500)

        mock_request_builder.build_get_ticker_params.return_value = mock_params
        mock_http_client.request.side_effect = api_error_instance

        mock_limiter_runtime = mock_rate_limiter_service.get_limiter.return_value

        with pytest.raises(APIError) as exc_info:
            await bp_market_data_service.get_ticker(symbol)

        assert exc_info.value == api_error_instance
        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", "/api/v1/ticker")
        mock_limiter_runtime.acquire.assert_awaited_once()
        mock_request_builder.build_get_ticker_params.assert_called_once_with(symbol=symbol)
        mock_http_client.request.assert_called_once_with(
            method="GET",
            endpoint_path="/api/v1/ticker",
            params=mock_params,
            rate_limiter_service=mock_rate_limiter_service,
        )

    @pytest.mark.asyncio
    async def test_get_order_book_success(
        self,
        bp_market_data_service: BackpackMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
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

        mock_request_builder.build_get_order_book_params.return_value = mock_params
        mock_http_client.request.return_value = (mock_raw_response_content, 200, MagicMock())
        mock_response_handler.handle_get_order_book_response.return_value = mock_validated_book

        mock_limiter_runtime = mock_rate_limiter_service.get_limiter.return_value

        result = await bp_market_data_service.get_order_book(symbol, depth=depth)

        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", "/api/v1/depth")
        mock_limiter_runtime.acquire.assert_awaited_once()
        mock_request_builder.build_get_order_book_params.assert_called_once_with(
            symbol=symbol, limit=depth
        )
        mock_http_client.request.assert_called_once_with(
            method="GET",
            endpoint_path="/api/v1/depth",
            params=mock_params,
            rate_limiter_service=mock_rate_limiter_service,
        )
        mock_response_handler.handle_get_order_book_response.assert_called_once_with(
            mock_raw_response_content,
            symbol,  # Backpack handler needs symbol
        )
        assert result == mock_validated_book

    @pytest.mark.asyncio
    async def test_get_recent_trades_success(
        self,
        bp_market_data_service: BackpackMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
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

        mock_request_builder.build_get_recent_trades_params.return_value = mock_params
        mock_http_client.request.return_value = (mock_raw_trades_data, 200, MagicMock())
        mock_response_handler.handle_get_recent_trades_response.return_value = mock_validated_trades

        mock_limiter_runtime = mock_rate_limiter_service.get_limiter.return_value

        result = await bp_market_data_service.get_recent_trades(symbol, limit=limit)

        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", "/api/v1/trades")
        mock_limiter_runtime.acquire.assert_awaited_once()
        mock_request_builder.build_get_recent_trades_params.assert_called_once_with(
            symbol=symbol, limit=limit
        )
        mock_http_client.request.assert_called_once_with(
            method="GET",
            endpoint_path="/api/v1/trades",
            params=mock_params,
            rate_limiter_service=mock_rate_limiter_service,
        )
        mock_response_handler.handle_get_recent_trades_response.assert_called_once_with(
            mock_raw_trades_data,
            symbol,  # Backpack handler needs symbol
        )
        assert result == mock_validated_trades

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(
        self,
        bp_market_data_service: BackpackMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
    ) -> None:
        """Test get_funding_rate successfully retrieves and processes funding rate data."""
        symbol = "SOL-PERP"
        formatted_symbol = "SOL_PERP"
        expected_endpoint = f"/api/v1/markets/{formatted_symbol}/funding"

        mock_params = {"symbol": symbol}
        mock_raw_response_content = {
            "symbol": symbol,
            "rate": "0.0001",
            "markPrice": "101.0",
            "indexPrice": "100.9",
            "time": 1678889400000,
        }
        mock_validated_funding = BackpackRawFundingRate(
            symbol=symbol,
            rate="0.0001",
            markPrice="101.0",
            indexPrice="100.9",
            time=1678889400000,
        )

        mock_request_builder.format_symbol.return_value = formatted_symbol
        mock_request_builder.build_get_funding_rate_params.return_value = mock_params
        mock_http_client.request.return_value = (mock_raw_response_content, 200, MagicMock())
        mock_response_handler.handle_get_funding_rate_response.return_value = mock_validated_funding

        mock_limiter_runtime = mock_rate_limiter_service.get_limiter.return_value

        result = await bp_market_data_service.get_funding_rate(symbol)

        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", expected_endpoint)
        mock_limiter_runtime.acquire.assert_awaited_once()
        mock_request_builder.format_symbol.assert_called_once_with(symbol)
        mock_request_builder.build_get_funding_rate_params.assert_called_once_with(symbol=symbol)
        mock_http_client.request.assert_called_once_with(
            method="GET",
            endpoint_path=expected_endpoint,
            params=mock_params,
            rate_limiter_service=mock_rate_limiter_service,
        )
        mock_response_handler.handle_get_funding_rate_response.assert_called_once_with(
            mock_raw_response_content, symbol
        )
        assert result == mock_validated_funding

    @pytest.mark.asyncio
    async def test_get_market_data_success(
        self,
        bp_market_data_service: BackpackMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_rate_limiter_service: MagicMock,
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
            BackpackRawKline.model_validate([
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
            ]),
            BackpackRawKline.model_validate([
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
            ]),
        ]

        mock_request_builder.build_get_market_data_params.return_value = (
            mock_params_returned_by_builder
        )
        mock_http_client.request.return_value = (mock_raw_kline_data, 200, MagicMock())
        mock_response_handler.handle_get_market_data_response.return_value = mock_validated_klines

        mock_limiter_runtime = mock_rate_limiter_service.get_limiter.return_value

        result = await bp_market_data_service.get_market_data(
            symbol, timeframe=timeframe, limit=limit
        )

        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", "/api/v1/klines")
        mock_limiter_runtime.acquire.assert_awaited_once()
        mock_request_builder.build_get_market_data_params.assert_called_once_with(
            symbol=symbol,
            timeframe_str=timeframe,
            limit=limit,
            start_time_ms=None,
            end_time_ms=None,
        )
        mock_http_client.request.assert_called_once_with(
            method="GET",
            endpoint_path="/api/v1/klines",
            params=mock_params_returned_by_builder,
            rate_limiter_service=mock_rate_limiter_service,
        )
        mock_response_handler.handle_get_market_data_response.assert_called_once_with(
            mock_raw_kline_data, symbol, timeframe
        )
        assert result == mock_validated_klines
