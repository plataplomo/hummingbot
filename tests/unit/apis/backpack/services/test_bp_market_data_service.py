"""
Unit tests for the BackpackMarketDataService.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawTicker  # Example Raw Model
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.connectivity.http_client import HttpClient
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Provides a mock HttpClient."""
    return AsyncMock(spec=HttpClient)


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Provides a mock BackpackRequestBuilder."""
    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Provides a mock BackpackResponseHandler."""
    return MagicMock(spec=BackpackResponseHandler)


@pytest.fixture
def mock_rate_limiter_service() -> AsyncMock:
    """Provides a mock RateLimiterService."""
    return AsyncMock(spec=RateLimiterService)


@pytest.fixture
def bp_market_data_service(
    mock_http_client: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_rate_limiter_service: AsyncMock,
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
        mock_rate_limiter_service: AsyncMock,  # Added mock_rate_limiter_service here
    ) -> None:
        """Test get_ticker successfully retrieves and processes ticker data."""
        symbol = "SOL_USDC"
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

        mock_request_builder.build_get_ticker_params.return_value = mock_params
        mock_http_client.request.return_value = (mock_raw_response_content, 200, MagicMock())
        mock_response_handler.handle_get_ticker_response.return_value = mock_validated_ticker

        result = await bp_market_data_service.get_ticker(symbol)

        mock_rate_limiter_service.wait_for_permission.assert_called_once_with("/api/v1/ticker")
        mock_request_builder.build_get_ticker_params.assert_called_once_with(symbol=symbol)
        mock_http_client.request.assert_called_once_with(
            method="GET",
            endpoint_path="/api/v1/ticker",
            params=mock_params,
            rate_limiter_service=mock_rate_limiter_service,  # Ensure this is passed
        )
        mock_response_handler.handle_get_ticker_response.assert_called_once_with(
            mock_raw_response_content
        )
        assert result == mock_validated_ticker

    # Placeholder for other tests (e.g., APIError handling)
    @pytest.mark.asyncio
    async def test_get_ticker_api_error_from_client(
        self,
        bp_market_data_service: BackpackMarketDataService,
        mock_http_client: AsyncMock,
        mock_request_builder: MagicMock,
        mock_rate_limiter_service: AsyncMock,  # Added mock_rate_limiter_service here
    ) -> None:
        """Test get_ticker handles APIError from http_client."""
        symbol = "SOL_USDC"
        mock_params = {"symbol": symbol}
        api_error_instance = APIError("Client error", code=500)

        mock_request_builder.build_get_ticker_params.return_value = mock_params
        mock_http_client.request.side_effect = api_error_instance

        with pytest.raises(APIError) as exc_info:
            await bp_market_data_service.get_ticker(symbol)

        assert exc_info.value == api_error_instance
        mock_rate_limiter_service.wait_for_permission.assert_called_once_with("/api/v1/ticker")
        mock_request_builder.build_get_ticker_params.assert_called_once_with(symbol=symbol)
        mock_http_client.request.assert_called_once_with(
            method="GET",
            endpoint_path="/api/v1/ticker",
            params=mock_params,
            rate_limiter_service=mock_rate_limiter_service,  # Ensure this is passed
        )

    # Add more tests for other methods: get_order_book, get_recent_trades, etc.
    # Example for get_order_book
    # @pytest.mark.asyncio
    # async def test_get_order_book_success(self, bp_market_data_service, ...):
    #     pass
