"""Unit tests for the HyperliquidMarketDataService.

Minimal test coverage for core functionality with new decomposed architecture.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.mappers.market_data.hl_historical_data_mapper import (
    HyperliquidHistoricalDataMapper,
)
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.models.service_args.market_data import GetMarketDataArgs
from cyberdelta.core.models.market import Candle


class TestHyperliquidMarketDataService:
    """Test HyperliquidMarketDataService with decomposed architecture."""

    @pytest.fixture
    def mock_http_client_requester(self) -> AsyncMock:
        """Create mock HTTP client requester.

        Returns:
            AsyncMock: Mocked async HTTP client requester function.
        """
        return AsyncMock()

    @pytest.fixture
    def mock_request_builder(self) -> MagicMock:
        """Create mock request builder.

        Returns:
            MagicMock: Mocked request builder instance.
        """
        return MagicMock()

    @pytest.fixture
    def mock_response_handler(self) -> MagicMock:
        """Create mock response handler.

        Returns:
            MagicMock: Mocked response handler instance.
        """
        return MagicMock()

    @pytest.fixture
    def mock_historical_data_mapper(self) -> MagicMock:
        """Create mock historical data mapper.

        Returns:
            MagicMock: Mocked HyperliquidHistoricalDataMapper instance.
        """
        return MagicMock(spec=HyperliquidHistoricalDataMapper)

    @pytest.fixture
    def market_data_service(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_historical_data_mapper: MagicMock,
    ) -> HyperliquidMarketDataService:
        """Create HyperliquidMarketDataService instance for testing.

        Returns:
            HyperliquidMarketDataService: Market data service with mocked dependencies.
        """
        return HyperliquidMarketDataService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_request_builder,
            response_handler=mock_response_handler,
            exchange_name="hyperliquid_test",
            historical_data_mapper=mock_historical_data_mapper,
        )

    def test_service_initialization(
        self, market_data_service: HyperliquidMarketDataService
    ) -> None:
        """Test service initialization with decomposed mappers.

        Since internal attributes are private, we verify initialization through:
        1. Successful instantiation without errors
        2. Presence of expected public methods
        3. Ability to call methods (tested in other test methods)
        """
        # The fact that the service was created without error indicates successful initialization
        assert market_data_service is not None

        # Verify expected public interface exists
        assert callable(getattr(market_data_service, "get_ticker", None))
        assert callable(getattr(market_data_service, "get_order_book", None))
        assert callable(getattr(market_data_service, "get_market_data", None))
        assert callable(getattr(market_data_service, "get_markets", None))

    def test_service_has_required_public_methods(
        self, market_data_service: HyperliquidMarketDataService
    ) -> None:
        """Test that service has required public methods.

        Verify the service exposes all expected public methods for market data operations.
        """
        # Price ticker operations
        assert hasattr(market_data_service, "get_ticker")
        assert hasattr(market_data_service, "get_funding_rate")
        assert hasattr(market_data_service, "get_funding_rates")
        assert hasattr(market_data_service, "get_all_mids")

        # Order book operations
        assert hasattr(market_data_service, "get_order_book")
        assert hasattr(market_data_service, "get_recent_trades")

        # Historical data operations
        assert hasattr(market_data_service, "get_historical_funding_rates")
        assert hasattr(market_data_service, "get_market_data")

        # Market metadata operations
        assert hasattr(market_data_service, "get_markets")
        assert hasattr(market_data_service, "get_market")

    @pytest.mark.asyncio
    async def test_service_uses_provided_mapper(
        self,
        market_data_service: HyperliquidMarketDataService,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_historical_data_mapper: MagicMock,
    ) -> None:
        """Test that service uses the provided mapper instance.

        This behavioral test verifies that the service correctly uses the injected
        dependencies without directly accessing private attributes.
        """
        # Setup mock response
        mock_response = {"some": "data"}
        mock_http_client_requester.return_value = (mock_response, 200, {})
        mock_request_builder.build_candles_request.return_value = ("GET", "/candles", {}, None)
        mock_response_handler.handle_candles_response.return_value = mock_response

        # Setup mock mapper to return test data
        test_candles = [MagicMock(spec=Candle)]
        mock_historical_data_mapper.transform_raw_candle_snapshot_to_candles.return_value = (
            test_candles
        )

        # Call the service method
        args = GetMarketDataArgs(symbol="BTC-USD", timeframe="1h", limit=100)
        result = await market_data_service.get_market_data(args)

        # Verify the mapper was called (proving it's being used)
        mock_historical_data_mapper.transform_raw_candle_snapshot_to_candles.assert_called_once()

        # Verify the result matches what the mapper returned
        assert result == test_candles
