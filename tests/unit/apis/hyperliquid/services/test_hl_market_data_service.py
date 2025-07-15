"""Unit tests for the HyperliquidMarketDataService.

Minimal test coverage for core functionality with new decomposed architecture.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.mappers.market_data.hl_historical_data_mapper import (
    HyperliquidHistoricalDataMapper,
)
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService


class TestHyperliquidMarketDataService:
    """Test HyperliquidMarketDataService with decomposed architecture."""

    @pytest.fixture
    def mock_http_client_requester(self) -> AsyncMock:
        """Create mock HTTP client requester."""
        return AsyncMock()

    @pytest.fixture
    def mock_request_builder(self) -> MagicMock:
        """Create mock request builder."""
        return MagicMock()

    @pytest.fixture
    def mock_response_handler(self) -> MagicMock:
        """Create mock response handler."""
        return MagicMock()

    @pytest.fixture
    def mock_historical_data_mapper(self) -> MagicMock:
        """Create mock historical data mapper."""
        return MagicMock(spec=HyperliquidHistoricalDataMapper)

    @pytest.fixture
    def market_data_service(
        self,
        mock_http_client_requester: AsyncMock,
        mock_request_builder: MagicMock,
        mock_response_handler: MagicMock,
        mock_historical_data_mapper: MagicMock,
    ) -> HyperliquidMarketDataService:
        """Create HyperliquidMarketDataService instance for testing."""
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
        """Test service initialization with decomposed mappers."""
        assert market_data_service._exchange_name == "hyperliquid_test"
        assert market_data_service._historical_data_mapper is not None

    def test_service_has_required_attributes(
        self, market_data_service: HyperliquidMarketDataService
    ) -> None:
        """Test that service has required attributes."""
        # Test that basic service attributes exist
        assert hasattr(market_data_service, "_exchange_name")
        assert hasattr(market_data_service, "_price_ticker_service")
        assert hasattr(market_data_service, "_order_book_service")
        assert hasattr(market_data_service, "_historical_data_service")
