"""Shared fixtures for HyperliquidMarketDataService tests."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Return mock http client requester for testing."""
    mock = AsyncMock()
    # Configure default return value as 3-tuple to align with business logic
    mock.return_value = ([], 200, {})
    return mock


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    """Return mock hl request builder for testing."""
    return MagicMock(spec=HyperliquidMarketDataRequestBuilder)


@pytest.fixture
def mock_hl_response_handler() -> MagicMock:
    """Return mock hl response handler for testing."""
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_hl_mapper() -> MagicMock:
    """Return mock hl mapper for testing."""
    return MagicMock()


@pytest.fixture
def hyperliquid_market_data_service(
    mock_http_client_requester: AsyncMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler: MagicMock,
    mock_hl_mapper: MagicMock,
) -> HyperliquidMarketDataService:
    """Create HyperliquidMarketDataService instance with mocked dependencies for testing.

    Args:
        mock_http_client_requester: Mock HTTP client for making requests
        mock_hl_request_builder: Mock request builder for creating requests
        mock_hl_response_handler: Mock response handler for processing responses
        mock_hl_mapper: Mock mapper for data transformations

    Returns:
        HyperliquidMarketDataService configured with all mock dependencies
    """
    # Since mock_hl_mapper is a combined mapper mock, we'll use it for all mapper types
    # to maintain backward compatibility with existing tests

    # Configure default return values for the mapper to avoid MagicMock return values
    mock_hl_mapper.transform_raw_candle_snapshot_to_candles.return_value = []
    mock_hl_mapper.transform_raw_meta_and_asset_ctxs_to_markets.return_value = []

    return HyperliquidMarketDataService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_hl_request_builder,
        response_handler=mock_hl_response_handler,
        price_ticker_mapper=mock_hl_mapper,
        order_book_mapper=mock_hl_mapper,
        historical_data_mapper=mock_hl_mapper,
        market_metadata_mapper=mock_hl_mapper,
        exchange_name="hyperliquid_test",
    )
