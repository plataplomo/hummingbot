"""Shared fixtures for HyperliquidMarketDataService tests.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper import HyperliquidMarketDataMapper
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Return mock http client requester for testing."""
    return AsyncMock()


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    """Return mock hl request builder for testing."""
    return MagicMock(spec=HyperliquidRequestBuilder)


@pytest.fixture
def mock_hl_response_handler() -> MagicMock:
    """Return mock hl response handler for testing."""
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_hl_mapper() -> MagicMock:
    """Return mock hl mapper for testing."""
    return MagicMock(spec=HyperliquidMarketDataMapper)


@pytest.fixture
def hyperliquid_market_data_service(
    mock_http_client_requester: AsyncMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler: MagicMock,
    mock_hl_mapper: MagicMock,
) -> HyperliquidMarketDataService:
    """Helper function for hyperliquid market data service."""
    return HyperliquidMarketDataService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_hl_request_builder,
        response_handler=mock_hl_response_handler,
        mapper=mock_hl_mapper,
        exchange_name="hyperliquid_test",
    )
