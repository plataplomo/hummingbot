"""
Shared fixtures for HyperliquidMarketDataService tests.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper import HyperliquidMarketDataMapper
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    return MagicMock(spec=HyperliquidRequestBuilder)


@pytest.fixture
def mock_hl_response_handler() -> MagicMock:
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_hl_mapper() -> MagicMock:
    return MagicMock(spec=HyperliquidMarketDataMapper)


@pytest.fixture
def hyperliquid_market_data_service(
    mock_http_client_requester: AsyncMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler: MagicMock,
    mock_hl_mapper: MagicMock,
) -> HyperliquidMarketDataService:
    return HyperliquidMarketDataService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_hl_request_builder,
        response_handler=mock_hl_response_handler,
        mapper=mock_hl_mapper,
        exchange_name="hyperliquid_test",
    )
