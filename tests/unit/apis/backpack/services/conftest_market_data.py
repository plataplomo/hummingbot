"""Shared fixtures for BackpackMarketDataService tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
from cyberdelta.apis.backpack.request_builders.bp_market_data_request_builder import (
    BackpackMarketDataRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_market_data_response_handler import (
    BackpackMarketDataResponseHandler,
)
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.enums import ExchangeName


# Removed in refactor


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Return a mock HTTP client requester for testing."""
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Return a mock BackpackMarketDataRequestBuilder for testing."""
    return MagicMock(spec=BackpackMarketDataRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Return a mock BackpackMarketDataResponseHandler for testing."""
    return MagicMock(spec=BackpackMarketDataResponseHandler)


# @pytest.fixture
# def mock_rate_limiter_service() -> MagicMock:
#     """Provides a mock RateLimiterService."""
#     # Mock get_limiter to return an AsyncMock for the runtime limiter


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Return a mock BackpackTickerMapper for testing market data operations."""
    return MagicMock(spec=BackpackTickerMapper)


@pytest.fixture
def backpack_market_data_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
) -> BackpackMarketDataService:
    """Return a BackpackMarketDataService instance configured with mocked dependencies."""
    return BackpackMarketDataService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        exchange_name=ExchangeName.BACKPACK,
    )
