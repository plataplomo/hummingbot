"""Shared fixtures for BackpackMarketDataService tests.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService

# from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
# Removed in refactor


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Provides a mock HTTP client requester."""
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Provides a mock BackpackRequestBuilder."""
    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Provides a mock BackpackResponseHandler."""
    return MagicMock(spec=BackpackResponseHandler)


# @pytest.fixture
# def mock_rate_limiter_service() -> MagicMock:
#     """Provides a mock RateLimiterService."""
#     mock_service = MagicMock(spec=RateLimiterService)
#     # Mock get_limiter to return an AsyncMock for the runtime limiter
#     limiter_runtime_mock = AsyncMock()  # This will have .acquire()
#     limiter_runtime_mock.acquire = AsyncMock()  # Ensure acquire is an AsyncMock
#     mock_service.get_limiter.return_value = limiter_runtime_mock
#     return mock_service  # Removed in refactor


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Provides a mock BackpackMarketDataMapper."""
    return MagicMock(spec=BackpackMarketDataMapper)


@pytest.fixture
def backpack_market_data_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
) -> BackpackMarketDataService:
    """Provides an instance of BackpackMarketDataService with mocked dependencies."""
    return BackpackMarketDataService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        exchange_name="backpack_test_market_data",
    )
