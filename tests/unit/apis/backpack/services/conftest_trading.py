"""Shared fixtures for BackpackTradingService tests."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.mappers.trading.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.request_builders.bp_trading_request_builder import (
    BackpackTradingRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_trading_response_handler import (
    BackpackTradingResponseHandler,
)
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.utils.typing import ParsedJsonResponse


# Removed in refactor

# Type alias for the HTTP client requester callable
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Return a mock HTTP client requester for trading API testing."""
    return AsyncMock(spec=HttpClientRequesterSig)


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Return a mock BackpackTradingRequestBuilder for trading request testing."""
    return MagicMock(spec=BackpackTradingRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Return a mock BackpackTradingResponseHandler for trading response testing."""
    return MagicMock(spec=BackpackTradingResponseHandler)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Return a mock IAuthenticator for trading authentication testing."""
    return MagicMock(spec=IAuthenticator)


# @pytest.fixture
# def mock_rate_limiter_service() -> AsyncMock:
#     """Provides a mock RateLimiterService."""


@pytest.fixture
def mock_order_mapper() -> MagicMock:
    """Return a mock BackpackOrderMapper for order data mapping testing."""
    return MagicMock(spec=BackpackOrderMapper)


@pytest.fixture
def bp_trading_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackTradingService:
    """Return a BackpackTradingService instance configured with mocked dependencies for testing."""
    return BackpackTradingService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name="backpack_test_trading",
    )
    # The service instantiates its own _order_mapper. Tests will patch this.
