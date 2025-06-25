"""Shared fixtures for BackpackAccountService unit tests."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.base.authenticator_interface import IAuthenticator

# from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
# Removed in refactor
from cyberdelta.core.models.enums import InternalWithdrawalStatus
from cyberdelta.core.models.operations import Withdrawal
from cyberdelta.utils.typing import ParsedJsonResponse


# Type alias for the HTTP client requester callable
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Provide a mock HTTP client requester for Backpack API testing."""
    return AsyncMock()


@pytest.fixture
def mock_http_client() -> MagicMock:
    """Provide a mock HTTP client for withdrawal testing scenarios."""
    return MagicMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Provide a mock BackpackRequestBuilder for API request testing."""
    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Return a mock BackpackResponseHandler for testing."""
    return MagicMock(spec=BackpackResponseHandler)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Return a mock IAuthenticator for testing."""
    return MagicMock(spec=IAuthenticator)


# @pytest.fixture
# def mock_rate_limiter_service() -> AsyncMock:
#     """Provides a mock RateLimiterService."""
#     return AsyncMock(spec=RateLimiterService)  # Removed in refactor


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Return a mock BackpackAccountDataMapper for testing."""
    return MagicMock(spec=BackpackAccountDataMapper)


@pytest.fixture
def bp_account_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackAccountService:
    """Return an instance of BackpackAccountService with mocked dependencies."""
    service = BackpackAccountService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name="backpack_test_account",
    )
    return service


# Withdrawal test fixtures
@pytest.fixture
def asset() -> str:
    """Return standard asset symbol for withdrawal tests."""
    return "USDC"


@pytest.fixture
def amount() -> Decimal:
    """Return standard amount for withdrawal tests."""
    return Decimal("100.0")


@pytest.fixture
def address() -> str:
    """Return standard withdrawal address for testing."""
    return "0x1234567890abcdef1234567890abcdef12345678"


@pytest.fixture
def withdrawal_result() -> Withdrawal:
    """Return standard withdrawal result for tests."""
    return Withdrawal(
        id="withdrawal_123",
        exchange="backpack_test_account",
        status=InternalWithdrawalStatus.COMPLETED,
        asset="USDC",
        quantity=Decimal("100.0"),
        address="0x1234567890abcdef1234567890abcdef12345678",
        timestamp=datetime.now(UTC),
        fee=Decimal("5.0"),
        tx_hash="0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        response_message="Withdrawal completed successfully",
    )
