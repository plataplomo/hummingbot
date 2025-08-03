"""Shared fixtures for BackpackAccountService unit tests."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.mappers.account.bp_balance_mapper import BackpackBalanceMapper
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.base.authenticator_interface import IAuthenticator

# Removed in refactor
from cyberdelta.core.enums import InternalWithdrawalStatus
from cyberdelta.models.operations import Withdrawal
from cyberdelta.utils.typing import ParsedJsonResponse


# Type alias for the HTTP client requester callable
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Provide a mock HTTP client requester for Backpack API testing.

    Returns:
        AsyncMock: Mock HTTP client requester for testing.
    """
    return AsyncMock()


@pytest.fixture
def mock_http_client() -> MagicMock:
    """Provide a mock HTTP client for withdrawal testing scenarios.

    Returns:
        MagicMock: Mock HTTP client for testing.
    """
    return MagicMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Provide a mock BackpackAccountRequestBuilder for API request testing.

    Returns:
        MagicMock: Mock BackpackAccountRequestBuilder instance.
    """
    return MagicMock(spec=BackpackAccountRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Return a mock BackpackAccountResponseHandler for testing.

    Returns:
        MagicMock: Mock BackpackAccountResponseHandler instance.
    """
    return MagicMock(spec=BackpackAccountResponseHandler)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Return a mock IAuthenticator for testing.

    Returns:
        MagicMock: Mock IAuthenticator instance.
    """
    return MagicMock(spec=IAuthenticator)


# @pytest.fixture
# def mock_rate_limiter_service() -> AsyncMock:
#     """Provides a mock RateLimiterService."""


@pytest.fixture
def mock_mapper() -> MagicMock:
    """Return a mock BackpackBalanceMapper for testing.

    Returns:
        MagicMock: Mock BackpackBalanceMapper instance.
    """
    return MagicMock(spec=BackpackBalanceMapper)


@pytest.fixture
def bp_account_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_authenticator: MagicMock,
) -> BackpackAccountService:
    """Return an instance of BackpackAccountService with mocked dependencies.

    Returns:
        BackpackAccountService: Service instance with mocked dependencies.
    """
    return BackpackAccountService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name="backpack_test_account",
    )


# Withdrawal test fixtures
@pytest.fixture
def asset() -> str:
    """Return standard asset symbol for withdrawal tests.

    Returns:
        str: Asset symbol 'USDC' for testing.
    """
    return "USDC"


@pytest.fixture
def amount() -> Decimal:
    """Return standard amount for withdrawal tests.

    Returns:
        Decimal: Amount value of 100.0 for testing.
    """
    return Decimal("100.0")


@pytest.fixture
def address() -> str:
    """Return standard withdrawal address for testing.

    Returns:
        str: Mock Ethereum address for testing.
    """
    return "0x1234567890abcdef1234567890abcdef12345678"


@pytest.fixture
def withdrawal_result() -> Withdrawal:
    """Return standard withdrawal result for tests.

    Returns:
        Withdrawal: Complete withdrawal object for testing.
    """
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
