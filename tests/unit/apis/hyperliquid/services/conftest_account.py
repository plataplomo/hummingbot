"""
Shared fixtures for HyperliquidAccountService tests.
"""

from collections.abc import Awaitable, Callable, Generator, Mapping
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService

# Type alias for the HTTP client requester callable
HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    return MagicMock(spec=HyperliquidRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_authenticator() -> MagicMock:
    return MagicMock(spec=IAuthenticator)


@pytest.fixture
def mock_hl_account_mapper() -> MagicMock:  # For general user state to balance/summary
    return MagicMock(spec=HyperliquidAccountDataMapper)


@pytest.fixture
def mock_hl_trading_mapper() -> MagicMock:  # For order/fill related mappings
    return MagicMock(spec=HyperliquidTradingDataMapper)


@pytest.fixture
def mock_hl_order_mapper() -> MagicMock:  # Backward compatibility alias
    return MagicMock(spec=HyperliquidTradingDataMapper)


@pytest.fixture
def mock_hl_user_fill_mapper() -> MagicMock:  # Backward compatibility alias
    return MagicMock(spec=HyperliquidTradingDataMapper)


@pytest.fixture
def mock_http_client() -> Generator[MagicMock, Any, Any]:
    with patch("cyberdelta.apis.connectivity.http_client.HttpClient") as mock:
        yield mock


@pytest.fixture
def hyperliquid_account_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_authenticator: MagicMock,
    mock_hl_account_mapper: MagicMock,
    mock_hl_trading_mapper: MagicMock,
) -> HyperliquidAccountService:
    service = HyperliquidAccountService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name="hyperliquid_test_account",
        wallet_address="0xTestWalletAddress",
        account_mapper=mock_hl_account_mapper,
        trading_mapper=mock_hl_trading_mapper,
    )
    return service
