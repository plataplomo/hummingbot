"""Shared fixtures for HyperliquidAccountService tests."""

from collections.abc import Awaitable, Callable, Generator, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.request_builders.hl_account_request_builder import (
    HyperliquidAccountRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.models.market.fill import Fill
from cyberdelta.utils.typing import ParsedJsonResponse
from tests.common_symbols import BTC_HL


# Type alias for the HTTP client requester callable
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Return mock http client requester for testing.

    Returns:
        AsyncMock: Mock HTTP client requester instance.
    """
    return AsyncMock()


@pytest.fixture
def mock_request_builder() -> MagicMock:
    """Return mock request builder for testing.

    Returns:
        MagicMock: Mock HyperliquidAccountRequestBuilder instance.
    """
    return MagicMock(spec=HyperliquidAccountRequestBuilder)


@pytest.fixture
def mock_response_handler() -> MagicMock:
    """Return mock response handler for testing.

    Returns:
        MagicMock: Mock response handler instance.
    """
    return MagicMock()


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Return mock authenticator for testing.

    Returns:
        MagicMock: Mock IAuthenticator instance.
    """
    return MagicMock(spec=IAuthenticator)


@pytest.fixture
def mock_hl_account_mapper() -> MagicMock:  # For general user state to balance/summary
    """Return mock hl account mapper for testing.

    Returns:
        MagicMock: Mock account mapper instance.
    """
    return MagicMock()


@pytest.fixture
def mock_hl_trading_mapper() -> MagicMock:  # For order/fill related mappings
    """Return mock hl trading mapper for testing.

    Returns:
        MagicMock: Mock HyperliquidOrderMapper instance.
    """
    return MagicMock(spec=HyperliquidOrderMapper)


@pytest.fixture
def mock_hl_order_mapper() -> MagicMock:  # Backward compatibility alias
    """Return mock hl order mapper for testing.

    Returns:
        MagicMock: Mock HyperliquidOrderMapper instance.
    """
    return MagicMock(spec=HyperliquidOrderMapper)


@pytest.fixture
def mock_hl_user_fill_mapper() -> MagicMock:  # Backward compatibility alias
    """Return mock hl user fill mapper for testing.

    Returns:
        MagicMock: Mock HyperliquidOrderMapper instance.
    """
    return MagicMock(spec=HyperliquidOrderMapper)


@pytest.fixture
def mock_http_client() -> Generator[MagicMock, Any, Any]:
    """Return mock http client for testing.

    Yields:
        MagicMock: Mock HTTP client instance.
    """
    with patch("cyberdelta.apis.connectivity.http_client.HttpClient") as mock:
        yield mock


@pytest.fixture
def mock_get_asset_index_callable() -> AsyncMock:
    """Create a mock for the get_asset_index_callable.

    Returns:
        AsyncMock: Mock callable for getting asset index.
    """
    mock = AsyncMock()
    mock.return_value = 0  # Default return value for asset index
    return mock


@pytest.fixture
def hyperliquid_account_service(
    mock_http_client_requester: AsyncMock,
    mock_request_builder: MagicMock,
    mock_response_handler: MagicMock,
    mock_authenticator: MagicMock,
    mock_hl_account_mapper: MagicMock,
    mock_hl_trading_mapper: MagicMock,
    mock_get_asset_index_callable: AsyncMock,
) -> HyperliquidAccountService:
    """Create HyperliquidAccountService instance with mocked dependencies for testing.

    Returns:
        HyperliquidAccountService: Service instance with mocked dependencies.
    """
    return HyperliquidAccountService(
        http_client_requester=mock_http_client_requester,
        request_builder=mock_request_builder,
        response_handler=mock_response_handler,
        authenticator=mock_authenticator,
        exchange_name=ExchangeName.HYPERLIQUID,
        wallet_address="0xTestWalletAddress",
        balance_mapper=mock_hl_account_mapper,  # For balance tests
        position_mapper=mock_hl_account_mapper,  # For position tests
        account_summary_mapper=mock_hl_account_mapper,  # For account summary tests
        order_mapper=mock_hl_trading_mapper,
        transaction_mapper=mock_hl_account_mapper,  # For trade history tests
    )


@pytest.fixture
def mock_trade() -> Fill:
    """Create a mock trade for testing.

    Returns:
        Fill instance with sample data for testing
    """
    return Fill(
        id="trade_12345",
        symbol=BTC_HL,
        executed_at=datetime.now(UTC),
        side=OrderSide.BUY,
        order_id="order_67890",
        exchange=ExchangeName.HYPERLIQUID,
        price=Decimal("50000.0"),
        quantity=Decimal("0.1"),
        fee=Decimal("0.05"),
        fee_asset="USD",
    )
