"""Shared fixtures for HyperliquidTradingService tests."""

from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService


@pytest.fixture
def mock_http_client_requester() -> AsyncMock:
    """Mock for the HTTP client requester used for all operations.

    Returns:
        AsyncMock: Mock HTTP client requester instance.
    """
    return AsyncMock()


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    """Mock for the HyperliquidRequestBuilder.

    Returns:
        MagicMock: Mock HyperliquidRequestBuilder instance.
    """
    return MagicMock(spec=HyperliquidRequestBuilder)


@pytest.fixture
def mock_get_asset_index_callable() -> AsyncMock:
    """Mock for the get_asset_index callable function.

    Returns:
        AsyncMock: Mock get_asset_index callable.
    """
    return AsyncMock()


@pytest.fixture
def mock_hl_trading_mapper() -> MagicMock:
    """Mock for the HyperliquidTradingDataMapper.

    Returns:
        MagicMock: Mock HyperliquidTradingDataMapper instance.
    """
    return MagicMock(spec=HyperliquidTradingDataMapper)


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Mock for the authenticator.

    Returns:
        MagicMock: Mock authenticator instance.
    """
    return MagicMock(spec=IAuthenticator)


@pytest.fixture
def mock_hl_response_handler() -> MagicMock:
    """Mock for the HyperliquidResponseHandler.

    Returns:
        MagicMock: Mock HyperliquidResponseHandler instance.
    """
    return MagicMock(spec=HyperliquidResponseHandler)


@pytest.fixture
def mock_hl_error_mapper() -> MagicMock:
    """Mock for the HyperliquidErrorMapper.

    Returns:
        MagicMock: Mock HyperliquidErrorMapper instance.
    """
    return MagicMock(spec=HyperliquidErrorMapper)


@pytest.fixture
def make_hl_trading_service(
    mock_http_client_requester: AsyncMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler: MagicMock,
    mock_authenticator: MagicMock,
    mock_get_asset_index_callable: AsyncMock,
    mock_hl_trading_mapper: MagicMock,
    mock_hl_error_mapper: MagicMock,
) -> Callable[..., HyperliquidTradingService]:
    """Create factory for HyperliquidTradingService instances with mocked dependencies.

    Returns:
        Callable[..., HyperliquidTradingService]: Factory function for creating service instances.

    Note:
        Returns a factory function that accepts optional parameters like wallet_address
    and returns a properly configured HyperliquidTradingService instance with all
    dependencies mocked.
    """

    def _factory(wallet_address: str = "0xTestWalletAddrTrading") -> HyperliquidTradingService:
        return HyperliquidTradingService(
            http_client_requester=mock_http_client_requester,
            request_builder=mock_hl_request_builder,
            response_handler=mock_hl_response_handler,
            authenticator=mock_authenticator,
            exchange_name="hyperliquid_test_trading",
            wallet_address=wallet_address,
            get_asset_index_callable=mock_get_asset_index_callable,
            trading_mapper=mock_hl_trading_mapper,
            error_mapper=mock_hl_error_mapper,
        )

    return _factory


@pytest.fixture
def hl_trading_service(
    make_hl_trading_service: Callable[..., HyperliquidTradingService],
) -> HyperliquidTradingService:
    """Return a default HyperliquidTradingService instance for testing.

    Uses the default wallet address for most tests that don't need custom configuration.
    """
    return make_hl_trading_service()
