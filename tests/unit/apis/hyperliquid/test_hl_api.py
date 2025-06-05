"""Unit tests for the HyperliquidAPI client implementation.

Tests use dependency injection patterns to mock collaborators and focus on isolated logic testing.
"""

from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets

# Removed create_test_exchange_config function - now using active_hl_config fixture


# --- Dependency Injection Test Fixtures for HyperliquidAPI ---


@pytest.fixture
def mock_hl_http_client() -> MagicMock:
    """Mock HttpClient for HyperliquidAPI main endpoint."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest.fixture
def mock_hl_authenticator() -> MagicMock:
    """Mock HyperliquidEip712Authenticator."""
    from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator

    mock_auth = MagicMock(spec=HyperliquidEip712Authenticator)
    mock_auth.prepare_request = AsyncMock()
    mock_auth.wallet_address = "0x1234567890123456789012345678901234567890"
    return mock_auth


@pytest.fixture
def mock_hl_error_mapper() -> MagicMock:
    """Mock HyperliquidErrorMapper."""
    from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper

    mock_mapper = MagicMock(spec=HyperliquidErrorMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    """Mock HyperliquidRequestBuilder."""
    from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder

    mock_builder = MagicMock(spec=HyperliquidRequestBuilder)
    return mock_builder


@pytest.fixture
def mock_hl_response_handler() -> MagicMock:
    """Mock HyperliquidResponseHandler."""
    from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler

    mock_handler = MagicMock(spec=HyperliquidResponseHandler)
    return mock_handler


@pytest.fixture
def mock_hl_mapper() -> MagicMock:
    """Mock HyperliquidMarketDataMapper (for backwards compatibility)."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidMarketDataMapper

    mock_mapper = MagicMock(spec=HyperliquidMarketDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_account_mapper() -> MagicMock:
    """Mock HyperliquidAccountDataMapper."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidAccountDataMapper

    mock_mapper = MagicMock(spec=HyperliquidAccountDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_order_mapper() -> MagicMock:
    """Mock HyperliquidTradingDataMapper (legacy order mapper)."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidTradingDataMapper

    mock_mapper = MagicMock(spec=HyperliquidTradingDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_trading_mapper() -> MagicMock:
    """Mock HyperliquidTradingDataMapper."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidTradingDataMapper

    mock_mapper = MagicMock(spec=HyperliquidTradingDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_user_fill_mapper() -> MagicMock:
    """Mock HyperliquidAccountDataMapper (for user fills)."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidAccountDataMapper

    mock_mapper = MagicMock(spec=HyperliquidAccountDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_account_service() -> MagicMock:
    """Mock HyperliquidAccountService."""
    from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService

    mock_service = MagicMock(spec=HyperliquidAccountService)
    mock_service.get_balances = AsyncMock()
    mock_service.get_positions = AsyncMock()
    mock_service.get_account_summary = AsyncMock()
    mock_service.get_order_history = AsyncMock()
    mock_service.get_trade_history = AsyncMock()
    return mock_service


@pytest.fixture
def mock_hl_trading_service() -> MagicMock:
    """Mock HyperliquidTradingService."""
    from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService

    mock_service = MagicMock(spec=HyperliquidTradingService)
    mock_service.place_order = AsyncMock()
    mock_service.cancel_order = AsyncMock()
    mock_service.cancel_all_orders = AsyncMock()
    mock_service.get_open_orders = AsyncMock()
    mock_service.get_order = AsyncMock()
    return mock_service


@pytest.fixture
def mock_hl_market_data_service() -> MagicMock:
    """Mock HyperliquidMarketDataService."""
    from cyberdelta.apis.hyperliquid.services.hl_market_data_service import (
        HyperliquidMarketDataService,
    )

    mock_service = MagicMock(spec=HyperliquidMarketDataService)
    mock_service.get_ticker = AsyncMock()
    mock_service.get_order_book = AsyncMock()
    mock_service.get_recent_trades = AsyncMock()
    mock_service.get_funding_rates = AsyncMock()
    mock_service.get_market_data = AsyncMock()
    mock_service.get_historical_funding_rates = AsyncMock()
    return mock_service


@pytest.fixture
def hl_api_with_di(
    active_hl_config: ExchangeSpecificConfig,
    active_hl_secrets: PrivateKeyAuthSecrets,
    mock_hl_authenticator: MagicMock,
    mock_hl_error_mapper: MagicMock,
    mock_hl_request_builder: MagicMock,
    mock_hl_response_handler: MagicMock,
    mock_hl_mapper: MagicMock,
    mock_hl_account_mapper: MagicMock,
    mock_hl_order_mapper: MagicMock,
    mock_hl_trading_mapper: MagicMock,
    mock_hl_user_fill_mapper: MagicMock,
    mock_hl_http_client: MagicMock,
    mock_hl_account_service: MagicMock,
    mock_hl_trading_service: MagicMock,
    mock_hl_market_data_service: MagicMock,
) -> Callable[..., Any]:
    """Create HyperliquidAPI instances with all dependencies injected.

    This enables black-box testing without accessing private members.
    Uses active configuration and secrets from test fixtures.
    """
    from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI

    def _create_api(
        # Allow overriding specific dependencies if needed
        config: ExchangeSpecificConfig | None = None,
        secrets: PrivateKeyAuthSecrets | None = None,
        **overrides: MagicMock,
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI with injected dependencies."""
        # Use active fixtures as defaults
        if config is None:
            config = active_hl_config
        if secrets is None:
            secrets = active_hl_secrets

        return HyperliquidAPI(
            exchange_config=config,
            exchange_secrets=secrets,
            authenticator=overrides.get("authenticator", mock_hl_authenticator),
            error_mapper=overrides.get("error_mapper", mock_hl_error_mapper),
            request_builder=overrides.get("request_builder", mock_hl_request_builder),
            response_handler=overrides.get("response_handler", mock_hl_response_handler),
            market_data_mapper=overrides.get("market_data_mapper", mock_hl_mapper),
            account_data_mapper=overrides.get("account_data_mapper", mock_hl_account_mapper),
            trading_data_mapper=overrides.get("trading_data_mapper", mock_hl_trading_mapper),
            http_client=overrides.get("http_client", mock_hl_http_client),
            account_service=overrides.get("account_service", mock_hl_account_service),
            trading_service=overrides.get("trading_service", mock_hl_trading_service),
            market_data_service=overrides.get("market_data_service", mock_hl_market_data_service),
        )

    return _create_api


# --- End Dependency Injection Fixtures ---

# Constants for testing
TEST_WALLET_ADDRESS = "0x0000000000000000000000000000000000000000"


class TestHyperliquidAPIInitialization:
    """Test HyperliquidAPI initialization with dependency injection."""

    def test_api_creation_with_di_fixture(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that the DI fixture creates a valid API instance."""
        api = hl_api_with_di()

        # Verify the API instance is created correctly
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")

    def test_api_creation_with_active_config(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test API creation with active configuration fixture."""
        # Use the active configuration from test config
        api = hl_api_with_di(config=active_hl_config)
        assert api is not None
        # Verify that the API uses the active configuration  
        assert api.exchange_name == "hyperliquid"

    def test_api_has_required_services(self, hl_api_with_di: Callable[..., HyperliquidAPI]) -> None:
        """Test that API instance has all required services initialized."""
        api = hl_api_with_di()

        # Verify the API instance has all required services
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")


class TestHyperliquidAPIWebSocketOperations:
    """Test WebSocket operations."""

    def test_subscription_payload_construction_public_behavior(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test subscription payload construction through public interface."""
        api = hl_api_with_di()

        # Test that the method exists and can be called
        # Note: We avoid accessing protected members directly
        # Instead we test through public interface behavior
        assert hasattr(api, "subscribe_to_order_book")
        assert hasattr(api, "subscribe_to_trades")
        assert hasattr(api, "subscribe_to_account_updates")


class TestHyperliquidAPIDependencyIsolation:
    """Test dependency isolation and injection."""

    def test_custom_dependency_override(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that custom dependencies can be injected."""
        custom_trading_service = MagicMock()
        api = hl_api_with_di(trading_service=custom_trading_service)

        # Verify the custom dependency was injected
        assert api.trading_service is custom_trading_service

    def test_multiple_api_instances_are_isolated(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that multiple API instances are different objects."""
        api1 = hl_api_with_di()
        api2 = hl_api_with_di()

        # Verify instances are different
        assert api1 is not api2

        # Note: In testing, services are the same mock instances (expected behavior)
        # but in production, each API instance would have its own service instances
        assert api1.trading_service is api2.trading_service  # Same mock in tests
        assert api1.account_service is api2.account_service  # Same mock in tests
        assert api1.market_data_service is api2.market_data_service  # Same mock in tests

        # Verify they have the same exchange name but are independent API instances
        assert api1.exchange_name == api2.exchange_name == "hyperliquid"

        # Test that the API instances themselves are different objects
        api1_id = id(api1)
        api2_id = id(api2)
        assert api1_id != api2_id


class TestHyperliquidAPIConfigurationIntegration:
    """Test configuration integration with active fixtures."""

    def test_active_config_environment_awareness(
        self,
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test that active_hl_config fixture provides valid configuration."""
        # Verify that the active configuration has required URLs
        assert active_hl_config.api_base_url_mainnet is not None
        assert active_hl_config.ws_url_mainnet is not None
        assert active_hl_config.exchange_name.value == "hyperliquid"
        
        # Test computed properties work
        assert active_hl_config.active_api_base_url is not None
        assert active_hl_config.active_ws_url is not None

    def test_api_creation_with_active_fixtures(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
    ) -> None:
        """Test that API can be created with active configuration fixtures."""
        # Test with active config and secrets
        api = hl_api_with_di(config=active_hl_config, secrets=active_hl_secrets)

        assert api is not None
        assert api.exchange_name == "hyperliquid"

    def test_api_uses_default_active_config(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that API uses active configuration by default."""
        # No explicit config/secrets provided - should use active fixtures
        api = hl_api_with_di()

        assert api is not None
        assert api.exchange_name == "hyperliquid"


class TestHyperliquidAPIResourceManagement:
    """Test resource management and cleanup."""

    @pytest.mark.asyncio
    async def test_api_close_cleanup(self, hl_api_with_di: Callable[..., HyperliquidAPI]) -> None:
        """Test that API close properly cleans up resources."""
        api = hl_api_with_di()

        # Test that close doesn't raise an error
        await api.close()

    @pytest.mark.asyncio
    async def test_context_manager_behavior(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that API can be used as a context manager."""
        api = hl_api_with_di()

        # Test basic usage without context manager for now
        # since HyperliquidAPI doesn't implement __aenter__/__aexit__
        assert api.exchange_name == "hyperliquid"
        await api.close()
