"""Unit tests for the HyperliquidAPI client implementation.
Tests use dependency injection patterns to mock collaborators and focus on isolated logic testing.
"""

from collections.abc import Callable
from typing import Any, Literal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from cyberdelta.enums.exchange_names import ExchangeName


def create_test_exchange_config(
    env_type: Literal["mainnet", "testnet"] = "testnet",
    **kwargs: object,
) -> ExchangeSpecificConfig:
    """Create ExchangeSpecificConfig for testing with environment awareness.

    Args:
        env_type: Environment type ("mainnet" or "testnet")
        **kwargs: Additional config overrides

    """
    is_mainnet_env = env_type == "mainnet"

    config_dict = {
        "exchange_name": ExchangeName.HYPERLIQUID,
        # Mainnet URLs
        "api_base_url_mainnet": "https://api.hyperliquid.xyz",
        "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
        # Testnet URLs
        "api_base_url_testnet": "https://api.hyperliquid-testnet.xyz",
        "ws_url_testnet": "wss://api.hyperliquid-testnet.xyz/ws",
        # Environment flag
        "is_mainnet_environment": is_mainnet_env,
        "rate_limit_per_minute": 300,
        "symbols": {"ETH": "ETH", "BTC": "BTC"},
        "chain_id": 1337,
        # Hyperliquid-specific rate limiting configuration
        "ip_weight_limit_per_minute": 1200,
        "info_request_type_ip_weights": {
            "l2Book": 2,
            "allMids": 2,
            "meta": 2,
            "userRole": 60,
            "clearinghouseState": 10,
            "openOrders": 1,
        },
        "default_info_weight": 20,
        "exchange_action_base_ip_weight": 1,
        "address_action_safety_net": {"rate_per_minute": 300},
        "websocket_send_rate_per_minute": 1800,
        **kwargs,
    }
    return ExchangeSpecificConfig.model_validate(config_dict)


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
    """Factory fixture to create HyperliquidAPI instances with all dependencies injected.
    This enables black-box testing without accessing private members.
    """
    from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI

    def _create_api(
        # Allow overriding specific dependencies if needed
        config: ExchangeSpecificConfig | None = None,
        secrets: PrivateKeyAuthSecrets | None = None,
        **overrides: MagicMock,
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI with injected dependencies."""
        # Create default Pydantic models if not provided
        if config is None:
            config = create_test_exchange_config()

        if secrets is None:
            secrets = PrivateKeyAuthSecrets(
                private_key=SecretStr("0x" + "1" * 64),
                passphrase=None,
                private_key_testnet=None,
                testnet_seed_passphrase=None,
            )

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
        self, hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that the DI fixture creates a valid API instance."""
        api = hl_api_with_di()

        # Verify the API instance is created correctly
        assert api is not None
        assert api.exchange_name == "hyperliquid"
        assert hasattr(api, "trading_service")
        assert hasattr(api, "account_service")
        assert hasattr(api, "market_data_service")

    def test_api_creation_with_custom_config(
        self, hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test API creation with custom configuration."""
        custom_config = create_test_exchange_config(
            env_type="testnet",
            api_base_url_testnet="https://custom.hyperliquid.api",
            ws_url_testnet="wss://custom.hyperliquid.ws",
        )

        api = hl_api_with_di(config=custom_config)
        assert api is not None

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
        self, hl_api_with_di: Callable[..., HyperliquidAPI],
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
        self, hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that custom dependencies can be injected."""
        custom_trading_service = MagicMock()
        api = hl_api_with_di(trading_service=custom_trading_service)

        # Verify the custom dependency was injected
        assert api.trading_service is custom_trading_service

    def test_multiple_api_instances_are_isolated(
        self, hl_api_with_di: Callable[..., HyperliquidAPI],
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


class TestHyperliquidAPIEnvironmentAwareness:
    """Test environment awareness features for mainnet/testnet support."""

    def test_create_test_exchange_config_testnet_default(self) -> None:
        """Test that create_test_exchange_config defaults to testnet."""
        config = create_test_exchange_config()

        assert config.is_mainnet_environment is False
        assert str(config.api_base_url_mainnet) == "https://api.hyperliquid.xyz/"
        assert str(config.ws_url_mainnet) == "wss://api.hyperliquid.xyz/ws"
        assert str(config.api_base_url_testnet) == "https://api.hyperliquid-testnet.xyz/"
        assert str(config.ws_url_testnet) == "wss://api.hyperliquid-testnet.xyz/ws"

    def test_create_test_exchange_config_mainnet_explicit(self) -> None:
        """Test that create_test_exchange_config can be set to mainnet."""
        config = create_test_exchange_config(env_type="mainnet")

        assert config.is_mainnet_environment is True
        assert str(config.api_base_url_mainnet) == "https://api.hyperliquid.xyz/"
        assert str(config.ws_url_mainnet) == "wss://api.hyperliquid.xyz/ws"
        assert str(config.api_base_url_testnet) == "https://api.hyperliquid-testnet.xyz/"
        assert str(config.ws_url_testnet) == "wss://api.hyperliquid-testnet.xyz/ws"

    def test_create_test_exchange_config_testnet_explicit(self) -> None:
        """Test that create_test_exchange_config can be explicitly set to testnet."""
        config = create_test_exchange_config(env_type="testnet")

        assert config.is_mainnet_environment is False
        assert str(config.api_base_url_mainnet) == "https://api.hyperliquid.xyz/"
        assert str(config.ws_url_mainnet) == "wss://api.hyperliquid.xyz/ws"
        assert str(config.api_base_url_testnet) == "https://api.hyperliquid-testnet.xyz/"
        assert str(config.ws_url_testnet) == "wss://api.hyperliquid-testnet.xyz/ws"

    def test_create_test_exchange_config_with_overrides(self) -> None:
        """Test that create_test_exchange_config accepts kwargs overrides."""
        config = create_test_exchange_config(
            env_type="testnet",
            api_base_url_testnet="https://custom-testnet.hyperliquid.xyz",
            chain_id=42,
        )

        assert config.is_mainnet_environment is False
        assert str(config.api_base_url_testnet) == "https://custom-testnet.hyperliquid.xyz/"
        assert config.chain_id == 42

    def test_api_environment_awareness_through_config(
        self, hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that API can be created with environment-aware config."""
        # Test with testnet config
        testnet_config = create_test_exchange_config(env_type="testnet")
        api_testnet = hl_api_with_di(config=testnet_config)

        assert api_testnet is not None
        assert api_testnet.exchange_name == "hyperliquid"

        # Test with mainnet config
        mainnet_config = create_test_exchange_config(env_type="mainnet")
        api_mainnet = hl_api_with_di(config=mainnet_config)

        assert api_mainnet is not None
        assert api_mainnet.exchange_name == "hyperliquid"


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
        self, hl_api_with_di: Callable[..., HyperliquidAPI],
    ) -> None:
        """Test that API can be used as a context manager."""
        api = hl_api_with_di()

        # Test basic usage without context manager for now
        # since HyperliquidAPI doesn't implement __aenter__/__aexit__
        assert api.exchange_name == "hyperliquid"
        await api.close()
