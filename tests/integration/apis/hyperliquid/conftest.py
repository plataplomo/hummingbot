# Integration test fixtures for Hyperliquid API
# Uses test configuration from tests/config/test_config.yaml

from collections.abc import Callable
from typing import Any, Literal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.config_models import AppSettings, ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets, SecretsConfig
from cyberdelta.enums.exchange_names import ExchangeName


@pytest.fixture(scope="session")
def active_hl_config(
    test_app_settings: AppSettings, hl_test_environment_from_config: str
) -> ExchangeSpecificConfig:
    """
    Get ExchangeSpecificConfig for Hyperliquid from test configuration.

    Uses test_config.yaml settings with environment override support.
    """
    hl_config_from_file = test_app_settings.exchanges["hyperliquid"]
    # Override is_mainnet_environment based on hl_test_environment_from_config fixture
    return hl_config_from_file.model_copy(
        update={"is_mainnet_environment": hl_test_environment_from_config == "mainnet"}
    )


@pytest.fixture(scope="session")
def active_hl_secrets(test_secrets_config: SecretsConfig) -> PrivateKeyAuthSecrets:
    """
    Get PrivateKeyAuthSecrets for Hyperliquid from test secrets.

    Uses test_secrets.yaml settings.
    """
    secrets = test_secrets_config.exchanges["hyperliquid"]
    if not isinstance(secrets, PrivateKeyAuthSecrets):
        pytest.fail("Hyperliquid secrets in test_secrets.yaml are not PrivateKeyAuthSecrets type.")
    return secrets


def create_test_exchange_config(
    env_type: Literal["mainnet", "testnet"] = "testnet",
    **kwargs: object,
) -> ExchangeSpecificConfig:
    """
    Create ExchangeSpecificConfig for testing with environment awareness.

    DEPRECATED: Use active_hl_config fixture instead.

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
def hl_api_for_test_env(
    active_hl_config: ExchangeSpecificConfig,
    active_hl_secrets: PrivateKeyAuthSecrets,
) -> HyperliquidAPI:
    """
    Create HyperliquidAPI instance for integration tests.

    Uses configuration from test_config.yaml and test_secrets.yaml.
    For cassette recording/playback, this uses real components.
    """
    # Let HyperliquidAPI create its own real components via factory
    return HyperliquidAPI(
        exchange_config=active_hl_config,
        exchange_secrets=active_hl_secrets,
    )


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
    """
    Factory fixture to create HyperliquidAPI instances with all dependencies injected.
    This enables black-box testing without accessing private members.

    UPDATED: Now uses active_hl_config and active_hl_secrets by default.
    """

    def _create_api(
        # Allow overriding specific dependencies if needed
        config: ExchangeSpecificConfig | None = None,
        secrets: PrivateKeyAuthSecrets | None = None,
        **overrides: MagicMock,
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI with injected dependencies."""
        # Use active fixtures if not overridden
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
