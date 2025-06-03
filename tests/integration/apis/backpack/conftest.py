# Integration test fixtures for Backpack API
# Provides configuration and dependency injection fixtures for Backpack integration tests

import os
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.enums.exchange_names import ExchangeName


@pytest.fixture(scope="session")
def active_bp_config() -> ExchangeSpecificConfig:
    """
    Active Backpack exchange configuration for integration tests.
    Backpack only has mainnet, no testnet.
    """
    return ExchangeSpecificConfig.model_validate({
        "exchange_name": ExchangeName.BACKPACK,
        "api_base_url_mainnet": "https://api.backpack.exchange",
        "ws_url_mainnet": "wss://ws.backpack.exchange",
        "api_base_url_testnet": None,  # Backpack has no testnet
        "ws_url_testnet": None,
        "is_mainnet_environment": True,  # Always True for Backpack
        "chain_id": None,
        "rate_limit_per_minute": 120,
        "symbols": {"SOL_USDC": "SOL_USDC", "BTC_USDC": "BTC_USDC"},
        # Backpack uses simple rate limiting, not IP weight-based
        "ip_weight_limit_per_minute": None,
        "info_request_type_ip_weights": None,
        "default_info_weight": None,
        "exchange_action_base_ip_weight": None,
        "address_action_safety_net": None,
        "websocket_send_rate_per_minute": None,
    })


@pytest.fixture(scope="session")
def active_bp_secrets() -> ApiKeyAuthSecrets:
    """
    Active Backpack secrets for integration tests.
    
    IMPORTANT: For actual E2E tests against live Backpack mainnet,
    these secrets MUST be sourced securely (e.g., env vars, CI secrets)
    and NOT committed. For cassette-based integration tests, placeholders
    will be fine once requests are filtered.
    """
    api_key = os.environ.get("BP_MAINNET_API_KEY", "B64_ENCODED_PUBLIC_KEY_PLACEHOLDER_FOR_TESTS")
    api_secret = os.environ.get("BP_MAINNET_API_SECRET", "B64_ENCODED_PRIVATE_KEY_PLACEHOLDER_FOR_TESTS")
    return ApiKeyAuthSecrets(
        api_key=SecretStr(api_key),
        api_secret=SecretStr(api_secret)
    )


# Mock fixtures for Backpack components
@pytest.fixture
def mock_bp_authenticator() -> MagicMock:
    """Mock BackpackEd25519Authenticator."""
    from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
    
    mock_auth = MagicMock(spec=BackpackEd25519Authenticator)
    mock_auth.prepare_request = AsyncMock()
    return mock_auth


@pytest.fixture
def mock_bp_error_mapper() -> MagicMock:
    """Mock BackpackErrorMapper."""
    from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
    
    return MagicMock(spec=BackpackErrorMapper)


@pytest.fixture
def mock_bp_request_builder() -> MagicMock:
    """Mock BackpackRequestBuilder."""
    from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
    
    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_bp_response_handler() -> MagicMock:
    """Mock BackpackResponseHandler."""
    from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
    
    return MagicMock(spec=BackpackResponseHandler)


@pytest.fixture
def mock_bp_account_data_mapper() -> MagicMock:
    """Mock BackpackAccountDataMapper."""
    from cyberdelta.apis.backpack.mappers import BackpackAccountDataMapper
    
    return MagicMock(spec=BackpackAccountDataMapper)


@pytest.fixture
def mock_bp_market_data_mapper() -> MagicMock:
    """Mock BackpackMarketDataMapper."""
    from cyberdelta.apis.backpack.mappers import BackpackMarketDataMapper
    
    return MagicMock(spec=BackpackMarketDataMapper)


@pytest.fixture
def mock_bp_trading_data_mapper() -> MagicMock:
    """Mock BackpackTradingDataMapper."""
    from cyberdelta.apis.backpack.mappers import BackpackTradingDataMapper
    
    return MagicMock(spec=BackpackTradingDataMapper)


@pytest.fixture
def mock_bp_account_service() -> MagicMock:
    """Mock BackpackAccountService."""
    from cyberdelta.apis.backpack.services import BackpackAccountService
    
    mock_service = MagicMock(spec=BackpackAccountService)
    # Add common async methods
    mock_service.get_balances = AsyncMock()
    mock_service.get_positions = AsyncMock()
    mock_service.get_account_summary = AsyncMock()
    mock_service.get_order_history = AsyncMock()
    mock_service.get_trade_history = AsyncMock()
    return mock_service


@pytest.fixture
def mock_bp_market_data_service() -> MagicMock:
    """Mock BackpackMarketDataService."""
    from cyberdelta.apis.backpack.services import BackpackMarketDataService
    
    mock_service = MagicMock(spec=BackpackMarketDataService)
    mock_service.get_ticker = AsyncMock()
    mock_service.get_order_book = AsyncMock()
    mock_service.get_recent_trades = AsyncMock()
    mock_service.get_funding_rates = AsyncMock()
    mock_service.get_candles = AsyncMock()
    return mock_service


@pytest.fixture
def mock_bp_trading_service() -> MagicMock:
    """Mock BackpackTradingService."""
    from cyberdelta.apis.backpack.services import BackpackTradingService
    
    mock_service = MagicMock(spec=BackpackTradingService)
    mock_service.place_order = AsyncMock()
    mock_service.cancel_order = AsyncMock()
    mock_service.cancel_all_orders = AsyncMock()
    mock_service.get_open_orders = AsyncMock()
    mock_service.get_order = AsyncMock()
    return mock_service


@pytest.fixture
def mock_bp_http_client() -> MagicMock:
    """Mock HttpClient for BackpackAPI."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest.fixture
def bp_api_with_di(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
    mock_bp_authenticator: MagicMock,
    mock_bp_error_mapper: MagicMock,
    mock_bp_request_builder: MagicMock,
    mock_bp_response_handler: MagicMock,
    mock_bp_account_data_mapper: MagicMock,
    mock_bp_market_data_mapper: MagicMock,
    mock_bp_trading_data_mapper: MagicMock,
    mock_bp_account_service: MagicMock,
    mock_bp_market_data_service: MagicMock,
    mock_bp_trading_service: MagicMock,
) -> Callable[..., BackpackAPI]:
    """
    Factory fixture to create BackpackAPI instances with all dependencies injected.
    This enables black-box testing without accessing protected members.
    """
    def _create_api(
        config: ExchangeSpecificConfig | None = None,
        secrets: ApiKeyAuthSecrets | None = None,
        **overrides: MagicMock,
    ) -> BackpackAPI:
        """Create BackpackAPI with injected dependencies."""
        final_config = config or active_bp_config
        final_secrets = secrets or active_bp_secrets
        
        # BackpackAPI takes both components and services via dependency injection
        return BackpackAPI(
            exchange_config=final_config,
            exchange_secrets=final_secrets,
            authenticator=overrides.get("authenticator", mock_bp_authenticator),
            error_mapper=overrides.get("error_mapper", mock_bp_error_mapper),
            request_builder=overrides.get("request_builder", mock_bp_request_builder),
            response_handler=overrides.get("response_handler", mock_bp_response_handler),
            account_data_mapper=overrides.get("account_data_mapper", mock_bp_account_data_mapper),
            market_data_mapper=overrides.get("market_data_mapper", mock_bp_market_data_mapper),
            trading_data_mapper=overrides.get("trading_data_mapper", mock_bp_trading_data_mapper),
            account_service=overrides.get("account_service", mock_bp_account_service),
            market_data_service=overrides.get("market_data_service", mock_bp_market_data_service),
            trading_service=overrides.get("trading_service", mock_bp_trading_service),
        )
    
    return _create_api