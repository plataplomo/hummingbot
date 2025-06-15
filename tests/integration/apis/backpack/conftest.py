"""Integration test fixtures for Backpack API testing.

Provides fixtures for both real and mocked BackpackAPI instances, supporting
both cassette-based integration tests and dependency-injected unit tests.
Uses test configuration from tests/config/test_config.yaml.
"""
# Integration test fixtures for Backpack API
# Uses test configuration from tests/config/test_config.yaml

from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_models import AppSettings, ExchangeSpecificConfig
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets, SecretsConfig


@pytest.fixture(scope="session")
def active_bp_config(test_app_settings: AppSettings) -> ExchangeSpecificConfig:
    """Provide ExchangeSpecificConfig for Backpack from test configuration.

    Uses test_config.yaml settings. Backpack always uses mainnet.
    """
    return test_app_settings.exchanges["backpack"]


@pytest.fixture(scope="session")
def active_bp_secrets(test_secrets_config: SecretsConfig) -> ApiKeyAuthSecrets:
    """Provide ApiKeyAuthSecrets for Backpack from test secrets.

    Uses test_secrets.yaml settings.
    """
    secrets = test_secrets_config.exchanges["backpack"]
    if not isinstance(secrets, ApiKeyAuthSecrets):
        pytest.fail("Backpack secrets in test_secrets.yaml are not ApiKeyAuthSecrets type.")
    return secrets


# Mock fixtures for Backpack components
@pytest.fixture
def mock_bp_authenticator() -> MagicMock:
    """Provide mock BackpackEd25519Authenticator."""
    from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator

    mock_auth = MagicMock(spec=BackpackEd25519Authenticator)
    mock_auth.prepare_request = AsyncMock()
    return mock_auth


@pytest.fixture
def mock_bp_error_mapper() -> MagicMock:
    """Provide mock BackpackErrorMapper."""
    from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper

    return MagicMock(spec=BackpackErrorMapper)


@pytest.fixture
def mock_bp_request_builder() -> MagicMock:
    """Provide mock BackpackRequestBuilder."""
    from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder

    return MagicMock(spec=BackpackRequestBuilder)


@pytest.fixture
def mock_bp_response_handler() -> MagicMock:
    """Provide mock BackpackResponseHandler."""
    from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler

    return MagicMock(spec=BackpackResponseHandler)


@pytest.fixture
def mock_bp_account_data_mapper() -> MagicMock:
    """Provide mock BackpackAccountDataMapper."""
    from cyberdelta.apis.backpack.mappers import BackpackAccountDataMapper

    return MagicMock(spec=BackpackAccountDataMapper)


@pytest.fixture
def mock_bp_market_data_mapper() -> MagicMock:
    """Provide mock BackpackMarketDataMapper."""
    from cyberdelta.apis.backpack.mappers import BackpackMarketDataMapper

    return MagicMock(spec=BackpackMarketDataMapper)


@pytest.fixture
def mock_bp_trading_data_mapper() -> MagicMock:
    """Provide mock BackpackTradingDataMapper."""
    from cyberdelta.apis.backpack.mappers import BackpackTradingDataMapper

    return MagicMock(spec=BackpackTradingDataMapper)


@pytest.fixture
def mock_bp_account_service() -> MagicMock:
    """Provide mock BackpackAccountService."""
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
    """Provide mock BackpackMarketDataService."""
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
    """Provide mock BackpackTradingService."""
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
    """Provide mock HttpClient for BackpackAPI."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest_asyncio.fixture
async def bp_api_for_test_env(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
) -> AsyncGenerator[BackpackAPI]:
    """Create BackpackAPI instance for integration tests.

    Uses configuration from test_config.yaml and test_secrets.yaml.
    For cassette recording/playback, this uses real components.
    """
    # Let BackpackAPI create its own real components via factory
    api = BackpackAPI(
        exchange_config=active_bp_config,
        exchange_secrets=active_bp_secrets,
    )
    yield api
    # Ensure proper cleanup
    await api.close()


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
    """Create factory fixture for BackpackAPI instances with all dependencies injected.

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


# Zero Balance Test Fixtures
@pytest.fixture(scope="session")
def test_secrets_zero_balance_file_path() -> Path:
    """Path to the zero balance test secrets file."""
    return Path(__file__).parent.parent.parent.parent / "config" / "test_secrets_zero_balance.yaml"


@pytest.fixture(scope="session")
def test_secrets_zero_balance_config(test_secrets_zero_balance_file_path: Path) -> SecretsConfig:
    """Load zero balance test-specific SecretsConfig from test_secrets_zero_balance.yaml."""
    if not test_secrets_zero_balance_file_path.exists():
        pytest.skip(
            f"Zero balance test secrets file not found at {test_secrets_zero_balance_file_path}, "
            "skipping zero balance tests."
        )
    try:
        manager = SecretsManager(str(test_secrets_zero_balance_file_path))
        if manager.secrets_data is None:
            raise RuntimeError("SecretsManager loaded but secrets_data is None.")
        return manager.secrets_data
    except Exception as e:
        pytest.fail(
            f"Failed to load zero balance test SecretsConfig from {test_secrets_zero_balance_file_path}: {e}"
        )


@pytest.fixture(scope="session")
def bp_secrets_for_zero_balance(
    test_secrets_zero_balance_config: SecretsConfig,
) -> ApiKeyAuthSecrets:
    """Provide ApiKeyAuthSecrets for Backpack zero balance account from test_secrets_zero_balance.yaml."""
    secrets = test_secrets_zero_balance_config.exchanges["backpack"]
    if not isinstance(secrets, ApiKeyAuthSecrets):
        pytest.fail(
            "Backpack secrets in test_secrets_zero_balance.yaml are not ApiKeyAuthSecrets type."
        )
    return secrets


@pytest_asyncio.fixture
async def bp_api_for_zero_balance_test(
    active_bp_config: ExchangeSpecificConfig,
    bp_secrets_for_zero_balance: ApiKeyAuthSecrets,
) -> AsyncGenerator[BackpackAPI]:
    """Create BackpackAPI instance for zero balance integration tests.

    Uses configuration from test_config.yaml and zero balance account secrets
    from test_secrets_zero_balance.yaml. This fixture is specifically for
    testing with an account that has:
    - Zero or minimal balances
    - No positions
    - No open orders
    - Minimal or no trading history
    """
    # Create BackpackAPI with zero balance account credentials
    api = BackpackAPI(
        exchange_config=active_bp_config,
        exchange_secrets=bp_secrets_for_zero_balance,
    )
    yield api
    # Ensure proper cleanup
    await api.close()
