"""Integration test fixtures for Hyperliquid API testing.

Provides fixtures for both real and mocked HyperliquidAPI instances, supporting
both cassette-based integration tests and dependency-injected unit tests.
Uses test configuration from tests/config/test_config.yaml.
"""
# Integration test fixtures for Hyperliquid API
# Uses test configuration from tests/config/test_config.yaml

from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets, SecretsConfig


# Note: active_hl_config and active_hl_secrets fixtures are now provided by
# tests.fixtures.config_fixtures
# to ensure consistency with the main configuration pattern


@pytest.fixture
def mock_hl_http_client() -> MagicMock:
    """Provide mock HttpClient for HyperliquidAPI main endpoint."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest.fixture
def mock_hl_authenticator() -> MagicMock:
    """Provide mock HyperliquidEip712Authenticator."""
    from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator

    mock_auth = MagicMock(spec=HyperliquidEip712Authenticator)
    mock_auth.prepare_request = AsyncMock()
    mock_auth.wallet_address = "0x1234567890123456789012345678901234567890"
    return mock_auth


@pytest.fixture
def mock_hl_error_mapper() -> MagicMock:
    """Provide mock HyperliquidErrorMapper."""
    from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper

    mock_mapper = MagicMock(spec=HyperliquidErrorMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_request_builder() -> MagicMock:
    """Provide mock HyperliquidRequestBuilder."""
    from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder

    mock_builder = MagicMock(spec=HyperliquidRequestBuilder)
    return mock_builder


@pytest.fixture
def mock_hl_response_handler() -> MagicMock:
    """Provide mock HyperliquidResponseHandler."""
    from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler

    mock_handler = MagicMock(spec=HyperliquidResponseHandler)
    return mock_handler


@pytest.fixture
def mock_hl_mapper() -> MagicMock:
    """Provide mock HyperliquidMarketDataMapper (for backwards compatibility)."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidMarketDataMapper

    mock_mapper = MagicMock(spec=HyperliquidMarketDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_account_mapper() -> MagicMock:
    """Provide mock HyperliquidAccountDataMapper."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidAccountDataMapper

    mock_mapper = MagicMock(spec=HyperliquidAccountDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_order_mapper() -> MagicMock:
    """Provide mock HyperliquidTradingDataMapper (legacy order mapper)."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidTradingDataMapper

    mock_mapper = MagicMock(spec=HyperliquidTradingDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_trading_mapper() -> MagicMock:
    """Provide mock HyperliquidTradingDataMapper."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidTradingDataMapper

    mock_mapper = MagicMock(spec=HyperliquidTradingDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_user_fill_mapper() -> MagicMock:
    """Provide mock HyperliquidAccountDataMapper (for user fills)."""
    from cyberdelta.apis.hyperliquid.mappers import HyperliquidAccountDataMapper

    mock_mapper = MagicMock(spec=HyperliquidAccountDataMapper)
    return mock_mapper


@pytest.fixture
def mock_hl_account_service() -> MagicMock:
    """Provide mock HyperliquidAccountService."""
    from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService

    mock_service = MagicMock(spec=HyperliquidAccountService)
    return mock_service


@pytest.fixture
def mock_hl_trading_service() -> MagicMock:
    """Provide mock HyperliquidTradingService."""
    from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService

    mock_service = MagicMock(spec=HyperliquidTradingService)
    return mock_service


@pytest.fixture
def mock_hl_market_data_service() -> MagicMock:
    """Provide mock HyperliquidMarketDataService."""
    from cyberdelta.apis.hyperliquid.services.hl_market_data_service import (
        HyperliquidMarketDataService,
    )

    mock_service = MagicMock(spec=HyperliquidMarketDataService)
    return mock_service


# REMOVED MOCK FIXTURES - SECURITY VIOLATION
# Mocking of financial operations (place_order, get_balances, get_ticker, etc.)
# is FORBIDDEN in integration tests as it bypasses real exchange validation.
# Integration tests MUST use real API calls with VCR cassettes for reproducibility.


# Note: hl_api_for_test_env fixture moved to parent apis/conftest.py
# to be shared with cross_exchange tests


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
    """Create factory fixture for HyperliquidAPI instances with all dependencies injected.

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


# Multi-Environment Test Fixtures (Zero Balance, Large Balance)


@pytest.fixture(scope="session")
def test_secrets_zero_balance_file_path() -> Path:
    """Path to the zero balance test secrets file."""
    return Path(__file__).parent.parent.parent.parent / "config" / "test_secrets_zero_balance.yaml"


@pytest.fixture(scope="session")
def test_secrets_large_balance_file_path() -> Path:
    """Path to the large balance test secrets file."""
    return Path(__file__).parent.parent.parent.parent / "config" / "test_secrets_large_balance.yaml"


@pytest.fixture(scope="session")
def test_secrets_zero_balance_config(test_secrets_zero_balance_file_path: Path) -> SecretsConfig:
    """Load zero balance test-specific SecretsConfig from test_secrets_zero_balance.yaml."""
    if not test_secrets_zero_balance_file_path.exists():
        pytest.skip(
            f"Zero balance test secrets file not found at {test_secrets_zero_balance_file_path}, "
            "skipping zero balance tests.",
        )
    try:
        manager = SecretsManager(str(test_secrets_zero_balance_file_path))
        if manager.secrets_data is None:
            raise RuntimeError("SecretsManager loaded but secrets_data is None.")
        return manager.secrets_data
    except Exception as e:
        pytest.fail(
            f"Failed to load zero balance test SecretsConfig from "
            f"{test_secrets_zero_balance_file_path}: {e}",
        )


@pytest.fixture(scope="session")
def test_secrets_large_balance_config(test_secrets_large_balance_file_path: Path) -> SecretsConfig:
    """Load large balance test-specific SecretsConfig from test_secrets_large_balance.yaml."""
    if not test_secrets_large_balance_file_path.exists():
        pytest.skip(
            f"Large balance test secrets file not found at {test_secrets_large_balance_file_path}, "
            "skipping large balance tests.",
        )
    try:
        manager = SecretsManager(str(test_secrets_large_balance_file_path))
        if manager.secrets_data is None:
            raise RuntimeError("SecretsManager loaded but secrets_data is None.")
        return manager.secrets_data
    except Exception as e:
        pytest.fail(
            f"Failed to load large balance test SecretsConfig from "
            f"{test_secrets_large_balance_file_path}: {e}",
        )


@pytest.fixture(scope="session")
def hl_secrets_for_zero_balance(
    test_secrets_zero_balance_config: SecretsConfig,
) -> PrivateKeyAuthSecrets:
    """Provide PrivateKeyAuthSecrets for zero balance account.

    Loads from test_secrets_zero_balance.yaml.
    """
    secrets = test_secrets_zero_balance_config.exchanges["hyperliquid"]
    if not isinstance(secrets, PrivateKeyAuthSecrets):
        pytest.fail(
            "Hyperliquid secrets in test_secrets_zero_balance.yaml are not "
            "PrivateKeyAuthSecrets type.",
        )
    return secrets


@pytest.fixture(scope="session")
def hl_secrets_for_large_balance(
    test_secrets_large_balance_config: SecretsConfig,
) -> PrivateKeyAuthSecrets:
    """Provide PrivateKeyAuthSecrets for large balance account.

    Loads from test_secrets_large_balance.yaml.
    """
    secrets = test_secrets_large_balance_config.exchanges["hyperliquid"]
    if not isinstance(secrets, PrivateKeyAuthSecrets):
        pytest.fail(
            "Hyperliquid secrets in test_secrets_large_balance.yaml are not "
            "PrivateKeyAuthSecrets type.",
        )
    return secrets


@pytest_asyncio.fixture
async def hl_api_for_zero_balance_test(
    active_hl_config: ExchangeSpecificConfig,
    hl_secrets_for_zero_balance: PrivateKeyAuthSecrets,
) -> AsyncGenerator[HyperliquidAPI]:
    """Create HyperliquidAPI instance for zero balance integration tests.

    Uses configuration from test_config.yaml and zero balance account secrets
    from test_secrets_zero_balance.yaml. This fixture is specifically for
    testing with an account that has:
    - Zero or minimal balances
    - No positions
    - No open orders
    - Minimal or no trading history
    """
    # Create HyperliquidAPI with zero balance account credentials
    api = HyperliquidAPI(
        exchange_config=active_hl_config,
        exchange_secrets=hl_secrets_for_zero_balance,
    )
    yield api
    # Ensure proper cleanup
    await api.close()


@pytest_asyncio.fixture
async def hl_api_for_large_balance_test(
    active_hl_config: ExchangeSpecificConfig,
    hl_secrets_for_large_balance: PrivateKeyAuthSecrets,
) -> AsyncGenerator[HyperliquidAPI]:
    """Create HyperliquidAPI instance for large balance integration tests.

    Uses configuration from test_config.yaml and large balance account secrets
    from test_secrets_large_balance.yaml. This fixture is specifically for
    testing with an account that has:
    - Large balance for maximum position testing
    - High leverage limits
    - Sufficient margin for edge case testing
    - Ability to open and close large positions
    """
    # Create HyperliquidAPI with large balance account credentials
    api = HyperliquidAPI(
        exchange_config=active_hl_config,
        exchange_secrets=hl_secrets_for_large_balance,
    )
    yield api
    # Ensure proper cleanup
    await api.close()
