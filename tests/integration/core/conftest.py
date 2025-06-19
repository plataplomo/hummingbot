"""Shared fixtures for core integration tests.

This module provides common fixtures used across core integration tests,
following the same patterns as API integration tests.
Uses test configuration from tests/config/test_config.yaml.
"""

from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest
import pytest_asyncio

from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.apis.hyperliquid import HyperliquidAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets, PrivateKeyAuthSecrets
from cyberdelta.core.execution.orders import (
    MarketOrder,
    MarketOrderConfig,
    MarketOrderService,
)

# Note: Configuration fixtures (test_app_settings, test_secrets_config,
# active_hl_config, active_hl_secrets, active_bp_config, active_bp_secrets)
# are imported from tests.fixtures.config_fixtures via tests/conftest.py


@pytest_asyncio.fixture(scope="session")
async def hyperliquid_api(
    active_hl_config: ExchangeSpecificConfig,
    active_hl_secrets: PrivateKeyAuthSecrets,
) -> AsyncGenerator[HyperliquidAPI]:
    """Create Hyperliquid API instance for integration tests.

    Uses configuration from test_config.yaml and test_secrets.yaml.
    Follows the same pattern as tests/integration/apis/hyperliquid/conftest.py.
    """
    api = HyperliquidAPI(
        exchange_config=active_hl_config,
        exchange_secrets=active_hl_secrets,
    )
    yield api
    # Ensure proper cleanup
    await api.close()


@pytest_asyncio.fixture(scope="session")
async def backpack_api(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
) -> AsyncGenerator[BackpackAPI]:
    """Create Backpack API instance for integration tests.

    Uses configuration from test_config.yaml and test_secrets.yaml.
    Follows the same pattern as tests/integration/apis/backpack/conftest.py.
    """
    api = BackpackAPI(
        exchange_config=active_bp_config,
        exchange_secrets=active_bp_secrets,
    )
    yield api
    # Ensure proper cleanup
    await api.close()


@pytest.fixture
def market_order_config() -> MarketOrderConfig:
    """Create market order configuration for testing."""
    return MarketOrderConfig(
        default_slippage_pct=Decimal("0.002"),  # 0.2% for testing
        max_slippage_pct=Decimal("0.05"),  # 5% max
        min_liquidity_ratio=Decimal("2.0"),  # Require 2x liquidity
        order_timeout_seconds=10,  # 10s timeout for tests
    )


@pytest.fixture
def hyperliquid_market_order(
    hyperliquid_api: HyperliquidAPI, market_order_config: MarketOrderConfig
) -> MarketOrder:
    """Create MarketOrder instance for Hyperliquid."""
    service = MarketOrderService(exchange_api=hyperliquid_api, config=market_order_config)
    return MarketOrder(
        exchange_api=hyperliquid_api, market_order_service=service, config=market_order_config
    )


@pytest.fixture
def backpack_market_order(
    backpack_api: BackpackAPI, market_order_config: MarketOrderConfig
) -> MarketOrder:
    """Create MarketOrder instance for Backpack."""
    service = MarketOrderService(exchange_api=backpack_api, config=market_order_config)
    return MarketOrder(
        exchange_api=backpack_api, market_order_service=service, config=market_order_config
    )
