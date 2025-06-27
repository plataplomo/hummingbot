"""Shared fixtures for API integration tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest_asyncio

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets, PrivateKeyAuthSecrets


@pytest_asyncio.fixture
async def bp_api_for_test_env(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
) -> AsyncGenerator[BackpackAPI]:
    """Create BackpackAPI instance for integration tests.

    Uses configuration from test_config.yaml and test_secrets.yaml.
    For cassette recording/playback, this uses real components.

    Yields:
        BackpackAPI instance configured for integration testing
    """
    # Let BackpackAPI create its own real components via factory
    api = BackpackAPI(
        exchange_config=active_bp_config,
        exchange_secrets=active_bp_secrets,
    )

    yield api

    # Cleanup
    await api.close()


@pytest_asyncio.fixture
async def hl_api_for_test_env(
    active_hl_config: ExchangeSpecificConfig,
    active_hl_secrets: PrivateKeyAuthSecrets,
) -> AsyncGenerator[HyperliquidAPI]:
    """Create HyperliquidAPI instance for integration tests.

    Uses configuration from test_config.yaml and test_secrets.yaml.
    For cassette recording/playback, this uses real components.

    Yields:
        HyperliquidAPI instance configured for integration testing
    """
    # Let HyperliquidAPI create its own real components via factory
    api = HyperliquidAPI(
        exchange_config=active_hl_config,
        exchange_secrets=active_hl_secrets,
    )

    yield api

    # Cleanup
    await api.close()
