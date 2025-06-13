"""Tests for Hyperliquid test configuration fixtures.

This module verifies that the Hyperliquid API fixtures are working correctly
with the test configuration system.
"""

from collections.abc import Callable
from typing import Any

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets

pytestmark = [pytest.mark.integration, pytest.mark.zero_balance]


class TestHyperliquidConfigFixtures:
    """Test the Hyperliquid configuration fixtures."""

    def test_active_hl_config(self, active_hl_config: ExchangeSpecificConfig) -> None:
        """Test that active_hl_config fixture provides correct config."""
        assert isinstance(active_hl_config, ExchangeSpecificConfig)
        assert active_hl_config.exchange_name == "hyperliquid"

        assert active_hl_config.api_base_url_mainnet is not None
        assert active_hl_config.ws_url_mainnet is not None
        assert active_hl_config.api_base_url_testnet is not None
        assert active_hl_config.ws_url_testnet is not None

        assert isinstance(active_hl_config.is_mainnet_environment, bool)

    def test_active_hl_secrets(self, active_hl_secrets: PrivateKeyAuthSecrets) -> None:
        """Test that active_hl_secrets fixture provides correct secrets."""
        assert isinstance(active_hl_secrets, PrivateKeyAuthSecrets)
        assert hasattr(active_hl_secrets, "private_key")
        assert active_hl_secrets.private_key is not None

    def test_hl_api_for_test_env(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test that hl_api_for_test_env creates a proper API instance."""
        assert isinstance(hl_api_for_test_env, HyperliquidAPI)

        expected_env = active_hl_config.is_mainnet_environment
        if expected_env:
            assert hl_api_for_test_env.rest_endpoint == str(active_hl_config.api_base_url_mainnet)
        else:
            assert hl_api_for_test_env.rest_endpoint == str(active_hl_config.api_base_url_testnet)

    def test_hl_api_with_di_uses_config_fixtures(
        self,
        hl_api_with_di: Callable[..., Any],
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test that hl_api_with_di factory uses config fixtures by default."""
        api = hl_api_with_di()
        assert isinstance(api, HyperliquidAPI)

        expected_env = active_hl_config.is_mainnet_environment
        if expected_env:
            assert api.rest_endpoint == str(active_hl_config.api_base_url_mainnet)
        else:
            assert api.rest_endpoint == str(active_hl_config.api_base_url_testnet)
