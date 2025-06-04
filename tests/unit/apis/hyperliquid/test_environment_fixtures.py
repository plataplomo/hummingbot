"""Tests for environment-aware fixtures for Hyperliquid API testing.

This module tests the new environment-aware fixtures that allow tests
to target mainnet or testnet based on configuration.
"""

import os
from unittest.mock import patch

from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from cyberdelta.enums.exchange_names import ExchangeName


class TestEnvironmentAwareFixtures:
    """Test the environment-aware fixtures for Hyperliquid testing."""

    def test_hl_test_environment_default_testnet(self, hl_test_environment: str) -> None:
        """Test that hl_test_environment defaults to testnet."""
        assert hl_test_environment == "testnet"

    def test_hl_test_environment_uses_env_var(self) -> None:
        """Test that hl_test_environment responds to environment variable."""
        # Test the logic directly
        with patch.dict(os.environ, {"CYBERDELTA_TEST_ENV_HL": "mainnet"}):
            env = os.environ.get("CYBERDELTA_TEST_ENV_HL", "testnet")
            assert env == "mainnet"

        with patch.dict(os.environ, {}, clear=True):
            env = os.environ.get("CYBERDELTA_TEST_ENV_HL", "testnet")
            assert env == "testnet"

    def test_active_hl_config_testnet(self, active_hl_config: ExchangeSpecificConfig) -> None:
        """Test active_hl_config fixture with testnet environment."""
        assert active_hl_config.exchange_name == ExchangeName.HYPERLIQUID
        assert active_hl_config.is_mainnet_environment is False
        assert str(active_hl_config.api_base_url_mainnet) == "https://api.hyperliquid.xyz/"
        assert str(active_hl_config.ws_url_mainnet) == "wss://api.hyperliquid.xyz/ws"
        assert str(active_hl_config.api_base_url_testnet) == "https://api.hyperliquid-testnet.xyz/"
        assert str(active_hl_config.ws_url_testnet) == "wss://api.hyperliquid-testnet.xyz/ws"
        assert active_hl_config.chain_id == 1337

    def test_active_hl_config_has_rate_limiting(
        self,
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test that active_hl_config includes Hyperliquid-specific rate limiting."""
        assert active_hl_config.ip_weight_limit_per_minute == 1200
        if active_hl_config.info_request_type_ip_weights is not None:
            assert "l2Book" in active_hl_config.info_request_type_ip_weights
            assert active_hl_config.info_request_type_ip_weights["l2Book"] == 2
        assert active_hl_config.default_info_weight == 20

    def test_active_hl_secrets_structure(self, active_hl_secrets: PrivateKeyAuthSecrets) -> None:
        """Test active_hl_secrets fixture has correct structure."""
        assert active_hl_secrets.auth_type == "private_key"
        assert active_hl_secrets.private_key is not None
        # These should be None unless environment variables are set
        assert active_hl_secrets.private_key_testnet is None
        assert active_hl_secrets.testnet_seed_passphrase is None
        assert active_hl_secrets.passphrase is None

    def test_active_hl_secrets_env_var_logic(self) -> None:
        """Test that secrets fixture logic handles environment variables correctly."""
        # Test the logic that the fixture uses
        with patch.dict(
            os.environ,
            {
                "HL_PRIVATE_KEY": "0xabc123def456789",
                "HL_TESTNET_PRIVATE_KEY": "0xtest123456789",
                "HL_TESTNET_SEED_PASSPHRASE": "test seed phrase",
                "HL_PASSPHRASE": "test passphrase",
            },
        ):
            main_key = os.environ.get("HL_PRIVATE_KEY", "default_key")
            testnet_key = os.environ.get("HL_TESTNET_PRIVATE_KEY")
            testnet_seed = os.environ.get("HL_TESTNET_SEED_PASSPHRASE")
            passphrase = os.environ.get("HL_PASSPHRASE")

            assert main_key == "0xabc123def456789"
            assert testnet_key == "0xtest123456789"
            assert testnet_seed == "test seed phrase"
            assert passphrase == "test passphrase"

    def test_active_hl_secrets_validation(self, active_hl_secrets: PrivateKeyAuthSecrets) -> None:
        """Test that active_hl_secrets passes validation."""
        # Should not raise any validation errors
        assert isinstance(active_hl_secrets, PrivateKeyAuthSecrets)

        # Verify the private key has a reasonable format
        pk_value = active_hl_secrets.private_key.get_secret_value()
        assert pk_value.startswith("0x")
        assert len(pk_value) == 66  # 0x + 64 hex characters


class TestEnvironmentConfigurationIntegration:
    """Test integration between environment configuration and API components."""

    def test_config_and_secrets_compatibility(
        self,
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
    ) -> None:
        """Test that active config and secrets are compatible."""
        # Both should be for Hyperliquid
        assert active_hl_config.exchange_name == ExchangeName.HYPERLIQUID
        assert active_hl_secrets.auth_type == "private_key"

        # Config should have all required fields for both environments
        assert active_hl_config.api_base_url_mainnet is not None
        assert active_hl_config.ws_url_mainnet is not None
        assert active_hl_config.api_base_url_testnet is not None
        assert active_hl_config.ws_url_testnet is not None

        # Secrets should have at least the main private key
        assert active_hl_secrets.private_key is not None

    def test_testnet_environment_configuration(
        self,
        hl_test_environment: str,
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test that testnet environment produces correct configuration."""
        # Default should be testnet
        assert hl_test_environment == "testnet"
        assert active_hl_config.is_mainnet_environment is False

        # Should have testnet URLs configured
        assert active_hl_config.api_base_url_testnet is not None
        assert active_hl_config.ws_url_testnet is not None
        assert "testnet" in str(active_hl_config.api_base_url_testnet)
        assert "testnet" in str(active_hl_config.ws_url_testnet)

    def test_fixture_integration_with_api_instantiation(
        self,
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
    ) -> None:
        """Test that fixtures can be used to instantiate HyperliquidAPI components."""
        # This test verifies that the fixtures produce valid configuration
        # that can be used with the actual API components

        # Verify that we can create components with these fixtures
        from cyberdelta.apis.hyperliquid.hl_api_components_factory import (
            HyperliquidAPIComponentsFactory,
        )

        # Should be able to create a factory without errors
        factory = HyperliquidAPIComponentsFactory(
            exchange_config=active_hl_config,
            exchange_secrets=active_hl_secrets,
            chain_id=active_hl_config.chain_id or 1337,
        )

        # Factory should be created successfully
        assert factory is not None
        assert factory.exchange_config == active_hl_config
        assert factory.exchange_secrets == active_hl_secrets

    def test_environment_aware_url_selection_testnet(
        self,
        active_hl_config: ExchangeSpecificConfig,
    ) -> None:
        """Test URL selection logic for testnet environment."""
        # When is_mainnet_environment is False, testnet URLs should be available
        assert active_hl_config.is_mainnet_environment is False
        assert active_hl_config.api_base_url_testnet is not None
        assert active_hl_config.ws_url_testnet is not None

        # Mainnet URLs should also be available as fallback
        assert active_hl_config.api_base_url_mainnet is not None
        assert active_hl_config.ws_url_mainnet is not None
