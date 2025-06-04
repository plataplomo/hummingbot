"""
Tests for Backpack test configuration fixtures.

This module verifies that the Backpack API fixtures are working correctly
with the test configuration system.
"""

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets


class TestBackpackConfigFixtures:
    """Test the Backpack configuration fixtures."""

    def test_active_bp_config(self, active_bp_config: ExchangeSpecificConfig) -> None:
        """Test that active_bp_config fixture provides correct config."""
        assert isinstance(active_bp_config, ExchangeSpecificConfig)
        assert active_bp_config.exchange_name == "backpack"

        # Check URLs are set (Backpack only has mainnet)
        assert active_bp_config.api_base_url_mainnet is not None
        assert active_bp_config.ws_url_mainnet is not None

        # Backpack should always be mainnet
        assert active_bp_config.is_mainnet_environment is True

    def test_active_bp_secrets(self, active_bp_secrets: ApiKeyAuthSecrets) -> None:
        """Test that active_bp_secrets fixture provides correct secrets."""
        assert isinstance(active_bp_secrets, ApiKeyAuthSecrets)
        assert hasattr(active_bp_secrets, "api_key")
        assert hasattr(active_bp_secrets, "api_secret")
        assert active_bp_secrets.api_key is not None
        assert active_bp_secrets.api_secret is not None

    def test_bp_api_for_test_env(
        self, bp_api_for_test_env: BackpackAPI, active_bp_config: ExchangeSpecificConfig,
    ) -> None:
        """Test that bp_api_for_test_env creates a proper API instance."""
        assert isinstance(bp_api_for_test_env, BackpackAPI)

        # Verify it's using mainnet (Backpack only has mainnet)
        assert bp_api_for_test_env.rest_endpoint == str(active_bp_config.api_base_url_mainnet)

    def test_bp_api_with_di_uses_config_fixtures(
        self,
        bp_api_with_di: object,  # This is a factory function
        active_bp_config: ExchangeSpecificConfig,
    ) -> None:
        """Test that bp_api_with_di factory uses config fixtures by default."""
        # Create API instance without providing config/secrets
        api = bp_api_with_di()  # type: ignore[operator]
        assert isinstance(api, BackpackAPI)

        # Verify it's using the config from fixtures (always mainnet for Backpack)
        assert api.rest_endpoint == str(active_bp_config.api_base_url_mainnet)
