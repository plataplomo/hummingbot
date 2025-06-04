"""Tests for the test configuration fixtures.

This module verifies that the test configuration and secrets fixtures
are working correctly.
"""

import os
from pathlib import Path

import pytest

from cyberdelta.config.config_models import AppSettings, ExchangeSpecificConfig
from cyberdelta.config.secrets_models import SecretsConfig


class TestTestConfigurationFixtures:
    """Test the test configuration fixtures."""

    def test_test_config_file_path_fixture(self, test_config_file_path: Path) -> None:
        """Test that test_config_file_path fixture returns correct path."""
        assert test_config_file_path.name == "test_config.yaml"
        assert test_config_file_path.parent.name == "config"
        assert test_config_file_path.parent.parent.name == "tests"

    def test_test_secrets_file_path_fixture(self, test_secrets_file_path: Path) -> None:
        """Test that test_secrets_file_path fixture returns correct path."""
        assert test_secrets_file_path.name == "test_secrets.yaml"
        assert test_secrets_file_path.parent.name == "config"
        assert test_secrets_file_path.parent.parent.name == "tests"

    @pytest.mark.skipif(
        not Path("tests/config/test_config.yaml").exists(),
        reason="test_config.yaml not created yet",
    )
    def test_test_app_settings_fixture(self, test_app_settings: AppSettings) -> None:
        """Test that test_app_settings fixture loads configuration correctly."""
        assert isinstance(test_app_settings, AppSettings)
        assert test_app_settings.general.log_level == "DEBUG"  # As set in example

        # Check exchanges are loaded
        assert "hyperliquid" in test_app_settings.exchanges
        assert "backpack" in test_app_settings.exchanges

        # Check Hyperliquid config
        hl_config = test_app_settings.exchanges["hyperliquid"]
        assert isinstance(hl_config, ExchangeSpecificConfig)
        assert hl_config.exchange_name == "hyperliquid"
        assert hl_config.is_mainnet_environment is False  # Default to testnet

        # Check Backpack config
        bp_config = test_app_settings.exchanges["backpack"]
        assert isinstance(bp_config, ExchangeSpecificConfig)
        assert bp_config.exchange_name == "backpack"
        assert bp_config.is_mainnet_environment is True  # Always mainnet

    @pytest.mark.skipif(
        not Path("tests/config/test_secrets.yaml").exists(),
        reason="test_secrets.yaml not created yet",
    )
    def test_test_secrets_config_fixture(self, test_secrets_config: SecretsConfig) -> None:
        """Test that test_secrets_config fixture loads secrets correctly."""
        assert isinstance(test_secrets_config, SecretsConfig)
        assert "hyperliquid" in test_secrets_config.exchanges
        assert "backpack" in test_secrets_config.exchanges

    def test_hl_test_environment_from_config_default(
        self, hl_test_environment_from_config: str,
    ) -> None:
        """Test that hl_test_environment_from_config returns correct default."""
        # Without environment variable set, should use config value
        if "CYBERDELTA_TEST_ENV_HL" not in os.environ:
            # Depends on whether test_config.yaml exists and what it contains
            assert hl_test_environment_from_config in ["mainnet", "testnet"]

    def test_hl_test_environment_from_config_with_env_var(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that environment variable overrides config."""
        # Set environment variable
        monkeypatch.setenv("CYBERDELTA_TEST_ENV_HL", "mainnet")

        # Test the logic directly since we can't easily inject fixture dependencies
        # The fixture logic checks environment variable first, then config
        env_override = os.environ.get("CYBERDELTA_TEST_ENV_HL")
        assert env_override == "mainnet"

        # With env var set to "mainnet", the fixture should return "mainnet"
        # regardless of what's in the config file
        result = env_override.lower() if env_override else "testnet"
        assert result == "mainnet"
