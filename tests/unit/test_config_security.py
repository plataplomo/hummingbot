#!/usr/bin/env python3
"""
Tests for the secure configuration system.

These tests verify that:
1. Secrets are properly loaded from secure locations
2. Environment variables correctly override default paths
3. Fallback paths are properly handled
4. Validation of configuration works correctly
5. Dot notation access to nested values works properly
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

# Add parent directory to path to import from cyberdelta
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager


class TestSecureConfigManager(unittest.TestCase):
    """Tests for the ConfigManager class with focus on security aspects"""

    def setUp(self):
        """Set up test case with temporary config files"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "config.yaml")

        # Create a test config file with all required sections
        with open(self.config_path, "w") as f:
            f.write("""
# General settings
general:
  log_level: DEBUG
  safe_mode: true
  state_file: "data/state.json"

# Exchange configuration
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.test.xyz"
    ws_url: "wss://ws.test.xyz"
  backpack:
    enabled: true
    api_base_url: "https://api.test2.xyz"
    ws_url: "wss://ws.test2.xyz"

# Strategy configuration
strategies:
  hl_perp_bp_spot:
    enabled: true
    symbols:
      hl_symbol: "BTC"
      bp_symbol: "BTC_USDC"

# Risk management
risk:
  global:
    max_position_usd: 100.0
    max_leverage: 2.0
            """)

        # Invalid config without required sections
        self.invalid_config_path = os.path.join(
            self.temp_dir.name, "invalid_config.yaml"
        )
        with open(self.invalid_config_path, "w") as f:
            f.write("""
# Missing required sections
general:
  log_level: DEBUG
            """)

        # Create a config manager with the valid config
        self.config_manager = ConfigManager(self.config_path)
        self.config_manager.load()

    def tearDown(self):
        """Clean up temporary files"""
        self.temp_dir.cleanup()

    def test_config_validation_success(self):
        """Test that a valid config passes validation"""
        config_manager = ConfigManager(self.config_path)
        result = config_manager.load()
        self.assertTrue(result)
        self.assertTrue(config_manager.loaded)

    def test_config_validation_failure(self):
        """Test that an invalid config fails validation"""
        config_manager = ConfigManager(self.invalid_config_path)
        result = config_manager.load()
        self.assertFalse(result)
        self.assertFalse(config_manager.loaded)

    def test_env_variable_config_path(self):
        """Test that environment variable overrides default config path"""
        with patch.dict("os.environ", {"CYBERDELTA_CONFIG_PATH": self.config_path}):
            # Create a config manager without specifying a path
            config_manager = ConfigManager()
            self.assertEqual(config_manager.config_path, self.config_path)

    def test_deep_nested_access(self):
        """Test accessing deeply nested configuration values"""
        self.assertEqual(
            self.config_manager.get("strategies.hl_perp_bp_spot.symbols.hl_symbol"),
            "BTC",
        )

    def test_missing_nested_access(self):
        """Test that missing nested paths return default value"""
        self.assertEqual(
            self.config_manager.get(
                "strategies.nonexistent.symbols.hl_symbol", "default"
            ),
            "default",
        )

    def test_reload_after_change(self):
        """Test that configuration changes are detected on reload"""
        # Modify the configuration with new values
        with open(self.config_path) as f:
            config_data = yaml.safe_load(f)

        config_data["general"]["log_level"] = "INFO"
        config_data["risk"]["global"]["max_position_usd"] = 200.0

        with open(self.config_path, "w") as f:
            yaml.dump(config_data, f)

        # Reload and verify changes are detected
        self.config_manager.reload()
        self.assertEqual(self.config_manager.get("general.log_level"), "INFO")
        self.assertEqual(self.config_manager.get("risk.global.max_position_usd"), 200.0)


class TestSecureSecretsManager(unittest.TestCase):
    """Tests for the SecretsManager class with focus on security aspects"""

    def setUp(self):
        """Set up test case with temporary secrets files"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.secrets_path = os.path.join(self.temp_dir.name, "secrets.yaml")

        # Create a test secrets file
        with open(self.secrets_path, "w") as f:
            f.write("""
exchanges:
  hyperliquid:
    api_key: "test_api_key_123"
    api_secret: "test_api_secret_456"
    private_key: "test_private_key_789"
  backpack:
    api_key: "test_api_key_abc"
    api_secret: "test_api_secret_def"

database:
  username: "db_user"
  password: "db_password_test"
            """)

        # Path for testing fallback behavior
        self.home_dir = tempfile.TemporaryDirectory()
        self.cyberdelta_dir = os.path.join(self.home_dir.name, ".cyberdelta")
        os.makedirs(self.cyberdelta_dir)
        self.home_secrets_path = os.path.join(self.cyberdelta_dir, "secrets.yaml")

        # Create a home directory secrets file
        with open(self.home_secrets_path, "w") as f:
            f.write("""
exchanges:
  hyperliquid:
    api_key: "home_api_key_123"
  backpack:
    api_key: "home_api_key_456"
            """)

    def tearDown(self):
        """Clean up temporary files"""
        self.temp_dir.cleanup()
        self.home_dir.cleanup()

    @patch("pathlib.Path.home")
    def test_fallback_to_home_dir(self, mock_home):
        """Test fallback to ~/.cyberdelta/secrets.yaml when env var not set"""
        # Mock the home directory to point to our temp directory
        mock_home.return_value = Path(self.home_dir.name)

        # Clear the environment variable if it exists
        original_env = os.environ.get("CYBERDELTA_SECRETS_PATH")
        if "CYBERDELTA_SECRETS_PATH" in os.environ:
            del os.environ["CYBERDELTA_SECRETS_PATH"]

        try:
            # Create secrets manager and load
            secrets_manager = SecretsManager()
            result = secrets_manager.load_secrets()

            # Verify it loaded from the home directory path
            self.assertTrue(result)
            self.assertEqual(
                secrets_manager.get("exchanges.hyperliquid.api_key"), "home_api_key_123"
            )
        finally:
            # Restore the original environment
            if original_env is not None:
                os.environ["CYBERDELTA_SECRETS_PATH"] = original_env

    def test_env_variable_override(self):
        """Test that environment variable overrides default paths"""
        with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": self.secrets_path}):
            secrets_manager = SecretsManager()
            result = secrets_manager.load_secrets()

            self.assertTrue(result)
            self.assertEqual(
                secrets_manager.get("exchanges.hyperliquid.api_key"), "test_api_key_123"
            )

    def test_nonexistent_secrets_file(self):
        """Test behavior when secrets file doesn't exist"""
        nonexistent_path = os.path.join(self.temp_dir.name, "nonexistent.yaml")

        with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": nonexistent_path}):
            secrets_manager = SecretsManager()
            result = secrets_manager.load_secrets()

            self.assertFalse(result)
            self.assertFalse(secrets_manager.secrets_loaded)

    def test_deep_nested_access(self):
        """Test accessing deeply nested secrets"""
        with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": self.secrets_path}):
            secrets_manager = SecretsManager()
            secrets_manager.load_secrets()

            self.assertEqual(
                secrets_manager.get("exchanges.hyperliquid.private_key"),
                "test_private_key_789",
            )

    def test_automatic_loading_on_get(self):
        """Test that secrets are automatically loaded when get is called"""
        with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": self.secrets_path}):
            secrets_manager = SecretsManager()
            # Don't explicitly call load_secrets

            # get should trigger loading
            value = secrets_manager.get("exchanges.backpack.api_key")
            self.assertEqual(value, "test_api_key_abc")
            self.assertTrue(secrets_manager.secrets_loaded)


class TestIntegrationConfigSecrets(unittest.TestCase):
    """Integration tests for ConfigManager and SecretsManager working together"""

    def setUp(self):
        """Set up test case with temporary config and secrets files"""
        self.temp_dir = tempfile.TemporaryDirectory()

        # Create config and secrets files
        self.config_path = os.path.join(self.temp_dir.name, "config.yaml")
        self.secrets_path = os.path.join(self.temp_dir.name, "secrets.yaml")

        # Config with API URLs but no credentials
        with open(self.config_path, "w") as f:
            f.write("""
general:
  log_level: INFO
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.test.xyz"
  backpack:
    enabled: true
    api_base_url: "https://api.test2.xyz"
strategies:
  hl_perp_bp_spot:
    enabled: true
risk:
  global:
    max_position_usd: 100.0
            """)

        # Secrets with credentials
        with open(self.secrets_path, "w") as f:
            f.write("""
exchanges:
  hyperliquid:
    api_key: "test_api_key_123"
    api_secret: "test_api_secret_456"
  backpack:
    api_key: "test_api_key_789"
    api_secret: "test_api_secret_abc"
            """)

    def tearDown(self):
        """Clean up temporary files"""
        self.temp_dir.cleanup()

    def test_config_secrets_integration(self):
        """Test that config and secrets can be used together correctly"""
        with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": self.secrets_path}):
            # Load both config and secrets
            config = ConfigManager(self.config_path)
            config.load()

            secrets = SecretsManager()
            secrets.load_secrets()

            # Combine data from both sources to form a connection URL
            base_url = config.get("exchanges.hyperliquid.api_base_url")
            api_key = secrets.get("exchanges.hyperliquid.api_key")

            self.assertEqual(base_url, "https://api.test.xyz")
            self.assertEqual(api_key, "test_api_key_123")

            # Simulate forming a connection URL with credentials
            connection_url = f"{base_url}?api_key={api_key}"
            self.assertEqual(
                connection_url, "https://api.test.xyz?api_key=test_api_key_123"
            )


if __name__ == "__main__":
    unittest.main()
