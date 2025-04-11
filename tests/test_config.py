#!/usr/bin/env python3
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

# Add parent directory to path to import from cyberdelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager


class TestConfigManager(unittest.TestCase):
    """Tests for the ConfigManager class"""

    def setUp(self):
        """Set up test case with a temporary config file"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "config.yaml")

        # Create a test config file
        with open(self.config_path, "w") as f:
            f.write("""
general:
  log_level: DEBUG
  safe_mode: true
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.test.xyz"
  backpack:
    enabled: true
strategies:
  hl_perp_bp_spot:
    enabled: true
risk:
  global:
    max_position_usd: 100.0
            """)

        self.config_manager = ConfigManager(self.config_path)
        self.config_manager.load()

    def tearDown(self):
        """Clean up temporary files"""
        self.temp_dir.cleanup()

    def test_load_config(self):
        """Test that config loads correctly"""
        self.assertTrue(self.config_manager.loaded)

    def test_get_existing_value(self):
        """Test retrieving existing values with dot notation"""
        self.assertEqual(self.config_manager.get("general.log_level"), "DEBUG")
        self.assertEqual(
            self.config_manager.get("exchanges.hyperliquid.api_base_url"),
            "https://api.test.xyz",
        )
        self.assertEqual(self.config_manager.get("risk.global.max_position_usd"), 100.0)

    def test_get_default_value(self):
        """Test retrieving non-existent values returns default"""
        self.assertEqual(
            self.config_manager.get("nonexistent.key", "default"), "default"
        )
        self.assertEqual(self.config_manager.get("general.nonexistent", 123), 123)

    def test_reload_config(self):
        """Test reloading config after changes"""
        # Modify the config file
        with open(self.config_path, "w") as f:
            f.write("""
general:
  log_level: INFO
  safe_mode: false
exchanges:
  hyperliquid:
    enabled: true
risk:
  global:
    max_position_usd: 200.0
            """)

        # Reload and check values
        self.config_manager.reload()
        self.assertEqual(self.config_manager.get("general.log_level"), "INFO")
        self.assertEqual(self.config_manager.get("risk.global.max_position_usd"), 200.0)
        self.assertFalse(self.config_manager.get("general.safe_mode"))


class TestSecretsManager(unittest.TestCase):
    """Tests for the SecretsManager class"""

    def setUp(self):
        """Set up test case with a temporary secrets file"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.secrets_path = os.path.join(self.temp_dir.name, "secrets.yaml")

        # Create a test secrets file
        with open(self.secrets_path, "w") as f:
            f.write("""
exchanges:
  hyperliquid:
    api_key: "test_api_key_123"
    api_secret: "test_api_secret_456"
  backpack:
    api_key: "test_api_key_789"
database:
  password: "db_password_test"
            """)

        # Create SecretsManager with environment variable
        self.env_patcher = patch.dict(
            "os.environ", {"CYBERDELTA_SECRETS_PATH": self.secrets_path}
        )
        self.env_patcher.start()
        self.secrets_manager = SecretsManager()
        self.secrets_manager.load_secrets()

    def tearDown(self):
        """Clean up temporary files and patchers"""
        self.env_patcher.stop()
        self.temp_dir.cleanup()

    def test_load_secrets(self):
        """Test that secrets load correctly"""
        self.assertTrue(self.secrets_manager.secrets_loaded)

    def test_get_existing_secret(self):
        """Test retrieving existing secrets with dot notation"""
        self.assertEqual(
            self.secrets_manager.get("exchanges.hyperliquid.api_key"),
            "test_api_key_123",
        )
        self.assertEqual(
            self.secrets_manager.get("exchanges.backpack.api_key"), "test_api_key_789"
        )
        self.assertEqual(
            self.secrets_manager.get("database.password"), "db_password_test"
        )

    def test_get_default_secret(self):
        """Test retrieving non-existent secrets returns default"""
        self.assertEqual(
            self.secrets_manager.get("nonexistent.key", "default"), "default"
        )
        self.assertEqual(
            self.secrets_manager.get("exchanges.nonexistent", "missing"), "missing"
        )

    def test_get_path_method(self):
        """Test _get_secrets_path method"""
        path = self.secrets_manager._get_secrets_path()
        self.assertEqual(str(path), self.secrets_path)


# Run tests
if __name__ == "__main__":
    unittest.main()
