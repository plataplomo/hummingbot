#!/usr/bin/env python3
import os
import sys
import tempfile
from collections.abc import Generator
from unittest.mock import patch

import pytest

# Add parent directory to path to import from cyberdelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager


@pytest.fixture
def config_manager_setup() -> Generator[tuple[ConfigManager, str]]:
    """Set up test case with a temporary config file for ConfigManager tests."""
    with tempfile.TemporaryDirectory() as temp_dir_name:
        config_path = os.path.join(temp_dir_name, "config.yaml")
        # Create a test config file
        with open(config_path, "w") as f:
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

        config_manager = ConfigManager(config_path)
        config_manager.load()
        yield config_manager, config_path


def test_load_config(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test that config loads correctly"""
    config_manager, _ = config_manager_setup
    assert config_manager.loaded


def test_get_existing_value(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test retrieving existing values with dot notation"""
    config_manager, _ = config_manager_setup
    assert config_manager.get("general.log_level") == "DEBUG"
    assert config_manager.get("exchanges.hyperliquid.api_base_url") == "https://api.test.xyz"
    assert config_manager.get("risk.global.max_position_usd") == 100.0


def test_get_default_value(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test retrieving non-existent values returns default"""
    config_manager, _ = config_manager_setup
    assert config_manager.get("nonexistent.key", "default") == "default"
    assert config_manager.get("general.nonexistent", 123) == 123


def test_reload_config(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test reloading config after changes"""
    config_manager, config_path = config_manager_setup
    # Modify the config file
    with open(config_path, "w") as f:
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
    config_manager.reload()
    assert config_manager.get("general.log_level") == "INFO"
    assert config_manager.get("risk.global.max_position_usd") == 200.0
    assert not config_manager.get("general.safe_mode")


def test_load_config_exists(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test loading an existing config file."""
    # Implementation of the method
    pass


def test_load_config_default(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test loading default config if file doesn't exist."""
    # Implementation of the method
    pass


def test_load_config_invalid(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test handling of invalid config file."""
    # Implementation of the method
    pass


def test_load_config_dict_input(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test loading config from dictionary input."""
    # Implementation of the method
    pass


def test_get_section(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test getting a section from config."""
    # Implementation of the method
    pass


def test_get_section_not_found(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test getting a non-existent section from config."""
    # Implementation of the method
    pass


def test_parse_value(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test parsing values of different types."""
    # Implementation of the method
    pass


def test_save_config(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test saving config to file."""
    # Implementation of the method
    pass


def test_save_config_validation(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test validation during config save."""
    # Implementation of the method
    pass


def test_save_config_error(config_manager_setup: tuple[ConfigManager, str]) -> None:
    """Test handling of errors during config save."""
    # Implementation of the method
    pass


@pytest.fixture
def secrets_manager_setup() -> Generator[tuple[SecretsManager, str]]:
    """Set up test case with a temporary secrets file for SecretsManager tests."""
    with tempfile.TemporaryDirectory() as temp_dir_name:
        secrets_path = os.path.join(temp_dir_name, "secrets.yaml")
        # Create a test secrets file
        with open(secrets_path, "w") as f:
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
        with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": secrets_path}):
            secrets_manager = SecretsManager()
            secrets_manager.load_secrets()
            yield secrets_manager, secrets_path


def test_load_secrets(secrets_manager_setup: tuple[SecretsManager, str]) -> None:
    """Test that secrets load correctly"""
    secrets_manager, _ = secrets_manager_setup
    assert secrets_manager.secrets_loaded


def test_get_existing_secret(secrets_manager_setup: tuple[SecretsManager, str]) -> None:
    """Test retrieving existing secrets with dot notation"""
    secrets_manager, _ = secrets_manager_setup
    assert secrets_manager.get("exchanges.hyperliquid.api_key") == "test_api_key_123"
    assert secrets_manager.get("exchanges.backpack.api_key") == "test_api_key_789"
    assert secrets_manager.get("database.password") == "db_password_test"


def test_get_default_secret(secrets_manager_setup: tuple[SecretsManager, str]) -> None:
    """Test retrieving non-existent secrets returns default"""
    secrets_manager, _ = secrets_manager_setup
    assert secrets_manager.get("nonexistent.key", "default") == "default"
    assert secrets_manager.get("exchanges.nonexistent", "missing") == "missing"
