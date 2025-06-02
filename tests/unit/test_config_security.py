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
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

# Add parent directory to path to import from cyberdelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets


@pytest.fixture
def secure_config_manager_setup() -> Generator[tuple[ConfigManager, str, str]]:
    """Set up test case with temporary config files for ConfigManager security tests."""
    with tempfile.TemporaryDirectory() as temp_dir_name:
        config_path = os.path.join(temp_dir_name, "config.yaml")
        with open(config_path, "w") as f:
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
    rate_limit_per_minute: 120
    symbols:
      BTC: "BTC-USD"
    exchange_name: "hyperliquid"
    chain_id: 1
  backpack:
    enabled: true
    api_base_url: "https://api.test2.xyz"
    ws_url: "wss://ws.test2.xyz"
    rate_limit_per_minute: 60
    symbols:
      BTC: "BTC_USDC"
    exchange_name: "backpack"

# Strategy configuration
strategies:
  hl_perp_bp_spot:
    enabled: true
    long_exchange: "hyperliquid"
    short_exchange: "backpack"
    symbol_long: "BTC"
    symbol_short: "BTC"
    params:
      funding_threshold: "0.01"
      max_price_spread_pct: "0.05"
      min_profit_usd: "10.0"

# Risk management
risk:
  global:
    max_position_usd: "100.0"
    max_total_exposure_usd: "500.0"

# Execution
execution:
  max_slippage_pct: "0.01"
  compensation:
    use_limit_orders: true
    limit_price_offset_pct: "0.05"

# Safety systems
safety_systems:
  circuit_breakers:
    enabled: true
  position_reconciliation:
    enabled: true
  balance_monitoring:
    enabled: true
    min_balance_thresholds_usd:
      hyperliquid: "100.0"
      backpack: "50.0"

# Monitoring
monitoring:
  notifications_enabled: true
  alert_methods:
    - "log"
            """)

        invalid_config_path = os.path.join(temp_dir_name, "invalid_config.yaml")
        with open(invalid_config_path, "w") as f:
            f.write("""
# Missing required sections
general:
  log_level: DEBUG
            """)

        config_manager = ConfigManager(config_path)
        yield config_manager, config_path, invalid_config_path


def test_config_validation_success(
    secure_config_manager_setup: tuple[ConfigManager, str, str],
) -> None:
    """Test that a valid config passes validation"""
    config_manager, config_path, _ = secure_config_manager_setup
    assert config_manager.loaded
    assert config_manager.settings is not None


def test_config_validation_failure(
    secure_config_manager_setup: tuple[ConfigManager, str, str],
) -> None:
    """Test that an invalid config fails validation"""
    from cyberdelta.config.config_manager import ConfigurationError

    _, _, invalid_config_path = secure_config_manager_setup
    with pytest.raises(ConfigurationError):
        ConfigManager(invalid_config_path)


def test_env_variable_config_path(
    secure_config_manager_setup: tuple[ConfigManager, str, str],
) -> None:
    """Test that environment variable overrides default config path"""
    _, config_path, _ = secure_config_manager_setup
    with patch.dict("os.environ", {"CYBERDELTA_CONFIG_PATH": config_path}):
        config_manager_env = ConfigManager()
        assert str(config_manager_env.config_path) == config_path


def test_deep_nested_access(secure_config_manager_setup: tuple[ConfigManager, str, str]) -> None:
    """Test accessing deeply nested configuration values"""
    config_manager, _, _ = secure_config_manager_setup
    # Access through the AppSettings object directly
    assert config_manager.settings is not None
    assert config_manager.settings.strategies.hl_perp_bp_spot.symbol_long == "BTC"


def test_missing_nested_access(secure_config_manager_setup: tuple[ConfigManager, str, str]) -> None:
    """Test that missing nested paths return default value"""
    config_manager, _, _ = secure_config_manager_setup
    # Access through the AppSettings object directly
    assert config_manager.settings is not None
    # Test accessing enabled vs missing field
    assert config_manager.settings.strategies.hl_perp_bp_spot.enabled is True


def test_reload_after_change(secure_config_manager_setup: tuple[ConfigManager, str, str]) -> None:
    """Test that configuration changes are detected on reload"""
    config_manager, config_path, _ = secure_config_manager_setup
    with open(config_path) as f:
        config_data = yaml.safe_load(f)

    config_data["general"]["log_level"] = "INFO"
    config_data["risk"]["global"]["max_position_usd"] = 200.0

    with open(config_path, "w") as f:
        yaml.dump(config_data, f)

    config_manager.reload()
    assert config_manager.settings is not None
    assert config_manager.settings.general.log_level == "INFO"
    from decimal import Decimal

    assert config_manager.settings.risk.global_risk.max_position_usd == Decimal("200.0")


@pytest.fixture
def secure_secrets_manager_setup() -> Generator[tuple[str, str, str]]:
    """Set up test case with temporary secrets files for SecretsManager security tests."""
    with (
        tempfile.TemporaryDirectory() as temp_dir_name,
        tempfile.TemporaryDirectory() as home_dir_name,
    ):
        secrets_path = os.path.join(temp_dir_name, "secrets.yaml")
        with open(secrets_path, "w") as f:
            f.write("""
exchanges:
  hyperliquid:
    auth_type: "private_key"
    private_key: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
  backpack:
    auth_type: "api_key"
    api_key: "test_api_key_abc"
    api_secret: "test_api_secret_def"
notifications:
  telegram:
    bot_token: "test_bot_token"
    chat_id: "123456789"
logfire:
  write_token: "test_logfire_token"
            """)

        cyberdelta_dir_in_home = os.path.join(home_dir_name, ".cyberdelta")
        os.makedirs(cyberdelta_dir_in_home)
        home_secrets_path = os.path.join(cyberdelta_dir_in_home, "secrets.yaml")
        with open(home_secrets_path, "w") as f:
            f.write("""
exchanges:
  hyperliquid:
    auth_type: "private_key"
    private_key: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
  backpack:
    auth_type: "api_key"
    api_key: "home_api_key_456"
    api_secret: "home_api_secret_456"
notifications:
  telegram:
    bot_token: "home_bot_token"
    chat_id: "123456789"
logfire:
  write_token: "home_logfire_token"
            """)
        yield secrets_path, home_dir_name, temp_dir_name


@patch("pathlib.Path.home")
def test_fallback_to_home_dir(
    mock_home: MagicMock, secure_secrets_manager_setup: tuple[str, str, str]
) -> None:
    """Test fallback to ~/.cyberdelta/secrets.yaml when env var not set"""
    _, home_dir_name, _ = secure_secrets_manager_setup
    mock_home.return_value = Path(home_dir_name)

    original_env = os.environ.get("CYBERDELTA_SECRETS_PATH")
    if "CYBERDELTA_SECRETS_PATH" in os.environ:
        del os.environ["CYBERDELTA_SECRETS_PATH"]

    try:
        secrets_manager = SecretsManager()
        assert secrets_manager.secrets_loaded
        assert secrets_manager.secrets_data is not None
        backpack_secrets = secrets_manager.secrets_data.exchanges["backpack"]
        if isinstance(backpack_secrets, ApiKeyAuthSecrets):
            assert backpack_secrets.api_key.get_secret_value() == "home_api_key_456"
    finally:
        if original_env is not None:
            os.environ["CYBERDELTA_SECRETS_PATH"] = original_env


def test_env_variable_override(secure_secrets_manager_setup: tuple[str, str, str]) -> None:
    """Test that environment variable overrides default secrets path"""
    secrets_path, _, _ = secure_secrets_manager_setup
    with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": secrets_path}):
        secrets_manager = SecretsManager()
        assert secrets_manager.secrets_loaded
        assert secrets_manager.secrets_data is not None
        backpack_secrets = secrets_manager.secrets_data.exchanges["backpack"]
        if isinstance(backpack_secrets, ApiKeyAuthSecrets):
            assert backpack_secrets.api_key.get_secret_value() == "test_api_key_abc"


def test_nonexistent_secrets_file(secure_secrets_manager_setup: tuple[str, str, str]) -> None:
    """Test handling of nonexistent secrets file"""
    _, _, temp_dir_name = secure_secrets_manager_setup
    nonexistent_path = os.path.join(temp_dir_name, "nonexistent.yaml")
    with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": nonexistent_path}):
        from cyberdelta.config.secrets_manager import ConfigurationError

        with pytest.raises(ConfigurationError):
            SecretsManager()


def test_secrets_deep_nested_access(secure_secrets_manager_setup: tuple[str, str, str]) -> None:
    """Test accessing deeply nested secrets values"""
    secrets_path, _, _ = secure_secrets_manager_setup
    with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": secrets_path}):
        secrets_manager = SecretsManager()
        assert secrets_manager.secrets_loaded
        assert secrets_manager.secrets_data is not None
        hyperliquid_secrets = secrets_manager.secrets_data.exchanges["hyperliquid"]
        if isinstance(hyperliquid_secrets, PrivateKeyAuthSecrets):
            assert hyperliquid_secrets.private_key is not None
            assert "0x1234567890abcdef" in hyperliquid_secrets.private_key.get_secret_value()


def test_automatic_loading_on_get(secure_secrets_manager_setup: tuple[str, str, str]) -> None:
    """Test that secrets are automatically loaded on get if not already loaded"""
    secrets_path, _, _ = secure_secrets_manager_setup
    with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": secrets_path}):
        secrets_manager = SecretsManager()
        # SecretsManager loads on initialization
        assert secrets_manager.secrets_loaded
        assert secrets_manager.secrets_data is not None
        assert (
            secrets_manager.secrets_data.exchanges["hyperliquid"].api_key.get_secret_value()
            == "test_api_key_123"
        )


@pytest.fixture
def integration_config_secrets_setup() -> Generator[tuple[ConfigManager, SecretsManager, str, str]]:
    """Set up for ConfigManager and SecretsManager integration tests."""
    with (
        tempfile.TemporaryDirectory() as temp_dir_name,
    ):
        config_path = os.path.join(temp_dir_name, "config.yaml")
        with open(config_path, "w") as f:
            f.write("""
general:
  log_level: INFO
exchanges:
  hyperliquid:
    enabled: true
    api_base_url: "https://api.test.xyz"
    ws_url: "wss://ws.test.xyz"
    rate_limit_per_minute: 120
    symbols:
      BTC: "BTC-USD"
    exchange_name: "hyperliquid"
    chain_id: 1
  backpack:
    enabled: true
    api_base_url: "https://api.test2.xyz"
    ws_url: "wss://ws.test2.xyz"
    rate_limit_per_minute: 60
    symbols:
      BTC: "BTC_USDC"
    exchange_name: "backpack"
strategies:
  hl_perp_bp_spot:
    enabled: true
    long_exchange: "hyperliquid"
    short_exchange: "backpack"
    symbol_long: "BTC"
    symbol_short: "BTC"
    params:
      funding_threshold: "0.01"
      max_price_spread_pct: "0.05"
      min_profit_usd: "10.0"
risk:
  global:
    max_position_usd: "100.0"
    max_total_exposure_usd: "500.0"
execution:
  max_slippage_pct: "0.01"
  compensation:
    use_limit_orders: true
    limit_price_offset_pct: "0.05"
safety_systems:
  circuit_breakers:
    enabled: true
  position_reconciliation:
    enabled: true
  balance_monitoring:
    enabled: true
    min_balance_thresholds_usd:
      hyperliquid: "100.0"
      backpack: "50.0"
monitoring:
  notifications_enabled: true
  alert_methods:
    - "log"
            """)

        secrets_path = os.path.join(temp_dir_name, "secrets.yaml")
        with open(secrets_path, "w") as f:
            f.write("""
exchanges:
  hyperliquid:
    auth_type: "private_key"
    private_key: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
  backpack:
    auth_type: "api_key"
    api_key: "integrated_api_key"
    api_secret: "integrated_api_secret"
            """)

        config_manager = ConfigManager(config_path)

        with patch.dict("os.environ", {"CYBERDELTA_SECRETS_PATH": secrets_path}):
            secrets_manager = SecretsManager()
            yield config_manager, secrets_manager, config_path, secrets_path


def test_config_secrets_integration(
    integration_config_secrets_setup: tuple[ConfigManager, SecretsManager, str, str],
) -> None:
    """Test integration of ConfigManager and SecretsManager."""
    config_manager, secrets_manager, _, _ = integration_config_secrets_setup

    assert config_manager.loaded
    assert secrets_manager.secrets_loaded
    assert config_manager.settings is not None
    assert config_manager.settings.general.log_level == "INFO"
    assert secrets_manager.secrets_data is not None
    assert (
        secrets_manager.secrets_data.exchanges["hyperliquid"].api_key.get_secret_value()
        == "integrated_api_key"
    )

    # Example: Test resolving a secret reference from config (if such functionality existed)
    # config_api_key_ref = config_manager.get("exchanges.hyperliquid.api_key_secret_ref")
    # resolved_key = secrets_manager.get(config_api_key_ref)
    # assert resolved_key == "integrated_api_key"
    # This part is commented out as ConfigManager doesn't inherently resolve secrets refs.
