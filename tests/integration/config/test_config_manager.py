"""Unit tests for cyberdelta.config.config_manager module.

Tests the ConfigManager class for loading and validating application configuration,
including file handling, validation, and error scenarios.
"""

import os
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
import yaml

from cyberdelta.config.config_manager import ConfigManager, ConfigurationError
from cyberdelta.config.config_models import AppSettings


class TestConfigManager:
    """Test cases for ConfigManager class."""

    def create_valid_config_dict(self) -> dict[str, Any]:
        """Create valid configuration dictionary for testing."""
        return {
            "general": {
                "log_level": "INFO",
                "safe_mode": True,
            },
            "exchanges": {
                "backpack": {
                    "enabled": True,
                    "api_base_url_mainnet": "https://api.backpack.exchange",
                    "ws_url_mainnet": "wss://ws.backpack.exchange",
                    "rate_limit_per_minute": 60,
                    "symbols": {"BTC": "BTC_USDC"},
                    "exchange_name": "backpack",
                },
                "hyperliquid": {
                    "enabled": True,
                    "api_base_url_mainnet": "https://api.hyperliquid.xyz",
                    "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
                    "symbols": {"BTC": "BTC-USD"},
                    "exchange_name": "hyperliquid",
                    "chain_id": 1337,
                    "ip_weight_limit_per_minute": 1200,
                    "info_request_type_ip_weights": {"meta": 2, "orderStatus": 1},
                    "default_info_weight": 2,
                    "exchange_action_base_ip_weight": 10,
                    "address_action_safety_net": {"rate_per_minute": 60},
                },
            },
            "strategies": {
                "hl_perp_bp_spot": {
                    "enabled": True,
                    "long_exchange": "hyperliquid",
                    "short_exchange": "backpack",
                    "symbol_long": "BTC",
                    "symbol_short": "BTC",
                    "params": {
                        "funding_threshold": "0.01",
                        "max_price_spread_pct": "0.05",
                        "min_profit_usd": "10.0",
                    },
                },
            },
            "risk": {
                "global": {
                    "max_position_usd": "1000.0",
                    "max_total_exposure_usd": "5000.0",
                },
            },
            "execution": {
                "max_slippage_pct": "0.01",
                "compensation": {
                    "use_limit_orders": True,
                    "limit_price_offset_pct": "0.05",
                },
            },
            "safety_systems": {
                "circuit_breakers": {
                    "enabled": True,
                },
                "position_reconciliation": {
                    "enabled": True,
                },
                "balance_monitoring": {
                    "enabled": True,
                    "min_balance_thresholds_usd": {
                        "backpack": "100.0",
                        "hyperliquid": "200.0",
                    },
                },
            },
            "monitoring": {
                "notifications_enabled": True,
                "alert_methods": ["log"],
            },
            "portfolio_tracker": {
                "data_freshness_seconds": 30,
                "initial_positions": [],
            },
        }

    def create_config_file(self, temp_dir: str, filename: str = "config.yaml") -> Path:
        """Create a temporary config file with valid content."""
        config_path = Path(temp_dir) / filename
        config_data = self.create_valid_config_dict()

        with open(config_path, "w") as f:
            yaml.safe_dump(config_data, f)

        return config_path

    def test_init_with_valid_config_file(self) -> None:
        """Test ConfigManager initialization with valid config file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config_file(temp_dir)

            manager = ConfigManager(str(config_path))

            assert manager.loaded is True
            assert manager.settings is not None
            assert isinstance(manager.settings, AppSettings)
            assert "backpack" in manager.settings.exchanges
            assert "hyperliquid" in manager.settings.exchanges

    def test_init_with_explicit_path(self) -> None:
        """Test ConfigManager initialization with explicit path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config_file(temp_dir, "custom_config.yaml")

            manager = ConfigManager(str(config_path))

            assert manager.config_path == config_path
            assert manager.loaded is True

    def test_init_file_not_found(self) -> None:
        """Test ConfigManager initialization when config file doesn't exist."""
        non_existent_path = "/path/that/does/not/exist/config.yaml"

        with pytest.raises(ConfigurationError) as exc_info:
            ConfigManager(non_existent_path)

        assert "Config file not found" in str(exc_info.value)
        assert non_existent_path in str(exc_info.value)

    def test_init_invalid_yaml(self) -> None:
        """Test ConfigManager initialization with invalid YAML."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "invalid.yaml"

            # Write invalid YAML
            with open(config_path, "w") as f:
                f.write("invalid: yaml: content: [\n")

            with pytest.raises(ConfigurationError) as exc_info:
                ConfigManager(str(config_path))

            assert "Error reading config file" in str(exc_info.value)

    def test_init_empty_file(self) -> None:
        """Test ConfigManager initialization with empty file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "empty.yaml"

            # Write empty file
            with open(config_path, "w") as f:
                f.write("")

            with pytest.raises(ConfigurationError) as exc_info:
                ConfigManager(str(config_path))

            assert "Invalid or empty content" in str(exc_info.value)

    def test_init_invalid_config_structure(self) -> None:
        """Test ConfigManager initialization with invalid config structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "invalid_structure.yaml"

            # Write invalid structure (missing required sections)
            invalid_data = {
                "general": {
                    "log_level": "INFO",
                },
                # Missing other required sections
            }

            with open(config_path, "w") as f:
                yaml.safe_dump(invalid_data, f)

            with pytest.raises(ConfigurationError) as exc_info:
                ConfigManager(str(config_path))

            assert "Invalid application configuration" in str(exc_info.value)

    @patch.dict(os.environ, {}, clear=True)
    def test_get_default_config_path_cwd(self) -> None:
        """Test _get_default_config_path with config in current directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to temp directory and create config.yaml
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                config_path = Path(temp_dir) / "config.yaml"

                # Create a valid config file
                with open(config_path, "w") as f:
                    yaml.safe_dump(self.create_valid_config_dict(), f)

                # Initialize without explicit path - should find the config in cwd
                manager = ConfigManager()

                assert manager.config_path == config_path
                assert manager.loaded is True

            finally:
                os.chdir(original_cwd)

    @patch.dict(os.environ, {}, clear=True)
    def test_get_default_config_path_config_subdir(self) -> None:
        """Test _get_default_config_path with config in config/ subdirectory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                config_dir = Path(temp_dir) / "config"
                config_dir.mkdir()
                config_path = config_dir / "config.yaml"

                # Create a valid config file
                with open(config_path, "w") as f:
                    yaml.safe_dump(self.create_valid_config_dict(), f)

                # Initialize without explicit path - should find the config in config/ subdir
                manager = ConfigManager()

                assert manager.config_path == config_path
                assert manager.loaded is True

            finally:
                os.chdir(original_cwd)

    @patch.dict(os.environ, {"CYBERDELTA_CONFIG_PATH": "/custom/path/config.yaml"})
    def test_get_default_config_path_from_env(self) -> None:
        """Test _get_default_config_path with environment variable."""
        # Test environment variable behavior by creating the file and testing initialization
        with tempfile.TemporaryDirectory() as temp_dir:
            custom_path = Path(temp_dir) / "custom_config.yaml"

            # Create the custom config file
            with open(custom_path, "w") as f:
                yaml.safe_dump(self.create_valid_config_dict(), f)

            # Patch the environment variable to point to our test file
            with patch.dict(os.environ, {"CYBERDELTA_CONFIG_PATH": str(custom_path)}):
                manager = ConfigManager()

                assert manager.config_path == custom_path
                assert manager.loaded is True

    @patch.dict(os.environ, {}, clear=True)
    @patch("cyberdelta.config.config_manager.ConfigManager._get_default_config_path")
    def test_get_default_config_path_fallback(self, mock_get_default_path: Mock) -> None:
        """Test _get_default_config_path fallback when no config files exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the method to return a non-existent path
            non_existent_path = Path(temp_dir) / "non_existent_config.yaml"
            mock_get_default_path.return_value = non_existent_path

            # Should raise ConfigurationError when trying to load the non-existent file
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigManager()

            assert "Config file not found" in str(exc_info.value)
            assert str(non_existent_path) in str(exc_info.value)

    def test_load_method_success(self) -> None:
        """Test load method with valid config."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config_file(temp_dir)

            # Create manager without auto-loading
            manager = ConfigManager.__new__(ConfigManager)
            manager.config_path = config_path
            manager.settings = None
            manager.loaded = False

            # Call load explicitly
            manager.load()

            # Test assertions after successful load
            # The load() method sets these attributes, so we check them
            # If load() raised an exception, we wouldn't reach here
            assert manager.loaded is True
            assert manager.settings is not None

    def test_load_method_file_not_found(self) -> None:
        """Test load method when file doesn't exist."""
        manager = ConfigManager.__new__(ConfigManager)
        manager.config_path = Path("/nonexistent/config.yaml")
        manager.settings = None
        manager.loaded = False

        with pytest.raises(ConfigurationError) as exc_info:
            manager.load()

        assert "Config file not found" in str(exc_info.value)
        assert manager.loaded is False
        assert manager.settings is None

    def test_load_method_yaml_error(self) -> None:
        """Test load method with YAML parsing error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "invalid.yaml"

            # Write invalid YAML
            with open(config_path, "w") as f:
                f.write("invalid: yaml: [unclosed\n")

            manager = ConfigManager.__new__(ConfigManager)
            manager.config_path = config_path
            manager.settings = None
            manager.loaded = False

            with pytest.raises(ConfigurationError) as exc_info:
                manager.load()

            assert "Error reading config file" in str(exc_info.value)
            assert manager.loaded is False
            assert manager.settings is None

    def test_load_method_validation_error(self) -> None:
        """Test load method with Pydantic validation error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "invalid_structure.yaml"

            # Write structurally valid YAML but invalid config structure
            invalid_data = {"invalid": "structure"}

            with open(config_path, "w") as f:
                yaml.safe_dump(invalid_data, f)

            manager = ConfigManager.__new__(ConfigManager)
            manager.config_path = config_path
            manager.settings = None
            manager.loaded = False

            with pytest.raises(ConfigurationError) as exc_info:
                manager.load()

            assert "Invalid application configuration" in str(exc_info.value)
            assert manager.loaded is False
            assert manager.settings is None

    def test_reload_method(self) -> None:
        """Test reload method."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config_file(temp_dir)

            # Initialize manager
            manager = ConfigManager(str(config_path))
            original_settings = manager.settings

            # Modify the config file
            modified_data = self.create_valid_config_dict()
            modified_data["general"]["log_level"] = "DEBUG"
            # Update existing backpack config instead of adding new exchange
            modified_data["exchanges"]["backpack"]["rate_limit_per_minute"] = 30
            modified_data["exchanges"]["backpack"]["symbols"]["ETH"] = "ETH-USD"

            with open(config_path, "w") as f:
                yaml.safe_dump(modified_data, f)

            # Reload
            manager.reload()

            # Verify data was reloaded
            assert manager.loaded is True
            assert manager.settings is not None
            assert manager.settings.general.log_level == "DEBUG"
            assert manager.settings.exchanges["backpack"].rate_limit_per_minute == 30
            assert "ETH" in manager.settings.exchanges["backpack"].symbols
            assert manager.settings is not original_settings  # Should be a new instance

    def test_reload_method_failure(self) -> None:
        """Test reload method when reload fails."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config_file(temp_dir)

            # Initialize manager
            manager = ConfigManager(str(config_path))

            # Corrupt the config file
            with open(config_path, "w") as f:
                f.write("invalid: yaml: [unclosed\n")

            # Reload should fail
            with pytest.raises(ConfigurationError):
                manager.reload()

            # State should be reset
            assert manager.loaded is False
            assert manager.settings is None

    def test_cross_reference_validation(self) -> None:
        """Test cross-reference validation through ConfigManager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_data = self.create_valid_config_dict()
            # Create invalid cross-reference
            config_data["strategies"]["hl_perp_bp_spot"]["long_exchange"] = "nonexistent"

            config_path = Path(temp_dir) / "config.yaml"
            with open(config_path, "w") as f:
                yaml.safe_dump(config_data, f)

            with pytest.raises(ConfigurationError) as exc_info:
                ConfigManager(str(config_path))

            assert "long_exchange" in str(exc_info.value)

    def test_decimal_field_validation(self) -> None:
        """Test Decimal field validation through ConfigManager."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_data = self.create_valid_config_dict()
            # Use various Decimal input formats
            config_data["strategies"]["hl_perp_bp_spot"]["params"]["funding_threshold"] = (
                0.01  # float
            )
            config_data["risk"]["global"]["max_position_usd"] = 1000  # int
            config_data["execution"]["max_slippage_pct"] = "0.01"  # string

            config_path = Path(temp_dir) / "config.yaml"
            with open(config_path, "w") as f:
                yaml.safe_dump(config_data, f)

            manager = ConfigManager(str(config_path))

            assert manager.loaded is True
            assert manager.settings is not None
            # All should be converted to Decimal
            from decimal import Decimal

            assert manager.settings.strategies.hl_perp_bp_spot.params.funding_threshold == Decimal(
                "0.01",
            )
            assert manager.settings.risk.global_risk.max_position_usd == Decimal("1000")
            assert manager.settings.execution.max_slippage_pct == Decimal("0.01")

    @patch("cyberdelta.config.config_manager.logger")
    def test_logging_on_success(self, mock_logger: Mock) -> None:
        """Test that successful loading logs appropriate messages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config_file(temp_dir)

            ConfigManager(str(config_path))

            # Check that info log was called for successful loading
            mock_logger.info.assert_called()
            info_calls = [call for call in mock_logger.info.call_args_list]
            assert any("loaded and validated successfully" in str(call) for call in info_calls)

    @patch("cyberdelta.config.config_manager.logger")
    def test_logging_on_file_not_found(self, mock_logger: Mock) -> None:
        """Test that file not found logs critical message."""
        non_existent_path = "/path/that/does/not/exist/config.yaml"

        with pytest.raises(ConfigurationError):
            ConfigManager(non_existent_path)

        # Check that critical log was called
        mock_logger.critical.assert_called()
        critical_calls = [call for call in mock_logger.critical.call_args_list]
        assert any("Config file not found" in str(call) for call in critical_calls)

    @patch("cyberdelta.config.config_manager.logger")
    def test_logging_on_validation_error(self, mock_logger: Mock) -> None:
        """Test that validation errors log critical messages."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "invalid.yaml"

            # Write invalid structure
            with open(config_path, "w") as f:
                yaml.safe_dump({"invalid": "structure"}, f)

            with pytest.raises(ConfigurationError):
                ConfigManager(str(config_path))

            # Check that critical log was called for validation failure
            mock_logger.critical.assert_called()
            critical_calls = [call for call in mock_logger.critical.call_args_list]
            assert any(
                "Application configuration validation failed" in str(call)
                for call in critical_calls
            )

    def test_settings_data_access(self) -> None:
        """Test accessing settings data after successful loading."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.create_config_file(temp_dir)

            manager = ConfigManager(str(config_path))

            # Test accessing various settings
            assert manager.settings is not None
            assert manager.settings.general.log_level == "INFO"
            assert manager.settings.general.safe_mode is True

            # Test accessing exchange settings
            backpack_config = manager.settings.exchanges["backpack"]
            assert backpack_config.enabled is True
            assert str(backpack_config.api_base_url_mainnet) == "https://api.backpack.exchange/"
            assert backpack_config.rate_limit_per_minute == 60

            # Test accessing strategy settings
            strategy_config = manager.settings.strategies.hl_perp_bp_spot
            assert strategy_config.enabled is True
            assert strategy_config.long_exchange == "hyperliquid"
            assert strategy_config.short_exchange == "backpack"

            # Test accessing risk settings
            risk_config = manager.settings.risk
            from decimal import Decimal

            assert risk_config.global_risk.max_position_usd == Decimal("1000.0")
            assert risk_config.simple_sizing_method == "fixed_fraction"

    def test_file_permissions_error(self) -> None:
        """Test handling of file permission errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"

            # Create file but make it unreadable
            with open(config_path, "w") as f:
                yaml.safe_dump(self.create_valid_config_dict(), f)

            # Make file unreadable (this might not work on all systems)
            try:
                config_path.chmod(0o000)

                with pytest.raises(ConfigurationError) as exc_info:
                    ConfigManager(str(config_path))

                assert "Error reading config file" in str(exc_info.value)

            finally:
                # Restore permissions for cleanup
                config_path.chmod(0o644)

    def test_yaml_safe_load_security(self) -> None:
        """Test that YAML loading uses safe_load for security."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"

            # Write YAML with potentially dangerous content (should be safe with safe_load)
            dangerous_yaml = """
general:
  log_level: "INFO"
exchanges:
  test:
    enabled: true
    api_base_url_mainnet: "https://api.test.com"
    ws_url_mainnet: "wss://ws.test.com"
    exchange_name: "test"
    rate_limit_per_minute: 60
    symbols:
      BTC: "BTC-USD"
strategies:
  hl_perp_bp_spot:
    enabled: true
    long_exchange: "test"
    short_exchange: "test"
    symbol_long: "BTC"
    symbol_short: "BTC"
    params:
      funding_threshold: "0.01"
      max_price_spread_pct: "0.05"
      min_profit_usd: "10.0"
risk:
  global:
    max_position_usd: "1000.0"
    max_total_exposure_usd: "5000.0"
execution:
  max_slippage_pct: "0.01"
  compensation: {}
safety_systems:
  circuit_breakers: {}
  position_reconciliation: {}
  balance_monitoring:
    min_balance_thresholds_usd:
      test: "100.0"
monitoring: {}
portfolio_tracker:
  data_freshness_seconds: 30
  initial_positions: []
# This would be dangerous with yaml.load but safe with yaml.safe_load
dangerous_tag: !!python/object/apply:os.system ["echo 'this should not execute'"]
"""

            with open(config_path, "w") as f:
                f.write(dangerous_yaml)

            # Should fail validation due to extra field, not execute dangerous code
            with pytest.raises(ConfigurationError) as exc_info:
                ConfigManager(str(config_path))

            # Should be a YAML parsing error about the dangerous tag, not a validation error
            assert "Error reading config file" in str(exc_info.value)
            assert "could not determine a constructor" in str(exc_info.value)

    def test_complex_config_validation(self) -> None:
        """Test complex configuration with all features."""
        with tempfile.TemporaryDirectory() as temp_dir:
            complex_config: dict[str, Any] = {
                "general": {
                    "log_level": "DEBUG",
                    "log_file": "/var/log/cyberdelta.log",
                    "module_log_levels": {
                        "cyberdelta.core": "DEBUG",
                        "cyberdelta.apis.backpack": "INFO",
                        "cyberdelta.apis.hyperliquid": "WARNING",
                    },
                    "safe_mode": False,
                    "state_file": "data/custom_state.json",
                    "state_backup_directory": "data/custom_backups",
                    "state_save_interval": 600,
                    "state_backup_count": 10,
                },
                "exchanges": {
                    "backpack": {
                        "enabled": True,
                        "api_base_url_mainnet": "https://api.backpack.exchange",
                        "ws_url_mainnet": "wss://ws.backpack.exchange",
                        "exchange_name": "backpack",
                        "rate_limit_per_minute": 60,
                        "symbols": {
                            "BTC": "BTC_USDC",
                            "ETH": "ETH_USDC",
                            "SOL": "SOL_USDC",
                        },
                    },
                    "hyperliquid": {
                        "enabled": True,
                        "api_base_url_mainnet": "https://api.hyperliquid.xyz",
                        "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
                        "exchange_name": "hyperliquid",
                        "chain_id": 1337,
                        "ip_weight_limit_per_minute": 1200,
                        "info_request_type_ip_weights": {"meta": 2, "orderStatus": 1},
                        "default_info_weight": 2,
                        "exchange_action_base_ip_weight": 10,
                        "address_action_safety_net": {"rate_per_minute": 60},
                        "rate_limit_per_minute": 120,
                        "symbols": {
                            "BTC": "BTC-USD",
                            "ETH": "ETH-USD",
                            "SOL": "SOL-USD",
                        },
                    },
                },
                "strategies": {
                    "hl_perp_bp_spot": {
                        "enabled": True,
                        "long_exchange": "hyperliquid",
                        "short_exchange": "backpack",
                        "symbol_long": "BTC",
                        "symbol_short": "BTC",
                        "params": {
                            "funding_threshold": "0.005",
                            "max_price_spread_pct": "0.02",
                            "min_profit_usd": "5.0",
                        },
                    },
                },
                "risk": {
                    "global": {
                        "max_position_usd": "2000.0",
                        "max_total_exposure_usd": "10000.0",
                    },
                    "use_simple_sizing_path": True,
                    "simple_sizing_method": "fixed_usd",
                    "simple_fixed_fraction": "0.2",
                    "simple_fixed_usd_size": "50.0",
                },
                "execution": {
                    "max_slippage_pct": "0.005",
                    "max_retries": 5,
                    "retry_delay_base_sec": "2.0",
                    "settlement_delay": "3.0",
                    "compensation": {
                        "use_limit_orders": True,
                        "limit_price_offset_pct": "0.1",
                    },
                },
                "safety_systems": {
                    "circuit_breakers": {
                        "enabled": True,
                        "global_consecutive_failures": 10,
                        "global_reset_timeout_sec": 600,
                        "exchange_consecutive_failures": 5,
                        "exchange_reset_timeout_sec": 300,
                    },
                    "position_reconciliation": {
                        "enabled": True,
                        "check_interval_sec": 300,
                        "max_discrepancy_pct": "0.005",
                    },
                    "balance_monitoring": {
                        "enabled": True,
                        "check_interval_sec": 180,
                        "min_balance_thresholds_usd": {
                            "backpack": "500.0",
                            "hyperliquid": "1000.0",
                        },
                    },
                },
                "monitoring": {
                    "notifications_enabled": True,
                    "alert_methods": ["log", "telegram"],
                },
                "portfolio_tracker": {
                    "data_freshness_seconds": 30,
                    "initial_positions": [],
                },
            }

            config_path = Path(temp_dir) / "complex_config.yaml"
            with open(config_path, "w") as f:
                yaml.safe_dump(complex_config, f)

            manager = ConfigManager(str(config_path))

            assert manager.loaded is True
            assert manager.settings is not None

            # Verify complex configuration was loaded correctly
            settings = manager.settings
            assert settings.general.log_level == "DEBUG"
            assert settings.general.module_log_levels is not None
            assert len(settings.general.module_log_levels) == 3
            assert settings.exchanges["backpack"].symbols["SOL"] == "SOL_USDC"
            assert settings.risk.simple_sizing_method == "fixed_usd"
            assert settings.safety_systems.circuit_breakers.global_consecutive_failures == 10
            assert settings.monitoring.alert_methods == ["log", "telegram"]


class TestConfigurationErrorInConfigManager:
    """Test cases for ConfigurationError exception in config manager context."""

    def test_configuration_error_inheritance(self) -> None:
        """Test that ConfigurationError inherits from Exception."""
        error = ConfigurationError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"

    def test_configuration_error_with_cause(self) -> None:
        """Test ConfigurationError with cause chain."""
        original_error = ValueError("original error")

        try:
            raise original_error
        except ValueError as e:
            config_error = ConfigurationError("config error")
            config_error.__cause__ = e

        assert isinstance(config_error, ConfigurationError)
        assert config_error.__cause__ is original_error
        assert str(config_error) == "config error"
