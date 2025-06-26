"""Unit tests for cyberdelta.config.config_models module.

Tests all Pydantic models for application configuration validation,
including field validation, model validation, and cross-references.
"""

from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.config.models.config_models import (
    AppSettings,
    BalanceMonitoringSettings,
    CircuitBreakerSettings,
    ExchangeSpecificConfig,
    ExecutionCompensationSettings,
    ExecutionSettings,
    GeneralSettings,
    GlobalRiskSettings,
    MonitoringSettings,
    RiskSettings,
    SafetySystemsSettings,
)
from cyberdelta.config.models.funding_strategy_models import (
    StrategiesSettings,
    StrategyParamsHLPerpBPSpot,
)


class TestGeneralSettings:
    """Test cases for GeneralSettings model."""

    def test_valid_general_settings_minimal(self) -> None:
        """Test valid GeneralSettings with minimal required fields."""
        data: dict[str, Any] = {}  # All fields have defaults

        settings = GeneralSettings.model_validate(data)

        assert settings.log_level == "INFO"
        assert settings.log_file is None
        assert settings.module_log_levels is None
        assert settings.safe_mode is True
        assert settings.state_file == "data/state.json"
        assert settings.state_backup_directory == "data/state_backups"
        assert settings.state_save_interval == 300
        assert settings.state_backup_count == 5

    def test_valid_general_settings_all_fields(self) -> None:
        """Test valid GeneralSettings with all fields specified."""
        data = {
            "log_level": "DEBUG",
            "log_file": "/var/log/cyberdelta.log",
            "module_log_levels": {
                "cyberdelta.core": "DEBUG",
                "cyberdelta.apis": "INFO",
            },
            "safe_mode": False,
            "state_file": "custom/state.json",
            "state_backup_directory": "custom/backups",
            "state_save_interval": 600,
            "state_backup_count": 10,
        }

        settings = GeneralSettings.model_validate(data)

        assert settings.log_level == "DEBUG"
        assert settings.log_file == "/var/log/cyberdelta.log"
        assert settings.module_log_levels == {
            "cyberdelta.core": "DEBUG",
            "cyberdelta.apis": "INFO",
        }
        assert settings.safe_mode is False
        assert settings.state_file == "custom/state.json"
        assert settings.state_backup_directory == "custom/backups"
        assert settings.state_save_interval == 600
        assert settings.state_backup_count == 10

    def test_log_level_validation(self) -> None:
        """Test log_level field validation."""
        # Valid log levels
        valid_levels = ["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"]
        for level in valid_levels:
            settings = GeneralSettings.model_validate({"log_level": level})
            assert settings.log_level == level

        # Invalid log level
        with pytest.raises(ValidationError) as exc_info:
            GeneralSettings.model_validate({"log_level": "INVALID"})
        assert "log_level" in str(exc_info.value)

    def test_module_log_levels_validation(self) -> None:
        """Test module_log_levels field validation."""
        # Valid module log levels
        valid_data = {
            "module_log_levels": {
                "module1": "DEBUG",
                "module.with.dots": "INFO",
                "another_module": "WARNING",
            },
        }
        settings = GeneralSettings.model_validate(valid_data)
        assert settings.module_log_levels == valid_data["module_log_levels"]

        # Invalid log level in module_log_levels
        with pytest.raises(ValidationError) as exc_info:
            GeneralSettings.model_validate({"module_log_levels": {"module1": "INVALID"}})
        assert "module_log_levels" in str(exc_info.value)

        # Empty module name
        with pytest.raises(ValidationError) as exc_info:
            GeneralSettings.model_validate({"module_log_levels": {"": "DEBUG"}})
        assert "module_log_levels" in str(exc_info.value)

    def test_positive_integer_constraints(self) -> None:
        """Test positive integer field constraints."""
        # Valid positive values
        settings = GeneralSettings.model_validate(
            {
                "state_save_interval": 1,
                "state_backup_count": 1,
            },
        )
        assert settings.state_save_interval == 1
        assert settings.state_backup_count == 1

        # Invalid zero values
        with pytest.raises(ValidationError) as exc_info:
            GeneralSettings.model_validate({"state_save_interval": 0})
        assert "state_save_interval" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            GeneralSettings.model_validate({"state_backup_count": 0})
        assert "state_backup_count" in str(exc_info.value)

        # Invalid negative values
        with pytest.raises(ValidationError) as exc_info:
            GeneralSettings.model_validate({"state_save_interval": -1})
        assert "state_save_interval" in str(exc_info.value)

    def test_string_field_validation(self) -> None:
        """Test string field validation."""
        # Empty strings should fail for required string fields
        with pytest.raises(ValidationError) as exc_info:
            GeneralSettings.model_validate({"state_file": ""})
        assert "state_file" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            GeneralSettings.model_validate({"state_backup_directory": ""})
        assert "state_backup_directory" in str(exc_info.value)


class TestExchangeSpecificConfig:
    """Test cases for ExchangeSpecificConfig model."""

    def create_valid_exchange_data(self) -> dict[str, Any]:
        """Create valid exchange configuration data."""
        data: dict[str, Any] = {
            "enabled": True,
            "api_base_url_mainnet": "https://api.exchange.com",
            "ws_url_mainnet": "wss://ws.exchange.com",
            "exchange_name": "hyperliquid",
            "rate_limit_per_minute": 60,
            "chain_id": 1337,
            "ip_weight_limit_per_minute": 1200,
            "info_request_type_ip_weights": {"meta": 2, "orderStatus": 1},
            "default_info_weight": 2,
            "exchange_action_base_ip_weight": 10,
            "address_action_safety_net": {"rate_per_minute": 60},
            "symbols": {
                "BTC": "BTC-USD",
                "ETH": "ETH-USD",
            },
        }
        return data

    def test_valid_exchange_config(self) -> None:
        """Test valid ExchangeSpecificConfig."""
        data = self.create_valid_exchange_data()

        config = ExchangeSpecificConfig.model_validate(data)

        assert config.enabled is True
        assert str(config.api_base_url_mainnet) == "https://api.exchange.com/"
        assert str(config.ws_url_mainnet) == "wss://ws.exchange.com/"
        assert config.rate_limit_per_minute == 60
        assert config.symbols == {"BTC": "BTC-USD", "ETH": "ETH-USD"}

    def test_exchange_config_defaults(self) -> None:
        """Test ExchangeSpecificConfig with default values."""
        data = {
            "api_base_url_mainnet": "https://api.exchange.com",
            "ws_url_mainnet": "wss://ws.exchange.com",
            "exchange_name": "backpack",
            "rate_limit_per_minute": 60,
            "symbols": {"BTC": "BTC-USD"},
        }

        config = ExchangeSpecificConfig.model_validate(data)

        assert config.enabled is True  # Default value

    def test_url_validation(self) -> None:
        """Test URL field validation."""
        base_data = self.create_valid_exchange_data()

        # Invalid HTTP URL
        base_data["api_base_url"] = "not-a-url"
        with pytest.raises(ValidationError) as exc_info:
            ExchangeSpecificConfig.model_validate(base_data)
        assert "api_base_url" in str(exc_info.value)

        # Invalid WebSocket URL
        base_data = self.create_valid_exchange_data()
        base_data["ws_url"] = "invalid-ws-url"
        with pytest.raises(ValidationError) as exc_info:
            ExchangeSpecificConfig.model_validate(base_data)
        assert "ws_url" in str(exc_info.value)

    def test_rate_limit_validation(self) -> None:
        """Test rate_limit_per_minute validation."""
        base_data = self.create_valid_exchange_data()

        # Zero rate limit should fail
        base_data["rate_limit_per_minute"] = 0
        with pytest.raises(ValidationError) as exc_info:
            ExchangeSpecificConfig.model_validate(base_data)
        assert "rate_limit_per_minute" in str(exc_info.value)

        # Negative rate limit should fail
        base_data["rate_limit_per_minute"] = -1
        with pytest.raises(ValidationError) as exc_info:
            ExchangeSpecificConfig.model_validate(base_data)
        assert "rate_limit_per_minute" in str(exc_info.value)

    def test_symbols_validation(self) -> None:
        """Test symbols field validation."""
        base_data = self.create_valid_exchange_data()

        # Empty symbol key
        base_data["symbols"] = {"": "BTC-USD"}
        with pytest.raises(ValidationError) as exc_info:
            ExchangeSpecificConfig.model_validate(base_data)
        assert "symbols" in str(exc_info.value)

        # Empty symbol value
        base_data["symbols"] = {"BTC": ""}
        with pytest.raises(ValidationError) as exc_info:
            ExchangeSpecificConfig.model_validate(base_data)
        assert "symbols" in str(exc_info.value)

        # Non-dict symbols
        base_data["symbols"] = ["BTC", "ETH"]
        with pytest.raises(ValidationError) as exc_info:
            ExchangeSpecificConfig.model_validate(base_data)
        assert "symbols" in str(exc_info.value)


class TestStrategyParamsHLPerpBPSpot:
    """Test cases for StrategyParamsHLPerpBPSpot model."""

    def test_valid_strategy_params(self) -> None:
        """Test valid StrategyParamsHLPerpBPSpot."""
        data = {
            "funding_threshold": "0.01",
            "max_price_spread_pct": "0.05",
            "min_profit_usd": "10.0",
        }

        params = StrategyParamsHLPerpBPSpot.model_validate(data)

        assert params.funding_threshold == Decimal("0.01")
        assert params.max_price_spread_pct == Decimal("0.05")
        assert params.min_profit_usd == Decimal("10.0")

    def test_decimal_conversion(self) -> None:
        """Test Decimal conversion from various input types."""
        # From string
        params = StrategyParamsHLPerpBPSpot.model_validate(
            {
                "funding_threshold": "0.01",
                "max_price_spread_pct": "0.05",
                "min_profit_usd": "10.0",
            },
        )
        assert isinstance(params.funding_threshold, Decimal)

        # From int
        params = StrategyParamsHLPerpBPSpot.model_validate(
            {
                "funding_threshold": 1,
                "max_price_spread_pct": 0.5,
                "min_profit_usd": 10,
            },
        )
        assert params.funding_threshold == Decimal("1")

        # From float
        params = StrategyParamsHLPerpBPSpot.model_validate(
            {
                "funding_threshold": 0.01,
                "max_price_spread_pct": 0.05,
                "min_profit_usd": 10.0,
            },
        )
        assert params.funding_threshold == Decimal("0.01")

    def test_positive_constraints(self) -> None:
        """Test positive value constraints."""
        base_data = {
            "funding_threshold": "0.01",
            "max_price_spread_pct": "0.05",
            "min_profit_usd": "10.0",
        }

        # Zero values should fail
        for field in base_data:
            invalid_data = base_data.copy()
            invalid_data[field] = "0"
            with pytest.raises(ValidationError) as exc_info:
                StrategyParamsHLPerpBPSpot.model_validate(invalid_data)
            assert field in str(exc_info.value)

        # Negative values should fail
        for field in base_data:
            invalid_data = base_data.copy()
            invalid_data[field] = "-1"
            with pytest.raises(ValidationError) as exc_info:
                StrategyParamsHLPerpBPSpot.model_validate(invalid_data)
            assert field in str(exc_info.value)

    def test_percentage_constraint(self) -> None:
        """Test max_price_spread_pct percentage constraint."""
        # Valid percentage (less than 1)
        params = StrategyParamsHLPerpBPSpot.model_validate(
            {
                "funding_threshold": "0.01",
                "max_price_spread_pct": "0.99",
                "min_profit_usd": "10.0",
            },
        )
        assert params.max_price_spread_pct == Decimal("0.99")

        # Invalid percentage (equal to 1)
        with pytest.raises(ValidationError) as exc_info:
            StrategyParamsHLPerpBPSpot.model_validate(
                {
                    "funding_threshold": "0.01",
                    "max_price_spread_pct": "1.0",
                    "min_profit_usd": "10.0",
                },
            )
        assert "max_price_spread_pct" in str(exc_info.value)

        # Invalid percentage (greater than 1)
        with pytest.raises(ValidationError) as exc_info:
            StrategyParamsHLPerpBPSpot.model_validate(
                {
                    "funding_threshold": "0.01",
                    "max_price_spread_pct": "1.5",
                    "min_profit_usd": "10.0",
                },
            )
        assert "max_price_spread_pct" in str(exc_info.value)


class TestRiskSettings:
    """Test cases for RiskSettings model."""

    def create_valid_risk_data(self) -> dict[str, Any]:
        """Create valid risk settings data."""
        data: dict[str, Any] = {
            "global": {
                "max_position_usd": "1000.0",
                "max_total_exposure_usd": "5000.0",
            },
            "use_simple_sizing_path": True,
            "simple_sizing_method": "fixed_fraction",
            "simple_fixed_fraction": "0.1",
            "simple_fixed_usd_size": "10.0",
        }
        return data

    def test_valid_risk_settings(self) -> None:
        """Test valid RiskSettings."""
        data = self.create_valid_risk_data()

        settings = RiskSettings.model_validate(data)

        assert isinstance(settings.global_risk, GlobalRiskSettings)
        assert settings.global_risk.max_position_usd == Decimal("1000.0")
        assert settings.global_risk.max_total_exposure_usd == Decimal("5000.0")
        assert settings.use_simple_sizing_path is True
        assert settings.simple_sizing_method == "fixed_fraction"
        assert settings.simple_fixed_fraction == Decimal("0.1")
        assert settings.simple_fixed_usd_size == Decimal("10.0")

    def test_risk_settings_defaults(self) -> None:
        """Test RiskSettings with default values."""
        data = {
            "global": {
                "max_position_usd": "1000.0",
                "max_total_exposure_usd": "5000.0",
            },
        }

        settings = RiskSettings.model_validate(data)

        assert settings.use_simple_sizing_path is True
        assert settings.simple_sizing_method == "fixed_fraction"
        assert settings.simple_fixed_fraction == Decimal("0.1")
        assert settings.simple_fixed_usd_size == Decimal("10.0")

    def test_alias_field(self) -> None:
        """Test that 'global' alias works for global_risk field."""
        # Using alias 'global'
        data = {
            "global": {
                "max_position_usd": "1000.0",
                "max_total_exposure_usd": "5000.0",
            },
        }
        settings = RiskSettings.model_validate(data)
        assert settings.global_risk.max_position_usd == Decimal("1000.0")

        # Using field name 'global_risk' should also work
        data = {
            "global_risk": {
                "max_position_usd": "1000.0",
                "max_total_exposure_usd": "5000.0",
            },
        }
        settings = RiskSettings.model_validate(data)
        assert settings.global_risk.max_position_usd == Decimal("1000.0")

    def test_sizing_method_validation(self) -> None:
        """Test simple_sizing_method validation."""
        base_data = self.create_valid_risk_data()

        # Valid methods
        for method in ["fixed_usd", "fixed_fraction"]:
            data = base_data.copy()
            data["simple_sizing_method"] = method
            settings = RiskSettings.model_validate(data)
            assert settings.simple_sizing_method == method

        # Invalid method
        base_data["simple_sizing_method"] = "invalid_method"
        with pytest.raises(ValidationError) as exc_info:
            RiskSettings.model_validate(base_data)
        assert "simple_sizing_method" in str(exc_info.value)

    def test_fraction_constraints(self) -> None:
        """Test simple_fixed_fraction constraints."""
        base_data = self.create_valid_risk_data()

        # Valid fraction (between 0 and 1)
        base_data["simple_fixed_fraction"] = "0.5"
        settings = RiskSettings.model_validate(base_data)
        assert settings.simple_fixed_fraction == Decimal("0.5")

        # Invalid fraction (equal to 0)
        base_data["simple_fixed_fraction"] = "0"
        with pytest.raises(ValidationError) as exc_info:
            RiskSettings.model_validate(base_data)
        assert "simple_fixed_fraction" in str(exc_info.value)

        # Invalid fraction (equal to 1)
        base_data["simple_fixed_fraction"] = "1"
        with pytest.raises(ValidationError) as exc_info:
            RiskSettings.model_validate(base_data)
        assert "simple_fixed_fraction" in str(exc_info.value)

        # Invalid fraction (greater than 1)
        base_data["simple_fixed_fraction"] = "1.5"
        with pytest.raises(ValidationError) as exc_info:
            RiskSettings.model_validate(base_data)
        assert "simple_fixed_fraction" in str(exc_info.value)


class TestBalanceMonitoringSettings:
    """Test cases for BalanceMonitoringSettings model."""

    def test_valid_balance_monitoring(self) -> None:
        """Test valid BalanceMonitoringSettings."""
        data = {
            "enabled": True,
            "check_interval_sec": 300,
            "min_balance_thresholds_usd": {
                "backpack": "100.0",
                "hyperliquid": "200.0",
            },
        }

        settings = BalanceMonitoringSettings.model_validate(data)

        assert settings.enabled is True
        assert settings.check_interval_sec == 300
        assert settings.min_balance_thresholds_usd["backpack"] == Decimal("100.0")
        assert settings.min_balance_thresholds_usd["hyperliquid"] == Decimal("200.0")

    def test_balance_monitoring_defaults(self) -> None:
        """Test BalanceMonitoringSettings with default values."""
        data = {
            "min_balance_thresholds_usd": {
                "backpack": "100.0",
            },
        }

        settings = BalanceMonitoringSettings.model_validate(data)

        assert settings.enabled is True
        assert settings.check_interval_sec == 300

    def test_threshold_validation(self) -> None:
        """Test min_balance_thresholds_usd validation."""
        # Valid thresholds
        data = {
            "min_balance_thresholds_usd": {
                "exchange1": "100.0",
                "exchange2": "200.5",
            },
        }
        settings = BalanceMonitoringSettings.model_validate(data)
        assert settings.min_balance_thresholds_usd["exchange1"] == Decimal("100.0")

        # Empty exchange name
        with pytest.raises(ValidationError) as exc_info:
            BalanceMonitoringSettings.model_validate(
                {
                    "min_balance_thresholds_usd": {"": "100.0"},
                },
            )
        assert "min_balance_thresholds_usd" in str(exc_info.value)

        # Zero threshold (should fail)
        with pytest.raises(ValidationError) as exc_info:
            BalanceMonitoringSettings.model_validate(
                {
                    "min_balance_thresholds_usd": {"exchange": "0"},
                },
            )
        assert "min_balance_thresholds_usd" in str(exc_info.value)

        # Negative threshold (should fail)
        with pytest.raises(ValidationError) as exc_info:
            BalanceMonitoringSettings.model_validate(
                {
                    "min_balance_thresholds_usd": {"exchange": "-100"},
                },
            )
        assert "min_balance_thresholds_usd" in str(exc_info.value)


class TestMonitoringSettings:
    """Test cases for MonitoringSettings model."""

    def test_valid_monitoring_settings(self) -> None:
        """Test valid MonitoringSettings."""
        data = {
            "notifications_enabled": True,
            "alert_methods": ["log", "telegram"],
        }

        settings = MonitoringSettings.model_validate(data)

        assert settings.notifications_enabled is True
        assert settings.alert_methods == ["log", "telegram"]

    def test_monitoring_defaults(self) -> None:
        """Test MonitoringSettings with default values."""
        data: dict[str, Any] = {}

        settings = MonitoringSettings.model_validate(data)

        assert settings.notifications_enabled is True
        assert settings.alert_methods == ["log"]

    def test_alert_methods_validation(self) -> None:
        """Test alert_methods validation."""
        # Valid methods
        for methods in [["log"], ["telegram"], ["log", "telegram"]]:
            settings = MonitoringSettings.model_validate({"alert_methods": methods})
            assert settings.alert_methods == methods

        # Invalid method
        with pytest.raises(ValidationError) as exc_info:
            MonitoringSettings.model_validate({"alert_methods": ["invalid"]})
        assert "alert_methods" in str(exc_info.value)

        # Non-list alert_methods
        with pytest.raises(ValidationError) as exc_info:
            MonitoringSettings.model_validate({"alert_methods": "log"})
        assert "alert_methods" in str(exc_info.value)


class TestAppSettings:
    """Test cases for AppSettings model."""

    def create_valid_app_data(self) -> dict[str, Any]:
        """Create valid AppSettings data."""
        data: dict[str, Any] = {
            "general": {
                "log_level": "INFO",
            },
            "exchanges": {
                "backpack": {
                    "enabled": True,
                    "api_base_url_mainnet": "https://api.backpack.exchange",
                    "ws_url_mainnet": "wss://ws.backpack.exchange",
                    "exchange_name": "backpack",
                    "rate_limit_per_minute": 60,
                    "symbols": {"BTC": "BTC_USDC"},
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
                    "symbols": {"BTC": "BTC-USD"},
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
                "compensation": {},
            },
            "safety_systems": {
                "circuit_breakers": {},
                "position_reconciliation": {},
                "balance_monitoring": {
                    "min_balance_thresholds_usd": {
                        "backpack": "100.0",
                        "hyperliquid": "200.0",
                    },
                },
            },
            "monitoring": {},
            "portfolio_tracker": {
                "data_freshness_seconds": 30,
                "initial_positions": [],
            },
        }
        return data

    def test_valid_app_settings(self) -> None:
        """Test valid AppSettings."""
        data = self.create_valid_app_data()

        settings = AppSettings.model_validate(data)

        assert isinstance(settings.general, GeneralSettings)
        assert "backpack" in settings.exchanges
        assert "hyperliquid" in settings.exchanges
        assert isinstance(settings.strategies, StrategiesSettings)
        assert isinstance(settings.risk, RiskSettings)
        assert isinstance(settings.execution, ExecutionSettings)
        assert isinstance(settings.safety_systems, SafetySystemsSettings)
        assert isinstance(settings.monitoring, MonitoringSettings)

    def test_exchanges_validation(self) -> None:
        """Test exchanges field validation."""
        data = self.create_valid_app_data()

        # Empty exchange name
        data["exchanges"][""] = data["exchanges"]["backpack"]
        with pytest.raises(ValidationError) as exc_info:
            AppSettings.model_validate(data)
        assert "exchanges" in str(exc_info.value)

        # Non-dict exchanges
        data = self.create_valid_app_data()
        data["exchanges"] = ["backpack", "hyperliquid"]
        with pytest.raises(ValidationError) as exc_info:
            AppSettings.model_validate(data)
        assert "exchanges" in str(exc_info.value)

    def test_cross_reference_validation(self) -> None:
        """Test cross-reference validation between sections."""
        data = self.create_valid_app_data()

        # Invalid long_exchange reference
        data["strategies"]["hl_perp_bp_spot"]["long_exchange"] = "nonexistent"
        with pytest.raises(ValidationError) as exc_info:
            AppSettings.model_validate(data)
        assert "long_exchange" in str(exc_info.value)

        # Invalid short_exchange reference
        data = self.create_valid_app_data()
        data["strategies"]["hl_perp_bp_spot"]["short_exchange"] = "nonexistent"
        with pytest.raises(ValidationError) as exc_info:
            AppSettings.model_validate(data)
        assert "short_exchange" in str(exc_info.value)

        # Invalid balance monitoring exchange reference
        data = self.create_valid_app_data()
        data["safety_systems"]["balance_monitoring"]["min_balance_thresholds_usd"][
            "nonexistent"
        ] = "100.0"
        with pytest.raises(ValidationError) as exc_info:
            AppSettings.model_validate(data)
        assert "unknown exchanges" in str(exc_info.value)

    def test_missing_required_sections(self) -> None:
        """Test missing required sections."""
        data = self.create_valid_app_data()

        # Test each required section
        required_sections = [
            "general",
            "exchanges",
            "strategies",
            "risk",
            "execution",
            "safety_systems",
            "monitoring",
        ]

        for section in required_sections:
            incomplete_data = data.copy()
            del incomplete_data[section]

            with pytest.raises(ValidationError) as exc_info:
                AppSettings.model_validate(incomplete_data)
            assert section in str(exc_info.value)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        data = self.create_valid_app_data()
        data["extra_field"] = "not_allowed"

        with pytest.raises(ValidationError) as exc_info:
            AppSettings.model_validate(data)
        assert "extra_field" in str(exc_info.value)

    def test_validate_assignment(self) -> None:
        """Test that validate_assignment is enabled."""
        data = self.create_valid_app_data()
        settings = AppSettings.model_validate(data)

        # Should validate on assignment
        with pytest.raises(ValidationError):
            settings.general = "invalid"  # type: ignore


class TestCircuitBreakerSettings:
    """Test cases for CircuitBreakerSettings model."""

    def test_valid_circuit_breaker_settings(self) -> None:
        """Test valid CircuitBreakerSettings."""
        data = {
            "enabled": True,
            "global_consecutive_failures": 5,
            "global_reset_timeout_sec": 300,
            "exchange_consecutive_failures": 3,
            "exchange_reset_timeout_sec": 180,
        }

        settings = CircuitBreakerSettings.model_validate(data)

        assert settings.enabled is True
        assert settings.global_consecutive_failures == 5
        assert settings.global_reset_timeout_sec == 300
        assert settings.exchange_consecutive_failures == 3
        assert settings.exchange_reset_timeout_sec == 180

    def test_circuit_breaker_defaults(self) -> None:
        """Test CircuitBreakerSettings with default values."""
        data: dict[str, Any] = {}

        settings = CircuitBreakerSettings.model_validate(data)

        assert settings.enabled is True
        assert settings.global_consecutive_failures == 5
        assert settings.global_reset_timeout_sec == 300
        assert settings.exchange_consecutive_failures == 3
        assert settings.exchange_reset_timeout_sec == 180

    def test_positive_constraints(self) -> None:
        """Test positive integer constraints."""
        # Zero values should fail
        with pytest.raises(ValidationError) as exc_info:
            CircuitBreakerSettings.model_validate({"global_consecutive_failures": 0})
        assert "global_consecutive_failures" in str(exc_info.value)

        # Negative values should fail
        with pytest.raises(ValidationError) as exc_info:
            CircuitBreakerSettings.model_validate({"exchange_reset_timeout_sec": -1})
        assert "exchange_reset_timeout_sec" in str(exc_info.value)


class TestExecutionSettings:
    """Test cases for ExecutionSettings model."""

    def test_valid_execution_settings(self) -> None:
        """Test valid ExecutionSettings."""
        data = {
            "max_slippage_pct": "0.01",
            "max_retries": 3,
            "retry_delay_base_sec": "1.0",
            "settlement_delay": "2.0",
            "compensation": {
                "use_limit_orders": True,
                "limit_price_offset_pct": "0.05",
            },
        }

        settings = ExecutionSettings.model_validate(data)

        assert settings.max_slippage_pct == Decimal("0.01")
        assert settings.max_retries == 3
        assert settings.retry_delay_base_sec == Decimal("1.0")
        assert settings.settlement_delay == Decimal("2.0")
        assert isinstance(settings.compensation, ExecutionCompensationSettings)

    def test_execution_defaults(self) -> None:
        """Test ExecutionSettings with default values."""
        data: dict[str, Any] = {
            "max_slippage_pct": "0.01",
            "compensation": {},
        }

        settings = ExecutionSettings.model_validate(data)

        assert settings.max_retries == 3
        assert settings.retry_delay_base_sec == Decimal("1.0")
        assert settings.settlement_delay == Decimal("2.0")

    def test_slippage_constraint(self) -> None:
        """Test max_slippage_pct constraint."""
        # Valid slippage (between 0 and 1)
        settings = ExecutionSettings.model_validate(
            {
                "max_slippage_pct": "0.5",
                "compensation": {},
            },
        )
        assert settings.max_slippage_pct == Decimal("0.5")

        # Invalid slippage (equal to 1)
        with pytest.raises(ValidationError) as exc_info:
            ExecutionSettings.model_validate(
                {
                    "max_slippage_pct": "1.0",
                    "compensation": {},
                },
            )
        assert "max_slippage_pct" in str(exc_info.value)

        # Invalid slippage (greater than 1)
        with pytest.raises(ValidationError) as exc_info:
            ExecutionSettings.model_validate(
                {
                    "max_slippage_pct": "1.5",
                    "compensation": {},
                },
            )
        assert "max_slippage_pct" in str(exc_info.value)


class TestExecutionCompensationSettings:
    """Test cases for ExecutionCompensationSettings model."""

    def test_valid_compensation_settings(self) -> None:
        """Test valid ExecutionCompensationSettings."""
        data = {
            "use_limit_orders": True,
            "limit_price_offset_pct": "0.05",
        }

        settings = ExecutionCompensationSettings.model_validate(data)

        assert settings.use_limit_orders is True
        assert settings.limit_price_offset_pct == Decimal("0.05")

    def test_compensation_defaults(self) -> None:
        """Test ExecutionCompensationSettings with default values."""
        data: dict[str, Any] = {}

        settings = ExecutionCompensationSettings.model_validate(data)

        assert settings.use_limit_orders is True
        assert settings.limit_price_offset_pct == Decimal("0.05")

    def test_offset_constraint(self) -> None:
        """Test limit_price_offset_pct constraint."""
        # Valid offset (>= 0)
        settings = ExecutionCompensationSettings.model_validate(
            {
                "limit_price_offset_pct": "0.0",
            },
        )
        assert settings.limit_price_offset_pct == Decimal("0.0")

        # Invalid offset (< 0)
        with pytest.raises(ValidationError) as exc_info:
            ExecutionCompensationSettings.model_validate(
                {
                    "limit_price_offset_pct": "-0.01",
                },
            )
        assert "limit_price_offset_pct" in str(exc_info.value)
