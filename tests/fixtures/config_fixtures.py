"""Configuration fixtures for testing.

This module provides pytest fixtures for configuration objects, settings,
and environment-aware test configuration loading.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import AnyUrl, HttpUrl

from cyberdelta.config.config_manager import ConfigManager, ConfigurationError
from cyberdelta.config.config_models import (
    AddressActionSafetyNetConfig,
    AppSettings,
    BalanceMonitoringSettings,
    CircuitBreakerSettings,
    ExchangeSpecificConfig,
    ExecutionCompensationSettings,
    ExecutionSettings,
    GeneralSettings,
    GlobalRiskSettings,
    MonitoringSettings,
    PortfolioTrackerConfig,
    PositionReconciliationSettings,
    RiskSettings,
    SafetySystemsSettings,
    StrategiesSettings,
    StrategyConfigHLPerpBPSpot,
    StrategyParamsHLPerpBPSpot,
)
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets, PrivateKeyAuthSecrets, SecretsConfig
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

# Import path setup to ensure cyberdelta can be imported

# --- Basic Configuration Fixtures ---


@pytest.fixture
def hyperliquid_config() -> dict[str, Any]:
    """Fixture to provide Hyperliquid API configuration."""
    return {
        "rest_endpoint": "https://api.hyperliquid.xyz",
        "ws_endpoint": "wss://api.hyperliquid.xyz/ws",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket": 50,
            "endpoints": {"POST:/user": {"rate": 5.0, "bucket": 20}},
        },
    }


@pytest.fixture
def backpack_config() -> dict[str, Any]:
    """Fixture to provide Backpack API configuration."""
    return {
        "rest_endpoint": "https://api.backpack.exchange",
        "ws_endpoint": "wss://ws.backpack.exchange",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket": 50,
            "endpoints": {"GET:/api/v1/depth": {"rate": 5.0, "bucket": 20}},
        },
    }


@pytest.fixture
def hyperliquid_secrets() -> dict[str, str]:
    """Fixture to provide Hyperliquid API secrets."""
    return {
        "HYPERLIQUID_WALLET_PRIVATE_KEY": (
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        ),
        "HYPERLIQUID_WALLET_ADDRESS": "0xabcdef1234567890abcdef1234567890abcdef12",
    }


@pytest.fixture
def backpack_secrets() -> dict[str, str]:
    """Fixture to provide Backpack API secrets."""
    return {
        "BACKPACK_API_KEY": "backpack-api-key-123456",
        "BACKPACK_API_SECRET": "backpack-api-secret-123456",
    }


# --- Mock Configuration Objects ---


@pytest.fixture
def mock_config() -> AppSettings:
    """Create a mock AppSettings object with test settings."""
    # Create a test AppSettings instance
    return AppSettings(
        general=GeneralSettings(
            log_level="INFO",
            log_file="logs/test.log",
            module_log_levels={},
            safe_mode=True,
            state_file="data/test_state.json",
            state_backup_directory="data/test_backups",
            state_save_interval=300,
            state_backup_count=5,
        ),
        exchanges={
            "hyperliquid": ExchangeSpecificConfig(
                exchange_name=ExchangeName.HYPERLIQUID,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.hyperliquid.xyz"),
                ws_url_mainnet=AnyUrl("wss://api.hyperliquid.xyz/ws"),
                api_base_url_testnet=HttpUrl("https://api.hyperliquid-testnet.xyz"),
                ws_url_testnet=AnyUrl("wss://api.hyperliquid-testnet.xyz/ws"),
                is_mainnet_environment=False,  # Default to testnet for testing
                chain_id=1337,
                rate_limit_per_minute=120,
                symbols={"BTC": "BTC", "ETH": "ETH"},
                # Hyperliquid-specific required fields
                ip_weight_limit_per_minute=1200,
                info_request_type_ip_weights={
                    "meta": 1,
                    "allMids": 2,
                    "openOrders": 1,
                    "userState": 1,
                },
                default_info_weight=1,
                exchange_action_base_ip_weight=1,
                address_action_safety_net=AddressActionSafetyNetConfig(rate_per_minute=600),
            ),
            "backpack": ExchangeSpecificConfig(
                exchange_name=ExchangeName.BACKPACK,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.backpack.exchange"),
                ws_url_mainnet=AnyUrl("wss://ws.backpack.exchange"),
                is_mainnet_environment=True,  # Backpack only has mainnet
                rate_limit_per_minute=120,
                symbols={"BTC": "BTC_USDC", "ETH": "ETH_USDC"},
            ),
        },
        strategies=StrategiesSettings(
            hl_perp_bp_spot=StrategyConfigHLPerpBPSpot(
                enabled=True,
                long_exchange="backpack",
                short_exchange="hyperliquid",
                symbol_long="BTC",
                symbol_short="BTC",
                params=StrategyParamsHLPerpBPSpot(
                    funding_threshold=Decimal("0.0001"),
                    max_price_spread_pct=Decimal("0.002"),
                    min_profit_usd=Decimal("1.0"),
                ),
            ),
        ),
        risk=RiskSettings(
            **{
                "global": GlobalRiskSettings(
                    max_position_usd=Decimal("200.0"),
                    max_total_exposure_usd=Decimal("1000.0"),
                ),
            },
            use_simple_sizing_path=True,
            simple_sizing_method="fixed_fraction",
            simple_fixed_fraction=Decimal("0.1"),
            simple_fixed_usd_size=Decimal("10.0"),
        ),
        execution=ExecutionSettings(
            max_slippage_pct=Decimal("0.001"),
            max_retries=3,
            retry_delay_base_sec=Decimal("1.0"),
            settlement_delay=Decimal("2.0"),
            compensation=ExecutionCompensationSettings(
                use_limit_orders=True,
                limit_price_offset_pct=Decimal("0.05"),
            ),
        ),
        safety_systems=SafetySystemsSettings(
            circuit_breakers=CircuitBreakerSettings(
                enabled=True,
                global_consecutive_failures=5,
                global_reset_timeout_sec=300,
                exchange_consecutive_failures=3,
                exchange_reset_timeout_sec=180,
            ),
            position_reconciliation=PositionReconciliationSettings(
                enabled=True,
                check_interval_sec=600,
                max_discrepancy_pct=Decimal("0.01"),
            ),
            balance_monitoring=BalanceMonitoringSettings(
                enabled=True,
                check_interval_sec=300,
                min_balance_thresholds_usd={
                    "hyperliquid": Decimal("100.0"),
                    "backpack": Decimal("100.0"),
                },
            ),
        ),
        monitoring=MonitoringSettings(
            notifications_enabled=True,
            alert_methods=["log"],
        ),
        portfolio_tracker=PortfolioTrackerConfig(
            data_freshness_seconds=60,
            initial_balances={},
            initial_positions=[],
        ),
    )


def mock_get_config() -> dict[str, Any]:
    """Fixture to provide a mock configuration dictionary."""
    return {
        "exchanges": {
            "hyperliquid": {
                "enabled": True,
                "symbols": {"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
                "websocket": {
                    "reconnect_delay": 1,
                    "max_reconnect_delay": 5,
                    "ping_interval": 10,
                },
                "risk_modifier": 0.9,
            },
            "backpack": {
                "enabled": True,
                "symbols": {"BTC": "BTCUSDC", "ETH": "ETHUSDC"},
                "websocket": {
                    "reconnect_delay": 1,
                    "max_reconnect_delay": 5,
                    "ping_interval": 10,
                },
                "risk_modifier": 1.0,
            },
        },
        "portfolio": {
            "reconciliation_interval": 300,  # 5 minutes
        },
        "risk": {
            "max_position_size": 1000.0,
            "max_total_exposure": 5000.0,
            "kelly_fraction": 0.5,
            "max_collateral_per_exchange": 0.8,
            "max_leverage": 5.0,
            "min_liquidation_buffer": 0.2,
        },
        "execution": {
            "max_slippage": 0.002,
            "max_retries": 3,
            "retry_delay_base": 1.0,
            "circuit_breaker": {"loss_threshold": 100.0, "failed_trades": 3},
        },
        "validation": {
            "circuit_breaker": {
                "enabled": True,
                "global": {
                    "api_errors": {
                        "enabled": True,
                        "threshold": 5,
                        "window_seconds": 120,
                        "cooldown_seconds": 600,
                    },
                },
                "exchanges": {
                    "hyperliquid": {
                        "enabled": True,
                        "api_errors": {
                            "enabled": True,
                            "type": "api_error",
                            "threshold": 3,
                            "window_seconds": 60,
                            "cooldown_seconds": 300,
                        },
                        "drawdown": {"enabled": False},
                        "volatility": {"enabled": False},
                        "liquidity": {"enabled": False},
                    },
                    "backpack": {
                        "enabled": True,
                        "api_errors": {
                            "enabled": True,
                            "type": "api_error",
                            "threshold": 3,
                            "window_seconds": 60,
                            "cooldown_seconds": 300,
                        },
                        "drawdown": {"enabled": False},
                        "volatility": {"enabled": False},
                        "liquidity": {"enabled": False},
                    },
                },
            },
            "position_reconciliation": {
                "enabled": True,
                "check_interval": 300,
                "reconciliation_threshold": 0.01,
                "auto_correct": False,
            },
        },
        "data": {"staleness_thresholds": {"ticker": 60, "funding_rate": 300, "orderbook": 60}},
    }


# --- Environment-Aware Test Configuration Fixtures ---


@pytest.fixture(scope="session")
def test_config_file_path() -> Path:
    """Path to the test configuration file."""
    # Assumes test_config.yaml is in tests/config/ relative to project root
    return Path(__file__).parent.parent / "config" / "test_config.yaml"


@pytest.fixture(scope="session")
def test_secrets_file_path() -> Path:
    """Path to the test secrets file."""
    return Path(__file__).parent.parent / "config" / "test_secrets.yaml"


@pytest.fixture(scope="session")
def test_app_settings(test_config_file_path: Path) -> AppSettings:
    """Load test-specific AppSettings from test_config.yaml."""
    if not test_config_file_path.exists():
        pytest.skip(
            f"Test config file not found at {test_config_file_path}, skipping tests that need it.",
        )
    try:
        manager = ConfigManager(str(test_config_file_path))
        if manager.settings is None:  # Should be caught by ConfigManager raising ConfigurationError
            raise ConfigurationError("ConfigManager loaded but settings are None.")
        return manager.settings
    except ConfigurationError as e:
        pytest.fail(f"Failed to load test AppSettings from {test_config_file_path}: {e}")
    # Add a default return to satisfy linters, though pytest.fail should exit
    # This path should ideally not be reached if pytest.fail works as expected.
    raise RuntimeError("test_app_settings fixture failed unexpectedly.")


@pytest.fixture(scope="session")
def test_secrets_config(test_secrets_file_path: Path) -> SecretsConfig:
    """Load test-specific SecretsConfig from test_secrets.yaml."""
    if not test_secrets_file_path.exists():
        pytest.skip(
            f"Test secrets file not found at {test_secrets_file_path}, "
            "skipping tests that need it.",
        )
    try:
        manager = SecretsManager(str(test_secrets_file_path))
        if (
            manager.secrets_data is None
        ):  # Should be caught by SecretsManager raising ConfigurationError
            raise ConfigurationError("SecretsManager loaded but secrets_data is None.")
        return manager.secrets_data
    except ConfigurationError as e:
        pytest.fail(f"Failed to load test SecretsConfig from {test_secrets_file_path}: {e}")
    # Add a default return to satisfy linters
    raise RuntimeError("test_secrets_config fixture failed unexpectedly.")


@pytest.fixture(scope="session")
def hl_test_environment() -> str:
    """Fixture to determine Hyperliquid test environment.

    Defaults to 'testnet' but can be overridden with CYBERDELTA_TEST_ENV_HL environment variable.
    """
    return os.environ.get("CYBERDELTA_TEST_ENV_HL", "testnet")


@pytest.fixture(scope="session")
def hl_test_environment_from_config(test_app_settings: AppSettings) -> str:
    """Get the default Hyperliquid test environment from test_config.yaml.

    Can be overridden with CYBERDELTA_TEST_ENV_HL environment variable.
    """
    hl_config = test_app_settings.exchanges.get("hyperliquid")
    is_mainnet_from_config = False  # Default to testnet
    if hl_config and hasattr(hl_config, "is_mainnet_environment"):
        is_mainnet_from_config = hl_config.is_mainnet_environment

    # Allow override via environment variable
    env_override = os.environ.get("CYBERDELTA_TEST_ENV_HL")
    if env_override:
        return env_override.lower()
    return "mainnet" if is_mainnet_from_config else "testnet"


@pytest.fixture(scope="session")
def active_hl_config(
    test_app_settings: AppSettings,
    hl_test_environment_from_config: str,
) -> ExchangeSpecificConfig:
    """Provide ExchangeSpecificConfig for Hyperliquid from test configuration.

    Uses test_config.yaml settings with environment override support.
    Matches Backpack pattern for consistency.
    """
    hl_config_from_file = test_app_settings.exchanges["hyperliquid"]
    # Override is_mainnet_environment based on hl_test_environment_from_config fixture
    return hl_config_from_file.model_copy(
        update={"is_mainnet_environment": hl_test_environment_from_config == "mainnet"},
    )


@pytest.fixture(scope="session")
def active_hl_secrets(test_secrets_config: SecretsConfig) -> PrivateKeyAuthSecrets:
    """Provide PrivateKeyAuthSecrets for Hyperliquid from test secrets.

    Uses test_secrets.yaml settings.
    Matches Backpack pattern for consistency.
    """
    secrets = test_secrets_config.exchanges["hyperliquid"]
    if not isinstance(secrets, PrivateKeyAuthSecrets):
        pytest.fail("Hyperliquid secrets in test_secrets.yaml are not PrivateKeyAuthSecrets type.")
    return secrets


@pytest.fixture(scope="session")
def active_bp_config(test_app_settings: AppSettings) -> ExchangeSpecificConfig:
    """Environment-aware ExchangeSpecificConfig fixture for Backpack.

    Uses test configuration from test_config.yaml. Backpack always uses mainnet.
    """
    return test_app_settings.exchanges["backpack"]


@pytest.fixture(scope="session")
def active_bp_secrets(test_secrets_config: SecretsConfig) -> ApiKeyAuthSecrets:
    """Environment-aware ApiKeyAuthSecrets fixture for Backpack.

    Uses test secrets from test_secrets.yaml.
    """
    from cyberdelta.config.secrets_models import ApiKeyAuthSecrets

    secrets = test_secrets_config.exchanges["backpack"]
    if not isinstance(secrets, ApiKeyAuthSecrets):
        pytest.fail("Backpack secrets in test_secrets.yaml are not ApiKeyAuthSecrets type.")
    return secrets


# --- Mock Secrets Manager ---


@pytest.fixture
def mock_secrets_manager_with_missing() -> MagicMock:
    """Fixture for SecretsManager where some keys are missing."""
    manager = MagicMock(spec=SecretsManager)

    # Configure get method to return None for specific keys
    # Simulate missing optional keys
    def mock_get(
        key: str,
        default: object = None,
        *,
        _deep_get: bool = False,
        getter: Callable[..., object] | None = None,
    ) -> object:
        """Return mock get for testing."""
        # Simulate missing keys for testing
        missing_keys = ["optional_key_1", "optional_key_2"]
        if key in missing_keys:
            return default
        return f"mock_value_for_{key}"

    manager.get.side_effect = mock_get
    return manager


# --- System Component Fixtures ---


@pytest.fixture
def circuit_breaker_system(mock_config: AppSettings) -> CircuitBreakerSystem:
    """Create a CircuitBreakerSystem instance using mock config."""
    system = CircuitBreakerSystem(mock_config)
    return system
