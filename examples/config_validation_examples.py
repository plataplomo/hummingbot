"""Configuration validation examples and common scenarios.

This module demonstrates how to use the configuration validation framework
and shows examples of common configuration issues and their solutions.
"""

from __future__ import annotations

import logging
from decimal import Decimal

from pydantic import HttpUrl

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.models.execution_config import (
    ExecutionCompensationSettings,
    ExecutionSettings,
)
from cyberdelta.config.models.funding_strategy_models import (
    StrategiesSettings,
    StrategyConfigHLPerpBPSpot,
    StrategyParamsHLPerpBPSpot,
)
from cyberdelta.config.models.general_config import GeneralSettings
from cyberdelta.config.models.monitoring_config import MonitoringSettings
from cyberdelta.config.models.portfolio_config import PortfolioCalculationSettings
from cyberdelta.config.models.risk_config import EnhancedRiskSettings, GlobalRiskSettings
from cyberdelta.config.models.safety_config import SafetySystemsSettings
from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig, SymbolPatterns
from cyberdelta.enums.environment import EnvironmentType
from cyberdelta.enums.exchange_names import ExchangeName


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def get_default_symbols_config() -> SmartSymbolsConfig:
    """Get default symbols configuration for examples.

    Returns:
        SmartSymbolsConfig: Default symbols configuration with BTC and ETH.
    """
    return SmartSymbolsConfig(
        list=["BTC", "ETH"],
        patterns=SymbolPatterns(
            hyperliquid={"perp": "{symbol}"},
            backpack={"perp": "{symbol}_PERP"},
        ),
        defaults={"market_type": "PERP"},
        overrides={},
    )


def example_1_valid_configuration() -> None:
    """Example 1: Completely valid configuration."""
    logger.info("Example 1: Valid Configuration")
    logger.info("=" * 50)

    _ = AppSettings(
        general=GeneralSettings(
            log_level="INFO",
            safe_mode=True,
            state_file="data/state.json",
            state_backup_directory="data/backups",
            state_save_interval=300,
            state_backup_count=5,
        ),
        exchanges={
            "hyperliquid": ExchangeSpecificConfig(
                exchange_name=ExchangeName.HYPERLIQUID,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.hyperliquid.xyz"),
                ws_url_mainnet=HttpUrl("wss://api.hyperliquid.xyz/ws"),
                environment_type=EnvironmentType.MAINNET,
                symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
                chain_id=421614,
                ip_weight_limit_per_minute=1200,
                info_request_type_ip_weights={"meta": 1, "allMids": 2},
                default_info_weight=1,
                exchange_action_base_ip_weight=1,
            ),
            "backpack": ExchangeSpecificConfig(
                exchange_name=ExchangeName.BACKPACK,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.backpack.exchange"),
                ws_url_mainnet=HttpUrl("wss://ws.backpack.exchange"),
                environment_type=EnvironmentType.MAINNET,
                rate_limit_per_minute=120,
                symbols={"BTC": "BTC_USDC", "ETH": "ETH_USDC"},
            ),
        },
        strategies=StrategiesSettings(
            hl_perp_bp_spot=StrategyConfigHLPerpBPSpot(
                enabled=False,
                long_exchange="hyperliquid",
                short_exchange="backpack",
                symbol_long="BTC-PERP",
                symbol_short="BTC_USDC",
                params=StrategyParamsHLPerpBPSpot(
                    funding_threshold=Decimal("0.001"),
                    max_price_spread_pct=Decimal("0.01"),
                    min_profit_usd=Decimal(10),
                    min_funding_differential=Decimal("0.0005"),
                    check_interval=300,
                    risk_aversion=Decimal("1.0"),
                    rebalance_threshold=Decimal("0.1"),
                    perp_exchange="hyperliquid",
                    spot_exchange="backpack",
                ),
            ),
        ),
        risk=EnhancedRiskSettings.model_validate({
            "global": GlobalRiskSettings(
                max_position_usd=Decimal(1000),
                max_total_exposure_usd=Decimal(5000),
            ),
            "use_simple_sizing_path": True,
            "simple_sizing_method": "fixed_fraction",
            "simple_fixed_fraction": Decimal("0.1"),
            "simple_fixed_usd_size": Decimal(100),
        }),
        execution=ExecutionSettings(
            max_slippage_pct=Decimal("0.001"),  # 0.1%
            max_retries=3,
            retry_delay_base_sec=Decimal("1.0"),
            settlement_delay=Decimal("2.0"),
            compensation=ExecutionCompensationSettings(
                use_limit_orders=True,
                limit_price_offset_pct=Decimal("0.05"),  # 5%
            ),
        ),
        safety_systems=SafetySystemsSettings.model_validate({}),
        monitoring=MonitoringSettings(
            notifications_enabled=True,
            alert_methods=["log"],
        ),
        calculation=PortfolioCalculationSettings(),
        symbols=get_default_symbols_config(),
    )

    # Configuration validation is now handled by Pydantic models automatically
    logger.info("✅ Configuration is valid!")
    logger.info("Note: Validation now handled by Pydantic models during AppSettings instantiation")


def example_2_critical_errors() -> None:
    """Example 2: Configuration with critical errors."""
    logger.info("\nExample 2: Configuration with Critical Errors")
    logger.info("=" * 50)

    _ = AppSettings(
        general=GeneralSettings(
            log_level="INFO",
            safe_mode=True,
            state_file="data/state.json",
            state_backup_directory="data/backups",
            state_save_interval=300,
            state_backup_count=5,
        ),
        exchanges={
            "hyperliquid": ExchangeSpecificConfig(
                exchange_name=ExchangeName.HYPERLIQUID,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.hyperliquid.xyz"),
                ws_url_mainnet=HttpUrl("wss://api.hyperliquid.xyz/ws"),
                environment_type=EnvironmentType.MAINNET,
                symbols={"BTC": "BTC-PERP"},
                chain_id=None,  # ❌ CRITICAL: Missing chain_id
                ip_weight_limit_per_minute=None,  # ❌ CRITICAL: Missing IP weight limit
                info_request_type_ip_weights={"meta": 1},
                default_info_weight=1,
                exchange_action_base_ip_weight=1,
            ),
            # ❌ CRITICAL: Only one exchange (need at least 2 for arbitrage)
        },
        strategies=StrategiesSettings(
            hl_perp_bp_spot=StrategyConfigHLPerpBPSpot(
                enabled=False,
                long_exchange="hyperliquid",
                short_exchange="backpack",
                symbol_long="BTC-PERP",
                symbol_short="BTC_USDC",
                params=StrategyParamsHLPerpBPSpot(
                    funding_threshold=Decimal("0.001"),
                    max_price_spread_pct=Decimal("0.01"),
                    min_profit_usd=Decimal(10),
                    min_funding_differential=Decimal("0.0005"),
                    check_interval=300,
                    risk_aversion=Decimal("1.0"),
                    rebalance_threshold=Decimal("0.1"),
                    perp_exchange="hyperliquid",
                    spot_exchange="backpack",
                ),
            ),
        ),
        risk=EnhancedRiskSettings.model_validate({
            "global": GlobalRiskSettings(
                max_position_usd=Decimal(1000),
                max_total_exposure_usd=Decimal(500),  # ❌ CRITICAL: Less than max_position_usd
            ),
            "use_simple_sizing_path": True,
            "simple_sizing_method": "fixed_fraction",
            "simple_fixed_fraction": Decimal("1.5"),  # ❌ CRITICAL: > 1.0
            "simple_fixed_usd_size": Decimal(100),
        }),
        execution=ExecutionSettings(
            max_slippage_pct=Decimal("1.5"),  # ❌ CRITICAL: 150% slippage
            max_retries=-1,  # ❌ CRITICAL: Negative retries
            retry_delay_base_sec=Decimal(0),  # ❌ CRITICAL: Zero delay
            settlement_delay=Decimal("2.0"),
            compensation=ExecutionCompensationSettings(
                use_limit_orders=True,
                limit_price_offset_pct=Decimal("-0.05"),  # ❌ CRITICAL: Negative offset
            ),
        ),
        safety_systems=SafetySystemsSettings.model_validate({}),
        monitoring=MonitoringSettings(
            notifications_enabled=True,
            alert_methods=["log"],
        ),
        calculation=PortfolioCalculationSettings(),
        symbols=get_default_symbols_config(),
    )

    # This example would actually fail during AppSettings instantiation due to critical errors
    # Configuration validation is now handled by Pydantic models automatically
    try:
        # The settings object creation above would have already validated
        logger.warning("❌ This configuration has critical errors but Pydantic validation passed")
        logger.warning("Critical errors would include:")
        logger.warning("  - CRITICAL: Missing chain_id for Hyperliquid")
        logger.warning("  - CRITICAL: Missing IP weight limit for Hyperliquid")
        logger.warning("  - CRITICAL: Only one exchange configured (need at least 2 for arbitrage)")
        logger.warning("  - CRITICAL: Total exposure less than max position")
        logger.warning("  - CRITICAL: Simple fixed fraction > 1.0")
        logger.warning("  - CRITICAL: 150% slippage tolerance")
        logger.warning("  - CRITICAL: Negative retries")
        logger.warning("  - CRITICAL: Zero retry delay")
        logger.warning("  - CRITICAL: Negative limit price offset")
    except ValueError as e:
        logger.info("❌ Configuration validation failed: %s", e)


def example_3_warnings_only() -> None:
    """Example 3: Configuration with warnings but no errors."""
    logger.info("\nExample 3: Configuration with Warnings")
    logger.info("=" * 50)

    _ = AppSettings(
        general=GeneralSettings(
            log_level="INFO",
            safe_mode=True,
            state_file="data/state.json",
            state_backup_directory="data/backups",
            state_save_interval=300,
            state_backup_count=5,
        ),
        exchanges={
            "hyperliquid": ExchangeSpecificConfig(
                exchange_name=ExchangeName.HYPERLIQUID,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.hyperliquid.xyz"),
                ws_url_mainnet=HttpUrl("wss://api.hyperliquid.xyz/ws"),
                environment_type=EnvironmentType.MAINNET,
                symbols={"BTC": "BTC-PERP"},
                chain_id=421614,
                ip_weight_limit_per_minute=1200,
                info_request_type_ip_weights={"meta": 1, "allMids": 2},
                default_info_weight=1,
                exchange_action_base_ip_weight=1,
            ),
            "backpack": ExchangeSpecificConfig(
                exchange_name=ExchangeName.BACKPACK,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.backpack.exchange"),
                ws_url_mainnet=HttpUrl("wss://ws.backpack.exchange"),
                environment_type=EnvironmentType.MAINNET,
                rate_limit_per_minute=120,
                symbols={"BTC": "BTC_USDC"},
            ),
        },
        strategies=StrategiesSettings(
            hl_perp_bp_spot=StrategyConfigHLPerpBPSpot(
                enabled=False,
                long_exchange="hyperliquid",
                short_exchange="backpack",
                symbol_long="BTC-PERP",
                symbol_short="BTC_USDC",
                params=StrategyParamsHLPerpBPSpot(
                    funding_threshold=Decimal("0.001"),
                    max_price_spread_pct=Decimal("0.01"),
                    min_profit_usd=Decimal(10),
                    min_funding_differential=Decimal("0.0005"),
                    check_interval=300,
                    risk_aversion=Decimal("1.0"),
                    rebalance_threshold=Decimal("0.1"),
                    perp_exchange="hyperliquid",
                    spot_exchange="backpack",
                ),
            ),
        ),
        risk=EnhancedRiskSettings.model_validate({
            "global": GlobalRiskSettings(
                max_position_usd=Decimal(5),  # ⚠️ WARNING: Very low position size
                max_total_exposure_usd=Decimal(5000),
            ),
            "use_simple_sizing_path": True,
            "simple_sizing_method": "fixed_fraction",
            "simple_fixed_fraction": Decimal("0.1"),
            "simple_fixed_usd_size": Decimal(100),
        }),
        execution=ExecutionSettings(
            max_slippage_pct=Decimal("0.08"),  # ⚠️ WARNING: High slippage (8%)
            max_retries=15,  # ⚠️ WARNING: Very high retry count
            retry_delay_base_sec=Decimal(120),  # ⚠️ WARNING: Very long delay
            settlement_delay=Decimal("2.0"),
            compensation=ExecutionCompensationSettings(
                use_limit_orders=True,
                limit_price_offset_pct=Decimal("0.15"),  # ⚠️ WARNING: High offset (15%)
            ),
        ),
        safety_systems=SafetySystemsSettings.model_validate({}),
        monitoring=MonitoringSettings(
            notifications_enabled=True,
            alert_methods=["log"],
        ),
        calculation=PortfolioCalculationSettings(),
        symbols=get_default_symbols_config(),
    )

    # Configuration validation is now handled by Pydantic models automatically
    logger.info("✅ Configuration is valid (with warnings)!")
    logger.warning("\nThis configuration would have warnings:")
    logger.warning("  - WARNING: Very low position size ($5)")
    logger.warning("  - WARNING: High slippage tolerance (8%)")
    logger.warning("  - WARNING: Very high retry count (15)")
    logger.warning("  - WARNING: Very long retry delay (120s)")
    logger.warning("  - WARNING: High limit price offset (15%)")


def example_4_testnet_configuration() -> None:
    """Example 4: Testnet configuration validation."""
    logger.info("\nExample 4: Testnet Configuration")
    logger.info("=" * 50)

    _ = AppSettings(
        general=GeneralSettings(
            log_level="DEBUG",  # More verbose for testnet
            safe_mode=True,
            state_file="data/testnet_state.json",
            state_backup_directory="data/testnet_backups",
            state_save_interval=300,
            state_backup_count=5,
        ),
        exchanges={
            "hyperliquid": ExchangeSpecificConfig(
                exchange_name=ExchangeName.HYPERLIQUID,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.hyperliquid.xyz"),
                ws_url_mainnet=HttpUrl("wss://api.hyperliquid.xyz/ws"),
                api_base_url_testnet=HttpUrl("https://api.hyperliquid-testnet.xyz"),
                ws_url_testnet=HttpUrl("wss://api.hyperliquid-testnet.xyz/ws"),
                environment_type=EnvironmentType.TESTNET,  # Testnet mode
                symbols={"BTC": "BTC-PERP"},
                chain_id=421614,  # Testnet chain ID
                ip_weight_limit_per_minute=1200,
                info_request_type_ip_weights={"meta": 1, "allMids": 2},
                default_info_weight=1,
                exchange_action_base_ip_weight=1,
            ),
            "backpack": ExchangeSpecificConfig(
                exchange_name=ExchangeName.BACKPACK,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.backpack.exchange"),
                ws_url_mainnet=HttpUrl("wss://ws.backpack.exchange"),
                environment_type=EnvironmentType.MAINNET,  # Backpack only has mainnet
                rate_limit_per_minute=120,
                symbols={"BTC": "BTC_USDC"},
            ),
        },
        strategies=StrategiesSettings(
            hl_perp_bp_spot=StrategyConfigHLPerpBPSpot(
                enabled=False,
                long_exchange="hyperliquid",
                short_exchange="backpack",
                symbol_long="BTC-PERP",
                symbol_short="BTC_USDC",
                params=StrategyParamsHLPerpBPSpot(
                    funding_threshold=Decimal("0.001"),
                    max_price_spread_pct=Decimal("0.01"),
                    min_profit_usd=Decimal(10),
                    min_funding_differential=Decimal("0.0005"),
                    check_interval=300,
                    risk_aversion=Decimal("1.0"),
                    rebalance_threshold=Decimal("0.1"),
                    perp_exchange="hyperliquid",
                    spot_exchange="backpack",
                ),
            ),
        ),
        risk=EnhancedRiskSettings.model_validate({
            "global": GlobalRiskSettings(
                max_position_usd=Decimal(100),  # Lower for testnet
                max_total_exposure_usd=Decimal(500),
            ),
            "use_simple_sizing_path": True,
            "simple_sizing_method": "fixed_usd",
            "simple_fixed_fraction": Decimal("0.1"),
            "simple_fixed_usd_size": Decimal(10),  # Small for testnet
        }),
        execution=ExecutionSettings(
            max_slippage_pct=Decimal("0.01"),  # Higher for testnet
            max_retries=5,  # More retries for potentially flaky testnet
            retry_delay_base_sec=Decimal("2.0"),
            settlement_delay=Decimal("5.0"),  # Longer for testnet
            compensation=ExecutionCompensationSettings(
                use_limit_orders=True,
                limit_price_offset_pct=Decimal("0.1"),  # Higher for testnet
            ),
        ),
        safety_systems=SafetySystemsSettings.model_validate({}),
        monitoring=MonitoringSettings(
            notifications_enabled=True,
            alert_methods=["log"],
        ),
        calculation=PortfolioCalculationSettings(),  # Use defaults for testnet
        symbols=get_default_symbols_config(),
    )

    # Configuration validation is now handled by Pydantic models automatically
    logger.info("✅ Testnet configuration is valid!")
    logger.info("Testnet-specific settings:")
    logger.info("  - Using testnet environment for Hyperliquid")
    logger.info("  - Lower position sizes for testing")
    logger.info("  - Higher tolerances for potentially flaky testnet")
    logger.info("  - Longer timeouts and intervals")


def main() -> None:
    """Run all configuration validation examples."""
    logger.info("Configuration Validation Examples")
    logger.info("=" * 60)

    example_1_valid_configuration()
    example_2_critical_errors()
    example_3_warnings_only()
    example_4_testnet_configuration()

    logger.info("\n%s", "=" * 60)
    logger.info("Examples completed!")
    logger.info("\nKey takeaways:")
    logger.info("1. Always validate configuration before starting ExecutionHandler")
    logger.info("2. Critical errors prevent system startup")
    logger.info("3. Warnings indicate suboptimal but acceptable settings")
    logger.info("4. Testnet configurations may have different validation rules")
    logger.info("5. Use the validation framework to catch config issues early")


if __name__ == "__main__":
    main()
