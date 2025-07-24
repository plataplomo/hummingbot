"""Common configuration for sizing tests."""

from decimal import Decimal
from typing import Any

from cyberdelta.config.models.config_models import (
    AddressActionSafetyNetConfig,
    AppSettings,
    BalanceMonitoringSettings,
    CircuitBreakerSettings,
    EnhancedRiskSettings,
    ExchangeSpecificConfig,
    ExecutionCompensationSettings,
    ExecutionSettings,
    GeneralSettings,
    GlobalRiskSettings,
    MonitoringSettings,
    PortfolioTrackerConfig,
    PositionReconciliationSettings,
    SafetySystemsSettings,
)
from cyberdelta.config.models.funding_strategy_models import (
    StrategiesSettings,
    StrategyConfigHLPerpBPSpot,
    StrategyParamsHLPerpBPSpot,
)
from cyberdelta.enums.exchange_names import ExchangeName


def create_test_app_settings(config: dict[str, Any]) -> AppSettings:
    """Create a test AppSettings instance with minimal required fields.

    Maps the config dict to proper AppSettings structure for simple sizer testing.
    """
    return AppSettings(
        general=GeneralSettings(
            log_level="INFO",
            safe_mode=True,
            state_file="data/test_state.json",
            state_backup_directory="data/test_state_backups",
            state_save_interval=300,
            state_backup_count=5,
        ),
        exchanges={
            "hyperliquid": ExchangeSpecificConfig.model_validate({
                "exchange_name": ExchangeName.HYPERLIQUID,
                "enabled": True,
                "api_base_url_mainnet": "https://api.hyperliquid.xyz",
                "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
                "symbols": {"BTC": "BTC", "ETH": "ETH"},
                "chain_id": 1337,
                "ip_weight_limit_per_minute": 1200,
                "info_request_type_ip_weights": {"meta": 2, "orderStatus": 1},
                "default_info_weight": 2,
                "exchange_action_base_ip_weight": 10,
                "address_action_safety_net": AddressActionSafetyNetConfig(rate_per_minute=60),
            }),
            "backpack": ExchangeSpecificConfig.model_validate({
                "exchange_name": ExchangeName.BACKPACK,
                "enabled": True,
                "api_base_url_mainnet": "https://api.backpack.exchange",
                "ws_url_mainnet": "wss://api.backpack.exchange/ws",
                "rate_limit_per_minute": 100,
                "symbols": {"BTC": "BTC-USDC", "ETH": "ETH-USDC"},
            }),
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
                    min_funding_differential=Decimal("0.0001"),
                    check_interval=10,
                    risk_aversion=Decimal("1.0"),
                    rebalance_threshold=Decimal("0.05"),
                    perp_exchange="hyperliquid",
                    spot_exchange="backpack",
                ),
            ),
        ),
        risk=EnhancedRiskSettings.model_validate({
            "global": GlobalRiskSettings(
                max_position_usd=config.get("max_position_size", Decimal("10000.0")),
                max_total_exposure_usd=Decimal("100000.0"),
            ),
            "sizing": {
                "max_position_size": config.get("max_position_size", Decimal("10000.0")),
                "min_position_size": config.get("min_position_size", Decimal("100.0")),
                "min_volatility": Decimal("0.001"),
                "max_volatility_bound": Decimal("0.5"),
                "volatility_lookback_hours": 24,
                # Kelly-specific settings if needed
                "kelly_multiplier": Decimal(str(config.get("kelly_multiplier", 0.25))),
                "kelly_max_allocation": Decimal(str(config.get("kelly_max_allocation", 0.05))),
                "kelly_min_allocation": Decimal(str(config.get("kelly_min_allocation", 0.001))),
                "kelly_risk_free_rate": Decimal(str(config.get("risk_free_rate", 0.02))),
                # Simple sizing method in the SizingSettings
                "simple_method": config.get("sizing_method", "fixed_fraction"),
                "simple_fixed_fraction": Decimal(str(config.get("fixed_fraction", 0.02))),
                "simple_fixed_usd": Decimal(str(config.get("fixed_usd_amount", 1000))),
                "enable_validation_factors": config.get("enable_validation_factors", True),
                "enable_volatility_adjustment": config.get("enable_volatility_adjustment", True),
                "enable_spread_adjustment": config.get("enable_spread_adjustment", True),
                "base_validation_factor": Decimal(str(config.get("base_validation_factor", 1.0))),
            },
            "use_simple_sizing_path": config.get("use_simple_sizing", True),
            "simple_sizing_method": config.get("sizing_method", "fixed_fraction"),
            "simple_fixed_fraction": Decimal(str(config.get("fixed_fraction", 0.02))),
            "simple_fixed_usd_size": Decimal(str(config.get("fixed_usd_amount", 1000))),
        }),
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
                min_balance_thresholds_usd={},
            ),
        ),
        monitoring=MonitoringSettings(
            notifications_enabled=True,
            alert_methods=["log"],
        ),
        portfolio_tracker=PortfolioTrackerConfig.model_validate({
            "data_freshness_seconds": 60,
            "initial_balances": {},
            "initial_positions": [],
            "validation": {"validation_timeout": 4.0},
            "state": {"update_timeout": 5.0},
        }),
    )
