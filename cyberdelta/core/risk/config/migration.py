"""migration.py.

Configuration migration utilities for transitioning from legacy to enhanced risk configuration.
"""

from __future__ import annotations

import copy
from decimal import Decimal
from typing import Any, TypeVar


T = TypeVar("T")


class ConfigurationMigrator:
    """Utility to migrate legacy configurations to enhanced structure."""

    @staticmethod
    def _get_with_default(
        config: dict[str, Any],
        key: str,
        default: T,
    ) -> T:
        """Get value from config, returning default if key is missing or None."""
        value = config.get(key)
        return default if value is None else value

    @staticmethod
    def migrate_legacy_config(legacy_config: dict[str, Any]) -> dict[str, Any]:
        """Migrate legacy configuration structure to enhanced structure.

        Args:
            legacy_config: Legacy configuration dictionary

        Returns:
            Enhanced configuration dictionary compatible with EnhancedRiskSettings

        """
        enhanced_config: dict[str, Any] = {
            "enabled": ConfigurationMigrator._get_with_default(legacy_config, "enabled", True),
            "global": ConfigurationMigrator._get_with_default(
                legacy_config,
                "global",
                {
                    "max_position_usd": 200.0,
                    "max_total_exposure_usd": 1000.0,
                },
            ),
            "checkers": {
                "enable_required_fields": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_required_fields_check", True
                ),
                "enable_profitability": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_profitability_check", True
                ),
                "enable_price_sanity": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_price_sanity_check", True
                ),
                "enable_volatility": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_volatility_check", True
                ),
                "enable_funding_rate": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_funding_rate_check", True
                ),
                "enable_circuit_breaker": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_circuit_breaker_check", True
                ),
                "enable_balance": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_balance_check", True
                ),
                "thresholds": {
                    "min_profitability": ConfigurationMigrator._get_with_default(
                        legacy_config, "min_profitability_threshold", 0.001
                    ),
                    "max_price_spread": ConfigurationMigrator._get_with_default(
                        legacy_config, "max_price_spread", 0.05
                    ),
                    "max_price_deviation": ConfigurationMigrator._get_with_default(
                        legacy_config, "max_price_deviation", 0.1
                    ),
                    "min_price": ConfigurationMigrator._get_with_default(
                        legacy_config, "min_price", 0.0000001
                    ),
                    "max_price": ConfigurationMigrator._get_with_default(
                        legacy_config, "max_price", 1000000
                    ),
                    "outlier_z_score_threshold": ConfigurationMigrator._get_with_default(
                        legacy_config, "outlier_z_score_threshold", 3.0
                    ),
                    "max_funding_rate": ConfigurationMigrator._get_with_default(
                        legacy_config, "max_funding_rate", 0.01
                    ),
                    "max_funding_rate_spread": ConfigurationMigrator._get_with_default(
                        legacy_config, "max_funding_rate_spread", 0.005
                    ),
                    "max_volatility": ConfigurationMigrator._get_with_default(
                        legacy_config, "max_volatility", 0.2
                    ),
                    "min_volatility": ConfigurationMigrator._get_with_default(
                        legacy_config, "min_volatility", 0.001
                    ),
                    "min_balance_ratio": ConfigurationMigrator._get_with_default(
                        legacy_config, "min_balance_ratio", 0.1
                    ),
                },
                "fail_fast": ConfigurationMigrator._get_with_default(
                    legacy_config, "fail_fast", True
                ),
                "max_concurrent_checks": ConfigurationMigrator._get_with_default(
                    legacy_config, "max_concurrent_checks", 5
                ),
                "check_timeout_seconds": ConfigurationMigrator._get_with_default(
                    legacy_config, "check_timeout_seconds", 5.0
                ),
                "funding_rate_lookback_hours": ConfigurationMigrator._get_with_default(
                    legacy_config, "funding_rate_lookback_hours", 24
                ),
                "volatility_lookback_hours": ConfigurationMigrator._get_with_default(
                    legacy_config, "volatility_lookback_hours", 24
                ),
                "include_fees_in_profitability": ConfigurationMigrator._get_with_default(
                    legacy_config, "include_fees", True
                ),
                "enable_outlier_detection": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_outlier_detection", True
                ),
                "check_both_exchanges": ConfigurationMigrator._get_with_default(
                    legacy_config, "check_both_exchanges", True
                ),
                "extra_config": legacy_config.get("extra_check_config"),
            },
            "sizing": {
                "method": (
                    "simple"
                    if ConfigurationMigrator._get_with_default(
                        legacy_config, "use_simple_sizing_path", True
                    )
                    else "kelly"
                ),
                "simple_method": ConfigurationMigrator._get_with_default(
                    legacy_config, "simple_sizing_method", "fixed_fraction"
                ),
                "simple_fixed_fraction": ConfigurationMigrator._get_with_default(
                    legacy_config, "simple_fixed_fraction", 0.1
                ),
                "simple_fixed_usd": ConfigurationMigrator._get_with_default(
                    legacy_config, "simple_fixed_usd_size", 10.0
                ),
                "kelly_multiplier": ConfigurationMigrator._get_with_default(
                    legacy_config, "kelly_multiplier", 0.25
                ),
                "kelly_max_allocation": ConfigurationMigrator._get_with_default(
                    legacy_config, "kelly_max_allocation", 0.1
                ),
                "kelly_min_allocation": ConfigurationMigrator._get_with_default(
                    legacy_config, "kelly_min_allocation", 0.01
                ),
                "kelly_risk_free_rate": ConfigurationMigrator._get_with_default(
                    legacy_config, "kelly_risk_free_rate", 0.02
                ),
                "min_position_size": ConfigurationMigrator._get_with_default(
                    legacy_config, "min_position_size", 100
                ),
                "max_position_size": ConfigurationMigrator._get_with_default(
                    legacy_config, "max_position_size", 10000
                ),
                "max_leverage": ConfigurationMigrator._get_with_default(
                    legacy_config, "max_leverage", 5.0
                ),
                "max_portfolio_allocation": ConfigurationMigrator._get_with_default(
                    legacy_config, "max_portfolio_allocation", 0.5
                ),
                "total_capital": legacy_config.get("total_capital"),
                "min_volatility": ConfigurationMigrator._get_with_default(
                    legacy_config, "min_volatility_for_sizing", 0.001
                ),
                "max_volatility_bound": ConfigurationMigrator._get_with_default(
                    legacy_config, "max_volatility_bound", 1.0
                ),
                "volatility_lookback_hours": ConfigurationMigrator._get_with_default(
                    legacy_config, "volatility_lookback_hours_for_sizing", 24
                ),
                "enable_validation_factors": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_validation_factors", True
                ),
                "enable_volatility_adjustment": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_volatility_adjustment", True
                ),
                "enable_spread_adjustment": ConfigurationMigrator._get_with_default(
                    legacy_config, "enable_spread_adjustment", True
                ),
                "base_validation_factor": ConfigurationMigrator._get_with_default(
                    legacy_config, "base_validation_factor", 0.8
                ),
                "sizing_timeout_seconds": ConfigurationMigrator._get_with_default(
                    legacy_config, "sizing_timeout_seconds", 10.0
                ),
            },
            # Preserve backward compatibility fields
            "use_simple_sizing_path": ConfigurationMigrator._get_with_default(
                legacy_config, "use_simple_sizing_path", True
            ),
            "simple_sizing_method": ConfigurationMigrator._get_with_default(
                legacy_config, "simple_sizing_method", "fixed_fraction"
            ),
            "simple_fixed_fraction": ConfigurationMigrator._get_with_default(
                legacy_config, "simple_fixed_fraction", 0.1
            ),
            "simple_fixed_usd_size": ConfigurationMigrator._get_with_default(
                legacy_config, "simple_fixed_usd_size", 10.0
            ),
            # System configuration
            "log_level": ConfigurationMigrator._get_with_default(
                legacy_config, "log_level", "INFO"
            ),
            "log_all_checks": ConfigurationMigrator._get_with_default(
                legacy_config, "log_all_checks", False
            ),
            "log_performance_metrics": ConfigurationMigrator._get_with_default(
                legacy_config, "log_performance_metrics", True
            ),
            "max_concurrent_checks": ConfigurationMigrator._get_with_default(
                legacy_config, "max_concurrent_checks_system", 10
            ),
            "max_concurrent_sizing": ConfigurationMigrator._get_with_default(
                legacy_config, "max_concurrent_sizing", 5
            ),
        }

        return enhanced_config

    @staticmethod
    def extract_preset_overrides(preset: str) -> dict[str, Any]:
        """Extract configuration overrides for a given preset.

        Args:
            preset: Preset name ("conservative", "moderate", "aggressive")

        Returns:
            Dictionary of configuration overrides to apply

        """
        presets = {
            "conservative": {
                "checkers.thresholds.min_profitability": Decimal("0.003"),
                "checkers.thresholds.max_volatility": Decimal("0.1"),
                "sizing.kelly_multiplier": Decimal("0.1"),
                "sizing.max_leverage": Decimal("3.0"),
                "sizing.max_portfolio_allocation": Decimal("0.3"),
            },
            "moderate": {
                "checkers.thresholds.min_profitability": Decimal("0.002"),
                "checkers.thresholds.max_volatility": Decimal("0.2"),
                "sizing.kelly_multiplier": Decimal("0.25"),
                "sizing.max_leverage": Decimal("5.0"),
                "sizing.max_portfolio_allocation": Decimal("0.5"),
            },
            "aggressive": {
                "checkers.thresholds.min_profitability": Decimal("0.001"),
                "checkers.thresholds.max_volatility": Decimal("0.4"),
                "sizing.kelly_multiplier": Decimal("0.5"),
                "sizing.max_leverage": Decimal("10.0"),
                "sizing.max_portfolio_allocation": Decimal("0.7"),
            },
        }

        return presets.get(preset, {})

    @staticmethod
    def apply_overrides(config: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
        """Apply configuration overrides using dot notation paths.

        Args:
            config: Base configuration dictionary
            overrides: Dictionary of overrides with dot notation keys

        Returns:
            Updated configuration dictionary

        """
        result = copy.deepcopy(config)

        for path, value in overrides.items():
            keys = path.split(".")
            current = result

            # Navigate to the parent of the target key
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]

            # Set the final value
            current[keys[-1]] = value

        return result

    @staticmethod
    def apply_preset(
        base_config: dict[str, Any],
        preset_name: str,
    ) -> dict[str, Any]:
        """Apply a configuration preset to base config.

        Args:
            base_config: Base configuration dictionary
            preset_name: Name of preset (conservative, moderate, aggressive)

        Returns:
            Configuration with preset applied
        """
        preset_config = base_config.copy()

        # Handle both direct risk config and full app config structures
        risk_config = preset_config.get("risk", preset_config)

        if preset_name == "conservative":
            # Conservative settings - lower risk, smaller positions
            risk_config["checkers"]["thresholds"]["min_profitability"] = "0.002"  # 0.2%
            risk_config["checkers"]["thresholds"]["max_volatility"] = "0.1"  # 10%
            risk_config["sizing"]["kelly_multiplier"] = "0.1"  # 10% of Kelly
            risk_config["sizing"]["simple_fixed_fraction"] = "0.01"  # 1%
            risk_config["sizing"]["max_portfolio_allocation"] = "0.3"  # 30%

        elif preset_name == "moderate":
            # Moderate settings - balanced risk/reward
            risk_config["checkers"]["thresholds"]["min_profitability"] = "0.001"  # 0.1%
            risk_config["checkers"]["thresholds"]["max_volatility"] = "0.2"  # 20%
            risk_config["sizing"]["kelly_multiplier"] = "0.25"  # 25% of Kelly
            risk_config["sizing"]["simple_fixed_fraction"] = "0.02"  # 2%
            risk_config["sizing"]["max_portfolio_allocation"] = "0.5"  # 50%

        elif preset_name == "aggressive":
            # Aggressive settings - higher risk, larger positions
            risk_config["checkers"]["thresholds"]["min_profitability"] = "0.0005"  # 0.05%
            risk_config["checkers"]["thresholds"]["max_volatility"] = "0.5"  # 50%
            risk_config["sizing"]["kelly_multiplier"] = "0.5"  # 50% of Kelly
            risk_config["sizing"]["simple_fixed_fraction"] = "0.05"  # 5%
            risk_config["sizing"]["max_portfolio_allocation"] = "0.8"  # 80%

        return preset_config
