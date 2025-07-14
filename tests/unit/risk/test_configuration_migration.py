"""Tests for configuration migration utilities.

This module tests the ConfigurationMigrator utility that handles migration from
legacy dict-based configuration to the new enhanced Pydantic configuration structure.
"""

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.core.risk.config.migration import ConfigurationMigrator


class TestConfigurationMigrator:
    """Tests for the ConfigurationMigrator utility."""

    def test_migrate_empty_legacy_config(self) -> None:
        """Test migration with empty legacy configuration."""
        legacy_config: dict[str, Any] = {}

        enhanced_config = ConfigurationMigrator.migrate_legacy_config(legacy_config)

        # Should create valid enhanced config with defaults
        assert isinstance(enhanced_config, dict)
        assert enhanced_config["enabled"] is True
        assert "global" in enhanced_config
        assert "checkers" in enhanced_config
        assert "sizing" in enhanced_config

    def test_migrate_basic_legacy_config(self) -> None:
        """Test migration of basic legacy configuration."""
        legacy_config = {
            "enabled": False,
            "min_profitability_threshold": 0.005,
            "max_volatility": 0.3,
            "kelly_multiplier": 0.3,
            "use_simple_sizing_path": False,
        }

        enhanced_config = ConfigurationMigrator.migrate_legacy_config(legacy_config)

        assert enhanced_config["enabled"] is False
        assert enhanced_config["checkers"]["thresholds"]["min_profitability"] == 0.005
        assert enhanced_config["checkers"]["thresholds"]["max_volatility"] == 0.3
        assert enhanced_config["sizing"]["kelly_multiplier"] == 0.3
        assert enhanced_config["sizing"]["method"] == "kelly"

    def test_migrate_comprehensive_legacy_config(self) -> None:
        """Test migration of comprehensive legacy configuration."""
        legacy_config = {
            "enabled": True,
            "min_profitability_threshold": 0.002,
            "max_price_spread": 0.08,
            "max_price_deviation": 0.15,
            "min_price": 0.001,
            "max_price": 500000,
            "outlier_z_score_threshold": 2.5,
            "max_funding_rate": 0.02,
            "max_funding_rate_spread": 0.01,
            "max_volatility": 0.25,
            "min_volatility": 0.002,
            "min_balance_ratio": 0.15,
            "kelly_multiplier": 0.4,
            "kelly_max_allocation": 0.15,
            "kelly_min_allocation": 0.02,
            "kelly_risk_free_rate": 0.03,
            "min_position_size": 200,
            "max_position_size": 20000,
            "max_leverage": 8.0,
            "max_portfolio_allocation": 0.6,
            "total_capital": 200000,
            "simple_fixed_fraction": 0.05,
            "simple_fixed_usd_size": 2000,
            "use_simple_sizing_path": True,
            "simple_sizing_method": "fixed_usd",
            "enable_required_fields_check": False,
            "enable_profitability_check": True,
            "enable_price_sanity_check": True,
            "enable_volatility_check": False,
            "enable_funding_rate_check": True,
            "enable_circuit_breaker_check": False,
            "enable_balance_check": True,
            "fail_fast": False,
            "max_concurrent_checks": 8,
            "check_timeout_seconds": 8.0,
            "funding_rate_lookback_hours": 48,
            "volatility_lookback_hours": 12,
            "include_fees": False,
            "enable_outlier_detection": False,
            "check_both_exchanges": False,
            "log_level": "DEBUG",
            "log_all_checks": True,
            "log_performance_metrics": False,
            "max_concurrent_checks_system": 15,
            "max_concurrent_sizing": 8,
            "min_volatility_for_sizing": 0.005,
            "max_volatility_bound": 1.5,
            "volatility_lookback_hours_for_sizing": 48,
            "enable_validation_factors": False,
            "enable_volatility_adjustment": False,
            "enable_spread_adjustment": False,
            "base_validation_factor": 0.9,
            "sizing_timeout_seconds": 15.0,
        }

        enhanced_config = ConfigurationMigrator.migrate_legacy_config(legacy_config)

        # Test global settings
        assert enhanced_config["global"]["max_position_usd"] == 200.0
        assert enhanced_config["global"]["max_total_exposure_usd"] == 1000.0

        # Test checker settings
        checkers = enhanced_config["checkers"]
        assert checkers["enable_required_fields"] is False
        assert checkers["enable_profitability"] is True
        assert checkers["enable_price_sanity"] is True
        assert checkers["enable_volatility"] is False
        assert checkers["enable_funding_rate"] is True
        assert checkers["enable_circuit_breaker"] is False
        assert checkers["enable_balance"] is True

        # Test checker thresholds
        thresholds = checkers["thresholds"]
        assert thresholds["min_profitability"] == 0.002
        assert thresholds["max_price_spread"] == 0.08
        assert thresholds["max_price_deviation"] == 0.15
        assert thresholds["min_price"] == 0.001
        assert thresholds["max_price"] == 500000
        assert thresholds["outlier_z_score_threshold"] == 2.5
        assert thresholds["max_funding_rate"] == 0.02
        assert thresholds["max_funding_rate_spread"] == 0.01
        assert thresholds["max_volatility"] == 0.25
        assert thresholds["min_volatility"] == 0.002
        assert thresholds["min_balance_ratio"] == 0.15

        # Test checker configuration
        assert checkers["fail_fast"] is False
        assert checkers["max_concurrent_checks"] == 8
        assert checkers["check_timeout_seconds"] == 8.0
        assert checkers["funding_rate_lookback_hours"] == 48
        assert checkers["volatility_lookback_hours"] == 12
        assert checkers["include_fees_in_profitability"] is False
        assert checkers["enable_outlier_detection"] is False
        assert checkers["check_both_exchanges"] is False

        # Test sizing settings
        sizing = enhanced_config["sizing"]
        assert sizing["method"] == "simple"  # use_simple_sizing_path = True
        assert sizing["simple_method"] == "fixed_usd"
        assert sizing["simple_fixed_fraction"] == 0.05
        assert sizing["simple_fixed_usd"] == 2000
        assert sizing["kelly_multiplier"] == 0.4
        assert sizing["kelly_max_allocation"] == 0.15
        assert sizing["kelly_min_allocation"] == 0.02
        assert sizing["kelly_risk_free_rate"] == 0.03
        assert sizing["min_position_size"] == 200
        assert sizing["max_position_size"] == 20000
        assert sizing["max_leverage"] == 8.0
        assert sizing["max_portfolio_allocation"] == 0.6
        assert sizing["total_capital"] == 200000
        assert sizing["min_volatility"] == 0.005
        assert sizing["max_volatility_bound"] == 1.5
        assert sizing["volatility_lookback_hours"] == 48
        assert sizing["enable_validation_factors"] is False
        assert sizing["enable_volatility_adjustment"] is False
        assert sizing["enable_spread_adjustment"] is False
        assert sizing["base_validation_factor"] == 0.9
        assert sizing["sizing_timeout_seconds"] == 15.0

        # Test backward compatibility fields
        assert enhanced_config["use_simple_sizing_path"] is True
        assert enhanced_config["simple_sizing_method"] == "fixed_usd"
        assert enhanced_config["simple_fixed_fraction"] == 0.05
        assert enhanced_config["simple_fixed_usd_size"] == 2000

        # Test system configuration
        assert enhanced_config["log_level"] == "DEBUG"
        assert enhanced_config["log_all_checks"] is True
        assert enhanced_config["log_performance_metrics"] is False
        assert enhanced_config["max_concurrent_checks"] == 15
        assert enhanced_config["max_concurrent_sizing"] == 8

    def test_extract_preset_overrides_conservative(self) -> None:
        """Test extracting conservative preset overrides."""
        overrides = ConfigurationMigrator.extract_preset_overrides("conservative")

        assert "checkers.thresholds.min_profitability" in overrides
        assert overrides["checkers.thresholds.min_profitability"] == Decimal("0.003")
        assert overrides["checkers.thresholds.max_volatility"] == Decimal("0.1")
        assert overrides["sizing.kelly_multiplier"] == Decimal("0.1")
        assert overrides["sizing.max_leverage"] == Decimal("3.0")
        assert overrides["sizing.max_portfolio_allocation"] == Decimal("0.3")

    def test_extract_preset_overrides_moderate(self) -> None:
        """Test extracting moderate preset overrides."""
        overrides = ConfigurationMigrator.extract_preset_overrides("moderate")

        assert overrides["checkers.thresholds.min_profitability"] == Decimal("0.002")
        assert overrides["checkers.thresholds.max_volatility"] == Decimal("0.2")
        assert overrides["sizing.kelly_multiplier"] == Decimal("0.25")
        assert overrides["sizing.max_leverage"] == Decimal("5.0")
        assert overrides["sizing.max_portfolio_allocation"] == Decimal("0.5")

    def test_extract_preset_overrides_aggressive(self) -> None:
        """Test extracting aggressive preset overrides."""
        overrides = ConfigurationMigrator.extract_preset_overrides("aggressive")

        assert overrides["checkers.thresholds.min_profitability"] == Decimal("0.001")
        assert overrides["checkers.thresholds.max_volatility"] == Decimal("0.4")
        assert overrides["sizing.kelly_multiplier"] == Decimal("0.5")
        assert overrides["sizing.max_leverage"] == Decimal("10.0")
        assert overrides["sizing.max_portfolio_allocation"] == Decimal("0.7")

    def test_extract_preset_overrides_unknown(self) -> None:
        """Test extracting overrides for unknown preset."""
        overrides = ConfigurationMigrator.extract_preset_overrides("unknown")

        assert overrides == {}

    def test_apply_overrides_simple(self) -> None:
        """Test applying simple overrides to configuration."""
        base_config = {
            "checkers": {
                "thresholds": {
                    "min_profitability": 0.001,
                    "max_volatility": 0.2,
                },
            },
            "sizing": {
                "kelly_multiplier": 0.25,
            },
        }

        overrides = {
            "checkers.thresholds.min_profitability": 0.005,
            "sizing.kelly_multiplier": 0.1,
        }

        result = ConfigurationMigrator.apply_overrides(base_config, overrides)

        assert result["checkers"]["thresholds"]["min_profitability"] == 0.005
        assert result["checkers"]["thresholds"]["max_volatility"] == 0.2  # Unchanged
        assert result["sizing"]["kelly_multiplier"] == 0.1

    def test_apply_overrides_nested_creation(self) -> None:
        """Test applying overrides that create new nested structures."""
        base_config = {"existing": {"field": "value"}}

        overrides = {
            "new.nested.field": "new_value",
            "existing.new_field": "another_value",
        }

        result = ConfigurationMigrator.apply_overrides(base_config, overrides)

        assert result["new"]["nested"]["field"] == "new_value"
        assert result["existing"]["field"] == "value"  # Unchanged
        assert result["existing"]["new_field"] == "another_value"

    def test_apply_overrides_deep_copy(self) -> None:
        """Test that apply_overrides creates a deep copy."""
        base_config = {
            "nested": {
                "list": [1, 2, 3],
                "dict": {"key": "value"},
            }
        }

        overrides = {"nested.new_field": "new_value"}

        result = ConfigurationMigrator.apply_overrides(base_config, overrides)

        # Modify original
        base_config["nested"]["list"].append(4)  # type: ignore[attr-defined]
        base_config["nested"]["dict"]["key"] = "modified"  # type: ignore[index]

        # Result should be unchanged
        assert result["nested"]["list"] == [1, 2, 3]
        assert result["nested"]["dict"]["key"] == "value"
        assert result["nested"]["new_field"] == "new_value"

    def test_apply_preset_conservative(self) -> None:
        """Test applying conservative preset to configuration."""
        base_config = {
            "risk": {
                "checkers": {
                    "thresholds": {
                        "min_profitability": "0.001",
                        "max_volatility": "0.2",
                    }
                },
                "sizing": {
                    "kelly_multiplier": "0.25",
                    "simple_fixed_fraction": "0.02",
                    "max_portfolio_allocation": "0.5",
                },
            }
        }

        result = ConfigurationMigrator.apply_preset(base_config, "conservative")

        # Conservative settings should be applied
        risk = result["risk"]
        assert risk["checkers"]["thresholds"]["min_profitability"] == "0.002"  # 0.2%
        assert risk["checkers"]["thresholds"]["max_volatility"] == "0.1"  # 10%
        assert risk["sizing"]["kelly_multiplier"] == "0.1"  # 10% of Kelly
        assert risk["sizing"]["simple_fixed_fraction"] == "0.01"  # 1%
        assert risk["sizing"]["max_portfolio_allocation"] == "0.3"  # 30%

    def test_apply_preset_moderate(self) -> None:
        """Test applying moderate preset to configuration."""
        base_config = {
            "risk": {
                "checkers": {
                    "thresholds": {
                        "min_profitability": "0.001",
                        "max_volatility": "0.2",
                    }
                },
                "sizing": {
                    "kelly_multiplier": "0.25",
                    "simple_fixed_fraction": "0.02",
                    "max_portfolio_allocation": "0.5",
                },
            }
        }

        result = ConfigurationMigrator.apply_preset(base_config, "moderate")

        # Moderate settings should be applied
        risk = result["risk"]
        assert risk["checkers"]["thresholds"]["min_profitability"] == "0.001"  # 0.1%
        assert risk["checkers"]["thresholds"]["max_volatility"] == "0.2"  # 20%
        assert risk["sizing"]["kelly_multiplier"] == "0.25"  # 25% of Kelly
        assert risk["sizing"]["simple_fixed_fraction"] == "0.02"  # 2%
        assert risk["sizing"]["max_portfolio_allocation"] == "0.5"  # 50%

    def test_apply_preset_aggressive(self) -> None:
        """Test applying aggressive preset to configuration."""
        base_config = {
            "risk": {
                "checkers": {
                    "thresholds": {
                        "min_profitability": "0.001",
                        "max_volatility": "0.2",
                    }
                },
                "sizing": {
                    "kelly_multiplier": "0.25",
                    "simple_fixed_fraction": "0.02",
                    "max_portfolio_allocation": "0.5",
                },
            }
        }

        result = ConfigurationMigrator.apply_preset(base_config, "aggressive")

        # Aggressive settings should be applied
        risk = result["risk"]
        assert risk["checkers"]["thresholds"]["min_profitability"] == "0.0005"  # 0.05%
        assert risk["checkers"]["thresholds"]["max_volatility"] == "0.5"  # 50%
        assert risk["sizing"]["kelly_multiplier"] == "0.5"  # 50% of Kelly
        assert risk["sizing"]["simple_fixed_fraction"] == "0.05"  # 5%
        assert risk["sizing"]["max_portfolio_allocation"] == "0.8"  # 80%

    def test_apply_preset_unknown(self) -> None:
        """Test applying unknown preset returns unchanged configuration."""
        base_config = {
            "risk": {
                "checkers": {
                    "thresholds": {
                        "min_profitability": "0.001",
                    }
                }
            }
        }

        result = ConfigurationMigrator.apply_preset(base_config, "unknown")

        # Should return unchanged
        assert result == base_config

    def test_legacy_config_edge_cases(self) -> None:
        """Test edge cases in legacy configuration migration."""
        # Test with None values
        legacy_config = {
            "min_profitability_threshold": None,
            "max_volatility": None,
            "enabled": None,
        }

        enhanced_config = ConfigurationMigrator.migrate_legacy_config(legacy_config)

        # Should use defaults for None values
        assert enhanced_config["enabled"] is True  # Default
        assert "min_profitability" in enhanced_config["checkers"]["thresholds"]
        assert "max_volatility" in enhanced_config["checkers"]["thresholds"]

    def test_sizing_method_detection(self) -> None:
        """Test proper sizing method detection from legacy config."""
        # Test Kelly sizing detection
        kelly_config = {
            "use_simple_sizing_path": False,
        }

        enhanced_config = ConfigurationMigrator.migrate_legacy_config(kelly_config)
        assert enhanced_config["sizing"]["method"] == "kelly"

        # Test simple sizing detection (default)
        simple_config = {
            "use_simple_sizing_path": True,
        }

        enhanced_config = ConfigurationMigrator.migrate_legacy_config(simple_config)
        assert enhanced_config["sizing"]["method"] == "simple"

        # Test default when not specified
        default_config: dict[str, Any] = {}

        enhanced_config = ConfigurationMigrator.migrate_legacy_config(default_config)
        assert enhanced_config["sizing"]["method"] == "simple"

    def test_backward_compatibility_preservation(self) -> None:
        """Test that backward compatibility fields are preserved."""
        legacy_config = {
            "use_simple_sizing_path": False,
            "simple_sizing_method": "fixed_usd",
            "simple_fixed_fraction": 0.15,
            "simple_fixed_usd_size": 5000,
        }

        enhanced_config = ConfigurationMigrator.migrate_legacy_config(legacy_config)

        # Check backward compatibility fields are preserved
        assert enhanced_config["use_simple_sizing_path"] is False
        assert enhanced_config["simple_sizing_method"] == "fixed_usd"
        assert enhanced_config["simple_fixed_fraction"] == 0.15
        assert enhanced_config["simple_fixed_usd_size"] == 5000

        # Also check they're mapped to new structure
        assert enhanced_config["sizing"]["method"] == "kelly"  # use_simple_sizing_path = False
        assert enhanced_config["sizing"]["simple_method"] == "fixed_usd"
        assert enhanced_config["sizing"]["simple_fixed_fraction"] == 0.15
        assert enhanced_config["sizing"]["simple_fixed_usd"] == 5000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
