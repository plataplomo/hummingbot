"""Tests for portfolio configuration validation and migration.

This module tests the Pydantic-based portfolio configuration system,
including field validation, business logic validation, and edge cases.
"""

import pytest
from pydantic import ValidationError

from cyberdelta.core.portfolio.config.factory import (
    PortfolioConfigFactory,
    create_dev_config,
    create_prod_config,
    create_test_config,
)
from cyberdelta.core.portfolio.config.portfolio_config import (
    BalanceConfiguration,
    CacheConfiguration,
    ConcurrencyConfiguration,
    PnLConfiguration,
    PortfolioConfiguration,
    PricingConfiguration,
    ScreeningConfiguration,
)
from cyberdelta.core.portfolio.config.validation import (
    ConfigurationValidator,
    create_validated_configuration,
)


class TestCacheConfiguration:
    """Test cases for CacheConfiguration validation."""

    def test_valid_cache_config(self) -> None:
        """Test valid cache configuration."""
        config = CacheConfiguration(
            max_size=1000, default_ttl=3600.0, cleanup_interval=300.0, enabled=True
        )
        assert config.max_size == 1000
        assert config.default_ttl == 3600.0
        assert config.cleanup_interval == 300.0
        assert config.enabled is True

    def test_invalid_cache_size(self) -> None:
        """Test cache size validation."""
        # Negative size
        with pytest.raises(ValidationError) as exc_info:
            CacheConfiguration(max_size=-1)
        assert "Cache max_size must be positive" in str(exc_info.value)

        # Too large size
        with pytest.raises(ValidationError) as exc_info:
            CacheConfiguration(max_size=1000001)
        assert "Cache max_size too large" in str(exc_info.value)

    def test_invalid_cache_ttl(self) -> None:
        """Test cache TTL validation."""
        # Negative TTL
        with pytest.raises(ValidationError) as exc_info:
            CacheConfiguration(default_ttl=-1.0)
        assert "Time intervals must be positive" in str(exc_info.value)

        # Too large TTL
        with pytest.raises(ValidationError) as exc_info:
            CacheConfiguration(default_ttl=86401.0)
        assert "Time interval too large" in str(exc_info.value)


class TestPricingConfiguration:
    """Test cases for PricingConfiguration validation."""

    def test_valid_pricing_config(self) -> None:
        """Test valid pricing configuration."""
        config = PricingConfiguration(
            default_cache_ttl=60.0,
            batch_size_limit=50,
            price_staleness_threshold=300.0,
            enabled=True,
        )
        assert config.default_cache_ttl == 60.0
        assert config.batch_size_limit == 50

    def test_invalid_batch_size(self) -> None:
        """Test batch size validation."""
        # Negative batch size
        with pytest.raises(ValidationError) as exc_info:
            PricingConfiguration(batch_size_limit=-1)
        assert "Batch size limit must be positive" in str(exc_info.value)

        # Too large batch size
        with pytest.raises(ValidationError) as exc_info:
            PricingConfiguration(batch_size_limit=1001)
        assert "Batch size limit too large" in str(exc_info.value)


class TestScreeningConfiguration:
    """Test cases for ScreeningConfiguration validation."""

    def test_valid_screening_config(self) -> None:
        """Test valid screening configuration."""
        config = ScreeningConfiguration(
            max_price_value="1000000",
            max_quantity_value="1000000",
            min_price_value="0.00001",
            min_quantity_value="0.00001",
        )
        assert config.max_price_value == "1000000"
        assert config.min_price_value == "0.00001"

    def test_invalid_numeric_strings(self) -> None:
        """Test numeric string validation."""
        # Invalid decimal string
        with pytest.raises(ValidationError) as exc_info:
            ScreeningConfiguration(max_price_value="not_a_number")
        assert "Invalid numeric string" in str(exc_info.value)

        # Non-finite value
        with pytest.raises(ValidationError) as exc_info:
            ScreeningConfiguration(max_price_value="inf")
        assert "Invalid numeric string" in str(exc_info.value)

        # Negative value
        with pytest.raises(ValidationError) as exc_info:
            ScreeningConfiguration(min_price_value="-1.0")
        assert "Invalid numeric string" in str(exc_info.value)

        # Value too large
        with pytest.raises(ValidationError) as exc_info:
            ScreeningConfiguration(max_price_value="1000000001")
        assert "Maximum value too large" in str(exc_info.value)

        # Value too small
        with pytest.raises(ValidationError) as exc_info:
            ScreeningConfiguration(min_price_value="0.000000001")
        assert "Minimum value too small" in str(exc_info.value)


class TestBalanceConfiguration:
    """Test cases for BalanceConfiguration validation."""

    def test_valid_balance_config(self) -> None:
        """Test valid balance configuration."""
        config = BalanceConfiguration(
            auto_cleanup_enabled=True, cleanup_interval=300, max_balance_age=3600, precision=8
        )
        assert config.cleanup_interval == 300
        assert config.precision == 8

    def test_invalid_precision(self) -> None:
        """Test precision validation."""
        # Negative precision
        with pytest.raises(ValidationError) as exc_info:
            BalanceConfiguration(precision=-1)
        assert "Precision cannot be negative" in str(exc_info.value)

        # Too high precision
        with pytest.raises(ValidationError) as exc_info:
            BalanceConfiguration(precision=19)
        assert "Precision too high" in str(exc_info.value)

    def test_invalid_time_intervals(self) -> None:
        """Test time interval validation."""
        # Negative interval
        with pytest.raises(ValidationError) as exc_info:
            BalanceConfiguration(cleanup_interval=-1)
        assert "Time interval must be positive" in str(exc_info.value)

        # Too large interval
        with pytest.raises(ValidationError) as exc_info:
            BalanceConfiguration(max_balance_age=86401)
        assert "Time interval too large" in str(exc_info.value)


class TestPnLConfiguration:
    """Test cases for PnLConfiguration validation."""

    def test_valid_pnl_config(self) -> None:
        """Test valid P&L configuration."""
        config = PnLConfiguration(
            calculation_method="FIFO", precision=8, cache_results=True, real_time_updates=True
        )
        assert config.calculation_method == "FIFO"
        assert config.precision == 8

    def test_invalid_calculation_method(self) -> None:
        """Test calculation method validation."""
        with pytest.raises(ValidationError) as exc_info:
            PnLConfiguration(calculation_method="INVALID")
        assert "Calculation method must be one of" in str(exc_info.value)


class TestConcurrencyConfiguration:
    """Test cases for ConcurrencyConfiguration validation."""

    def test_valid_concurrency_config(self) -> None:
        """Test valid concurrency configuration."""
        config = ConcurrencyConfiguration(
            max_concurrent_operations=10, lock_timeout=30.0, deadlock_detection=True
        )
        assert config.max_concurrent_operations == 10
        assert config.lock_timeout == 30.0

    def test_invalid_max_operations(self) -> None:
        """Test max operations validation."""
        # Negative operations
        with pytest.raises(ValidationError) as exc_info:
            ConcurrencyConfiguration(max_concurrent_operations=-1)
        assert "Max concurrent operations must be positive" in str(exc_info.value)

        # Too many operations
        with pytest.raises(ValidationError) as exc_info:
            ConcurrencyConfiguration(max_concurrent_operations=101)
        assert "Max concurrent operations too large" in str(exc_info.value)

    def test_invalid_lock_timeout(self) -> None:
        """Test lock timeout validation."""
        # Negative timeout
        with pytest.raises(ValidationError) as exc_info:
            ConcurrencyConfiguration(lock_timeout=-1.0)
        assert "Lock timeout must be positive" in str(exc_info.value)

        # Too large timeout
        with pytest.raises(ValidationError) as exc_info:
            ConcurrencyConfiguration(lock_timeout=301.0)
        assert "Lock timeout too large" in str(exc_info.value)


class TestPortfolioConfiguration:
    """Test cases for PortfolioConfiguration validation."""

    def test_default_configuration(self) -> None:
        """Test default portfolio configuration."""
        config = PortfolioConfiguration()
        assert config.debug_mode is False
        assert config.log_level == "INFO"
        assert config.cache.max_size == 10000
        assert config.pricing.default_cache_ttl == 60.0

    def test_invalid_log_level(self) -> None:
        """Test log level validation."""
        with pytest.raises(ValidationError) as exc_info:
            PortfolioConfiguration(log_level="INVALID")
        assert "Log level must be one of" in str(exc_info.value)

    def test_create_development_config(self) -> None:
        """Test development configuration creation."""
        config = PortfolioConfiguration.create_development()
        assert config.debug_mode is True
        assert config.log_level == "DEBUG"
        assert config.cache.cleanup_interval == 30.0
        assert config.pricing.default_cache_ttl == 10.0

    def test_create_production_config(self) -> None:
        """Test production configuration creation."""
        config = PortfolioConfiguration.create_production()
        assert config.debug_mode is False
        assert config.log_level == "INFO"
        assert config.cache.max_size == 50000
        assert config.cache.default_ttl == 7200.0

    def test_create_test_config(self) -> None:
        """Test test configuration creation."""
        config = PortfolioConfiguration.create_test()
        assert config.debug_mode is True
        assert config.log_level == "DEBUG"
        assert config.cache.max_size == 100
        assert config.concurrency.max_concurrent_operations == 3

    def test_merge_with_dict(self) -> None:
        """Test merging configuration with dictionary."""
        config = PortfolioConfiguration()
        config.merge_with_dict({
            "debug_mode": True,
            "log_level": "DEBUG",
            "cache": {"max_size": 5000, "default_ttl": 1800.0},
        })
        assert config.debug_mode is True
        assert config.log_level == "DEBUG"
        assert config.cache.max_size == 5000
        assert config.cache.default_ttl == 1800.0

    def test_merge_with_invalid_values(self) -> None:
        """Test merging with invalid values raises validation errors."""
        config = PortfolioConfiguration()
        with pytest.raises(ValidationError):
            config.merge_with_dict({
                "cache": {
                    "max_size": -1  # Invalid negative size
                }
            })


class TestConfigurationValidator:
    """Test cases for ConfigurationValidator business logic validation."""

    def test_validate_financial_limits(self) -> None:
        """Test financial limits validation."""
        config = PortfolioConfiguration()
        # Set invalid financial ranges
        config.screening.min_price_value = "100"
        config.screening.max_price_value = "50"  # Invalid: min > max

        errors = ConfigurationValidator.validate_financial_limits(config)
        assert any("must be less than maximum price" in error for error in errors)

    def test_validate_cache_settings(self) -> None:
        """Test cache settings validation."""
        config = PortfolioConfiguration()
        # Set pricing cache TTL higher than general cache TTL
        config.cache.default_ttl = 3600.0
        config.pricing.default_cache_ttl = 7200.0  # Invalid: pricing > general

        errors = ConfigurationValidator.validate_cache_settings(config)
        assert any("should not exceed general cache TTL" in error for error in errors)

    def test_validate_monitoring_intervals(self) -> None:
        """Test monitoring intervals validation."""
        config = PortfolioConfiguration()
        # Set health check interval higher than metrics interval
        config.monitoring.health_check_interval = 60
        config.monitoring.metrics_interval = 30  # Invalid: health > metrics

        errors = ConfigurationValidator.validate_monitoring_intervals(config)
        assert any("should not exceed metrics interval" in error for error in errors)

    def test_validate_precision_settings(self) -> None:
        """Test precision settings validation."""
        config = PortfolioConfiguration()
        # Set inconsistent precision values
        config.balance.precision = 8
        config.position.precision = 6
        config.pnl.precision = 4  # Inconsistent precision

        errors = ConfigurationValidator.validate_precision_settings(config)
        assert any("Inconsistent decimal precision" in error for error in errors)


class TestConfigurationFactory:
    """Test cases for PortfolioConfigFactory."""

    def test_factory_development(self) -> None:
        """Test factory development configuration."""
        config = PortfolioConfigFactory.create_development()
        assert config.debug_mode is True
        assert config.log_level == "DEBUG"

    def test_factory_production(self) -> None:
        """Test factory production configuration."""
        config = PortfolioConfigFactory.create_production()
        assert config.debug_mode is False
        assert config.log_level == "INFO"

    def test_factory_test(self) -> None:
        """Test factory test configuration."""
        config = PortfolioConfigFactory.create_test()
        assert config.debug_mode is True
        assert config.cache.max_size == 100

    def test_factory_minimal(self) -> None:
        """Test factory minimal configuration."""
        config = PortfolioConfigFactory.create_minimal()
        assert config.cache.max_size == 100
        assert config.monitoring.enabled is False
        assert config.balance.precision == 2

    def test_factory_high_performance(self) -> None:
        """Test factory high performance configuration."""
        config = PortfolioConfigFactory.create_high_performance()
        assert config.cache.max_size == 100000
        assert config.concurrency.max_concurrent_operations == 50
        assert config.log_level == "WARNING"

    def test_factory_custom(self) -> None:
        """Test factory custom configuration."""
        cache_config = CacheConfiguration(max_size=2000)
        config = PortfolioConfigFactory.create_custom(debug_mode=True, cache=cache_config)
        assert config.debug_mode is True
        assert config.cache.max_size == 2000

    def test_factory_validation_report(self) -> None:
        """Test factory validation report."""
        config = PortfolioConfiguration()
        # Create invalid configuration
        config.screening.min_price_value = "100"
        config.screening.max_price_value = "50"

        report = PortfolioConfigFactory.get_validation_report(config)
        assert report.is_valid is False
        assert report.error_count > 0
        assert len(report.critical_errors) > 0


class TestValidatedConfiguration:
    """Test cases for create_validated_configuration function."""

    def test_create_validated_config_valid(self) -> None:
        """Test creating valid configuration."""
        config = create_validated_configuration(
            debug_mode=True, cache={"max_size": 1000}, pricing={"default_cache_ttl": 30.0}
        )
        assert config.debug_mode is True
        assert config.cache.max_size == 1000
        assert config.pricing.default_cache_ttl == 30.0

    def test_create_validated_config_invalid_field(self) -> None:
        """Test creating configuration with invalid field."""
        with pytest.raises(ValidationError):
            create_validated_configuration(
                cache={"max_size": -1}  # Invalid negative size
            )

    def test_create_validated_config_invalid_business_logic(self) -> None:
        """Test creating configuration with invalid business logic."""
        with pytest.raises(ValueError) as exc_info:
            create_validated_configuration(
                screening={
                    "min_price_value": "100",
                    "max_price_value": "50",  # Invalid: min > max
                }
            )
        assert "Configuration validation failed" in str(exc_info.value)
        assert "must be less than maximum price" in str(exc_info.value)


class TestConvenienceFunctions:
    """Test cases for convenience configuration functions."""

    def test_create_dev_config(self) -> None:
        """Test create_dev_config convenience function."""
        config = create_dev_config()
        assert config.debug_mode is True
        assert config.log_level == "DEBUG"

    def test_create_prod_config(self) -> None:
        """Test create_prod_config convenience function."""
        config = create_prod_config()
        assert config.debug_mode is False
        assert config.log_level == "INFO"

    def test_create_test_config(self) -> None:
        """Test create_test_config convenience function."""
        config = create_test_config()
        assert config.debug_mode is True
        assert config.cache.max_size == 100


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_configuration(self) -> None:
        """Test empty configuration uses all defaults."""
        config = create_validated_configuration()
        assert config.debug_mode is False
        assert config.log_level == "INFO"
        assert config.cache.max_size == 10000

    def test_partial_nested_config(self) -> None:
        """Test partial nested configuration merging."""
        config = create_validated_configuration(
            cache={"max_size": 5000}  # Only override one field
        )
        assert config.cache.max_size == 5000
        assert config.cache.default_ttl == 3600.0  # Default preserved
        assert config.cache.cleanup_interval == 300.0  # Default preserved

    def test_string_coercion(self) -> None:
        """Test string values are properly coerced to correct types."""
        config = PortfolioConfiguration()
        config.merge_with_dict({
            "cache": {
                "max_size": "1000",  # String that should be coerced to int
                "default_ttl": "60.5",  # String that should be coerced to float
            }
        })
        assert config.cache.max_size == 1000
        assert config.cache.default_ttl == 60.5

    def test_extreme_but_valid_values(self) -> None:
        """Test extreme but still valid values."""
        # Test maximum valid values
        config = CacheConfiguration(
            max_size=1000000,  # Maximum allowed
            default_ttl=86400.0,  # Maximum allowed (24 hours)
            cleanup_interval=86400.0,  # Maximum allowed
        )
        assert config.max_size == 1000000

        # Test minimum valid values
        config2 = BalanceConfiguration(
            precision=0,  # Minimum allowed
            cleanup_interval=1,  # Minimum positive
            max_balance_age=1,  # Minimum positive
        )
        assert config2.precision == 0

    def test_configuration_to_dict(self) -> None:
        """Test configuration serialization to dictionary."""
        config = PortfolioConfiguration()
        config_dict = config.to_dict()

        assert isinstance(config_dict, dict)
        assert config_dict["debug_mode"] is False
        assert config_dict["log_level"] == "INFO"
        assert isinstance(config_dict["cache"], dict)
        assert config_dict["cache"]["max_size"] == 10000
