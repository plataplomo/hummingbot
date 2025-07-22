"""Configuration factory with enhanced validation for portfolio system.

This module provides factory functions for creating validated portfolio configurations
using the enhanced validation utilities.
"""

from __future__ import annotations

import os

from pydantic import BaseModel, ConfigDict

from cyberdelta.core.portfolio.config.portfolio_config import (
    BalanceConfiguration,
    CacheConfiguration,
    ConcurrencyConfiguration,
    MonitoringConfiguration,
    OrderConfiguration,
    PnLConfiguration,
    PortfolioConfiguration,
    PositionConfiguration,
    PricingConfiguration,
    ScreeningConfiguration,
    StateManagerConfiguration,
    SymbolConfiguration,
)
from cyberdelta.core.portfolio.config.validation import (
    ConfigurationValidator,
    validate_startup_configuration,
)


class ValidationReport(BaseModel):
    """Validation report for portfolio configuration."""

    model_config = ConfigDict(frozen=True)

    is_valid: bool
    error_count: int
    errors: list[str]
    critical_errors: list[str]
    warnings: list[str]
    configuration_summary: ConfigurationSummary


class ConfigurationSummary(BaseModel):
    """Summary of configuration values."""

    model_config = ConfigDict(frozen=True)

    cache_size: int
    cache_ttl: float
    pricing_ttl: float
    max_concurrent_ops: int
    lock_timeout: float
    precision: PrecisionSummary
    debug_mode: bool
    log_level: str


class PrecisionSummary(BaseModel):
    """Summary of precision settings."""

    model_config = ConfigDict(frozen=True)

    balance: int
    position: int
    pnl: int


class PortfolioConfigFactory:
    """Factory for creating validated portfolio configurations."""

    @staticmethod
    def create_development() -> PortfolioConfiguration:
        """Create development-friendly configuration with enhanced validation.

        Returns:
            Validated portfolio configuration for development
        """
        config = PortfolioConfiguration.create_development()
        validate_startup_configuration(config)
        return config

    @staticmethod
    def create_production() -> PortfolioConfiguration:
        """Create production-optimized configuration with enhanced validation.

        Returns:
            Validated portfolio configuration for production
        """
        config = PortfolioConfiguration.create_production()
        validate_startup_configuration(config)
        return config

    @staticmethod
    def create_test() -> PortfolioConfiguration:
        """Create test-friendly configuration with enhanced validation.

        Returns:
            Validated portfolio configuration for testing
        """
        config = PortfolioConfiguration.create_test()
        validate_startup_configuration(config)
        return config

    @staticmethod
    def create_from_environment() -> PortfolioConfiguration:
        """Create configuration from environment variables with validation.

        Environment variables:
        - PORTFOLIO_DEBUG_MODE: Enable debug mode (default: false)
        - PORTFOLIO_LOG_LEVEL: Log level (default: INFO)
        - PORTFOLIO_CACHE_SIZE: Cache max size (default: 10000)
        - PORTFOLIO_CACHE_TTL: Cache TTL in seconds (default: 3600)
        - PORTFOLIO_PRICING_TTL: Pricing cache TTL (default: 60)
        - PORTFOLIO_MAX_LEVERAGE: Maximum leverage (default: 100)
        - PORTFOLIO_STRICT_VALIDATION: Enable strict validation (default: true)

        Returns:
            Validated portfolio configuration from environment
        """
        # Get environment variables with defaults
        debug_mode = os.getenv("PORTFOLIO_DEBUG_MODE", "false").lower() == "true"
        log_level = os.getenv("PORTFOLIO_LOG_LEVEL", "INFO").upper()

        # Cache settings
        cache_size = int(os.getenv("PORTFOLIO_CACHE_SIZE", "10000"))
        cache_ttl = float(os.getenv("PORTFOLIO_CACHE_TTL", "3600"))
        pricing_ttl = float(os.getenv("PORTFOLIO_PRICING_TTL", "60"))

        # Validation settings
        strict_validation = os.getenv("PORTFOLIO_STRICT_VALIDATION", "true").lower() == "true"

        # Build configuration using Pydantic dataclasses directly
        cache_config = CacheConfiguration(max_size=cache_size, default_ttl=cache_ttl)
        pricing_config = PricingConfiguration(default_cache_ttl=pricing_ttl)
        screening_config = ScreeningConfiguration(strict_mode=strict_validation)
        state_manager_config = StateManagerConfiguration(strict_validation=strict_validation)

        config = PortfolioConfiguration(
            cache=cache_config,
            pricing=pricing_config,
            screening=screening_config,
            state_manager=state_manager_config,
            debug_mode=debug_mode,
            log_level=log_level,
        )

        validate_startup_configuration(config)
        return config

    @staticmethod
    def create_custom(
        cache: CacheConfiguration | None = None,
        pricing: PricingConfiguration | None = None,
        symbol: SymbolConfiguration | None = None,
        screening: ScreeningConfiguration | None = None,
        balance: BalanceConfiguration | None = None,
        position: PositionConfiguration | None = None,
        order: OrderConfiguration | None = None,
        pnl: PnLConfiguration | None = None,
        concurrency: ConcurrencyConfiguration | None = None,
        state_manager: StateManagerConfiguration | None = None,
        monitoring: MonitoringConfiguration | None = None,
        debug_mode: bool | None = None,
        log_level: str | None = None,
    ) -> PortfolioConfiguration:
        """Create custom configuration with validation.

        Args:
            cache: Cache configuration override
            pricing: Pricing configuration override
            symbol: Symbol configuration override
            screening: Screening configuration override
            balance: Balance configuration override
            position: Position configuration override
            order: Order configuration override
            pnl: P&L configuration override
            concurrency: Concurrency configuration override
            state_manager: State manager configuration override
            monitoring: Monitoring configuration override
            debug_mode: Debug mode override
            log_level: Log level override

        Returns:
            Validated custom portfolio configuration
        """
        # Start with production base
        base_config = PortfolioConfiguration.create_production()

        # Apply overrides using proper Pydantic dataclasses
        config = PortfolioConfiguration(
            cache=cache or base_config.cache,
            pricing=pricing or base_config.pricing,
            symbol=symbol or base_config.symbol,
            screening=screening or base_config.screening,
            balance=balance or base_config.balance,
            position=position or base_config.position,
            order=order or base_config.order,
            pnl=pnl or base_config.pnl,
            concurrency=concurrency or base_config.concurrency,
            state_manager=state_manager or base_config.state_manager,
            monitoring=monitoring or base_config.monitoring,
            debug_mode=debug_mode if debug_mode is not None else base_config.debug_mode,
            log_level=log_level or base_config.log_level,
        )

        validate_startup_configuration(config)
        return config

    @staticmethod
    def create_minimal() -> PortfolioConfiguration:
        """Create minimal configuration for basic operations.

        Returns:
            Minimal validated portfolio configuration
        """
        minimal_config = PortfolioConfiguration(
            cache=CacheConfiguration(max_size=100, default_ttl=60.0),
            pricing=PricingConfiguration(default_cache_ttl=10.0, batch_size_limit=10),
            symbol=SymbolConfiguration(fallback_enabled=False, strict_mode=True),
            screening=ScreeningConfiguration(strict_mode=True),
            balance=BalanceConfiguration(precision=2),  # Lower precision for minimal
            position=PositionConfiguration(precision=2),
            order=OrderConfiguration(max_orders_per_exchange=100),
            pnl=PnLConfiguration(precision=2),
            concurrency=ConcurrencyConfiguration(max_concurrent_operations=5),
            state_manager=StateManagerConfiguration(
                auto_cleanup_enabled=False, enable_snapshots=False
            ),
            monitoring=MonitoringConfiguration(
                enabled=False,  # Minimal monitoring
                metrics_interval=300,
                health_check_interval=60,
            ),
            debug_mode=True,
            log_level="DEBUG",
        )

        validate_startup_configuration(minimal_config)
        return minimal_config

    @staticmethod
    def create_high_performance() -> PortfolioConfiguration:
        """Create high-performance configuration for trading systems.

        Returns:
            High-performance validated portfolio configuration
        """
        high_perf_config = PortfolioConfiguration(
            cache=CacheConfiguration(
                max_size=100000,  # Large cache
                default_ttl=1800.0,  # 30 minutes
                cleanup_interval=60.0,  # Frequent cleanup
            ),
            pricing=PricingConfiguration(
                default_cache_ttl=5.0,  # Very fast price updates
                batch_size_limit=200,  # Large batches
                price_staleness_threshold=10.0,  # Strict staleness
            ),
            symbol=SymbolConfiguration(
                fallback_enabled=True,
                strict_mode=False,  # Flexible for performance
                cache_enabled=True,
            ),
            screening=ScreeningConfiguration(
                strict_mode=False,  # Less strict for speed
                log_validation_errors=False,  # Reduce logging overhead
                log_validation_warnings=False,
            ),
            balance=BalanceConfiguration(
                auto_cleanup_enabled=True,
                cleanup_interval=30,  # Frequent cleanup
                precision=8,  # High precision
            ),
            position=PositionConfiguration(
                auto_cleanup_enabled=True, cleanup_interval=30, precision=8
            ),
            order=OrderConfiguration(
                max_orders_per_exchange=10000,  # High order capacity
                cleanup_completed_orders=True,
                cleanup_interval=60,
            ),
            pnl=PnLConfiguration(
                calculation_method="FIFO", precision=8, cache_results=True, real_time_updates=True
            ),
            concurrency=ConcurrencyConfiguration(
                max_concurrent_operations=50,  # High concurrency
                lock_timeout=10.0,  # Fast timeouts
                deadlock_detection=True,
            ),
            state_manager=StateManagerConfiguration(
                cleanup_interval=30,  # Frequent cleanup
                strict_validation=False,  # Faster validation
                enable_snapshots=True,
                snapshot_interval=300,  # 5-minute snapshots
            ),
            monitoring=MonitoringConfiguration(
                enabled=True,
                metrics_interval=10,  # Frequent metrics
                health_check_interval=5,  # Frequent health checks
                performance_tracking=True,
            ),
            debug_mode=False,
            log_level="WARNING",  # Reduced logging for performance
        )

        validate_startup_configuration(high_perf_config)
        return high_perf_config

    @classmethod
    def validate_and_create(cls, config: PortfolioConfiguration) -> PortfolioConfiguration:
        """Validate a portfolio configuration.

        Args:
            config: Portfolio configuration to validate

        Returns:
            Validated portfolio configuration

        Raises:
            ValidationError: If configuration is invalid
        """
        validate_startup_configuration(config)
        return config

    @classmethod
    def get_validation_report(cls, config: PortfolioConfiguration) -> ValidationReport:
        """Get a detailed validation report for a configuration.

        Args:
            config: Portfolio configuration to validate

        Returns:
            Validation report with results and recommendations
        """
        errors = ConfigurationValidator.validate_configuration(config)

        critical_errors = [
            error
            for error in errors
            if any(keyword in error for keyword in ["must", "exceeds reasonable limit"])
        ]

        warnings = [
            error
            for error in errors
            if not any(keyword in error for keyword in ["must", "exceeds reasonable limit"])
        ]

        precision_summary = PrecisionSummary(
            balance=config.balance.precision,
            position=config.position.precision,
            pnl=config.pnl.precision,
        )

        configuration_summary = ConfigurationSummary(
            cache_size=config.cache.max_size,
            cache_ttl=config.cache.default_ttl,
            pricing_ttl=config.pricing.default_cache_ttl,
            max_concurrent_ops=config.concurrency.max_concurrent_operations,
            lock_timeout=config.concurrency.lock_timeout,
            precision=precision_summary,
            debug_mode=config.debug_mode,
            log_level=config.log_level,
        )

        return ValidationReport(
            is_valid=len(errors) == 0,
            error_count=len(errors),
            errors=errors,
            critical_errors=critical_errors,
            warnings=warnings,
            configuration_summary=configuration_summary,
        )


# Convenience functions for common configurations
def create_dev_config() -> PortfolioConfiguration:
    """Create development configuration."""
    return PortfolioConfigFactory.create_development()


def create_prod_config() -> PortfolioConfiguration:
    """Create production configuration."""
    return PortfolioConfigFactory.create_production()


def create_test_config() -> PortfolioConfiguration:
    """Create test configuration."""
    return PortfolioConfigFactory.create_test()


def create_config_from_env() -> PortfolioConfiguration:
    """Create configuration from environment variables."""
    return PortfolioConfigFactory.create_from_environment()
