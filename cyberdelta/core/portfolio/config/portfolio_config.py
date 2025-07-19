"""Portfolio configuration classes for the modular architecture."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast


@dataclass
class CacheConfiguration:
    """Configuration for caching services."""

    max_size: int = 10000
    default_ttl: float = 3600.0  # 1 hour
    cleanup_interval: float = 300.0  # 5 minutes
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "max_size": self.max_size,
            "default_ttl": self.default_ttl,
            "cleanup_interval": self.cleanup_interval,
            "enabled": self.enabled,
        }


@dataclass
class PricingConfiguration:
    """Configuration for pricing services."""

    default_cache_ttl: float = 60.0  # 1 minute
    batch_size_limit: int = 50
    price_staleness_threshold: float = 300.0  # 5 minutes
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "default_cache_ttl": self.default_cache_ttl,
            "batch_size_limit": self.batch_size_limit,
            "price_staleness_threshold": self.price_staleness_threshold,
            "enabled": self.enabled,
        }


@dataclass
class SymbolConfiguration:
    """Configuration for symbol services."""

    fallback_enabled: bool = True
    strict_mode: bool = False
    symbol_validation_enabled: bool = True
    cache_enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "fallback_enabled": self.fallback_enabled,
            "strict_mode": self.strict_mode,
            "symbol_validation_enabled": self.symbol_validation_enabled,
            "cache_enabled": self.cache_enabled,
        }


@dataclass
class ScreeningConfiguration:
    """Configuration for data screening components."""

    strict_mode: bool = False
    allow_zero_quantities: bool = False
    allow_negative_prices: bool = False
    max_price_value: str = "1000000"
    max_quantity_value: str = "1000000"
    min_price_value: str = "0.00001"
    min_quantity_value: str = "0.00001"
    require_trade_id: bool = True
    require_exchange_id: bool = True
    symbol_validation_enabled: bool = True
    log_validation_errors: bool = True
    log_validation_warnings: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "strict_mode": self.strict_mode,
            "allow_zero_quantities": self.allow_zero_quantities,
            "allow_negative_prices": self.allow_negative_prices,
            "max_price_value": self.max_price_value,
            "max_quantity_value": self.max_quantity_value,
            "min_price_value": self.min_price_value,
            "min_quantity_value": self.min_quantity_value,
            "require_trade_id": self.require_trade_id,
            "require_exchange_id": self.require_exchange_id,
            "symbol_validation_enabled": self.symbol_validation_enabled,
            "log_validation_errors": self.log_validation_errors,
            "log_validation_warnings": self.log_validation_warnings,
        }


@dataclass
class BalanceConfiguration:
    """Configuration for balance management."""

    auto_cleanup_enabled: bool = True
    cleanup_interval: int = 300  # 5 minutes
    max_balance_age: int = 3600  # 1 hour
    precision: int = 8

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "auto_cleanup_enabled": self.auto_cleanup_enabled,
            "cleanup_interval": self.cleanup_interval,
            "max_balance_age": self.max_balance_age,
            "precision": self.precision,
        }


@dataclass
class PositionConfiguration:
    """Configuration for position management."""

    auto_cleanup_enabled: bool = True
    cleanup_interval: int = 300  # 5 minutes
    max_position_age: int = 3600  # 1 hour
    precision: int = 8

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "auto_cleanup_enabled": self.auto_cleanup_enabled,
            "cleanup_interval": self.cleanup_interval,
            "max_position_age": self.max_position_age,
            "precision": self.precision,
        }


@dataclass
class OrderConfiguration:
    """Configuration for order management."""

    max_orders_per_exchange: int = 1000
    cleanup_completed_orders: bool = True
    auto_cleanup_enabled: bool = True
    cleanup_interval: int = 300  # 5 minutes

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "max_orders_per_exchange": self.max_orders_per_exchange,
            "cleanup_completed_orders": self.cleanup_completed_orders,
            "auto_cleanup_enabled": self.auto_cleanup_enabled,
            "cleanup_interval": self.cleanup_interval,
        }


@dataclass
class PnLConfiguration:
    """Configuration for P&L calculations."""

    calculation_method: str = "FIFO"
    precision: int = 8
    cache_results: bool = True
    real_time_updates: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "calculation_method": self.calculation_method,
            "precision": self.precision,
            "cache_results": self.cache_results,
            "real_time_updates": self.real_time_updates,
        }


@dataclass
class ConcurrencyConfiguration:
    """Configuration for concurrency management."""

    max_concurrent_operations: int = 10
    lock_timeout: float = 30.0  # 30 seconds
    deadlock_detection: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "max_concurrent_operations": self.max_concurrent_operations,
            "lock_timeout": self.lock_timeout,
            "deadlock_detection": self.deadlock_detection,
        }


@dataclass
class StateManagerConfiguration:
    """Configuration for state management."""

    auto_cleanup_enabled: bool = True
    cleanup_interval: int = 300  # 5 minutes
    strict_validation: bool = True
    enable_snapshots: bool = True
    snapshot_interval: int = 3600  # 1 hour

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "auto_cleanup_enabled": self.auto_cleanup_enabled,
            "cleanup_interval": self.cleanup_interval,
            "strict_validation": self.strict_validation,
            "enable_snapshots": self.enable_snapshots,
            "snapshot_interval": self.snapshot_interval,
        }


@dataclass
class MonitoringConfiguration:
    """Configuration for monitoring and observability."""

    enabled: bool = True
    metrics_interval: int = 60  # 1 minute
    health_check_interval: int = 30  # 30 seconds
    performance_tracking: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "enabled": self.enabled,
            "metrics_interval": self.metrics_interval,
            "health_check_interval": self.health_check_interval,
            "performance_tracking": self.performance_tracking,
        }


@dataclass
class PortfolioConfiguration:
    """Main configuration class for the portfolio system."""

    # Component configurations
    cache: CacheConfiguration = field(default_factory=CacheConfiguration)
    pricing: PricingConfiguration = field(default_factory=PricingConfiguration)
    symbol: SymbolConfiguration = field(default_factory=SymbolConfiguration)
    screening: ScreeningConfiguration = field(default_factory=ScreeningConfiguration)
    balance: BalanceConfiguration = field(default_factory=BalanceConfiguration)
    position: PositionConfiguration = field(default_factory=PositionConfiguration)
    order: OrderConfiguration = field(default_factory=OrderConfiguration)
    pnl: PnLConfiguration = field(default_factory=PnLConfiguration)
    concurrency: ConcurrencyConfiguration = field(default_factory=ConcurrencyConfiguration)
    state_manager: StateManagerConfiguration = field(default_factory=StateManagerConfiguration)
    monitoring: MonitoringConfiguration = field(default_factory=MonitoringConfiguration)

    # Global settings
    debug_mode: bool = False
    log_level: str = "INFO"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format for factory usage."""
        return {
            "cache": self.cache.to_dict(),
            "pricing": self.pricing.to_dict(),
            "symbol": self.symbol.to_dict(),
            "screening": self.screening.to_dict(),
            "balance": self.balance.to_dict(),
            "position": self.position.to_dict(),
            "order": self.order.to_dict(),
            "pnl": self.pnl.to_dict(),
            "concurrency": self.concurrency.to_dict(),
            "state_manager": self.state_manager.to_dict(),
            "monitoring": self.monitoring.to_dict(),
            "debug_mode": self.debug_mode,
            "log_level": self.log_level,
        }

    @classmethod
    def create_default(cls) -> PortfolioConfiguration:
        """Create default configuration."""
        return cls()

    @classmethod
    def create_development(cls) -> PortfolioConfiguration:
        """Create development-friendly configuration."""
        config = cls()
        config.debug_mode = True
        config.log_level = "DEBUG"
        config.cache.cleanup_interval = 30.0
        config.pricing.default_cache_ttl = 10.0
        config.screening.strict_mode = True
        config.state_manager.cleanup_interval = 60
        config.monitoring.metrics_interval = 10
        return config

    @classmethod
    def create_production(cls) -> PortfolioConfiguration:
        """Create production-optimized configuration."""
        config = cls()
        config.debug_mode = False
        config.log_level = "INFO"
        config.cache.max_size = 50000
        config.cache.default_ttl = 7200.0  # 2 hours
        config.pricing.batch_size_limit = 100
        config.screening.strict_mode = False
        config.state_manager.strict_validation = True
        config.monitoring.enabled = True
        return config

    @classmethod
    def create_test(cls) -> PortfolioConfiguration:
        """Create test-friendly configuration."""
        config = cls()
        config.debug_mode = True
        config.log_level = "DEBUG"
        config.cache.max_size = 100
        config.cache.default_ttl = 60.0
        config.cache.cleanup_interval = 5.0
        config.pricing.default_cache_ttl = 1.0
        config.pricing.batch_size_limit = 5
        config.screening.strict_mode = True
        config.state_manager.auto_cleanup_enabled = False
        config.state_manager.cleanup_interval = 10
        config.concurrency.max_concurrent_operations = 3
        config.concurrency.lock_timeout = 2.0
        config.monitoring.metrics_interval = 5
        config.monitoring.health_check_interval = 5
        return config

    def _validate_cache_config(self) -> list[str]:
        """Validate cache configuration."""
        errors: list[str] = []
        if self.cache.max_size <= 0:
            errors.append("Cache max_size must be positive")
        if self.cache.default_ttl <= 0:
            errors.append("Cache default_ttl must be positive")
        if self.cache.cleanup_interval <= 0:
            errors.append("Cache cleanup_interval must be positive")
        return errors

    def _validate_pricing_config(self) -> list[str]:
        """Validate pricing configuration."""
        errors: list[str] = []
        if self.pricing.batch_size_limit <= 0:
            errors.append("Pricing batch_size_limit must be positive")
        if self.pricing.default_cache_ttl <= 0:
            errors.append("Pricing default_cache_ttl must be positive")
        return errors

    def _validate_screening_config(self) -> list[str]:
        """Validate screening configuration."""
        errors: list[str] = []
        try:
            float(self.screening.max_price_value)
            float(self.screening.min_price_value)
            float(self.screening.max_quantity_value)
            float(self.screening.min_quantity_value)
        except ValueError:
            errors.append("Screening price/quantity values must be valid numbers")
        return errors

    def _validate_pnl_config(self) -> list[str]:
        """Validate P&L configuration."""
        errors: list[str] = []
        valid_methods = ["FIFO", "LIFO", "WEIGHTED_AVERAGE"]
        if self.pnl.calculation_method not in valid_methods:
            errors.append(f"PnL calculation_method must be one of {valid_methods}")
        return errors

    def _validate_concurrency_config(self) -> list[str]:
        """Validate concurrency configuration."""
        errors: list[str] = []
        if self.concurrency.max_concurrent_operations <= 0:
            errors.append("Concurrency max_concurrent_operations must be positive")
        if self.concurrency.lock_timeout <= 0:
            errors.append("Concurrency lock_timeout must be positive")
        return errors

    def _validate_log_level(self) -> list[str]:
        """Validate log level."""
        errors: list[str] = []
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level not in valid_levels:
            errors.append(f"Log level must be one of {valid_levels}")
        return errors

    def validate(self) -> list[str]:
        """Validate configuration and return list of errors."""
        errors: list[str] = []
        errors.extend(self._validate_cache_config())
        errors.extend(self._validate_pricing_config())
        errors.extend(self._validate_screening_config())
        errors.extend(self._validate_pnl_config())
        errors.extend(self._validate_concurrency_config())
        errors.extend(self._validate_log_level())
        return errors

    def merge_with_dict(self, config_dict: dict[str, Any]) -> None:
        """Merge configuration with dictionary values using known fields."""
        # Define known configuration fields for type-safe access
        component_fields = {
            "cache": self.cache,
            "pricing": self.pricing,
            "symbol": self.symbol,
            "screening": self.screening,
            "balance": self.balance,
            "position": self.position,
            "order": self.order,
            "pnl": self.pnl,
            "concurrency": self.concurrency,
            "state_manager": self.state_manager,
            "monitoring": self.monitoring,
        }

        for key, value in config_dict.items():
            if key in component_fields and isinstance(value, dict):
                component_config = component_fields[key]
                # Merge sub-configuration using dataclass fields
                # After isinstance check, value is known to be dict
                for sub_key, sub_value in cast(dict[str, Any], value).items():
                    if getattr(component_config, sub_key, None) is not None:
                        setattr(component_config, sub_key, sub_value)
            elif key in {"debug_mode", "log_level"}:
                # Handle global settings
                setattr(self, key, value)
