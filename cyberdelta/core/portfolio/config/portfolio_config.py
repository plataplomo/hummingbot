"""Portfolio configuration classes for the modular architecture."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from pydantic import Field, TypeAdapter, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.portfolio.exceptions import (
    ConfigPrecisionTooHighError,
    ConfigValueTooLargeError,
    ConfigValueTooSmallError,
    InvalidConfigChoiceError,
    InvalidNumericStringError,
    NegativeConfigValueError,
    NonFiniteConfigValueError,
    NonPositiveConfigValueError,
)


if TYPE_CHECKING:
    pass

# Constants
MAX_CACHE_SIZE = 1000000  # 1M items max
MAX_TTL_SECONDS = 86400  # 24 hours max
MAX_CLEANUP_INTERVAL_SECONDS = 3600  # 1 hour max
MAX_MESSAGES_PER_INTERVAL = 1000  # Max messages per interval
SECONDS_PER_DAY = 86400
MAX_DECIMAL_PRECISION = 18  # Max decimal precision
MAX_BATCH_SIZE = 10000  # Max batch size
MAX_LEVERAGE = 100  # Max leverage
MAX_BATCH_SIZE_LIMIT = 1000  # Max batch size limit
MAX_TIME_VALUE_SECONDS = 3600  # 1 hour max
MAX_CONCURRENT_OPERATIONS = 100  # Max concurrent operations
MAX_LOCK_TIMEOUT_SECONDS = 300  # 5 minutes max


@dataclass
class CacheConfiguration:
    """Configuration for caching services."""

    max_size: int = Field(default=10000, gt=0)
    default_ttl: float = Field(default=3600.0, gt=0)  # 1 hour
    cleanup_interval: float = Field(default=300.0, gt=0)  # 5 minutes
    enabled: bool = True

    @field_validator("max_size", mode="before")
    @classmethod
    def validate_max_size(cls, v: int | str) -> int:
        """Validate cache max size is positive and within limits.
        
        Returns:
            int: The validated max size value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_CACHE_SIZE.
        """
        # Convert string to int if needed
        if isinstance(v, str):
            v = int(v)
        if v <= 0:
            raise NonPositiveConfigValueError(value=str(v), field_name="max_size", section="cache")
        if v > MAX_CACHE_SIZE:
            raise ConfigValueTooLargeError(
                value=str(v), field_name="max_size", section="cache", max_value=str(MAX_CACHE_SIZE)
            )
        return v

    @field_validator("default_ttl", "cleanup_interval", mode="before")
    @classmethod
    def validate_intervals(cls, v: float | str) -> float:
        """Validate TTL and cleanup intervals are positive.
        
        Returns:
            float: The validated interval value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_TTL_SECONDS.
        """
        # Convert string to float if needed
        if isinstance(v, str):
            v = float(v)
        if v <= 0:
            raise NonPositiveConfigValueError(value=str(v), field_name="interval", section="cache")
        if v > MAX_TTL_SECONDS:
            raise ConfigValueTooLargeError(
                value=str(v), field_name="interval", section="cache", max_value=str(MAX_TTL_SECONDS)
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
        return {
            "max_size": self.max_size,
            "default_ttl": self.default_ttl,
            "cleanup_interval": self.cleanup_interval,
            "enabled": self.enabled,
        }


@dataclass
class PricingConfiguration:
    """Configuration for pricing services."""

    default_cache_ttl: float = Field(default=60.0, gt=0)  # 1 minute
    batch_size_limit: int = Field(default=50, gt=0)
    price_staleness_threshold: float = Field(default=300.0, gt=0)  # 5 minutes
    enabled: bool = True

    @field_validator("batch_size_limit", mode="before")
    @classmethod
    def validate_batch_size(cls, v: int) -> int:
        """Validate batch size limit is positive.
        
        Returns:
            int: The validated batch size value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_BATCH_SIZE_LIMIT.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v), field_name="batch_size_limit", section="pricing"
            )
        if v > MAX_BATCH_SIZE_LIMIT:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name="batch_size_limit",
                section="pricing",
                max_value=str(MAX_BATCH_SIZE_LIMIT),
            )
        return v

    @field_validator("default_cache_ttl", "price_staleness_threshold", mode="before")
    @classmethod
    def validate_time_values(cls, v: float) -> float:
        """Validate time values are positive and within limits.
        
        Returns:
            float: The validated time value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_TIME_VALUE_SECONDS.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v), field_name="time_value", section="pricing"
            )
        if v > MAX_TIME_VALUE_SECONDS:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name="time_value",
                section="pricing",
                max_value=str(MAX_TIME_VALUE_SECONDS),
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
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
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
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

    @field_validator(
        "max_price_value",
        "max_quantity_value",
        "min_price_value",
        "min_quantity_value",
        mode="before",
    )
    @classmethod
    def validate_numeric_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate numeric string fields can be converted to Decimal.
        
        Returns:
            str: The validated numeric string.
            
        Raises:
            InvalidNumericStringError: If the string cannot be converted to Decimal.
            NonFiniteConfigValueError: If the value is not finite.
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds max bounds.
            ConfigValueTooSmallError: If the value is below min bounds.
        """
        # First convert to Decimal
        try:
            value = Decimal(v)
        except (ValueError, TypeError) as e:
            raise InvalidNumericStringError(
                value=str(v), field_name=info.field_name or "numeric_field", section="screening"
            ) from e
        except Exception as e:
            raise InvalidNumericStringError(
                value=str(v), field_name=info.field_name or "numeric_field", section="screening"
            ) from e

        # Then validate the value
        if not value.is_finite():
            raise NonFiniteConfigValueError(
                value=str(v), field_name=info.field_name or "numeric_field", section="screening"
            )
        if value <= 0:
            raise NonPositiveConfigValueError(
                value=str(v), field_name=info.field_name or "numeric_field", section="screening"
            )

        # Check bounds based on field name
        field_name = info.field_name or "numeric_field"
        # 1 billion max
        if field_name in {"max_price_value", "max_quantity_value"} and value > Decimal(1000000000):
            raise ConfigValueTooLargeError(
                value=str(v), field_name=field_name, section="screening", max_value="1000000000"
            )
        # 8 decimal places min
        if field_name in {"min_price_value", "min_quantity_value"} and value < Decimal(
            "0.00000001"
        ):
            raise ConfigValueTooSmallError(
                value=str(v), field_name=field_name, section="screening", min_value="0.00000001"
            )

        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
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
    cleanup_interval: int = Field(default=300, gt=0)  # 5 minutes
    max_balance_age: int = Field(default=3600, gt=0)  # 1 hour
    precision: int = Field(default=8, ge=0, le=18)

    @field_validator("cleanup_interval", "max_balance_age", mode="before")
    @classmethod
    def validate_time_intervals(cls, v: int, info: ValidationInfo) -> int:
        """Validate time interval values are positive.
        
        Returns:
            int: The validated time interval value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_TTL_SECONDS.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v), field_name=info.field_name or "interval_field", section="balance"
            )
        if v > MAX_TTL_SECONDS:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name=info.field_name or "interval_field",
                section="balance",
                max_value=str(MAX_TTL_SECONDS),
            )
        return v

    @field_validator("precision", mode="before")
    @classmethod
    def validate_precision(cls, v: int, info: ValidationInfo) -> int:
        """Validate precision is non-negative and within limits.
        
        Returns:
            int: The validated precision value.
            
        Raises:
            NegativeConfigValueError: If the value is negative.
            ConfigPrecisionTooHighError: If the value exceeds MAX_DECIMAL_PRECISION.
        """
        if v < 0:
            raise NegativeConfigValueError(
                value=str(v), field_name=info.field_name or "precision", section="balance"
            )
        if v > MAX_DECIMAL_PRECISION:
            raise ConfigPrecisionTooHighError(
                value=str(v),
                field_name=info.field_name or "precision",
                section="balance",
                max_precision=MAX_DECIMAL_PRECISION,
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
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
    cleanup_interval: int = Field(default=300, gt=0)  # 5 minutes
    max_position_age: int = Field(default=3600, gt=0)  # 1 hour
    precision: int = Field(default=8, ge=0, le=18)

    @field_validator("cleanup_interval", "max_position_age", mode="before")
    @classmethod
    def validate_time_intervals(cls, v: int, info: ValidationInfo) -> int:
        """Validate time intervals are positive.
        
        Returns:
            int: The validated time interval value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_TTL_SECONDS.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v), field_name=info.field_name or "interval_field", section="position"
            )
        if v > MAX_TTL_SECONDS:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name=info.field_name or "interval_field",
                section="position",
                max_value=str(MAX_TTL_SECONDS),
            )
        return v

    @field_validator("precision", mode="before")
    @classmethod
    def validate_precision(cls, v: int, info: ValidationInfo) -> int:
        """Validate precision is non-negative and within limits.
        
        Returns:
            int: The validated precision value.
            
        Raises:
            NegativeConfigValueError: If the value is negative.
            ConfigPrecisionTooHighError: If the value exceeds MAX_DECIMAL_PRECISION.
        """
        if v < 0:
            raise NegativeConfigValueError(
                value=str(v), field_name=info.field_name or "precision", section="position"
            )
        if v > MAX_DECIMAL_PRECISION:
            raise ConfigPrecisionTooHighError(
                value=str(v),
                field_name=info.field_name or "precision",
                section="position",
                max_precision=MAX_DECIMAL_PRECISION,
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
        return {
            "auto_cleanup_enabled": self.auto_cleanup_enabled,
            "cleanup_interval": self.cleanup_interval,
            "max_position_age": self.max_position_age,
            "precision": self.precision,
        }


@dataclass
class OrderConfiguration:
    """Configuration for order management."""

    max_orders_per_exchange: int = Field(default=1000, gt=0)
    cleanup_completed_orders: bool = True
    auto_cleanup_enabled: bool = True
    cleanup_interval: int = Field(default=300, gt=0)  # 5 minutes

    @field_validator("max_orders_per_exchange", mode="before")
    @classmethod
    def validate_max_orders(cls, v: int, info: ValidationInfo) -> int:
        """Validate maximum orders per exchange is positive.
        
        Returns:
            int: The validated max orders value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_BATCH_SIZE.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v),
                field_name=info.field_name or "max_orders_per_exchange",
                section="order",
            )
        if v > MAX_BATCH_SIZE:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name=info.field_name or "max_orders_per_exchange",
                section="order",
                max_value=str(MAX_BATCH_SIZE),
            )
        return v

    @field_validator("cleanup_interval", mode="before")
    @classmethod
    def validate_cleanup_interval(cls, v: int, info: ValidationInfo) -> int:
        """Validate cleanup interval is positive.
        
        Returns:
            int: The validated cleanup interval value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_TTL_SECONDS.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v), field_name=info.field_name or "cleanup_interval", section="order"
            )
        if v > MAX_TTL_SECONDS:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name=info.field_name or "cleanup_interval",
                section="order",
                max_value=str(MAX_TTL_SECONDS),
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
        return {
            "max_orders_per_exchange": self.max_orders_per_exchange,
            "cleanup_completed_orders": self.cleanup_completed_orders,
            "auto_cleanup_enabled": self.auto_cleanup_enabled,
            "cleanup_interval": self.cleanup_interval,
        }


@dataclass
class PnLConfiguration:
    """Configuration for P&L calculations."""

    calculation_method: str = Field(default="FIFO")
    precision: int = Field(default=8, ge=0, le=18)
    cache_results: bool = True
    real_time_updates: bool = True

    @field_validator("calculation_method", mode="before")
    @classmethod
    def validate_calculation_method(cls, v: str, info: ValidationInfo) -> str:
        """Validate calculation method is a supported value.
        
        Returns:
            str: The validated calculation method.
            
        Raises:
            InvalidConfigChoiceError: If the method is not in valid_methods.
        """
        valid_methods = {"FIFO", "LIFO", "WEIGHTED_AVERAGE"}
        if v not in valid_methods:
            raise InvalidConfigChoiceError(
                value=v,
                field_name=info.field_name or "calculation_method",
                section="pnl",
                valid_choices=list(valid_methods),
            )
        return v

    @field_validator("precision", mode="before")
    @classmethod
    def validate_precision(cls, v: int, info: ValidationInfo) -> int:
        """Validate precision is non-negative and within limits.
        
        Returns:
            int: The validated precision value.
            
        Raises:
            NegativeConfigValueError: If the value is negative.
            ConfigPrecisionTooHighError: If the value exceeds MAX_DECIMAL_PRECISION.
        """
        if v < 0:
            raise NegativeConfigValueError(
                value=str(v), field_name=info.field_name or "precision", section="pnl"
            )
        if v > MAX_DECIMAL_PRECISION:
            raise ConfigPrecisionTooHighError(
                value=str(v),
                field_name=info.field_name or "precision",
                section="pnl",
                max_precision=MAX_DECIMAL_PRECISION,
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
        return {
            "calculation_method": self.calculation_method,
            "precision": self.precision,
            "cache_results": self.cache_results,
            "real_time_updates": self.real_time_updates,
        }


@dataclass
class ConcurrencyConfiguration:
    """Configuration for concurrency management."""

    max_concurrent_operations: int = Field(default=10, gt=0)
    lock_timeout: float = Field(default=30.0, gt=0)  # 30 seconds
    deadlock_detection: bool = True

    @field_validator("max_concurrent_operations", mode="before")
    @classmethod
    def validate_max_operations(cls, v: int, info: ValidationInfo) -> int:
        """Validate maximum concurrent operations is positive.
        
        Returns:
            int: The validated max operations value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_CONCURRENT_OPERATIONS.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v),
                field_name=info.field_name or "max_concurrent_operations",
                section="concurrency",
            )
        if v > MAX_CONCURRENT_OPERATIONS:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name=info.field_name or "max_concurrent_operations",
                section="concurrency",
                max_value=str(MAX_CONCURRENT_OPERATIONS),
            )
        return v

    @field_validator("lock_timeout", mode="before")
    @classmethod
    def validate_lock_timeout(cls, v: float, info: ValidationInfo) -> float:
        """Validate lock timeout is positive.
        
        Returns:
            float: The validated lock timeout value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_LOCK_TIMEOUT_SECONDS.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v), field_name=info.field_name or "lock_timeout", section="concurrency"
            )
        if v > MAX_LOCK_TIMEOUT_SECONDS:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name=info.field_name or "lock_timeout",
                section="concurrency",
                max_value=str(MAX_LOCK_TIMEOUT_SECONDS),
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
        return {
            "max_concurrent_operations": self.max_concurrent_operations,
            "lock_timeout": self.lock_timeout,
            "deadlock_detection": self.deadlock_detection,
        }


@dataclass
class StateManagerConfiguration:
    """Configuration for state management."""

    auto_cleanup_enabled: bool = True
    cleanup_interval: int = Field(default=300, gt=0)  # 5 minutes
    strict_validation: bool = True
    enable_snapshots: bool = True
    snapshot_interval: int = Field(default=3600, gt=0)  # 1 hour

    @field_validator("cleanup_interval", "snapshot_interval", mode="before")
    @classmethod
    def validate_intervals(cls, v: int, info: ValidationInfo) -> int:
        """Validate intervals are positive.
        
        Returns:
            int: The validated interval value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_TTL_SECONDS.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v),
                field_name=info.field_name or "interval_field",
                section="state_manager",
            )
        if v > MAX_TTL_SECONDS:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name=info.field_name or "interval_field",
                section="state_manager",
                max_value=str(MAX_TTL_SECONDS),
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
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
    metrics_interval: int = Field(default=60, gt=0)  # 1 minute
    health_check_interval: int = Field(default=30, gt=0)  # 30 seconds
    performance_tracking: bool = True

    @field_validator("metrics_interval", "health_check_interval", mode="before")
    @classmethod
    def validate_intervals(cls, v: int, info: ValidationInfo) -> int:
        """Validate metrics and health check intervals are positive.
        
        Returns:
            int: The validated interval value.
            
        Raises:
            NonPositiveConfigValueError: If the value is not positive.
            ConfigValueTooLargeError: If the value exceeds MAX_TIME_VALUE_SECONDS.
        """
        if v <= 0:
            raise NonPositiveConfigValueError(
                value=str(v), field_name=info.field_name or "interval_field", section="monitoring"
            )
        if v > MAX_TIME_VALUE_SECONDS:
            raise ConfigValueTooLargeError(
                value=str(v),
                field_name=info.field_name or "interval_field",
                section="monitoring",
                max_value=str(MAX_TIME_VALUE_SECONDS),
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
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
    cache: CacheConfiguration = Field(default_factory=CacheConfiguration)
    pricing: PricingConfiguration = Field(default_factory=PricingConfiguration)
    symbol: SymbolConfiguration = Field(default_factory=SymbolConfiguration)
    screening: ScreeningConfiguration = Field(default_factory=ScreeningConfiguration)
    balance: BalanceConfiguration = Field(default_factory=BalanceConfiguration)
    position: PositionConfiguration = Field(default_factory=PositionConfiguration)
    order: OrderConfiguration = Field(default_factory=OrderConfiguration)
    pnl: PnLConfiguration = Field(default_factory=PnLConfiguration)
    concurrency: ConcurrencyConfiguration = Field(default_factory=ConcurrencyConfiguration)
    state_manager: StateManagerConfiguration = Field(default_factory=StateManagerConfiguration)
    monitoring: MonitoringConfiguration = Field(default_factory=MonitoringConfiguration)

    # Global settings
    debug_mode: bool = False
    log_level: str = Field(default="INFO")

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, v: str, info: ValidationInfo) -> str:
        """Validate log level is a supported value.
        
        Returns:
            str: The validated log level.
            
        Raises:
            InvalidConfigChoiceError: If the log level is not in valid_levels.
        """
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v not in valid_levels:
            raise InvalidConfigChoiceError(
                value=v,
                field_name=info.field_name or "log_level",
                section="global",
                valid_choices=list(valid_levels),
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format for factory usage.
        
        Returns:
            dict[str, Any]: Dictionary representation of the configuration.
        """
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

    def _update_nested_config(
        self, key: str, current_config: object, updates: dict[str, Any]
    ) -> None:
        """Update nested configuration with validation."""
        # Mapping of keys to config types and attributes
        config_map = {
            "cache": (CacheConfiguration, "cache"),
            "pricing": (PricingConfiguration, "pricing"),
            "symbol": (SymbolConfiguration, "symbol"),
            "screening": (ScreeningConfiguration, "screening"),
            "balance": (BalanceConfiguration, "balance"),
            "position": (PositionConfiguration, "position"),
            "order": (OrderConfiguration, "order"),
            "pnl": (PnLConfiguration, "pnl"),
            "concurrency": (ConcurrencyConfiguration, "concurrency"),
            "state_manager": (StateManagerConfiguration, "state_manager"),
            "monitoring": (MonitoringConfiguration, "monitoring"),
        }

        if key in config_map:
            config_type, attr_name = config_map[key]
            adapter: TypeAdapter[Any] = TypeAdapter(config_type)
            temp_dict = adapter.dump_python(current_config)
            temp_dict.update(updates)
            validated_config = adapter.validate_python(temp_dict)

            # Direct property assignment using mapping
            self._assign_config_property(attr_name, validated_config)

    def _assign_cache_config(self, validated_config: object) -> None:
        """Assign cache configuration."""
        self.cache = cast(CacheConfiguration, validated_config)

    def _assign_pricing_config(self, validated_config: object) -> None:
        """Assign pricing configuration."""
        self.pricing = cast(PricingConfiguration, validated_config)

    def _assign_symbol_config(self, validated_config: object) -> None:
        """Assign symbol configuration."""
        self.symbol = cast(SymbolConfiguration, validated_config)

    def _assign_screening_config(self, validated_config: object) -> None:
        """Assign screening configuration."""
        self.screening = cast(ScreeningConfiguration, validated_config)

    def _assign_balance_config(self, validated_config: object) -> None:
        """Assign balance configuration."""
        self.balance = cast(BalanceConfiguration, validated_config)

    def _assign_position_config(self, validated_config: object) -> None:
        """Assign position configuration."""
        self.position = cast(PositionConfiguration, validated_config)

    def _assign_order_config(self, validated_config: object) -> None:
        """Assign order configuration."""
        self.order = cast(OrderConfiguration, validated_config)

    def _assign_pnl_config(self, validated_config: object) -> None:
        """Assign pnl configuration."""
        self.pnl = cast(PnLConfiguration, validated_config)

    def _assign_concurrency_config(self, validated_config: object) -> None:
        """Assign concurrency configuration."""
        self.concurrency = cast(ConcurrencyConfiguration, validated_config)

    def _assign_state_manager_config(self, validated_config: object) -> None:
        """Assign state manager configuration."""
        self.state_manager = cast(StateManagerConfiguration, validated_config)

    def _assign_monitoring_config(self, validated_config: object) -> None:
        """Assign monitoring configuration."""
        self.monitoring = cast(MonitoringConfiguration, validated_config)

    def _assign_config_property(self, attr_name: str, validated_config: object) -> None:
        """Assign validated configuration to appropriate property."""
        assignment_methods = {
            "cache": self._assign_cache_config,
            "pricing": self._assign_pricing_config,
            "symbol": self._assign_symbol_config,
            "screening": self._assign_screening_config,
            "balance": self._assign_balance_config,
            "position": self._assign_position_config,
            "order": self._assign_order_config,
            "pnl": self._assign_pnl_config,
            "concurrency": self._assign_concurrency_config,
            "state_manager": self._assign_state_manager_config,
            "monitoring": self._assign_monitoring_config,
        }

        if attr_name in assignment_methods:
            assignment_methods[attr_name](validated_config)

    def merge_with_dict(self, updates: dict[str, Any]) -> None:
        """Merge configuration with dictionary updates.

        Args:
            updates: Dictionary containing configuration updates
        """
        # Define known configuration field mappings for type safety
        field_mappings = {
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

        for key, value in updates.items():
            if key in field_mappings and isinstance(value, dict):
                current_config = field_mappings[key]
                # Type-cast to satisfy pyright - we know it's a dict from isinstance check
                typed_value = cast(dict[str, Any], value)
                self._update_nested_config(key, current_config, typed_value)
            elif key == "debug_mode":
                self.debug_mode = value
            elif key == "log_level":
                self.log_level = value

    @classmethod
    def create_default(cls) -> PortfolioConfiguration:
        """Create default configuration.
        
        Returns:
            PortfolioConfiguration: A default configuration instance.
        """
        return cls()

    @classmethod
    def create_development(cls) -> PortfolioConfiguration:
        """Create development-friendly configuration.
        
        Returns:
            PortfolioConfiguration: A development configuration instance.
        """
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
        """Create production-optimized configuration.
        
        Returns:
            PortfolioConfiguration: A production configuration instance.
        """
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
        """Create test-friendly configuration.
        
        Returns:
            PortfolioConfiguration: A test configuration instance.
        """
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
        """Validate cache configuration.
        
        Returns:
            list[str]: List of validation errors (empty if valid).
        """
        errors: list[str] = []
        # Validation now handled by Pydantic
        return errors

    def _validate_pricing_config(self) -> list[str]:
        """Validate pricing configuration.
        
        Returns:
            list[str]: List of validation errors (empty if valid).
        """
        errors: list[str] = []
        # Validation now handled by Pydantic
        return errors

    def _validate_screening_config(self) -> list[str]:
        """Validate screening configuration.
        
        Returns:
            list[str]: List of validation errors (empty if valid).
        """
        errors: list[str] = []
        # Validation now handled by Pydantic
        return errors

    def _validate_pnl_config(self) -> list[str]:
        """Validate P&L configuration.
        
        Returns:
            list[str]: List of validation errors (empty if valid).
        """
        errors: list[str] = []
        # Validation now handled by Pydantic
        return errors

    def _validate_concurrency_config(self) -> list[str]:
        """Validate concurrency configuration.
        
        Returns:
            list[str]: List of validation errors (empty if valid).
        """
        errors: list[str] = []
        # Validation now handled by Pydantic
        return errors

    def _validate_log_level(self) -> list[str]:
        """Validate log level.
        
        Returns:
            list[str]: List of validation errors (empty if valid).
        """
        errors: list[str] = []
        # Validation now handled by Pydantic
        return errors

    def validate(self) -> list[str]:
        """Validate configuration and return list of errors.
        
        Returns:
            list[str]: List of validation errors (empty if valid).
        """
        errors: list[str] = []
        # Most validation is now handled by Pydantic automatically
        # This method is kept for backward compatibility
        return errors

    # Note: merge_with_dict method removed in favor of direct Pydantic dataclass usage
