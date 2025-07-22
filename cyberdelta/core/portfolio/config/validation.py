"""Configuration validation utilities for portfolio system.

This module provides additional validation helpers for portfolio configurations
beyond the basic Pydantic field validation.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any, cast

from pydantic import TypeAdapter

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
from cyberdelta.core.portfolio.exceptions import ConfigurationValidationError


# Constants for validation limits
MAX_CACHE_TTL_SECONDS = 86400  # 24 hours
MAX_CACHE_SIZE_ENTRIES = 1000000  # 1M entries
MAX_LOCK_TIMEOUT_SECONDS = 300  # 5 minutes
MAX_CONCURRENT_OPERATIONS = 100
MAX_PRICE_LIMIT = 10000000  # 10M
MAX_QUANTITY_LIMIT = 1000000  # 1M
CLEANUP_INTERVAL_RATIO_THRESHOLD = 10  # Max ratio between cleanup intervals


class ConfigurationValidator:
    """Advanced configuration validation for portfolio settings."""

    @staticmethod
    def validate_financial_limits(config: PortfolioConfiguration) -> list[str]:
        """Validate financial configuration limits for consistency.

        Args:
            config: Portfolio configuration to validate

        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        # Validate screening configuration ranges
        screening = config.screening
        try:
            min_price = Decimal(screening.min_price_value)
            max_price = Decimal(screening.max_price_value)
            min_quantity = Decimal(screening.min_quantity_value)
            max_quantity = Decimal(screening.max_quantity_value)

            if min_price >= max_price:
                errors.append(
                    f"Minimum price ({min_price}) must be less than maximum price ({max_price})"
                )

            if min_quantity >= max_quantity:
                errors.append(
                    f"Minimum quantity ({min_quantity}) must be less than "
                    f"maximum quantity ({max_quantity})"
                )

            # Check for reasonable financial ranges
            if max_price > MAX_PRICE_LIMIT:
                errors.append(
                    f"Maximum price ({max_price}) exceeds reasonable limit ({MAX_PRICE_LIMIT:,})"
                )

            if max_quantity > MAX_QUANTITY_LIMIT:
                errors.append(
                    f"Maximum quantity ({max_quantity}) exceeds reasonable limit "
                    f"({MAX_QUANTITY_LIMIT:,})"
                )

        except (ValueError, TypeError) as e:
            errors.append(f"Invalid numeric string in screening configuration: {e}")

        return errors

    @staticmethod
    def validate_cache_settings(config: PortfolioConfiguration) -> list[str]:
        """Validate cache configuration settings.

        Args:
            config: Portfolio configuration to validate

        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        cache = config.cache
        pricing = config.pricing

        # Cache TTL should be reasonable
        if cache.default_ttl > MAX_CACHE_TTL_SECONDS:
            errors.append(
                f"Cache TTL ({cache.default_ttl}s) exceeds 24 hours - may cause stale data"
            )

        # Pricing cache should be shorter than general cache
        if pricing.default_cache_ttl > cache.default_ttl:
            errors.append(
                f"Pricing cache TTL ({pricing.default_cache_ttl}s) should not exceed "
                f"general cache TTL ({cache.default_ttl}s)"
            )

        # Cache size should be reasonable
        if cache.max_size > MAX_CACHE_SIZE_ENTRIES:
            errors.append(f"Cache size ({cache.max_size}) may consume excessive memory")

        return errors

    @staticmethod
    def validate_concurrency_settings(config: PortfolioConfiguration) -> list[str]:
        """Validate concurrency configuration settings.

        Args:
            config: Portfolio configuration to validate

        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        concurrency = config.concurrency

        # Lock timeout should be reasonable
        if concurrency.lock_timeout > MAX_LOCK_TIMEOUT_SECONDS:
            errors.append(
                f"Lock timeout ({concurrency.lock_timeout}s) exceeds 5 minutes - "
                "may cause deadlocks"
            )

        # Concurrent operations should match system capabilities
        if concurrency.max_concurrent_operations > MAX_CONCURRENT_OPERATIONS:
            errors.append(
                f"Max concurrent operations ({concurrency.max_concurrent_operations}) "
                "may overwhelm system resources"
            )

        return errors

    @staticmethod
    def validate_monitoring_intervals(config: PortfolioConfiguration) -> list[str]:
        """Validate monitoring configuration intervals.

        Args:
            config: Portfolio configuration to validate

        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        monitoring = config.monitoring
        state_manager = config.state_manager
        balance = config.balance
        position = config.position
        order = config.order

        # Health checks should be frequent enough
        if monitoring.health_check_interval > monitoring.metrics_interval:
            errors.append(
                f"Health check interval ({monitoring.health_check_interval}s) should not exceed "
                f"metrics interval ({monitoring.metrics_interval}s)"
            )

        # Cleanup intervals should be coordinated
        cleanup_intervals = [
            ("state_manager", state_manager.cleanup_interval),
            ("balance", balance.cleanup_interval),
            ("position", position.cleanup_interval),
            ("order", order.cleanup_interval),
        ]

        min_cleanup = min(interval for _, interval in cleanup_intervals)
        max_cleanup = max(interval for _, interval in cleanup_intervals)

        if max_cleanup > min_cleanup * CLEANUP_INTERVAL_RATIO_THRESHOLD:
            errors.append(
                f"Cleanup intervals vary too widely: {min_cleanup}s to {max_cleanup}s - "
                "consider coordinating cleanup schedules"
            )

        return errors

    @staticmethod
    def validate_precision_settings(config: PortfolioConfiguration) -> list[str]:
        """Validate decimal precision settings across components.

        Args:
            config: Portfolio configuration to validate

        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        balance_precision = config.balance.precision
        position_precision = config.position.precision
        pnl_precision = config.pnl.precision

        # All precision values should be consistent for financial calculations
        precisions = [
            ("balance", balance_precision),
            ("position", position_precision),
            ("pnl", pnl_precision),
        ]

        max_precision = max(precision for _, precision in precisions)
        min_precision = min(precision for _, precision in precisions)

        if max_precision != min_precision:
            errors.append(
                f"Inconsistent decimal precision across components: "
                f"balance({balance_precision}), position({position_precision}), "
                f"pnl({pnl_precision}) - financial calculations may have precision mismatches"
            )

        return errors

    @classmethod
    def validate_configuration(cls, config: PortfolioConfiguration) -> list[str]:
        """Perform comprehensive configuration validation.

        Args:
            config: Portfolio configuration to validate

        Returns:
            List of all validation error messages
        """
        all_errors: list[str] = []

        # Run all validation checks
        all_errors.extend(cls.validate_financial_limits(config))
        all_errors.extend(cls.validate_cache_settings(config))
        all_errors.extend(cls.validate_concurrency_settings(config))
        all_errors.extend(cls.validate_monitoring_intervals(config))
        all_errors.extend(cls.validate_precision_settings(config))

        return all_errors


def validate_startup_configuration(config: PortfolioConfiguration) -> None:
    """Validate configuration at startup and raise if critical errors found.

    Args:
        config: Portfolio configuration to validate

    Raises:
        ValidationError: If critical configuration errors are found
    """
    errors = ConfigurationValidator.validate_configuration(config)

    if errors:
        # Filter critical vs warning errors
        critical_keywords = ["must", "exceeds reasonable limit", "precision mismatches"]
        critical_errors = [
            error for error in errors if any(keyword in error for keyword in critical_keywords)
        ]

        if critical_errors:
            raise ConfigurationValidationError(critical_errors=critical_errors)


def _update_top_level_fields(config: PortfolioConfiguration, kwargs: dict[str, object]) -> None:
    """Update top-level configuration fields."""
    if "debug_mode" in kwargs and isinstance(kwargs["debug_mode"], bool):
        config.debug_mode = kwargs["debug_mode"]
    if "log_level" in kwargs and isinstance(kwargs["log_level"], str):
        config.log_level = kwargs["log_level"]


def _update_single_section(
    config: PortfolioConfiguration, section_name: str, section_config: dict[str, object]
) -> None:
    """Update a single configuration section with validation."""
    # Mapping of section names to config types and attributes
    section_map = {
        "cache": (CacheConfiguration, config.cache),
        "pricing": (PricingConfiguration, config.pricing),
        "symbol": (SymbolConfiguration, config.symbol),
        "screening": (ScreeningConfiguration, config.screening),
        "balance": (BalanceConfiguration, config.balance),
        "position": (PositionConfiguration, config.position),
        "order": (OrderConfiguration, config.order),
        "pnl": (PnLConfiguration, config.pnl),
        "concurrency": (ConcurrencyConfiguration, config.concurrency),
        "state_manager": (StateManagerConfiguration, config.state_manager),
        "monitoring": (MonitoringConfiguration, config.monitoring),
    }

    if section_name in section_map:
        config_type, current_config = section_map[section_name]
        adapter: TypeAdapter[Any] = TypeAdapter(config_type)
        temp_dict = adapter.dump_python(current_config)
        temp_dict.update(section_config)
        validated_config = adapter.validate_python(temp_dict)

        # Replace setattr with direct property assignment
        _assign_section_config(config, section_name, validated_config)


def _get_section_assignment_map(
    config: PortfolioConfiguration,
) -> dict[str, Callable[[object], None]]:
    """Get mapping of section names to assignment functions."""
    return {
        "cache": lambda cfg: _assign_cache(config, cfg),
        "pricing": lambda cfg: _assign_pricing(config, cfg),
        "symbol": lambda cfg: _assign_symbol(config, cfg),
        "screening": lambda cfg: _assign_screening(config, cfg),
        "balance": lambda cfg: _assign_balance(config, cfg),
        "position": lambda cfg: _assign_position(config, cfg),
        "order": lambda cfg: _assign_order(config, cfg),
        "pnl": lambda cfg: _assign_pnl(config, cfg),
        "concurrency": lambda cfg: _assign_concurrency(config, cfg),
        "state_manager": lambda cfg: _assign_state_manager(config, cfg),
        "monitoring": lambda cfg: _assign_monitoring(config, cfg),
    }


def _assign_cache(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign cache configuration."""
    config.cache = cast(CacheConfiguration, cfg)


def _assign_pricing(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign pricing configuration."""
    config.pricing = cast(PricingConfiguration, cfg)


def _assign_symbol(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign symbol configuration."""
    config.symbol = cast(SymbolConfiguration, cfg)


def _assign_screening(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign screening configuration."""
    config.screening = cast(ScreeningConfiguration, cfg)


def _assign_balance(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign balance configuration."""
    config.balance = cast(BalanceConfiguration, cfg)


def _assign_position(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign position configuration."""
    config.position = cast(PositionConfiguration, cfg)


def _assign_order(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign order configuration."""
    config.order = cast(OrderConfiguration, cfg)


def _assign_pnl(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign pnl configuration."""
    config.pnl = cast(PnLConfiguration, cfg)


def _assign_concurrency(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign concurrency configuration."""
    config.concurrency = cast(ConcurrencyConfiguration, cfg)


def _assign_state_manager(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign state manager configuration."""
    config.state_manager = cast(StateManagerConfiguration, cfg)


def _assign_monitoring(config: PortfolioConfiguration, cfg: object) -> None:
    """Assign monitoring configuration."""
    config.monitoring = cast(MonitoringConfiguration, cfg)


def _assign_section_config(
    config: PortfolioConfiguration, section_name: str, validated_config: object
) -> None:
    """Assign validated configuration to appropriate section."""
    assignment_map = _get_section_assignment_map(config)
    if section_name in assignment_map:
        assignment_map[section_name](validated_config)


def _update_nested_sections(config: PortfolioConfiguration, kwargs: dict[str, object]) -> None:
    """Update nested configuration sections."""
    nested_sections = {
        "cache",
        "pricing",
        "symbol",
        "screening",
        "balance",
        "position",
        "order",
        "pnl",
        "concurrency",
        "state_manager",
        "monitoring",
    }

    for section_name in nested_sections:
        if section_name in kwargs:
            section_config = kwargs[section_name]
            if isinstance(section_config, dict):
                # Type-cast to satisfy pyright - we know it's a dict from isinstance check
                typed_section_config = cast(dict[str, object], section_config)
                _update_single_section(config, section_name, typed_section_config)


def _validate_critical_errors(errors: list[str]) -> None:
    """Validate and raise critical configuration errors."""
    if not errors:
        return

    critical_keywords = ["must", "exceeds reasonable limit"]
    critical_errors = [
        error for error in errors if any(keyword in error for keyword in critical_keywords)
    ]

    if critical_errors:
        raise ConfigurationValidationError(critical_errors=critical_errors)


def create_validated_configuration(**kwargs: object) -> PortfolioConfiguration:
    """Create and validate a portfolio configuration using Pydantic.

    This function creates a PortfolioConfiguration with Pydantic validation
    and then runs additional business logic validation.

    Args:
        **kwargs: Configuration parameters to pass to PortfolioConfiguration

    Returns:
        Validated PortfolioConfiguration instance

    Raises:
        ValueError: If configuration validation fails
    """
    # Create configuration with Pydantic validation
    config = PortfolioConfiguration()

    if kwargs:
        # Convert kwargs to dict for type safety
        kwargs_dict = dict(kwargs)
        _update_top_level_fields(config, kwargs_dict)
        _update_nested_sections(config, kwargs_dict)

    # Run additional business logic validation
    errors = ConfigurationValidator.validate_configuration(config)
    _validate_critical_errors(errors)

    return config
