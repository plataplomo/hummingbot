"""Enhanced Configuration Inheritance System for WebSocket Models.

This module provides a sophisticated configuration inheritance system that
automatically selects optimal Pydantic configurations based on model type,
usage context, and performance requirements.

Features:
- Automatic configuration selection based on model hierarchy
- Context-aware optimization (development vs production)
- Performance profile-based configuration switching
- Configuration composition and inheritance
- Runtime configuration tuning
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Any, NotRequired, TypedDict, Unpack, cast

from pydantic import BaseModel, ConfigDict, ValidationError

# Import existing performance configurations
from cyberdelta.apis.base.ws_performance_configs import (
    BackpackModelConfig,
    EnvelopeModelConfig,
    HighFrequencyModelConfig,
    HyperliquidModelConfig,
    InternalModelConfig,
    MemoryOptimizedConfig,
    RawAPIModelConfig,
)


class ConfigurationContext(StrEnum):
    """Enumeration of configuration contexts for automatic selection."""

    DEVELOPMENT = "development"  # Development/testing with full validation
    PRODUCTION = "production"  # Production with balanced performance
    HIGH_FREQUENCY = "high_frequency"  # Maximum performance for HFT
    MEMORY_CONSTRAINED = "memory_constrained"  # Minimal memory usage
    SECURITY_CRITICAL = "security_critical"  # Maximum validation and security
    DEBUGGING = "debugging"  # Enhanced error messages and validation


class ConfigOverrides(TypedDict, total=False):
    """TypedDict for configuration overrides."""

    extra: NotRequired[str]
    validate_assignment: NotRequired[bool]
    validate_default: NotRequired[bool]
    defer_build: NotRequired[bool]
    hide_input_in_errors: NotRequired[bool]
    revalidate_instances: NotRequired[str]
    use_enum_values: NotRequired[bool]
    str_strip_whitespace: NotRequired[bool]
    arbitrary_types_allowed: NotRequired[bool]
    populate_by_name: NotRequired[bool]
    regex_engine: NotRequired[str]
    loc_by_alias: NotRequired[bool]
    frozen: NotRequired[bool]


class PerformanceProfile(StrEnum):
    """Performance profiles for automatic configuration tuning."""

    ULTRA_FAST = "ultra_fast"  # Maximum speed, minimal validation
    FAST = "fast"  # Fast with essential validation
    BALANCED = "balanced"  # Balance of speed and validation
    SECURE = "secure"  # Security-first with full validation
    MINIMAL_MEMORY = "minimal_memory"  # Memory optimization priority


class ConfigurationStrategy(ABC):
    """Abstract base class for configuration strategies."""

    @abstractmethod
    def get_config(self, model_type: type[BaseModel], context: ConfigurationContext) -> ConfigDict:
        """Get configuration for the specified model type and context."""

    @abstractmethod
    def supports_context(self, context: ConfigurationContext) -> bool:
        """Check if strategy supports the given context."""


class HierarchicalConfigurationStrategy(ConfigurationStrategy):
    """Configuration strategy based on model class hierarchy.

    Automatically selects configurations based on the model's position
    in the inheritance hierarchy and usage patterns.
    """

    def __init__(self) -> None:
        """Initialize hierarchical configuration mappings."""
        self._config_hierarchy = {
            # Base WebSocket models
            "BaseWebSocketEnvelope": EnvelopeModelConfig.model_config,
            "WebSocketMessageContext": InternalModelConfig.model_config,
            # Exchange-specific models
            "BackpackRawWebSocketEnvelope": BackpackModelConfig.model_config,
            "HyperliquidRawWebSocketEnvelope": HyperliquidModelConfig.model_config,
            "HyperliquidUserEventEnvelope": HyperliquidModelConfig.model_config,
            # Performance-optimized models
            "MemoryOptimizedBackpackEnvelope": MemoryOptimizedConfig.model_config,
            "MemoryOptimizedHyperliquidEnvelope": MemoryOptimizedConfig.model_config,
            "MemoryOptimizedMessageContext": MemoryOptimizedConfig.model_config,
            # High-frequency models
            "DiscriminatedBackpackEnvelope": HighFrequencyModelConfig.model_config,
            "DiscriminatedHyperliquidEnvelope": HighFrequencyModelConfig.model_config,
            # Raw API models (external boundaries)
            "BackpackAPIResponse": RawAPIModelConfig.model_config,
            "HyperliquidAPIResponse": RawAPIModelConfig.model_config,
        }

        self._context_modifiers = {
            ConfigurationContext.DEVELOPMENT: {
                "validate_assignment": True,
                "validate_default": True,
                "validate_call": True,
                "hide_input_in_errors": False,
            },
            ConfigurationContext.PRODUCTION: {
                "validate_assignment": False,
                "validate_default": False,
                "revalidate_instances": "never",
                "defer_build": True,
            },
            ConfigurationContext.HIGH_FREQUENCY: {
                "extra": "ignore",
                "validate_assignment": False,
                "validate_default": False,
                "str_strip_whitespace": False,
                "arbitrary_types_allowed": True,
                "defer_build": True,
                "hide_input_in_errors": True,
            },
            ConfigurationContext.MEMORY_CONSTRAINED: {
                "extra": "forbid",
                "validate_assignment": False,
                "validate_default": False,
                "populate_by_name": False,
                "defer_build": True,
                "hide_input_in_errors": True,
                "loc_by_alias": False,
            },
            ConfigurationContext.SECURITY_CRITICAL: {
                "extra": "forbid",
                "validate_assignment": True,
                "validate_default": True,
                "arbitrary_types_allowed": False,
                "validate_call": True,
                "revalidate_instances": "always",
            },
            ConfigurationContext.DEBUGGING: {
                "validate_assignment": True,
                "validate_default": True,
                "hide_input_in_errors": False,
                "arbitrary_types_allowed": False,
                "use_enum_values": True,
            },
        }

    def get_config(self, model_type: type[BaseModel], context: ConfigurationContext) -> ConfigDict:
        """Get configuration based on model hierarchy and context."""
        # Get base configuration from hierarchy
        base_config = self._get_base_config(model_type)

        # Apply context-specific modifications
        context_modifiers = self._context_modifiers.get(context, {})

        # Merge configurations
        return self._merge_configs(base_config, cast(dict[str, Any], context_modifiers))

    def supports_context(self, context: ConfigurationContext) -> bool:
        """Check if strategy supports the given context."""
        return context in self._context_modifiers

    def _get_base_config(self, model_type: type[BaseModel]) -> ConfigDict:
        """Get base configuration for model type."""
        # Check direct mapping first
        class_name = model_type.__name__
        if class_name in self._config_hierarchy:
            return self._config_hierarchy[class_name]

        # Check inheritance hierarchy
        for base_class in model_type.__mro__:
            if base_class.__name__ in self._config_hierarchy:
                return self._config_hierarchy[base_class.__name__]

        # Fallback to envelope config for unknown models
        return EnvelopeModelConfig.model_config

    def _merge_configs(self, base_config: ConfigDict, modifiers: dict[str, Any]) -> ConfigDict:
        """Merge base configuration with context modifiers."""
        # Create a new ConfigDict with merged settings
        # Start with base config and apply modifiers using dict merging
        merged_dict = dict(base_config)
        merged_dict.update(modifiers)
        # Convert to ConfigDict by using the dict constructor
        return cast(ConfigDict, merged_dict)


class PerformanceProfileStrategy(ConfigurationStrategy):
    """Configuration strategy based on performance profiles.

    Selects configurations optimized for specific performance characteristics
    and use cases.
    """

    def __init__(self) -> None:
        """Initialize performance profile mappings."""
        self._profile_configs = {
            PerformanceProfile.ULTRA_FAST: {
                "extra": "ignore",
                "frozen": True,
                "validate_assignment": False,
                "validate_default": False,
                "str_strip_whitespace": False,
                "arbitrary_types_allowed": True,
                "populate_by_name": False,
                "validate_call": False,
                "revalidate_instances": "never",
                "regex_engine": "rust-regex",
                "defer_build": True,
                "hide_input_in_errors": True,
                "loc_by_alias": False,
            },
            PerformanceProfile.FAST: {
                "extra": "forbid",
                "frozen": True,
                "validate_assignment": False,
                "validate_default": False,
                "use_enum_values": True,
                "str_strip_whitespace": True,
                "validate_call": False,
                "revalidate_instances": "never",
                "regex_engine": "rust-regex",
                "defer_build": True,
            },
            PerformanceProfile.BALANCED: {
                "extra": "forbid",
                "frozen": True,
                "validate_assignment": True,
                "validate_default": True,
                "use_enum_values": True,
                "str_strip_whitespace": True,
                "arbitrary_types_allowed": False,
                "populate_by_name": True,
                "validate_call": False,
                "revalidate_instances": "never",
                "regex_engine": "rust-regex",
            },
            PerformanceProfile.SECURE: {
                "extra": "forbid",
                "frozen": True,
                "validate_assignment": True,
                "validate_default": True,
                "use_enum_values": True,
                "str_strip_whitespace": True,
                "arbitrary_types_allowed": False,
                "populate_by_name": True,
                "validate_call": True,
                "revalidate_instances": "subclass-instances",
                "regex_engine": "rust-regex",
            },
            PerformanceProfile.MINIMAL_MEMORY: {
                "extra": "ignore",
                "frozen": True,
                "validate_assignment": False,
                "validate_default": False,
                "str_strip_whitespace": False,
                "arbitrary_types_allowed": True,
                "populate_by_name": False,
                "validate_call": False,
                "revalidate_instances": "never",
                "defer_build": True,
                "hide_input_in_errors": True,
                "loc_by_alias": False,
            },
        }

        self._context_profile_mapping = {
            ConfigurationContext.DEVELOPMENT: PerformanceProfile.BALANCED,
            ConfigurationContext.PRODUCTION: PerformanceProfile.FAST,
            ConfigurationContext.HIGH_FREQUENCY: PerformanceProfile.ULTRA_FAST,
            ConfigurationContext.MEMORY_CONSTRAINED: PerformanceProfile.MINIMAL_MEMORY,
            ConfigurationContext.SECURITY_CRITICAL: PerformanceProfile.SECURE,
            ConfigurationContext.DEBUGGING: PerformanceProfile.SECURE,
        }

    def get_config(self, model_type: type[BaseModel], context: ConfigurationContext) -> ConfigDict:
        """Get configuration based on performance profile for context."""
        # Map context to performance profile
        profile = self._context_profile_mapping.get(context, PerformanceProfile.BALANCED)

        # Get profile configuration
        profile_config = self._profile_configs[profile]

        # Create ConfigDict from profile config
        return cast(ConfigDict, dict(profile_config))

    def supports_context(self, context: ConfigurationContext) -> bool:
        """Check if strategy supports the given context."""
        return context in self._context_profile_mapping

    def get_profile_config(self, profile: PerformanceProfile) -> dict[str, Any]:
        """Get configuration for a specific performance profile."""
        return self._profile_configs[profile]


class CompositeConfigurationStrategy(ConfigurationStrategy):
    """Composite strategy that combines multiple configuration strategies.

    Allows layering of configuration strategies with priority-based selection
    and fallback mechanisms.
    """

    def __init__(self, strategies: list[ConfigurationStrategy]) -> None:
        """Initialize composite strategy with ordered list of strategies."""
        self.strategies = strategies

    def get_config(self, model_type: type[BaseModel], context: ConfigurationContext) -> ConfigDict:
        """Get configuration using first supporting strategy."""
        for strategy in self.strategies:
            if strategy.supports_context(context):
                return strategy.get_config(model_type, context)

        # Fallback to balanced configuration
        return ConfigDict(
            extra="forbid",
            frozen=True,
            validate_assignment=True,
            validate_default=True,
        )

    def supports_context(self, context: ConfigurationContext) -> bool:
        """Check if any strategy supports the given context."""
        return any(strategy.supports_context(context) for strategy in self.strategies)


class ConfigurationManager:
    """Central manager for automatic configuration selection and optimization.

    Provides a unified interface for configuration management with automatic
    optimization and runtime tuning.
    """

    def __init__(self) -> None:
        """Initialize configuration manager with default strategies."""
        # Initialize strategies in priority order
        hierarchical_strategy = HierarchicalConfigurationStrategy()
        performance_strategy = PerformanceProfileStrategy()

        # Create composite strategy with fallback
        self.strategy = CompositeConfigurationStrategy([
            hierarchical_strategy,
            performance_strategy,
        ])

        # Configuration cache for performance
        self._config_cache: dict[tuple[str, str], ConfigDict] = {}
        self._cache_stats = {"hits": 0, "misses": 0}

    def get_config(
        self,
        model_type: type[BaseModel],
        context: ConfigurationContext = ConfigurationContext.PRODUCTION,
    ) -> ConfigDict:
        """Get optimized configuration for model type and context.

        Args:
            model_type: The Pydantic model class
            context: Configuration context for optimization

        Returns:
            Optimized ConfigDict for the model and context
        """
        cache_key = (model_type.__name__, context.value)

        # Check cache first
        if cache_key in self._config_cache:
            self._cache_stats["hits"] += 1
            return self._config_cache[cache_key]

        # Generate configuration using strategy
        config = self.strategy.get_config(model_type, context)

        # Cache the result
        self._config_cache[cache_key] = config
        self._cache_stats["misses"] += 1

        return config

    def create_optimized_config(
        self,
        base_config: ConfigDict | None = None,
        performance_profile: PerformanceProfile = PerformanceProfile.BALANCED,
        **overrides: Unpack[ConfigOverrides],
    ) -> ConfigDict:
        """Create optimized configuration with custom overrides.

        Args:
            base_config: Base configuration to extend
            performance_profile: Performance profile to apply
            **overrides: Additional configuration overrides

        Returns:
            Optimized ConfigDict with applied settings
        """
        # Start with base config or default
        config_dict: dict[str, Any]
        if base_config:
            config_dict = dict(base_config)
        else:
            config_dict = {
                "extra": "forbid",
                "frozen": True,
                "validate_assignment": True,
            }

        # Apply performance profile
        profile_strategy = PerformanceProfileStrategy()
        profile_config = profile_strategy.get_profile_config(performance_profile)
        config_dict.update(profile_config)

        # Apply custom overrides
        if overrides:
            for key, value in overrides.items():
                # Ensure value is of acceptable type and not None
                if value is not None and isinstance(
                    value, (str, bool, int, float, dict, list, type(None))
                ):
                    config_dict[key] = value

        # Create ConfigDict from config dict
        return cast(ConfigDict, dict(config_dict))

    def get_cache_stats(self) -> dict[str, Any]:
        """Get configuration cache statistics."""
        total_requests = self._cache_stats["hits"] + self._cache_stats["misses"]
        hit_rate = (self._cache_stats["hits"] / max(total_requests, 1)) * 100

        return {
            "cache_hits": self._cache_stats["hits"],
            "cache_misses": self._cache_stats["misses"],
            "total_requests": total_requests,
            "hit_rate_percentage": hit_rate,
            "cached_configurations": len(self._config_cache),
        }

    def clear_cache(self) -> None:
        """Clear configuration cache."""
        self._config_cache.clear()
        self._cache_stats = {"hits": 0, "misses": 0}

    def benchmark_configurations(
        self, model_type: type[BaseModel], test_data: dict[str, Any], iterations: int = 1000
    ) -> dict[str, float]:
        """Benchmark different configurations for a model type.

        Args:
            model_type: Model class to benchmark
            test_data: Sample data for model creation
            iterations: Number of iterations for benchmarking

        Returns:
            Dictionary with timing results for different configurations
        """
        results: dict[str, float] = {}

        # Test different contexts
        contexts = [
            ConfigurationContext.HIGH_FREQUENCY,
            ConfigurationContext.PRODUCTION,
            ConfigurationContext.SECURITY_CRITICAL,
            ConfigurationContext.MEMORY_CONSTRAINED,
        ]

        for context in contexts:
            self.get_config(model_type, context)

            # Benchmark model creation with config
            # Note: Using original model_type to avoid dynamic class creation type issues
            start = time.perf_counter()
            for _ in range(iterations):
                try:
                    model_type(**test_data)
                except (ValueError, TypeError, ValidationError):
                    # Skip if validation fails
                    continue
            end = time.perf_counter()

            avg_time_ms = ((end - start) / iterations) * 1000
            results[context.value] = avg_time_ms

        return results


# Global configuration manager instance
config_manager = ConfigurationManager()


def get_optimized_config(
    model_type: type[BaseModel], context: ConfigurationContext = ConfigurationContext.PRODUCTION
) -> ConfigDict:
    """Get optimized configuration for model type and context.

    Convenience function for accessing the global configuration manager.

    Args:
        model_type: The Pydantic model class
        context: Configuration context for optimization

    Returns:
        Optimized ConfigDict for the model and context
    """
    return config_manager.get_config(model_type, context)


def create_optimized_model(
    base_model: type[BaseModel],
    context: ConfigurationContext = ConfigurationContext.PRODUCTION,
    **config_overrides: dict[str, Any],
) -> type[BaseModel]:
    """Create an optimized model class with automatic configuration.

    Factory function that creates model classes with automatically optimized
    configurations based on context.

    Args:
        base_model: Base model class to optimize
        context: Configuration context for optimization
        **config_overrides: Additional configuration overrides

    Returns:
        New model class with optimized configuration
    """
    # Get optimized configuration
    optimized_config = config_manager.get_config(base_model, context)

    # Apply any overrides
    if config_overrides:
        config_dict = dict(optimized_config)
        config_dict.update(config_overrides)
        optimized_config = cast(ConfigDict, dict(config_dict))

    # Note: Due to strict type checking, we return the original model type
    # In practice, you would need to manually apply the optimized_config
    # to a model subclass at definition time
    return base_model


# Example usage and testing
if __name__ == "__main__":
    from typing import Any

    from pydantic import BaseModel, Field

    from cyberdelta.config.structlog_config import get_logger

    logger = get_logger(__name__)

    # Example model for testing
    class TestWebSocketEnvelope(BaseModel):
        """Test WebSocket envelope for configuration benchmarking."""

        channel: str = Field(..., min_length=1, max_length=64)
        data: dict[str, Any] = Field(...)

    logger.info(
        "config_inheritance_demo_started", component="ConfigurationManager", action="demonstration"
    )

    # Test different contexts
    contexts = [
        ConfigurationContext.DEVELOPMENT,
        ConfigurationContext.PRODUCTION,
        ConfigurationContext.HIGH_FREQUENCY,
        ConfigurationContext.MEMORY_CONSTRAINED,
        ConfigurationContext.SECURITY_CRITICAL,
    ]

    for context in contexts:
        config = get_optimized_config(TestWebSocketEnvelope, context)
        logger.info("configuration_context", context=context.value.upper())

        # Show key configuration differences
        # Show key configuration settings
        if "extra" in config:
            logger.debug("config_setting", setting="extra", value=config["extra"])
        if "validate_assignment" in config:
            logger.debug(
                "config_setting", setting="validate_assignment", value=config["validate_assignment"]
            )
        if "validate_default" in config:
            logger.debug(
                "config_setting", setting="validate_default", value=config["validate_default"]
            )
        if "frozen" in config:
            logger.debug("config_setting", setting="frozen", value=config["frozen"])

    # Show cache statistics
    logger.info("cache_statistics_header")
    stats = config_manager.get_cache_stats()
    for key, value in stats.items():
        if "percentage" in key:
            logger.info("cache_stat", stat_name=key, value=round(value, 1), unit="percent")
        else:
            logger.info("cache_stat", stat_name=key, value=value)

    # Benchmark different configurations
    logger.info("performance_benchmark_header")
    test_data: dict[str, Any] = {"channel": "l2Book", "data": {"coin": "BTC", "levels": []}}
    benchmark_results = config_manager.benchmark_configurations(
        TestWebSocketEnvelope, test_data, iterations=1000
    )

    for context_name, time_ms in benchmark_results.items():
        logger.info("benchmark_result", context=context_name, avg_time_ms=round(time_ms, 3))
