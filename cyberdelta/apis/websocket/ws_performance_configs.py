"""High-Performance Model Configurations for WebSocket Processing.

This module provides optimized ConfigDict configurations for different use cases
in high-frequency WebSocket message processing.

Different model types benefit from different optimization strategies based on
their usage patterns and performance requirements.
"""

from __future__ import annotations

from typing import Any

from pydantic import ConfigDict


class RawAPIModelConfig:
    """Configuration for raw API models requiring maximum security.

    Optimized for boundary validation with strict security requirements
    for external data validation.
    """

    model_config = ConfigDict(
        extra="forbid",  # Strict - no extra fields
        frozen=True,  # Immutable after creation
        validate_assignment=True,  # Validate on assignment
        validate_default=True,  # Validate default values
        use_enum_values=True,  # Better enum serialization
        str_strip_whitespace=True,  # Clean string inputs
        arbitrary_types_allowed=False,  # Enforce strict typing
        populate_by_name=True,  # Allow field aliases
        # Performance optimizations
        regex_engine="rust-regex",  # Use fast regex engine
        revalidate_instances="never",  # Don't revalidate existing instances
    )


class InternalModelConfig:
    """Configuration for internal domain models prioritizing performance.

    Optimized for internal processing where data has already been validated
    at the boundary.
    """

    model_config = ConfigDict(
        extra="forbid",  # Maintain structure control
        frozen=False,  # Allow mutation for state models
        validate_assignment=False,  # Skip validation for performance
        validate_default=False,  # Skip default validation
        use_enum_values=True,  # Efficient enum handling
        str_strip_whitespace=True,  # Basic string cleanup
        arbitrary_types_allowed=False,  # Maintain type safety
        populate_by_name=True,  # Support aliases
        # Maximum performance settings
        revalidate_instances="never",  # No revalidation
        regex_engine="rust-regex",  # Fast regex
    )


class EnvelopeModelConfig:
    """Configuration for WebSocket envelope models (balanced approach).

    Balanced configuration for envelope validation that maintains security
    while optimizing for frequent validation.
    """

    model_config = ConfigDict(
        extra="forbid",  # Strict envelope structure
        frozen=True,  # Immutable envelopes
        validate_assignment=True,  # Validate assignments
        validate_default=True,  # Validate defaults
        use_enum_values=True,  # Efficient enums
        str_strip_whitespace=True,  # Clean inputs
        arbitrary_types_allowed=False,  # Type safety
        populate_by_name=True,  # Field aliases
        # Performance optimizations
        revalidate_instances="never",  # Don't revalidate existing instances
        regex_engine="rust-regex",  # Fast regex engine
    )


class BackpackModelConfig:
    """Configuration optimized for Backpack models with modern validation.

    Specialized configuration for Backpack's specific requirements including
    case sensitivity control and field validation.
    """

    model_config = ConfigDict(
        extra="forbid",  # Control structure
        frozen=True,  # Immutable data
        validate_assignment=True,  # Validate assignments
        validate_default=True,  # Validate defaults
        use_enum_values=True,  # Efficient enums
        str_strip_whitespace=True,  # Clean strings
        arbitrary_types_allowed=False,  # Type safety
        populate_by_name=True,  # Support aliases
        # Backpack-specific optimizations
        # Performance settings
        revalidate_instances="never",  # No revalidation
        regex_engine="rust-regex",  # Fast regex
    )


class HyperliquidModelConfig:
    """Configuration optimized for Hyperliquid models with strict validation.

    Specialized configuration for Hyperliquid's strict API requirements and
    high-frequency trading data.
    """

    model_config = ConfigDict(
        extra="forbid",  # Strict structure
        frozen=True,  # Immutable data
        validate_assignment=True,  # Validate assignments
        validate_default=True,  # Validate defaults
        use_enum_values=True,  # Efficient enums
        str_strip_whitespace=True,  # Clean strings
        arbitrary_types_allowed=False,  # Type safety
        populate_by_name=True,  # Support aliases
        # Hyperliquid-specific optimizations
        # Performance settings
        revalidate_instances="never",  # No revalidation
        regex_engine="rust-regex",  # Fast regex
    )


class HighFrequencyModelConfig:
    """Configuration for ultra-high-frequency validation (trades, order books).

    Maximum performance configuration for high-frequency data like trades and
    order book updates where speed is critical.
    """

    model_config = ConfigDict(
        extra="ignore",  # Allow extra fields for performance
        frozen=True,  # Immutable for safety
        validate_assignment=False,  # Skip assignment validation
        validate_default=False,  # Skip default validation
        use_enum_values=True,  # Efficient enums
        str_strip_whitespace=False,  # Skip whitespace stripping
        arbitrary_types_allowed=True,  # Allow Any types for speed
        populate_by_name=False,  # Skip alias resolution
        # Maximum performance settings
        regex_engine="rust-regex",  # Fast regex
        revalidate_instances="never",  # No revalidation
        defer_build=True,  # Defer schema building
        # Additional performance optimizations
        loc_by_alias=False,  # Skip alias location lookup
        hide_input_in_errors=True,  # Reduce error message overhead
    )


class MemoryOptimizedConfig:
    """Configuration optimized for memory usage with __slots__.

    Configuration for models that need to minimize memory footprint for
    large-scale processing.
    """

    model_config = ConfigDict(
        extra="forbid",  # Strict structure
        frozen=True,  # Immutable
        validate_assignment=False,  # Skip validation for memory
        validate_default=False,  # Skip defaults
        use_enum_values=True,  # Efficient enums
        str_strip_whitespace=False,  # Skip processing
        arbitrary_types_allowed=False,  # Type safety
        populate_by_name=False,  # Skip aliases for memory
        # Memory optimizations
        revalidate_instances="never",  # No revalidation
        regex_engine="rust-regex",  # Fast regex
        defer_build=True,  # Defer schema building
        hide_input_in_errors=True,  # Reduce error data
        # Enable __slots__ for memory efficiency
        # Note: This requires manual __slots__ definition in the model class
    )


# Configuration selector utility
def get_config_for_context(context: str) -> ConfigDict:
    """Get optimized configuration for specific use context.

    Utility function to select the optimal configuration based on the
    specific use case context.

    Args:
        context: Use case context ('raw_api', 'internal', 'envelope',
                'backpack', 'hyperliquid', 'high_frequency', 'memory')

    Returns:
        Optimized ConfigDict for the specified context

    Raises:
        ValueError: If context is not recognized
    """
    configs = {
        "raw_api": RawAPIModelConfig.model_config,
        "internal": InternalModelConfig.model_config,
        "envelope": EnvelopeModelConfig.model_config,
        "backpack": BackpackModelConfig.model_config,
        "hyperliquid": HyperliquidModelConfig.model_config,
        "high_frequency": HighFrequencyModelConfig.model_config,
        "memory": MemoryOptimizedConfig.model_config,
    }

    if context not in configs:
        msg = f"Unknown context: {context}. Available: {list(configs.keys())}"
        raise ValueError(msg)

    return configs[context]


# Performance comparison utility
def compare_config_performance() -> dict[str, dict[str, Any]]:
    """Compare performance characteristics of different configurations.

    Returns:
        Dictionary with performance characteristics for each configuration
    """
    return {
        "raw_api": {
            "validation_speed": "medium",
            "memory_usage": "medium",
            "security_level": "maximum",
            "use_case": "boundary validation",
        },
        "internal": {
            "validation_speed": "fast",
            "memory_usage": "medium",
            "security_level": "high",
            "use_case": "internal processing",
        },
        "envelope": {
            "validation_speed": "medium-fast",
            "memory_usage": "medium",
            "security_level": "high",
            "use_case": "envelope processing",
        },
        "backpack": {
            "validation_speed": "medium-fast",
            "memory_usage": "medium",
            "security_level": "high",
            "use_case": "backpack messages",
        },
        "hyperliquid": {
            "validation_speed": "medium",
            "memory_usage": "medium",
            "security_level": "maximum",
            "use_case": "hyperliquid messages",
        },
        "high_frequency": {
            "validation_speed": "maximum",
            "memory_usage": "low",
            "security_level": "medium",
            "use_case": "high-frequency trading",
        },
        "memory": {
            "validation_speed": "fast",
            "memory_usage": "minimum",
            "security_level": "medium",
            "use_case": "memory-constrained environments",
        },
    }


# Example usage
if __name__ == "__main__":
    from cyberdelta.config.structlog_config import get_logger

    logger = get_logger(__name__)

    # Print configuration comparison
    configs = compare_config_performance()

    logger.info(
        "configuration_performance_comparison",
        component="PerformanceConfigs",
        action="comparison",
    )

    for config_name, characteristics in configs.items():
        logger.info("configuration_details", config_name=config_name.upper())
        for key, value in characteristics.items():
            logger.debug("config_setting", config_name=config_name, key=key, value=value)

    logger.info(
        "configuration_selection_hint",
        hint="Use get_config_for_context() to select optimal configuration",
    )
