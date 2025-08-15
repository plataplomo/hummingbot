"""WebSocket configuration management.

This module provides sophisticated configuration management for WebSocket
models, including automatic configuration selection, performance optimization,
and context-aware settings.

Modules:
- config_inheritance: Configuration inheritance patterns and optimization
"""

from .config_inheritance import (
    CompositeConfigurationStrategy,
    ConfigOverrides,
    ConfigurationContext,
    ConfigurationManager,
    ConfigurationStrategy,
    HierarchicalConfigurationStrategy,
    PerformanceProfile,
    PerformanceProfileStrategy,
    config_manager,
    create_optimized_model,
    get_optimized_config,
)


__all__ = [
    # Configuration strategies
    "CompositeConfigurationStrategy",
    # Configuration types
    "ConfigOverrides",
    "ConfigurationContext",
    # Configuration management
    "ConfigurationManager",
    "ConfigurationStrategy",
    "HierarchicalConfigurationStrategy",
    "PerformanceProfile",
    "PerformanceProfileStrategy",
    "config_manager",
    # Utility functions
    "create_optimized_model",
    "get_optimized_config",
]
