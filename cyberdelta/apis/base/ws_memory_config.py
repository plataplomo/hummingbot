"""Memory optimization configuration for WebSocket routers.

This module provides configuration classes for different memory optimization
scenarios in high-frequency trading environments.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


# Constants for performance mode thresholds
MEMORY_LIMIT_THRESHOLD_MB = 100
HIGH_FREQUENCY_THRESHOLD_MESSAGES_PER_SECOND = 1000


class PerformanceMode(StrEnum):
    """Performance modes for WebSocket routers."""

    STANDARD = "standard"
    HIGH_FREQUENCY = "high_frequency"
    ULTRA_LOW_LATENCY = "ultra_low_latency"
    MEMORY_OPTIMIZED = "memory_optimized"


@dataclass
class MemoryOptimizationConfig:
    """Configuration for memory optimization features."""

    # Memory pool settings
    pool_size: int = 1000
    enable_pooling: bool = True

    # Performance settings
    enable_slots_optimization: bool = True
    enable_computed_field_caching: bool = True
    use_minimal_validation: bool = False

    # GC optimization
    enable_gc_optimization: bool = False
    gc_threshold_multiplier: float = 2.0

    # Memory monitoring
    enable_memory_monitoring: bool = True
    memory_warning_threshold_mb: float = 100.0
    memory_critical_threshold_mb: float = 200.0


class PerformanceModePresets:
    """Predefined configurations for different performance modes."""

    @staticmethod
    def standard() -> MemoryOptimizationConfig:
        """Standard configuration for regular trading scenarios."""
        return MemoryOptimizationConfig(
            pool_size=500,
            enable_pooling=False,
            enable_slots_optimization=False,
            enable_computed_field_caching=True,
            use_minimal_validation=False,
            enable_gc_optimization=False,
            enable_memory_monitoring=True,
            memory_warning_threshold_mb=50.0,
            memory_critical_threshold_mb=100.0,
        )

    @staticmethod
    def high_frequency() -> MemoryOptimizationConfig:
        """High-frequency trading configuration."""
        return MemoryOptimizationConfig(
            pool_size=2000,
            enable_pooling=True,
            enable_slots_optimization=True,
            enable_computed_field_caching=True,
            use_minimal_validation=True,
            enable_gc_optimization=True,
            gc_threshold_multiplier=3.0,
            enable_memory_monitoring=True,
            memory_warning_threshold_mb=200.0,
            memory_critical_threshold_mb=500.0,
        )

    @staticmethod
    def ultra_low_latency() -> MemoryOptimizationConfig:
        """Ultra-low latency configuration for market making."""
        return MemoryOptimizationConfig(
            pool_size=5000,
            enable_pooling=True,
            enable_slots_optimization=True,
            enable_computed_field_caching=True,
            use_minimal_validation=True,
            enable_gc_optimization=True,
            gc_threshold_multiplier=5.0,
            enable_memory_monitoring=False,  # Disable to reduce overhead
            memory_warning_threshold_mb=500.0,
            memory_critical_threshold_mb=1000.0,
        )

    @staticmethod
    def memory_optimized() -> MemoryOptimizationConfig:
        """Memory-constrained environment configuration."""
        return MemoryOptimizationConfig(
            pool_size=200,
            enable_pooling=True,
            enable_slots_optimization=True,
            enable_computed_field_caching=False,  # Reduce memory usage
            use_minimal_validation=True,
            enable_gc_optimization=True,
            gc_threshold_multiplier=1.5,
            enable_memory_monitoring=True,
            memory_warning_threshold_mb=25.0,
            memory_critical_threshold_mb=50.0,
        )

    @classmethod
    def get_config(cls, mode: PerformanceMode) -> MemoryOptimizationConfig:
        """Get configuration for specified performance mode.

        Args:
            mode: Performance mode to get configuration for

        Returns:
            Memory optimization configuration

        Raises:
            ValueError: If performance mode is not recognized
        """
        configs = {
            PerformanceMode.STANDARD: cls.standard,
            PerformanceMode.HIGH_FREQUENCY: cls.high_frequency,
            PerformanceMode.ULTRA_LOW_LATENCY: cls.ultra_low_latency,
            PerformanceMode.MEMORY_OPTIMIZED: cls.memory_optimized,
        }

        if mode not in configs:
            available_modes = list(configs.keys())
            msg = f"Unknown performance mode: {mode}. Available modes: {available_modes}"
            raise ValueError(msg)

        return configs[mode]()


def get_recommended_mode_for_scenario(
    message_rate_per_second: int,
    memory_limit_mb: float | None = None,
    latency_requirement_ms: float | None = None,
) -> PerformanceMode:
    """Recommend performance mode based on trading scenario.

    Args:
        message_rate_per_second: Expected message processing rate
        memory_limit_mb: Memory limit in megabytes (if constrained)
        latency_requirement_ms: Maximum acceptable latency in milliseconds

    Returns:
        Recommended performance mode
    """
    # Memory-constrained scenarios
    if memory_limit_mb is not None and memory_limit_mb < MEMORY_LIMIT_THRESHOLD_MB:
        return PerformanceMode.MEMORY_OPTIMIZED

    # Ultra-low latency requirements
    if latency_requirement_ms is not None and latency_requirement_ms < 1.0:
        return PerformanceMode.ULTRA_LOW_LATENCY

    # High-frequency scenarios
    if message_rate_per_second > HIGH_FREQUENCY_THRESHOLD_MESSAGES_PER_SECOND:
        return PerformanceMode.HIGH_FREQUENCY

    # Standard scenarios
    return PerformanceMode.STANDARD


def get_performance_characteristics() -> dict[PerformanceMode, dict[str, Any]]:
    """Get performance characteristics for each mode.

    Returns:
        Dictionary mapping performance modes to their characteristics
    """
    return {
        PerformanceMode.STANDARD: {
            "memory_usage": "low",
            "cpu_usage": "low",
            "latency": "standard",
            "throughput": "standard",
            "gc_pressure": "low",
            "use_case": "Regular trading, development, testing",
        },
        PerformanceMode.HIGH_FREQUENCY: {
            "memory_usage": "medium-high",
            "cpu_usage": "medium",
            "latency": "low",
            "throughput": "high",
            "gc_pressure": "medium",
            "use_case": "High-frequency trading, algorithmic strategies",
        },
        PerformanceMode.ULTRA_LOW_LATENCY: {
            "memory_usage": "high",
            "cpu_usage": "low",
            "latency": "ultra-low",
            "throughput": "very-high",
            "gc_pressure": "very-low",
            "use_case": "Market making, ultra-fast arbitrage",
        },
        PerformanceMode.MEMORY_OPTIMIZED: {
            "memory_usage": "very-low",
            "cpu_usage": "medium",
            "latency": "medium",
            "throughput": "medium",
            "gc_pressure": "very-low",
            "use_case": "Memory-constrained environments, embedded systems",
        },
    }


# Example usage and recommendations
if __name__ == "__main__":
    from cyberdelta.config.structlog_config import get_logger

    logger = get_logger(__name__)

    # Show performance mode characteristics
    characteristics = get_performance_characteristics()

    logger.info("performance_mode_analysis_started")

    for mode, chars in characteristics.items():
        logger.info(
            "performance_mode_characteristics",
            mode=mode.value,
            memory_usage=chars["memory_usage"],
            latency=chars["latency"],
            throughput=chars["throughput"],
            use_case=chars["use_case"],
        )

    # Show recommendations for different scenarios
    scenarios: list[dict[str, Any]] = [
        {"name": "Development", "rate": 10, "memory_mb": None, "latency_ms": None},
        {"name": "Production Trading", "rate": 500, "memory_mb": None, "latency_ms": None},
        {"name": "High-Frequency Trading", "rate": 2000, "memory_mb": None, "latency_ms": None},
        {"name": "Market Making", "rate": 5000, "memory_mb": None, "latency_ms": 0.5},
        {"name": "Memory Constrained", "rate": 100, "memory_mb": 50.0, "latency_ms": None},
    ]

    logger.info("scenario_recommendations_started")

    for scenario in scenarios:
        memory_mb = float(scenario["memory_mb"]) if scenario["memory_mb"] is not None else None
        latency_ms = float(scenario["latency_ms"]) if scenario["latency_ms"] is not None else None
        recommended_mode = get_recommended_mode_for_scenario(
            message_rate_per_second=int(scenario["rate"]),
            memory_limit_mb=memory_mb,
            latency_requirement_ms=latency_ms,
        )

        config = PerformanceModePresets.get_config(recommended_mode)

        logger.info(
            "scenario_recommendation",
            scenario=scenario["name"],
            recommended_mode=recommended_mode.value,
            pool_size=config.pool_size,
            enable_pooling=config.enable_pooling,
            slots_optimization=config.enable_slots_optimization,
        )
