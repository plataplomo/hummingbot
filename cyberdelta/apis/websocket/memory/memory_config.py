"""Simplified memory optimization configuration.

Provides basic memory optimization settings without overengineering.
Follows YAGNI principle - only what's actually needed.
"""

from __future__ import annotations

from enum import StrEnum


class PerformanceMode(StrEnum):
    """Performance modes for WebSocket routers."""

    STANDARD = "standard"
    HIGH_FREQUENCY = "high_frequency"
    ULTRA_LOW_LATENCY = "ultra_low_latency"
    MEMORY_OPTIMIZED = "memory_optimized"


class MemoryOptimizationConfig:
    """Simple memory optimization configuration - only what's actually used."""

    def __init__(self, pool_size: int = 1000) -> None:
        """Initialize with basic pool size.

        Args:
            pool_size: Size of the memory pool for object reuse
        """
        self.pool_size = pool_size


class PerformanceModePresets:
    """Simple presets for different performance modes."""

    @staticmethod
    def standard() -> MemoryOptimizationConfig:
        """Standard memory configuration."""
        return MemoryOptimizationConfig(pool_size=1000)

    @staticmethod
    def high_frequency() -> MemoryOptimizationConfig:
        """High frequency memory configuration."""
        return MemoryOptimizationConfig(pool_size=2000)

    @staticmethod
    def memory_optimized() -> MemoryOptimizationConfig:
        """Memory optimized configuration."""
        return MemoryOptimizationConfig(pool_size=500)

    @staticmethod
    def get_config(mode: PerformanceMode) -> MemoryOptimizationConfig:
        """Get config for performance mode."""
        if mode == PerformanceMode.HIGH_FREQUENCY:
            return PerformanceModePresets.high_frequency()
        if mode == PerformanceMode.MEMORY_OPTIMIZED:
            return PerformanceModePresets.memory_optimized()
        return PerformanceModePresets.standard()


def get_recommended_mode_for_scenario(
    message_rate_per_second: int = 100,
    memory_limit_mb: float | None = None,
    latency_requirement_ms: float | None = None,
) -> PerformanceMode:
    """Get recommended performance mode for scenario."""
    if latency_requirement_ms and latency_requirement_ms < 1.0:
        return PerformanceMode.ULTRA_LOW_LATENCY
    if message_rate_per_second > 1000:
        return PerformanceMode.HIGH_FREQUENCY
    if memory_limit_mb and memory_limit_mb < 100:
        return PerformanceMode.MEMORY_OPTIMIZED
    return PerformanceMode.STANDARD
