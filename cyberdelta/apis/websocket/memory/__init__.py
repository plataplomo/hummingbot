"""WebSocket memory optimization components.

This module provides memory-efficient implementations and configurations
for WebSocket message processing, including object pooling, memory-optimized
models, and efficient logging data structures.

Modules:
- memory_config: Configuration for memory optimization settings
- memory_optimized: Memory-optimized models using __slots__ and pooling
- stream_log_data: Memory-efficient logging data structures
"""

from .memory_config import (
    MemoryOptimizationConfig,
    PerformanceMode,
    PerformanceModePresets,
    get_recommended_mode_for_scenario,
)
from .memory_optimized import (
    MemoryOptimizedBackpackEnvelope,
    MemoryOptimizedHyperliquidEnvelope,
    MemoryOptimizedMessageContext,
    MemoryPool,
)
from .stream_log_data import (
    WebSocketStreamLogData,
)


__all__ = [
    # Memory configuration
    "MemoryOptimizationConfig",
    # Memory optimized models
    "MemoryOptimizedBackpackEnvelope",
    "MemoryOptimizedHyperliquidEnvelope",
    "MemoryOptimizedMessageContext",
    "MemoryPool",
    "PerformanceMode",
    "PerformanceModePresets",
    # Stream logging
    "WebSocketStreamLogData",
    "get_recommended_mode_for_scenario",
]
