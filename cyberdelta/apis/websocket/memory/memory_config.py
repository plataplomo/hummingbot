"""Simple memory configuration functions.

Provides minimal memory configuration functions without overengineering.
Follows YAGNI principle - memory optimization is rarely used.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cyberdelta.apis.base.infrastructure_config_domain import MemoryOptimizationMode


if TYPE_CHECKING:
    from cyberdelta.config.models.websocket_processor_config import ProcessorMemoryConfig


def get_memory_config_for_router(
    memory_config: ProcessorMemoryConfig,
) -> tuple[MemoryOptimizationMode, int]:
    """Get memory configuration for router from config.

    Args:
        memory_config: Memory configuration from processor config

    Returns:
        Tuple of (memory_optimization_mode, pool_size) from config
    """
    # Get memory optimization mode from config
    optimization_mode = (
        MemoryOptimizationMode.ENABLED
        if memory_config.enable_memory_optimization
        else MemoryOptimizationMode.DISABLED
    )

    # Get pool size from config
    pool_size = memory_config.pool_size

    return optimization_mode, pool_size
