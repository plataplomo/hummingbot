"""WebSocket memory optimization components.

This module provides memory-efficient implementations and configurations
for WebSocket message processing, including object pooling, memory-optimized
models, and efficient logging data structures.

Modules:
- memory_config: Configuration for memory optimization settings
- memory_optimized: Memory-optimized models using __slots__ and pooling
- stream_log_data: Memory-efficient logging data structures
"""

from cyberdelta.apis.models.websocket.stream_log import WebSocketStreamLogData

from .memory_config import get_memory_config_for_router
from .memory_optimized import MemoryOptimizedMessageContext, MemoryPool


__all__ = [
    "MemoryOptimizedMessageContext",
    "MemoryPool",
    "WebSocketStreamLogData",
    "get_memory_config_for_router",
]
