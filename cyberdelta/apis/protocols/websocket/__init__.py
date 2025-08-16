"""WebSocket protocol definitions.

This module contains protocol definitions for WebSocket components,
ensuring consistent interfaces across different implementations.
"""

from .registry_builder import WebSocketRegistryBuilder


__all__ = [
    "WebSocketRegistryBuilder",
]
