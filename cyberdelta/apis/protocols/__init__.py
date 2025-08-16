"""API protocol definitions.

This package contains protocol definitions for various API components,
ensuring consistent interfaces across different implementations.
"""

from .websocket import WebSocketRegistryBuilder


__all__ = [
    "WebSocketRegistryBuilder",
]
