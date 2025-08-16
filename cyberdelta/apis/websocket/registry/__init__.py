"""WebSocket registry and factory components.

This module provides factory implementations for managing WebSocket
components and eliminating circular imports.

Modules:
- registry_factory: Factory to eliminate circular imports
"""

from .registry_factory import (
    WebSocketRegistryFactory,
)


__all__ = [
    # Registry factory
    "WebSocketRegistryFactory",
]
