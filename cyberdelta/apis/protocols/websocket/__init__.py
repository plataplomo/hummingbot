"""WebSocket protocol definitions.

This module contains protocol definitions for WebSocket components,
ensuring consistent interfaces across different implementations.
"""

from .event_protocols import EventFilterProtocol, EventHandlerProtocol
from .registry_builder import WebSocketRegistryBuilder


__all__ = [
    "EventFilterProtocol",
    "EventHandlerProtocol",
    "WebSocketRegistryBuilder",
]
