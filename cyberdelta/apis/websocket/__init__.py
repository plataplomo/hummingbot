"""CyberDeltaEngine: WebSocket infrastructure package.

This package contains all WebSocket-related base classes, protocols, and utilities
that are used across different exchange implementations.
"""

from __future__ import annotations

# Import unified ExchangeName enum
from cyberdelta.enums import ExchangeName

# Core WebSocket components
from .ws_context import WebSocketMessageContext
from .ws_context_registry import WebSocketContextRegistry
from .ws_protocols import WebSocketContextProtocol
from .ws_registry_factory import WebSocketRegistryFactory
from .ws_typed_processor import TypeSafeWebSocketProcessor


__all__ = [
    "ExchangeName",
    "TypeSafeWebSocketProcessor",
    "WebSocketContextProtocol",
    "WebSocketContextRegistry",
    "WebSocketMessageContext",
    "WebSocketRegistryFactory",
]
