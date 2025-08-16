"""CyberDeltaEngine: WebSocket infrastructure package.

This package contains all WebSocket-related base classes, protocols, and utilities
that are used across different exchange implementations.
"""

from __future__ import annotations

# Import unified ExchangeName enum
from cyberdelta.enums import ExchangeName

from .registry.registry_factory import WebSocketRegistryFactory

# Core WebSocket components
from .ws_context import WebSocketMessageContext
from .ws_context_factory import WebSocketContextFactory
from .ws_context_registry import WebSocketContextRegistry
from .ws_protocols import WebSocketContextProtocol
from .ws_type_adapters import WebSocketTypeAdapters


__all__ = [
    "ExchangeName",
    "WebSocketContextFactory",
    "WebSocketContextProtocol",
    "WebSocketContextRegistry",
    "WebSocketMessageContext",
    "WebSocketRegistryFactory",
    "WebSocketTypeAdapters",
]
