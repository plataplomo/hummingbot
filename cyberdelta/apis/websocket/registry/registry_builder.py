"""WebSocket Registry Builder - Protocol for building registries.

This module defines the protocol for building WebSocket registries,
allowing each exchange to implement its own builder without circular dependencies.
"""

from __future__ import annotations

from typing import Protocol

from cyberdelta.apis.websocket.ws_context_registry import WebSocketContextRegistry


class WebSocketRegistryBuilder(Protocol):
    """Protocol for building WebSocket context registries.

    Each exchange implements this protocol to build its own registry
    with the appropriate components registered.
    """

    def build_registry(self) -> WebSocketContextRegistry:
        """Build and return a configured registry.

        Returns:
            WebSocketContextRegistry with exchange-specific components registered
        """
        ...
