"""Backpack WebSocket Registry Builder.

This module handles building Backpack-specific WebSocket registry
without circular imports.
"""

from __future__ import annotations

from cyberdelta.apis.backpack.bp_ws_context import BackpackMessageContext
from cyberdelta.apis.backpack.models.bp_ws_envelope import validate_backpack_envelope
from cyberdelta.apis.websocket.ws_context_registry import WebSocketContextRegistry
from cyberdelta.enums import ExchangeName


class BackpackRegistryBuilder:
    """Builds WebSocket registry for Backpack exchange."""

    def build_registry(self) -> WebSocketContextRegistry:
        """Build and return a configured registry for Backpack.

        Returns:
            WebSocketContextRegistry with Backpack components registered
        """
        registry = WebSocketContextRegistry()

        # Register Backpack components directly
        registry.register_context_type(
            exchange_type=ExchangeName.BACKPACK,
            context_class=BackpackMessageContext,
            envelope_validator=validate_backpack_envelope,
        )

        return registry
