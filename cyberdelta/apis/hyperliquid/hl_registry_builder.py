"""Hyperliquid WebSocket Registry Builder.

This module handles building Hyperliquid-specific WebSocket registry
without circular imports.
"""

from __future__ import annotations

from cyberdelta.apis.hyperliquid.hl_ws_context import HyperliquidMessageContext
from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import validate_hyperliquid_envelope
from cyberdelta.apis.websocket.ws_context import ExchangeType
from cyberdelta.apis.websocket.ws_context_registry import WebSocketContextRegistry


class HyperliquidRegistryBuilder:
    """Builds WebSocket registry for Hyperliquid exchange."""

    def build_registry(self) -> WebSocketContextRegistry:
        """Build and return a configured registry for Hyperliquid.

        Returns:
            WebSocketContextRegistry with Hyperliquid components registered
        """
        registry = WebSocketContextRegistry()

        # Register Hyperliquid components directly
        registry.register_context_type(
            exchange_type=ExchangeType.HYPERLIQUID,
            context_class=HyperliquidMessageContext,
            envelope_validator=validate_hyperliquid_envelope,
        )

        return registry
