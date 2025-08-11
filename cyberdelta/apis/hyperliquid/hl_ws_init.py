"""Hyperliquid WebSocket initialization.

This module handles registration of Hyperliquid-specific WebSocket components
with the global registry to avoid circular imports.
"""

from cyberdelta.apis.hyperliquid.hl_ws_context import HyperliquidMessageContext
from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import validate_hyperliquid_envelope
from cyberdelta.apis.websocket.ws_context_registry import WebSocketContextRegistry
from cyberdelta.enums import ExchangeName


def initialize_hyperliquid_ws(registry: WebSocketContextRegistry) -> None:
    """Initialize Hyperliquid WebSocket components in the provided registry.

    Args:
        registry: WebSocketContextRegistry instance to register components with
    """
    registry.register_context_type(
        exchange_type=ExchangeName.HYPERLIQUID,
        context_class=HyperliquidMessageContext,
        envelope_validator=validate_hyperliquid_envelope,
    )
