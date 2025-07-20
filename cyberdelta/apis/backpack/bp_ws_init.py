"""Backpack WebSocket initialization.

This module handles registration of Backpack-specific WebSocket components
with the global registry to avoid circular imports.
"""

from cyberdelta.apis.backpack.bp_ws_context import BackpackMessageContext
from cyberdelta.apis.backpack.models.bp_ws_envelope import validate_backpack_envelope
from cyberdelta.apis.websocket.ws_context import ExchangeType
from cyberdelta.apis.websocket.ws_context_registry import WebSocketContextRegistry


def initialize_backpack_ws(registry: WebSocketContextRegistry) -> None:
    """Initialize Backpack WebSocket components in the provided registry.

    Args:
        registry: WebSocketContextRegistry instance to register components with
    """
    registry.register_context_type(
        exchange_type=ExchangeType.BACKPACK,
        context_class=BackpackMessageContext,
        envelope_validator=validate_backpack_envelope,
    )
