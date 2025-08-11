"""Type-safe WebSocket message processor.

This module provides a type-safe message processor that creates properly
typed contexts using the registry pattern to avoid circular imports.
"""

from __future__ import annotations

import uuid
from typing import Any

from cyberdelta.apis.websocket.ws_context_registry import WebSocketContextRegistry
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_type_guards import WebSocketTypeGuards
from cyberdelta.enums import ExchangeName


class TypeSafeWebSocketProcessor:
    """Type-safe WebSocket message processor using registry pattern.

    This processor creates properly typed contexts based on message format,
    using a provided context registry instance.
    """

    def __init__(self, registry: WebSocketContextRegistry) -> None:
        """Initialize the type-safe processor.

        Args:
            registry: Configured WebSocketContextRegistry instance
        """
        self.registry = registry
        self.type_guards = WebSocketTypeGuards()

    def create_typed_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str | None = None,
    ) -> WebSocketContextProtocol:
        """Create properly typed context based on message format.

        Args:
            raw_data: Raw WebSocket message data
            connection_id: Connection identifier
            message_id: Optional message identifier

        Returns:
            Typed context appropriate for the exchange

        Raises:
            ValueError: If message format is not recognized
        """
        # Generate message ID if not provided
        if message_id is None:
            message_id = str(uuid.uuid4())

        # Validate input parameters
        if not self.type_guards.is_valid_connection_id(connection_id):
            msg = f"Invalid connection_id format: {connection_id}"
            raise ValueError(msg)

        if not self.type_guards.is_valid_message_id(message_id):
            msg = f"Invalid message_id format: {message_id}"
            raise ValueError(msg)

        # Determine exchange type and create appropriate context
        if self.type_guards.is_backpack_message(raw_data):
            exchange_type = ExchangeName.BACKPACK
        elif self.type_guards.is_hyperliquid_message(raw_data):
            exchange_type = ExchangeName.HYPERLIQUID
        else:
            msg = f"Unknown message format: {list(raw_data.keys())}"
            raise ValueError(msg)

        # Use registry to create context
        return self.registry.create_context(
            exchange_type=exchange_type,
            raw_message=raw_data,
            connection_id=connection_id,
            message_id=message_id,
        )

    def process_message_with_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str | None = None,
    ) -> tuple[WebSocketContextProtocol, Any]:
        """Process message and return typed context with validated envelope.

        Args:
            raw_data: Raw WebSocket message data
            connection_id: Connection identifier
            message_id: Optional message identifier

        Returns:
            Tuple of (typed_context, validated_envelope)
        """
        context = self.create_typed_context(raw_data, connection_id, message_id)
        return context, context.validated_envelope


# NOTE: No global instance - create via factory pattern.
# Use WebSocketRegistryFactory.create_configured_registry() to get a registry,
# then pass it to TypeSafeWebSocketProcessor constructor.
