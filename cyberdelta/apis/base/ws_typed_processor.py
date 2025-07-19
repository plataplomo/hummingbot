"""Type-safe WebSocket message processor.

This module provides a type-safe message processor that creates properly
typed contexts and eliminates the need for manual type checking throughout
the WebSocket processing pipeline.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from cyberdelta.apis.base.ws_context import (
    BackpackMessageContext,
    ExchangeType,
    HyperliquidMessageContext,
    WebSocketContextUnion,
)
from cyberdelta.apis.base.ws_type_guards import WebSocketTypeGuards
from cyberdelta.apis.base.ws_validators import ExchangeSpecificValidators


if TYPE_CHECKING:
    # Import envelope models only for type checking
    from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
    from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import (
        HyperliquidRawWebSocketEnvelope,
        HyperliquidUserEventEnvelope,
    )


class BackpackEnvelopeProtocol(Protocol):
    """Protocol for Backpack envelope objects."""

    stream: str


class HyperliquidEnvelopeProtocol(Protocol):
    """Protocol for Hyperliquid envelope objects."""

    channel: str
    data: dict[str, Any] | list[Any]


class TypeSafeWebSocketProcessor:
    """Type-safe WebSocket message processor.

    This processor creates properly typed contexts based on message format,
    eliminating the need for dict[str, Any] contexts and manual type checking.
    """

    def __init__(self) -> None:
        """Initialize the type-safe processor."""
        self.type_guards = WebSocketTypeGuards()

    def create_typed_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str | None = None,
    ) -> WebSocketContextUnion:
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
            return self._create_backpack_context(raw_data, connection_id, message_id)
        if self.type_guards.is_hyperliquid_message(raw_data):
            return self._create_hyperliquid_context(raw_data, connection_id, message_id)
        msg = f"Unknown message format: {list(raw_data.keys())}"
        raise ValueError(msg)

    def _create_backpack_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str,
    ) -> BackpackMessageContext:
        """Create Backpack-specific typed context.

        Args:
            raw_data: Raw Backpack message data
            connection_id: Connection identifier
            message_id: Message identifier

        Returns:
            Backpack-specific typed context
        """
        # Validate and create envelope
        envelope = BackpackRawWebSocketEnvelope.model_validate(raw_data)

        # Extract routing key and symbol with type safety
        routing_key = self._extract_backpack_routing_key(envelope)
        symbol = self._extract_backpack_symbol(envelope)

        return BackpackMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeType.BACKPACK,
            routing_key=routing_key,
            timestamp=datetime.now(UTC),
            message_id=message_id,
            connection_id=connection_id,
            symbol=symbol,
        )

    def _create_hyperliquid_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str,
    ) -> HyperliquidMessageContext:
        """Create Hyperliquid-specific typed context.

        Args:
            raw_data: Raw Hyperliquid message data
            connection_id: Connection identifier
            message_id: Message identifier

        Returns:
            Hyperliquid-specific typed context
        """
        # Determine if this is a user event and create appropriate envelope
        envelope: HyperliquidUserEventEnvelope | HyperliquidRawWebSocketEnvelope
        if self.type_guards.is_hyperliquid_user_event(raw_data):
            envelope = HyperliquidUserEventEnvelope.model_validate(raw_data)
        else:
            envelope = HyperliquidRawWebSocketEnvelope.model_validate(raw_data)

        # Extract routing key and symbol with type safety
        routing_key = self._extract_hyperliquid_routing_key(envelope)
        symbol = self._extract_hyperliquid_symbol(envelope)

        return HyperliquidMessageContext(
            validated_envelope=envelope,
            exchange_type=ExchangeType.HYPERLIQUID,
            routing_key=routing_key,
            timestamp=datetime.now(UTC),
            message_id=message_id,
            connection_id=connection_id,
            symbol=symbol,
        )

    def _extract_backpack_routing_key(self, envelope: BackpackEnvelopeProtocol) -> str:
        """Extract routing key from Backpack envelope with type safety.

        Args:
            envelope: Validated Backpack envelope

        Returns:
            Routing key string
        """
        try:
            return ExchangeSpecificValidators.validate_backpack_topic(envelope.stream)[0]
        except (ValueError, AttributeError):
            return "unknown"

    def _extract_hyperliquid_routing_key(self, envelope: HyperliquidEnvelopeProtocol) -> str:
        """Extract routing key from Hyperliquid envelope with type safety.

        Args:
            envelope: Validated Hyperliquid envelope

        Returns:
            Routing key string
        """
        try:
            return str(envelope.channel)
        except AttributeError:
            return "unknown"

    def _extract_backpack_symbol(self, envelope: BackpackEnvelopeProtocol) -> str | None:
        """Extract symbol from Backpack envelope with type safety.

        Args:
            envelope: Validated Backpack envelope

        Returns:
            Symbol string or None if not available
        """
        try:
            return ExchangeSpecificValidators.validate_backpack_topic(envelope.stream)[1]
        except (ValueError, AttributeError):
            return None

    def _extract_hyperliquid_symbol(self, envelope: HyperliquidEnvelopeProtocol) -> str | None:
        """Extract symbol from Hyperliquid envelope with type safety.

        Args:
            envelope: Validated Hyperliquid envelope

        Returns:
            Symbol string or None if not available
        """
        try:
            if hasattr(envelope, "data") and isinstance(envelope.data, dict):
                coin = envelope.data.get("coin")
                return coin if isinstance(coin, str) else None
        except (AttributeError, TypeError):
            pass

        return None

    def process_message_with_context(
        self,
        raw_data: dict[str, Any],
        connection_id: str,
        message_id: str | None = None,
    ) -> tuple[WebSocketContextUnion, Any]:
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


# Global instance for convenience
typed_processor = TypeSafeWebSocketProcessor()
