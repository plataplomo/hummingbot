"""WebSocket context registry for managing context types without circular imports.

This module provides a registry pattern for WebSocket contexts, allowing
dynamic registration of context types without requiring imports at module level.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar

from cyberdelta.apis.exceptions.request_validation import MissingRequiredParameterError
from cyberdelta.apis.websocket.ws_context import WebSocketMessageContext
from cyberdelta.apis.websocket.ws_protocols import (
    WebSocketContextProtocol,
    WebSocketEnvelopeProtocol,
)
from cyberdelta.enums import ExchangeName


if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import BaseModel


# Type variable for context types
T = TypeVar("T", bound=WebSocketContextProtocol)


class WebSocketContextRegistry:
    """Registry for WebSocket context types.

    This registry allows exchanges to register their context types at runtime,
    avoiding the need for module-level imports that cause circular dependencies.
    """

    def __init__(self) -> None:
        """Initialize the registry."""
        self._context_types: dict[ExchangeName, type[WebSocketMessageContext[Any]]] = {}
        self._envelope_validators: dict[ExchangeName, Callable[[dict[str, Any]], BaseModel]] = {}

    def register_context_type(
        self,
        exchange_type: ExchangeName,
        context_class: type[WebSocketMessageContext[Any]],
        envelope_validator: Callable[[dict[str, Any]], BaseModel],
    ) -> None:
        """Register a context type for an exchange.

        Args:
            exchange_type: The exchange type
            context_class: The context class implementing WebSocketContextProtocol
            envelope_validator: Function to validate raw messages into envelopes
        """
        self._context_types[exchange_type] = context_class
        self._envelope_validators[exchange_type] = envelope_validator

    def get_context_type(
        self,
        exchange_type: ExchangeName,
    ) -> type[WebSocketMessageContext[Any]] | None:
        """Get the context type for an exchange.

        Args:
            exchange_type: The exchange type

        Returns:
            The context class or None if not registered
        """
        return self._context_types.get(exchange_type)

    def get_envelope_validator(
        self,
        exchange_type: ExchangeName,
    ) -> Callable[[dict[str, Any]], BaseModel] | None:
        """Get the envelope validator for an exchange.

        Args:
            exchange_type: The exchange type

        Returns:
            The envelope validator or None if not registered
        """
        return self._envelope_validators.get(exchange_type)

    def create_context(
        self,
        exchange_type: ExchangeName,
        raw_message: dict[str, Any],
        connection_id: str,
        message_id: str,
    ) -> WebSocketContextProtocol:
        """Create a context instance for the given exchange type.

        Args:
            exchange_type: The exchange type
            raw_message: The raw WebSocket message
            connection_id: Connection ID
            message_id: Message ID

        Returns:
            A context instance

        Raises:
            MissingRequiredParameterError: If exchange type is not registered,
                context type is missing, or envelope validator is missing.
        """
        # Ensure exchange type is registered
        if not self.is_registered(exchange_type):
            raise MissingRequiredParameterError(
                parameter_name="exchange_registration",
                operation=(
                    f"WebSocket context creation for {exchange_type}. "
                    "Use WebSocketRegistryFactory to create a properly configured registry."
                ),
            )

        context_class = self._context_types.get(exchange_type)
        if not context_class:
            raise MissingRequiredParameterError(
                parameter_name="context_type",
                operation=f"WebSocket context registry for {exchange_type}",
            )

        envelope_validator = self._envelope_validators.get(exchange_type)
        if not envelope_validator:
            raise MissingRequiredParameterError(
                parameter_name="envelope_validator",
                operation=f"WebSocket envelope validation for {exchange_type}",
            )

        # Validate envelope
        validated_envelope = envelope_validator(raw_message)

        # Extract routing key from envelope
        routing_key: str
        if isinstance(validated_envelope, WebSocketEnvelopeProtocol):
            routing_key = validated_envelope.get_routing_key()
        else:
            # Fallback for envelopes without routing key method
            routing_key = str(exchange_type)

        # Create context with standard constructor
        # The concrete context classes inherit from WebSocketMessageContext
        # and implement the protocol methods
        # Since WebSocketMessageContext now implements all required protocol methods,
        # this should be a valid WebSocketContextProtocol
        return context_class(
            validated_envelope=validated_envelope,
            exchange_type=exchange_type,
            routing_key=routing_key,
            connection_id=connection_id,
            message_id=message_id,
            timestamp=datetime.now(UTC),
        )

    def is_registered(self, exchange_type: ExchangeName) -> bool:
        """Check if an exchange type is registered.

        Args:
            exchange_type: The exchange type

        Returns:
            True if registered, False otherwise
        """
        return exchange_type in self._context_types


# NOTE: No global registry instance
# Use WebSocketRegistryFactory.create_configured_registry() instead
