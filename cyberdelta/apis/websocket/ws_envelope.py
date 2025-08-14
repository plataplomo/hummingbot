"""WebSocket Envelope Protocol and Base Classes.

This module defines the WebSocketEnvelope protocol that all envelope types
must implement, enhancing type safety and providing a unified interface
for envelope operations across all exchanges.

Based on the analysis in ws_base_class_refactor.md, this protocol enables
better type safety and consistent envelope handling patterns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable


if TYPE_CHECKING:
    from collections.abc import Callable


# Constants for validation limits
MAX_PAYLOAD_DICT_SIZE = 1000
MAX_PAYLOAD_LIST_SIZE = 10000


# ============================================================================
# Exception Imports (Step 30: Migration to unified hierarchy completed)
# ============================================================================

from cyberdelta.apis.websocket.ws_exceptions import (
    EmptyRoutingKeyError,
    EnvelopeValidationFailedError,
    InvalidRoutingKeyFormatError,
    PayloadTooLargeError,
)


@runtime_checkable
class WebSocketEnvelope(Protocol):
    """Protocol for WebSocket envelope types.

    All WebSocket envelope models must implement this protocol to ensure
    consistent routing key extraction and payload access across exchanges.

    This protocol enables:
    - Type-safe envelope operations in the base router
    - Consistent interface across all exchanges
    - Runtime type checking capabilities
    - Better IDE support and documentation
    """

    def get_routing_key(self) -> str:
        """Get routing key for message routing.

        Returns:
            String routing key used to determine which processor handles the message.

        Raises:
            ValueError: If routing key cannot be determined from envelope.
        """
        ...

    def get_payload(self) -> dict[str, Any] | list[Any]:
        """Get payload data for processing.

        Returns:
            The payload data extracted from the envelope, either as a dictionary
            or list depending on the exchange's message format.
        """
        ...

    def get_envelope_type(self) -> str:
        """Get human-readable envelope type name.

        Returns:
            String identifying the envelope type for logging and debugging.
        """
        ...


class BaseEnvelopeValidator:
    """Base utilities for envelope validation and type checking.

    This class provides common validation utilities that can be used
    by exchange-specific envelope validators to ensure consistent
    validation patterns across all exchanges.
    """

    @staticmethod
    def validate_envelope_protocol(envelope: object) -> bool:
        """Validate that an object implements the WebSocketEnvelope protocol.

        Args:
            envelope: Object to validate.

        Returns:
            True if object implements WebSocketEnvelope protocol.
        """
        return isinstance(envelope, WebSocketEnvelope)

    @staticmethod
    def validate_routing_key(routing_key: str | None) -> str:
        """Validate and normalize routing key.

        Args:
            routing_key: Routing key to validate.

        Returns:
            Validated routing key.

        Raises:
            EmptyRoutingKeyError: If routing key is empty or None.
            InvalidRoutingKeyFormatError: If routing key contains invalid characters.
        """
        if not routing_key or not routing_key.strip():
            raise EmptyRoutingKeyError

        # Normalize routing key (remove extra whitespace, convert to lowercase)
        normalized = routing_key.strip().lower()

        # Validate routing key format (alphanumeric + underscore only)
        if not normalized.replace("_", "").replace("-", "").isalnum():
            raise InvalidRoutingKeyFormatError(routing_key)

        return normalized

    @staticmethod
    def validate_payload_structure(
        payload: dict[str, Any] | list[Any],
    ) -> dict[str, Any] | list[Any]:
        """Validate payload structure for common issues.

        Args:
            payload: Payload to validate.

        Returns:
            Validated payload.

        Raises:
            PayloadTooLargeError: If payload exceeds size limits.
        """
        if isinstance(payload, dict):
            # Check for reasonable dict size
            if len(payload) > MAX_PAYLOAD_DICT_SIZE:
                raise PayloadTooLargeError("dict", len(payload), MAX_PAYLOAD_DICT_SIZE)
        # Must be list since type annotation guarantees dict or list
        # Check for reasonable list size
        elif len(payload) > MAX_PAYLOAD_LIST_SIZE:
            raise PayloadTooLargeError("list", len(payload), MAX_PAYLOAD_LIST_SIZE)

        return payload


# ============================================================================
# Envelope Validation Utilities
# ============================================================================


def create_envelope_validator_decorator(
    exchange_name: str,
) -> Callable[[Callable[[dict[str, Any]], object]], Callable[[dict[str, Any]], object]]:
    """Create a decorator for envelope validation functions.

    This decorator adds standard error handling and logging to envelope
    validation functions, ensuring consistent error reporting across exchanges.

    Args:
        exchange_name: Name of the exchange for error context.

    Returns:
        Decorator function for envelope validators.
    """

    def decorator(
        validation_func: Callable[[dict[str, Any]], object],
    ) -> Callable[[dict[str, Any]], object]:
        def wrapper(message: dict[str, Any]) -> object:
            try:
                return validation_func(message)
            except Exception as e:
                # Wrap errors in EnvelopeValidationFailedError
                raise EnvelopeValidationFailedError(exchange_name, str(e)) from e

        return wrapper

    return decorator
