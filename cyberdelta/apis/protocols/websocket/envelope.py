"""Envelope protocols for WebSocket message handling.

This module contains protocol definitions for WebSocket envelopes
and envelope validation in the WebSocket system.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


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


class WebSocketEnvelopeProtocol(Protocol):
    """Protocol for WebSocket envelope validation and processing.

    Defines the interface that envelope types must implement for
    consistent validation and processing across exchanges.
    """

    def validate(self) -> bool:
        """Validate the envelope structure.

        Returns:
            True if envelope is valid, False otherwise.
        """
        ...

    def extract_metadata(self) -> dict[str, Any]:
        """Extract metadata from the envelope.

        Returns:
            Dictionary containing envelope metadata.
        """
        ...
