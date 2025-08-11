"""WebSocket context protocols for type-safe handling without circular imports.

This module defines Protocol classes that describe the interface of WebSocket contexts
without requiring concrete imports, thus avoiding circular dependencies.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from cyberdelta.apis.common.base_types import BaseContextProtocol


if TYPE_CHECKING:
    from cyberdelta.enums import ExchangeName


@runtime_checkable
class WebSocketEnvelopeProtocol(Protocol):
    """Protocol defining the interface for WebSocket envelope models.

    This protocol describes what any WebSocket envelope must provide,
    ensuring type safety when accessing envelope data.
    """

    # Required attributes that all envelopes have
    data: dict[str, Any] | list[Any]

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        """Pydantic model serialization method."""
        ...

    def get_routing_key(self) -> str:
        """Get routing key for message routing."""
        ...


@runtime_checkable
class WebSocketContextProtocol(BaseContextProtocol, Protocol):
    """Protocol defining the interface for WebSocket message contexts.

    This protocol describes what any WebSocket context must provide,
    allowing type checking without importing concrete implementations.

    This protocol extends the minimal BaseContextProtocol interface with
    WebSocket-specific attributes and methods.
    """

    # Override with more specific type
    @property
    def validated_envelope(self) -> WebSocketEnvelopeProtocol | None:
        """Get validated envelope if available."""
        ...

    # Additional WebSocket-specific attributes
    exchange_type: ExchangeName
    connection_id: str
    message_id: str
    timestamp: Any  # datetime in implementation

    # Optional attributes that may be None
    symbol: str | None
    routing_key: str

    # Domain model attribute - set by processor after transformation
    # Type is Any because it varies based on the transformer used
    domain_model: Any

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        """Pydantic model serialization method."""
        ...

    def get_transformer_params(self) -> dict[str, str]:
        """Get parameters needed by transformers for this exchange.

        Returns a dictionary of parameters that transformers need for this
        specific exchange. Common keys include:
        - 'symbol': For exchanges that use symbol-based routing (Backpack)
        - 'coin': For exchanges that use coin-based routing (Hyperliquid)

        Returns:
            Dictionary of transformer parameters
        """
        ...

    def get_symbol_param(self) -> dict[str, str] | None:
        """Get symbol parameter if applicable to this exchange.

        Returns:
            Dictionary with symbol parameter or None if not applicable
        """
        ...

    def get_coin_param(self) -> dict[str, str] | None:
        """Get coin parameter if applicable to this exchange.

        Returns:
            Dictionary with coin parameter or None if not applicable
        """
        ...
