"""Backpack-specific WebSocket context models.

This module provides Backpack-specific context for WebSocket message processing.
"""

from __future__ import annotations

from pydantic import computed_field

from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.models.websocket import StreamErrorContext
from cyberdelta.apis.websocket.ws_context import WebSocketMessageContext


class BackpackMessageContext(WebSocketMessageContext[BackpackRawWebSocketEnvelope]):
    """Backpack-specific message context with enhanced typing."""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def stream_symbol(self) -> str | None:
        """Extract symbol from Backpack stream format."""
        parts = self.validated_envelope.stream.split(".")
        return parts[1] if len(parts) > 1 else None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def stream_details(self) -> str | None:
        """Extract additional details from Backpack stream format."""
        parts = self.validated_envelope.stream.split(".")
        # Extract additional details (3rd part)
        stream_details_index = 2
        return parts[stream_details_index] if len(parts) > stream_details_index else None

    def get_transformer_params(self) -> dict[str, str]:
        """Get parameters needed by transformers for Backpack.

        Backpack uses symbol-based routing for market data streams.

        Returns:
            Dictionary with symbol parameter if available
        """
        params: dict[str, str] = {}

        # Backpack uses symbol for market data streams
        if self.symbol:
            params["symbol"] = self.symbol
        else:
            # Get stream symbol value and check if it's valid
            stream_sym = self.stream_symbol
            if stream_sym is not None:
                params["symbol"] = stream_sym

        return params

    def get_symbol_param(self) -> dict[str, str] | None:
        """Get symbol parameter for Backpack transformers.

        Returns:
            Dictionary with symbol parameter or None if not available
        """
        # Check direct symbol first, then computed stream_symbol (property)
        symbol = self.symbol or self.stream_symbol
        return {"symbol": symbol} if symbol else None

    def get_coin_param(self) -> dict[str, str] | None:
        """Get coin parameter - not applicable for Backpack.

        Backpack uses symbol-based routing, not coin-based.

        Returns:
            None as Backpack doesn't use coin parameters
        """
        return None

    def create_error_context(
        self,
        channel: str | None = None,
        sequence_number: int | None = None,
        message_type: str | None = None,
    ) -> StreamErrorContext:
        """Create Backpack-specific error context.

        Overrides base implementation to provide Backpack-specific details.

        Args:
            channel: Optional channel name override (defaults to stream)
            sequence_number: Optional sequence number
            message_type: Optional message type override

        Returns:
            StreamErrorContext: Backpack-specific error context
        """
        # StreamErrorContext imported at module level

        # Use stream as default channel for Backpack
        error_channel = channel or self.validated_envelope.stream

        # Use stream symbol as topic if no symbol is set
        topic = self.symbol or self.stream_symbol

        # Get sequence from envelope if available
        if sequence_number is None and hasattr(self.validated_envelope, "sequence"):
            sequence_number = getattr(self.validated_envelope, "sequence", None)

        # Create base context using parent method
        context = super().create_error_context(
            channel=error_channel,
            sequence_number=sequence_number,
            message_type=message_type,
        )

        # Update Backpack-specific fields
        context.topic = topic
        context.environment = "production"  # Backpack specific

        return context
