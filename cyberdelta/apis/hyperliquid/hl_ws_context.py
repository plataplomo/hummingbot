"""Hyperliquid-specific WebSocket context models.

This module provides Hyperliquid-specific context for WebSocket message processing.
"""

from __future__ import annotations

from pydantic import computed_field

from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import HyperliquidRawWebSocketEnvelope
from cyberdelta.apis.websocket.ws_context import WebSocketMessageContext


class HyperliquidMessageContext(WebSocketMessageContext[HyperliquidRawWebSocketEnvelope]):
    """Hyperliquid-specific message context with enhanced typing."""

    @property
    def coin(self) -> str | None:
        """Extract coin from Hyperliquid data with proper typing."""
        data = self.validated_envelope.data
        if isinstance(data, dict) and "coin" in data:
            coin = data["coin"]
            return coin if isinstance(coin, str) else None
        return None

    @computed_field
    def subscription_type(self) -> str | None:
        """Extract subscription type if available."""
        # Hyperliquid doesn't have a subscription field in the envelope
        # This is kept for potential future use
        return None

    def get_transformer_params(self) -> dict[str, str]:
        """Get parameters needed by transformers for Hyperliquid.

        Hyperliquid uses coin-based routing for market data streams.

        Returns:
            Dictionary with coin parameter if available
        """
        params: dict[str, str] = {}

        # Hyperliquid primarily uses coin for market data
        # First check computed coin field, then symbol as fallback
        coin_value = self.coin  # Access the computed field
        if coin_value is None:
            coin_value = self.symbol
        if coin_value:
            params["coin"] = coin_value

        return params

    def get_symbol_param(self) -> dict[str, str] | None:
        """Get symbol parameter - maps to coin for Hyperliquid.

        Hyperliquid uses coin terminology, but we map it to symbol
        for compatibility with generic interfaces.

        Returns:
            Dictionary with symbol parameter or None if not available
        """
        # Map coin to symbol for compatibility
        coin_value = self.coin  # Access the computed field
        if coin_value is None:
            coin_value = self.symbol
        return {"symbol": coin_value} if coin_value else None

    def get_coin_param(self) -> dict[str, str] | None:
        """Get coin parameter for Hyperliquid transformers.

        Returns:
            Dictionary with coin parameter or None if not available
        """
        coin_value = self.coin  # Access the computed field
        if coin_value is None:
            coin_value = self.symbol
        return {"coin": coin_value} if coin_value else None
