"""WebSocket Channel Security Classifier.

This module provides security classification for WebSocket channels,
determining which channels contain private/sensitive data requiring authentication.
"""

from typing import Final


class ChannelClassifier:
    """Classifies WebSocket channels for security and access control."""

    # Patterns that indicate private/authenticated channels
    PRIVATE_PATTERNS: Final[set[str]] = {
        "account",
        "user",
        "balance",
        "orders",
        "fills",
        "positions",
        "wallet",
        "portfolio",
        "pnl",
        "margin",
        "collateral",
        "funding",
        "settlement",
        "withdrawal",
        "deposit",
    }

    # Patterns that indicate public market data channels
    PUBLIC_PATTERNS: Final[set[str]] = {
        "ticker",
        "depth",
        "trades",
        "stats",
        "kline",
        "orderbook",
        "market",
        "candle",
        "ohlc",
        "volume",
        "price",
        "spread",
        "index",
    }

    @classmethod
    def classify(cls, routing_key: str) -> str:
        """Classify a channel as PUBLIC or PRIVATE based on routing key.

        Args:
            routing_key: The WebSocket routing key or channel name.

        Returns:
            "PRIVATE" if channel contains sensitive data, "PUBLIC" otherwise.
        """
        if not routing_key:
            return "PUBLIC"

        routing_key_lower = routing_key.lower()

        # Check for private patterns first (more restrictive)
        if any(pattern in routing_key_lower for pattern in cls.PRIVATE_PATTERNS):
            return "PRIVATE"

        # Explicit public patterns
        if any(pattern in routing_key_lower for pattern in cls.PUBLIC_PATTERNS):
            return "PUBLIC"

        # Default to public for unknown patterns (fail open for market data)
        return "PUBLIC"

    @classmethod
    def requires_authentication(cls, routing_key: str) -> bool:
        """Check if a channel requires authentication.

        Args:
            routing_key: The WebSocket routing key or channel name.

        Returns:
            True if channel requires authentication, False otherwise.
        """
        return cls.classify(routing_key) == "PRIVATE"

    @classmethod
    def is_public_channel(cls, routing_key: str) -> bool:
        """Check if a channel is public (no auth required).

        Args:
            routing_key: The WebSocket routing key or channel name.

        Returns:
            True if channel is public, False if private.
        """
        return cls.classify(routing_key) == "PUBLIC"

    @classmethod
    def is_private_channel(cls, routing_key: str) -> bool:
        """Check if a channel contains private/sensitive data.

        Args:
            routing_key: The WebSocket routing key or channel name.

        Returns:
            True if channel is private, False if public.
        """
        return cls.classify(routing_key) == "PRIVATE"

    @classmethod
    def get_security_level(cls, routing_key: str) -> str:
        """Get detailed security level for monitoring and logging.

        Args:
            routing_key: The WebSocket routing key or channel name.

        Returns:
            Security level string for logging/monitoring.
        """
        classification = cls.classify(routing_key)

        if classification == "PRIVATE":
            # Further classify private channels by sensitivity
            routing_key_lower = routing_key.lower()

            # Highest sensitivity - financial operations
            if any(p in routing_key_lower for p in ["withdrawal", "deposit", "wallet"]):
                return "PRIVATE_CRITICAL"

            # High sensitivity - positions and orders
            if any(p in routing_key_lower for p in ["positions", "orders", "margin"]):
                return "PRIVATE_HIGH"

            # Standard sensitivity - account data
            return "PRIVATE_STANDARD"

        return "PUBLIC"
