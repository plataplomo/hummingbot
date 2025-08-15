"""Backpack-specific validators.

This module contains validation logic specific to Backpack exchange,
keeping exchange-specific code within the exchange package.
"""

from __future__ import annotations

from cyberdelta.apis.common.base_types import InvalidTopicFormatError, InvalidTopicTypeError
from cyberdelta.apis.websocket.security.validators import WebSocketPayloadValidators


class BackpackValidators:
    """Backpack-specific validation utilities."""

    @staticmethod
    def validate_backpack_topic(topic: str) -> tuple[str, str]:
        """Validate and parse Backpack topic format.

        Args:
            topic: Topic string in format "type.symbol" (e.g., "depth.BTC_USDC").

        Returns:
            Tuple of (topic_type, symbol).

        Raises:
            InvalidTopicFormatError: If topic format is invalid.
            InvalidTopicTypeError: If topic type is invalid.

        """
        if "." not in topic:
            raise InvalidTopicFormatError(topic)

        parts = topic.split(".", 1)
        expected_parts = 2
        if len(parts) != expected_parts:
            raise InvalidTopicFormatError(topic)

        topic_type, symbol = parts

        # Valid Backpack topic types
        # NOTE: Backpack uses "trade" (singular) for WebSocket streams,
        # but "trades" is also accepted for compatibility
        valid_topic_types = {"depth", "ticker", "trade", "trades"}

        if topic_type not in valid_topic_types:
            raise InvalidTopicTypeError(topic, topic_type, valid_topic_types)

        # Validate symbol format using generic validator
        WebSocketPayloadValidators.validate_symbol(symbol, context="Backpack topic symbol")

        return topic_type, symbol
