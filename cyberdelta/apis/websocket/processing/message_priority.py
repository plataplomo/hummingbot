"""WebSocket Message Processing Priority System.

This module defines the priority system for WebSocket message processing,
enabling future implementation of priority-based async message handling.
"""

from enum import IntEnum
from typing import Final


class MessagePriority(IntEnum):
    """WebSocket message processing priorities.

    Lower numbers = higher priority (processed first).
    """

    CRITICAL = 1  # Time-critical: trades, fills, user events
    HIGH = 2  # Important: order book updates, depth
    NORMAL = 3  # Standard: tickers, statistics
    LOW = 4  # Non-critical: info messages, metadata
    BACKGROUND = 5  # Can be delayed: historical data, cleanup


# Mapping of routing key patterns to priorities
MESSAGE_ROUTING_PRIORITIES: Final[dict[str, MessagePriority]] = {
    # Critical priority - execution and user events
    "trades": MessagePriority.CRITICAL,
    "trade": MessagePriority.CRITICAL,
    "fills": MessagePriority.CRITICAL,
    "fill": MessagePriority.CRITICAL,
    "userEvents": MessagePriority.CRITICAL,
    "execution": MessagePriority.CRITICAL,
    "order": MessagePriority.CRITICAL,
    "orders": MessagePriority.CRITICAL,
    # High priority - market state updates
    "depth": MessagePriority.HIGH,
    "l2Book": MessagePriority.HIGH,
    "orderbook": MessagePriority.HIGH,
    "orderBook": MessagePriority.HIGH,
    "book": MessagePriority.HIGH,
    "positions": MessagePriority.HIGH,
    "position": MessagePriority.HIGH,
    "margin": MessagePriority.HIGH,
    # Normal priority - market information
    "ticker": MessagePriority.NORMAL,
    "tickers": MessagePriority.NORMAL,
    "stats": MessagePriority.NORMAL,
    "statistics": MessagePriority.NORMAL,
    "kline": MessagePriority.NORMAL,
    "candle": MessagePriority.NORMAL,
    "ohlc": MessagePriority.NORMAL,
    "volume": MessagePriority.NORMAL,
    # Low priority - reference data
    "instrument": MessagePriority.LOW,
    "instruments": MessagePriority.LOW,
    "symbol": MessagePriority.LOW,
    "symbols": MessagePriority.LOW,
    "market": MessagePriority.LOW,
    "markets": MessagePriority.LOW,
    "info": MessagePriority.LOW,
    # Background priority - non-trading data
    "snapshot": MessagePriority.BACKGROUND,
    "history": MessagePriority.BACKGROUND,
    "historical": MessagePriority.BACKGROUND,
}


class MessagePriorityClassifier:
    """Classifies WebSocket messages by processing priority."""

    @staticmethod
    def get_priority(routing_key: str) -> MessagePriority:
        """Get processing priority for a message based on routing key.

        Args:
            routing_key: The WebSocket routing key or message type.

        Returns:
            MessagePriority enum value for the message.
        """
        if not routing_key:
            return MessagePriority.LOW

        routing_key_lower = routing_key.lower()

        # Check each pattern in priority order
        for pattern, priority in MESSAGE_ROUTING_PRIORITIES.items():
            if pattern in routing_key_lower:
                return priority

        # Default to normal priority for unknown message types
        return MessagePriority.NORMAL

    @staticmethod
    def get_priority_value(routing_key: str) -> int:
        """Get numeric priority value for sorting/queueing.

        Lower values = higher priority.

        Args:
            routing_key: The WebSocket routing key or message type.

        Returns:
            Integer priority value (1-5).
        """
        return MessagePriorityClassifier.get_priority(routing_key).value

    @staticmethod
    def is_high_priority(routing_key: str) -> bool:
        """Check if message is high priority (CRITICAL or HIGH).

        Args:
            routing_key: The WebSocket routing key or message type.

        Returns:
            True if message is high priority.
        """
        priority = MessagePriorityClassifier.get_priority(routing_key)
        return priority <= MessagePriority.HIGH

    @staticmethod
    def is_time_critical(routing_key: str) -> bool:
        """Check if message is time-critical (CRITICAL priority).

        Args:
            routing_key: The WebSocket routing key or message type.

        Returns:
            True if message is time-critical.
        """
        return MessagePriorityClassifier.get_priority(routing_key) == MessagePriority.CRITICAL
