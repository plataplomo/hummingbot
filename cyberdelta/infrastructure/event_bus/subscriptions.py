"""Event subscription management utilities.

Provides utilities for managing event subscriptions, including
batch subscriptions and subscription tracking.
"""

from collections.abc import Awaitable, Callable
from typing import Any

import msgspec

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.event_bus import HandlerPriority
from cyberdelta.infrastructure.event_bus import EventBus


logger = get_logger(__name__)


class SubscriptionManager:
    """Manages event subscriptions for easier batch operations.

    Tracks all subscriptions and provides utilities for:
    - Batch subscribe/unsubscribe
    - Subscription analytics
    - Cleanup operations
    """

    def __init__(self, event_bus: EventBus) -> None:
        """Initialize subscription manager.

        Args:
            event_bus: The EventBus instance to manage
        """
        self.event_bus = event_bus
        # handler_id -> set of (event_type, handler)
        self._subscriptions: dict[str, set[tuple[Any, ...]]] = {}

    def batch_subscribe(
        self,
        handler_id: str,
        subscriptions: list[
            tuple[type[msgspec.Struct], Callable[..., Awaitable[None]], HandlerPriority]
        ],
    ) -> None:
        """Subscribe multiple event types at once.

        Args:
            handler_id: ID of the handler subscribing
            subscriptions: List of (event_type, handler, priority) tuples
        """
        if handler_id not in self._subscriptions:
            self._subscriptions[handler_id] = set()

        for event_type, handler, priority in subscriptions:
            self.event_bus.subscribe(event_type, handler, priority)
            self._subscriptions[handler_id].add((event_type, handler))

        logger.info("batch_subscribed", handler_id=handler_id, event_count=len(subscriptions))

    def batch_unsubscribe(self, handler_id: str) -> None:
        """Unsubscribe all events for a specific handler.

        Args:
            handler_id: ID of the handler to unsubscribe
        """
        if handler_id not in self._subscriptions:
            return

        for event_type, handler in self._subscriptions[handler_id]:
            self.event_bus.unsubscribe(event_type, handler)

        count = len(self._subscriptions[handler_id])
        del self._subscriptions[handler_id]
        logger.info("batch_unsubscribed", handler_id=handler_id, event_count=count)

    def get_subscription_count(self, handler_id: str) -> int:
        """Get number of subscriptions for a handler.

        Args:
            handler_id: ID of the handler

        Returns:
            Number of active subscriptions
        """
        return len(self._subscriptions.get(handler_id, set()))

    def get_all_subscriptions(self) -> dict[str, int]:
        """Get subscription counts for all handlers.

        Returns:
            Dictionary mapping handler_id to subscription count
        """
        return {handler_id: len(subs) for handler_id, subs in self._subscriptions.items()}

    def clear_all_subscriptions(self) -> None:
        """Clear all tracked subscriptions."""
        for handler_id in list(self._subscriptions.keys()):
            self.batch_unsubscribe(handler_id)
        logger.info("Cleared all subscriptions")
