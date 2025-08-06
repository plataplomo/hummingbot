"""Event bus infrastructure for domain event distribution.

This module provides the event bus system for distributing domain events
across services in a decoupled manner.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.events.base_event import DomainEvent


logger = get_logger(__name__)


class EventBus:
    """Event distribution system for domain events.

    Provides async event publishing and subscription with proper error handling.


    IMPORTANT: Following CODING_STANDARDS.md:
    - NO hardcoded retry counts or delays
    - Explicit error handling, no silent failures
    - Type-safe event handling
    """

    def __init__(self) -> None:
        """Initialize event bus with empty subscriber registry."""
        self._subscribers: dict[str, list[Callable[[DomainEvent], None]]] = {}
        self._running = True

    async def subscribe(self, event_type: str, handler: Callable[[DomainEvent], None]) -> None:
        """Subscribe to events of a specific type.

        Args:
            event_type: Type of event to subscribe to
            handler: Async function to handle the event


        IMPORTANT: No assumptions about event delivery or ordering.
        Handlers must be idempotent and handle failures explicitly.
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []

        self._subscribers[event_type].append(handler)

        logger.info(
            "event_subscription_registered",
            event_type=event_type,
            handler=handler.__name__,
            subscriber_count=len(self._subscribers[event_type]),
        )

    async def unsubscribe(self, event_type: str, handler: Callable[[DomainEvent], None]) -> None:
        """Unsubscribe from events of a specific type.

        Args:
            event_type: Type of event to unsubscribe from
            handler: Handler function to remove
        """
        if event_type in self._subscribers:
            try:
                self._subscribers[event_type].remove(handler)
                logger.info(
                    "event_subscription_removed", event_type=event_type, handler=handler.__name__
                )
            except ValueError:
                logger.warning(
                    "event_subscription_not_found", event_type=event_type, handler=handler.__name__
                )

    async def publish(self, event: DomainEvent) -> None:
        """Publish an event to all subscribers.

        Args:
            event: Domain event to publish

        Raises:
            RuntimeError: If EventBus is not running

        Note:
            Following CODING_STANDARDS.md:
            - NO silent failures - all handler errors are logged
            - NO retry logic - handlers must implement their own retry if needed
            - Fail fast on critical errors
        """
        if not self._running:
            msg = "EventBus is not running"
            raise RuntimeError(msg)

        event_type = event.__class__.__name__
        subscribers = self._subscribers.get(event_type, [])

        if not subscribers:
            logger.debug(
                "event_published_no_subscribers", event_type=event_type, event_id=event.event_id
            )
            return

        logger.info(
            "event_published",
            event_type=event_type,
            event_id=event.event_id,
            subscriber_count=len(subscribers),
        )

        # Execute all handlers concurrently
        tasks: list[asyncio.Task[None]] = []
        for handler in subscribers:
            task = asyncio.create_task(self._handle_event_safely(handler, event, event_type))
            tasks.append(task)

        # Wait for all handlers to complete
        # NOTE: Not using timeout here as per CODING_STANDARDS -
        # handlers should implement their own timeouts
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _handle_event_safely(
        self, handler: Callable[[DomainEvent], None], event: DomainEvent, event_type: str
    ) -> None:
        """Handle event with proper error logging.

        Args:
            handler: Event handler function
            event: Event to handle
            event_type: Type name for logging
        """
        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                # Handle sync handlers in thread pool to avoid blocking
                await asyncio.get_event_loop().run_in_executor(None, handler, event)

            logger.debug(
                "event_handler_completed",
                event_type=event_type,
                event_id=event.event_id,
                handler=handler.__name__,
            )

        except Exception as e:
            # Log error but don't re-raise - one handler failure shouldn't
            # stop other handlers from processing the event
            logger.exception(
                "event_handler_failed",
                event_type=event_type,
                event_id=event.event_id,
                handler=handler.__name__,
                error=str(e),
            )

    async def shutdown(self) -> None:
        """Shutdown the event bus.

        Prevents new events from being published and clears all subscriptions.
        """
        self._running = False
        self._subscribers.clear()

        logger.info("event_bus_shutdown_complete")

    def get_subscription_count(self, event_type: str) -> int:
        """Get number of subscribers for an event type.

        Args:
            event_type: Event type to check


        Returns:
            Number of subscribers for the event type
        """
        return len(self._subscribers.get(event_type, []))
