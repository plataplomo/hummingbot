"""Event bus infrastructure for domain event distribution.

This module provides the event bus system for distributing domain events
across services in a decoupled manner using the unified event system.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.events import EventType
from cyberdelta.models.events import DomainEvent


logger = get_logger(__name__)


class EventBus:
    """Event distribution system for domain events.

    Provides async event publishing and subscription with proper error handling.
    Uses the DomainEvent system exclusively.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO hardcoded retry counts or delays
    - Explicit error handling, no silent failures
    - Type-safe event handling
    """

    def __init__(self) -> None:
        """Initialize event bus with empty subscriber registry."""
        self._subscribers: dict[EventType, list[Callable[[DomainEvent], Awaitable[None]]]] = {}
        self._running = True

    async def subscribe(
        self, event_type: EventType, handler: Callable[[DomainEvent], Awaitable[None]]
    ) -> None:
        """Subscribe to events of a specific type.

        Args:
            event_type: Type of event to subscribe to (from EventType enum)
            handler: Async function to handle the event

        IMPORTANT: No assumptions about event delivery or ordering.
        Handlers must be idempotent and handle failures explicitly.
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []

        self._subscribers[event_type].append(handler)

        logger.info(
            "event_subscription_registered",
            event_type=event_type.value,
            handler=handler.__name__,
            subscriber_count=len(self._subscribers[event_type]),
        )

    async def unsubscribe(
        self, event_type: EventType, handler: Callable[[DomainEvent], Awaitable[None]]
    ) -> None:
        """Unsubscribe from events of a specific type.

        Args:
            event_type: Type of event to unsubscribe from
            handler: Handler function to remove
        """
        if event_type in self._subscribers:
            try:
                self._subscribers[event_type].remove(handler)
                logger.info(
                    "event_subscription_removed",
                    event_type=event_type.value,
                    handler=handler.__name__,
                )
            except ValueError:
                logger.warning(
                    "event_subscription_not_found",
                    event_type=event_type.value,
                    handler=handler.__name__,
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

        subscribers = self._subscribers.get(event.event_type, [])

        if not subscribers:
            logger.debug(
                "event_published_no_subscribers",
                event_type=event.event_type.value,
                event_id=event.event_id,
            )
            return

        logger.info(
            "event_published",
            event_type=event.event_type.value,
            event_id=event.event_id,
            subscriber_count=len(subscribers),
        )

        # Execute all handlers concurrently
        tasks: list[asyncio.Task[None]] = []
        for handler in subscribers:
            task = asyncio.create_task(self._execute_handler(handler, event))
            tasks.append(task)

        # Wait for all handlers to complete
        results: list[BaseException | None] = await asyncio.gather(*tasks, return_exceptions=True)

        # Log any failures
        for handler, result in zip(subscribers, results, strict=True):
            if isinstance(result, Exception):
                logger.error(
                    "event_handler_failed",
                    event_type=event.event_type.value,
                    event_id=event.event_id,
                    handler=handler.__name__,
                    error=str(result),
                    exc_info=result,
                )

    async def _execute_handler(
        self, handler: Callable[[DomainEvent], Awaitable[None]], event: DomainEvent
    ) -> None:
        """Execute a single event handler with error handling.

        Args:
            handler: Handler function to execute
            event: Event to pass to handler

        Note:
            Following CODING_STANDARDS.md:
            - NO retry logic - handlers must implement their own
            - Errors are propagated to caller for logging
        """
        await handler(event)

    async def shutdown(self) -> None:
        """Shutdown the event bus gracefully.

        Clears all subscriptions and prevents new events.
        """
        self._running = False
        self._subscribers.clear()

        logger.info("event_bus_shutdown")
