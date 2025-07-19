"""Event handler protocols and interfaces."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.events.base.base_event import (
        BasePortfolioEvent,
        EventFilter,
        EventHandler,
    )


@runtime_checkable
class EventPublisher(Protocol):
    """Protocol for event publishers."""

    async def publish(self, event: BasePortfolioEvent[Any]) -> None:
        """Publish an event.

        Args:
            event: Event to publish
        """
        ...

    async def publish_batch(self, events: list[BasePortfolioEvent[Any]]) -> None:
        """Publish multiple events.

        Args:
            events: List of events to publish
        """
        ...


@runtime_checkable
class EventSubscriber(Protocol):
    """Protocol for event subscribers."""

    async def subscribe(
        self,
        handler: EventHandler[Any],
        event_filter: EventFilter | None = None,
    ) -> str:
        """Subscribe to events.

        Args:
            handler: Event handler
            event_filter: Optional event filter

        Returns:
            Subscription ID
        """
        ...

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events.

        Args:
            subscription_id: ID returned from subscribe

        Returns:
            True if unsubscribed successfully
        """
        ...


@runtime_checkable
class EventStore(Protocol):
    """Protocol for event storage."""

    async def store(self, event: BasePortfolioEvent[Any]) -> None:
        """Store an event.

        Args:
            event: Event to store
        """
        ...

    async def retrieve(
        self,
        event_id: str,
    ) -> BasePortfolioEvent[Any] | None:
        """Retrieve an event by ID.

        Args:
            event_id: Event ID

        Returns:
            Event if found, None otherwise
        """
        ...

    async def query(
        self,
        event_filter: EventFilter,
        limit: int = 100,
        offset: int = 0,
    ) -> list[BasePortfolioEvent[Any]]:
        """Query events with filter.

        Args:
            event_filter: Event filter
            limit: Maximum number of events
            offset: Offset for pagination

        Returns:
            List of matching events
        """
        ...


@runtime_checkable
class EventProcessor(Protocol):
    """Protocol for event processors."""

    async def process(self, event: BasePortfolioEvent[Any]) -> None:
        """Process an event.

        Args:
            event: Event to process
        """
        ...

    def can_process(self, event: BasePortfolioEvent[Any]) -> bool:
        """Check if processor can handle event.

        Args:
            event: Event to check

        Returns:
            True if can process
        """
        ...


@runtime_checkable
class EventDispatcherProtocol(Protocol):
    """Protocol for event dispatcher."""

    async def dispatch(self, event: BasePortfolioEvent[Any]) -> None:
        """Dispatch an event to handlers.

        Args:
            event: Event to dispatch
        """
        ...

    async def register_handler(
        self,
        handler: EventHandler[Any],
        event_filter: EventFilter | None = None,
    ) -> str:
        """Register an event handler.

        Args:
            handler: Event handler
            event_filter: Optional event filter

        Returns:
            Registration ID
        """
        ...

    async def unregister_handler(self, registration_id: str) -> bool:
        """Unregister an event handler.

        Args:
            registration_id: ID from register_handler

        Returns:
            True if unregistered successfully
        """
        ...

    async def start(self) -> None:
        """Start the dispatcher."""
        ...

    async def stop(self) -> None:
        """Stop the dispatcher."""
        ...


@runtime_checkable
class EventMetrics(Protocol):
    """Protocol for event metrics collection."""

    def record_event_published(self, event: BasePortfolioEvent[Any]) -> None:
        """Record event publication.

        Args:
            event: Published event
        """
        ...

    def record_event_processed(
        self,
        event: BasePortfolioEvent[Any],
        handler_name: str,
        duration_ms: float,
        success: bool,
    ) -> None:
        """Record event processing.

        Args:
            event: Processed event
            handler_name: Name of handler
            duration_ms: Processing duration in milliseconds
            success: Whether processing succeeded
        """
        ...

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get metrics summary.

        Returns:
            Dictionary with metrics data
        """
        ...
