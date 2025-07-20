"""Event handling protocols for portfolio system.

This module provides protocol definitions for type-safe event handling
with zero runtime overhead.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from cyberdelta.core.portfolio.models.events import PortfolioEvent


@runtime_checkable
class EventHandler(Protocol):
    """Protocol for handling portfolio events with type safety.

    This protocol defines the interface for event handlers that can
    process portfolio events with compile-time type checking.
    """

    def can_handle(self, event: PortfolioEvent) -> bool:
        """Check if this handler can process the given event.

        Args:
            event: Portfolio event to check

        Returns:
            True if the handler can process this event type
        """
        ...

    async def handle(self, event: PortfolioEvent) -> None:
        """Handle a portfolio event.

        Args:
            event: Portfolio event to handle

        Raises:
            EventHandlingError: If event handling fails
        """
        ...

    def get_supported_event_types(self) -> list[str]:
        """Get list of event types this handler supports.

        Returns:
            List of supported event type identifiers
        """
        ...


@runtime_checkable
class TypedEventHandler(Protocol):
    """Protocol for type-specific event handlers.

    This protocol provides type-safe handling for specific event types
    using generics for complete type preservation.
    """

    def can_handle(self, event: PortfolioEvent) -> bool:
        """Check if this handler can process the given typed event.

        Args:
            event: Typed event to check

        Returns:
            True if the handler can process this specific event
        """
        ...

    async def handle(self, event: PortfolioEvent) -> None:
        """Handle a typed portfolio event.

        Args:
            event: Typed event to handle

        Raises:
            EventHandlingError: If event handling fails
        """
        ...


@runtime_checkable
class EventBus(Protocol):
    """Protocol for event bus implementations.

    This protocol defines the interface for event distribution
    systems with type safety and performance considerations.
    """

    async def publish(self, event: PortfolioEvent) -> None:
        """Publish an event to all interested handlers.

        Args:
            event: Portfolio event to publish

        Raises:
            EventPublishError: If event publishing fails
        """
        ...

    def subscribe(self, handler: EventHandler) -> str:
        """Subscribe an event handler to receive events.

        Args:
            handler: Event handler to subscribe

        Returns:
            Subscription ID for managing the subscription
        """
        ...

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe an event handler.

        Args:
            subscription_id: ID of the subscription to remove

        Returns:
            True if subscription was found and removed
        """
        ...

    def get_handler_count(self) -> int:
        """Get the number of registered handlers.

        Returns:
            Number of currently registered event handlers
        """
        ...


@runtime_checkable
class EventFilter(Protocol):
    """Protocol for filtering events before processing.

    This protocol allows for flexible event filtering with
    type safety and performance optimization.
    """

    def should_process(self, event: PortfolioEvent) -> bool:
        """Check if an event should be processed.

        Args:
            event: Portfolio event to check

        Returns:
            True if the event should be processed
        """
        ...

    def get_filter_criteria(self) -> dict[str, Any]:
        """Get the criteria used for filtering.

        Returns:
            Dictionary describing filter criteria
        """
        ...


@runtime_checkable
class EventTransformer(Protocol):
    """Protocol for transforming events before handling.

    This protocol allows for event transformation with
    full type safety and generic type preservation.
    """

    def can_transform(self, event: PortfolioEvent) -> bool:
        """Check if this transformer can process the event.

        Args:
            event: Portfolio event to check

        Returns:
            True if the transformer can process this event
        """
        ...

    async def transform(self, event: PortfolioEvent) -> PortfolioEvent:
        """Transform a portfolio event to a specific type.

        Args:
            event: Portfolio event to transform

        Returns:
            Transformed event

        Raises:
            EventTransformError: If transformation fails
        """
        ...


@runtime_checkable
class EventStore(Protocol):
    """Protocol for storing and retrieving events.

    This protocol defines the interface for event persistence
    with type safety and query capabilities.
    """

    async def store_event(self, event: PortfolioEvent) -> str:
        """Store an event and return its ID.

        Args:
            event: Portfolio event to store

        Returns:
            Unique identifier for the stored event

        Raises:
            EventStoreError: If event storage fails
        """
        ...

    async def get_event(self, event_id: str) -> PortfolioEvent | None:
        """Retrieve an event by its ID.

        Args:
            event_id: Unique identifier of the event

        Returns:
            Retrieved event or None if not found

        Raises:
            EventStoreError: If event retrieval fails
        """
        ...

    async def query_events(
        self,
        event_type: str | None = None,
        entity_id: str | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
        limit: int = 100,
    ) -> list[PortfolioEvent]:
        """Query events with optional filters.

        Args:
            event_type: Optional event type filter
            entity_id: Optional entity ID filter
            start_time: Optional start time filter (timestamp)
            end_time: Optional end time filter (timestamp)
            limit: Maximum number of events to return

        Returns:
            List of matching events

        Raises:
            EventStoreError: If query fails
        """
        ...

    async def count_events(
        self,
        event_type: str | None = None,
        entity_id: str | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> int:
        """Count events matching the given criteria.

        Args:
            event_type: Optional event type filter
            entity_id: Optional entity ID filter
            start_time: Optional start time filter (timestamp)
            end_time: Optional end time filter (timestamp)

        Returns:
            Number of matching events

        Raises:
            EventStoreError: If count fails
        """
        ...


@runtime_checkable
class EventRouter(Protocol):
    """Protocol for routing events to appropriate handlers.

    This protocol defines the interface for intelligent event
    routing based on event content and handler capabilities.
    """

    def get_handlers_for_event(self, event: PortfolioEvent) -> list[EventHandler]:
        """Get all handlers that can process the given event.

        Args:
            event: Portfolio event to route

        Returns:
            List of handlers capable of processing the event
        """
        ...

    def register_handler(self, handler: EventHandler, event_types: list[str] | None = None) -> str:
        """Register an event handler for specific event types.

        Args:
            handler: Event handler to register
            event_types: Optional list of event types to handle (all if None)

        Returns:
            Registration ID for managing the handler
        """
        ...

    def unregister_handler(self, registration_id: str) -> bool:
        """Unregister an event handler.

        Args:
            registration_id: ID of the handler registration

        Returns:
            True if handler was found and unregistered
        """
        ...

    def get_routing_stats(self) -> dict[str, Any]:
        """Get statistics about event routing.

        Returns:
            Dictionary with routing statistics
        """
        ...


@runtime_checkable
class EventMiddleware(Protocol):
    """Protocol for event processing middleware.

    This protocol allows for cross-cutting concerns in event
    processing like logging, metrics, and authentication.
    """

    async def before_handle(self, event: PortfolioEvent) -> PortfolioEvent:
        """Process event before it's handled.

        Args:
            event: Portfolio event to process

        Returns:
            Potentially modified event

        Raises:
            EventMiddlewareError: If pre-processing fails
        """
        ...

    async def after_handle(
        self, event: PortfolioEvent, result: object | None = None, error: Exception | None = None
    ) -> None:
        """Process event after it's been handled.

        Args:
            event: Portfolio event that was processed
            result: Optional result from event handling
            error: Optional error that occurred during handling

        Raises:
            EventMiddlewareError: If post-processing fails
        """
        ...

    def get_middleware_name(self) -> str:
        """Get the name of this middleware.

        Returns:
            Human-readable middleware name
        """
        ...


@runtime_checkable
class EventMetrics(Protocol):
    """Protocol for collecting event processing metrics.

    This protocol defines the interface for gathering performance
    and usage metrics from event processing systems.
    """

    def record_event_published(self, event: PortfolioEvent) -> None:
        """Record that an event was published.

        Args:
            event: Published event
        """
        ...

    def record_event_handled(
        self, event: PortfolioEvent, handler_name: str, processing_time_ms: float, success: bool
    ) -> None:
        """Record that an event was handled.

        Args:
            event: Handled event
            handler_name: Name of the handler
            processing_time_ms: Time taken to process in milliseconds
            success: Whether handling was successful
        """
        ...

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of event processing metrics.

        Returns:
            Dictionary with metrics summary
        """
        ...

    def reset_metrics(self) -> None:
        """Reset all collected metrics."""
        ...


@runtime_checkable
class EventSerializer(Protocol):
    """Protocol for serializing and deserializing events.

    This protocol defines the interface for converting events
    to and from various storage or transmission formats.
    """

    def serialize(self, event: PortfolioEvent) -> bytes:
        """Serialize an event to bytes.

        Args:
            event: Portfolio event to serialize

        Returns:
            Serialized event data

        Raises:
            EventSerializationError: If serialization fails
        """
        ...

    def deserialize(self, data: bytes) -> PortfolioEvent:
        """Deserialize bytes to an event.

        Args:
            data: Serialized event data

        Returns:
            Deserialized portfolio event

        Raises:
            EventSerializationError: If deserialization fails
        """
        ...

    def get_content_type(self) -> str:
        """Get the content type for serialized data.

        Returns:
            MIME type or format identifier
        """
        ...


# Factory protocol for creating event-related objects


@runtime_checkable
class EventSystemFactory(Protocol):
    """Protocol for creating event system components.

    This protocol provides a factory interface for creating
    event system components with proper configuration.
    """

    def create_event_bus(self, **config: str | float | bool) -> EventBus:
        """Create an event bus instance.

        Args:
            **config: Configuration parameters

        Returns:
            Configured event bus
        """
        ...

    def create_event_store(self, **config: str | float | bool) -> EventStore:
        """Create an event store instance.

        Args:
            **config: Configuration parameters

        Returns:
            Configured event store
        """
        ...

    def create_event_router(self, **config: str | float | bool) -> EventRouter:
        """Create an event router instance.

        Args:
            **config: Configuration parameters

        Returns:
            Configured event router
        """
        ...

    def create_event_serializer(self, format_type: str) -> EventSerializer:
        """Create an event serializer for the specified format.

        Args:
            format_type: Serialization format (e.g., 'json', 'protobuf')

        Returns:
            Event serializer for the specified format

        Raises:
            UnsupportedFormatError: If format is not supported
        """
        ...
