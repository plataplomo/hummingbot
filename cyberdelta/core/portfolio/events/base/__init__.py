"""Re-export event infrastructure from new location.

Note: Event infrastructure has been moved to cyberdelta.core.infrastructure.events
This module now only re-exports for compatibility.
"""

from cyberdelta.core.infrastructure.events import (
    BaseEvent,
    CompositeEventFilter,
    EventDispatcher,
    EventFilter,
    EventHandler,
    EventMetadata,
    EventPriority,
    EventType,
    ExchangeEventFilter,
    HandlerRegistration,
    PriorityEventFilter,
    TypeEventFilter,
)

# These protocols need to be moved or defined elsewhere
EventDispatcherProtocol = None
EventMetrics = None
EventProcessor = None  
EventPublisher = None
EventStore = None
EventSubscriber = None


__all__ = [
    # Base event classes
    "BaseEvent",
    "CompositeEventFilter",
    # Dispatcher
    "EventDispatcher",
    "EventDispatcherProtocol",
    "EventFilter",
    "EventHandler",
    "EventMetadata",
    "EventMetrics",
    "EventPriority",
    "EventProcessor",
    # Protocols
    "EventPublisher",
    "EventStore",
    "EventSubscriber",
    "EventType",
    "ExchangeEventFilter",
    "HandlerRegistration",
    "PriorityEventFilter",
    # Filters
    "TypeEventFilter",
]
