"""Event system infrastructure.

Generic event dispatching and handling infrastructure that can be used
by any module in the system.
"""

from .base_event import (
    BaseEvent,
    EventFilter,
    EventHandler,
    EventMetadata,
    EventMetadataKwargs,
    EventMetadataKwargsWithoutExchange,
    EventMetadataKwargsWithoutSymbol,
    EventPriority,
    EventType,
    TypeEventFilter,
    PriorityEventFilter,
    ExchangeEventFilter,
    CompositeEventFilter,
)
from .event_dispatcher import EventDispatcher, HandlerRegistration
from .service_dispatcher import EventDispatcher as ServiceLevelEventDispatcher

__all__ = [
    # Core classes
    "BaseEvent",
    "EventDispatcher",
    "ServiceLevelEventDispatcher",
    "EventHandler",
    "EventMetadata",
    "EventMetadataKwargs",
    "EventMetadataKwargsWithoutExchange",
    "EventMetadataKwargsWithoutSymbol",
    "EventPriority",
    "EventType",
    "HandlerRegistration",
    # Filters
    "EventFilter",
    "TypeEventFilter",
    "PriorityEventFilter",
    "ExchangeEventFilter",
    "CompositeEventFilter",
]