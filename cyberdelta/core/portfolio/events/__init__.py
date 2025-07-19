"""Portfolio event system for event-driven architecture."""

from .balance_events import (
    BalanceChange,
    BalanceErrorEvent,
    BalanceReconciledEvent,
    BalanceSnapshot,
    BalanceUpdatedEvent,
)
from .base import (
    BasePortfolioEvent,
    CompositeEventFilter,
    EventDispatcher,
    EventFilter,
    EventHandler,
    EventMetadata,
    EventPriority,
    EventType,
    ExchangeEventFilter,
    PriorityEventFilter,
    TypeEventFilter,
)
from .error_events import (
    ComponentInitializedEvent,
    ComponentShutdownEvent,
    ComponentStateData,
    ErrorData,
    ErrorOccurredEvent,
    ErrorRecoveredEvent,
    StateRestoredEvent,
    StateSnapshotCreatedEvent,
    StateSnapshotData,
)
from .position_events import (
    PositionClosedEvent,
    PositionData,
    PositionErrorEvent,
    PositionOpenedEvent,
    PositionUpdatedEvent,
)
from .trade_events import (
    TradeProcessedEvent,
    TradeReceivedEvent,
    TradeRejectedEvent,
    TradeValidatedEvent,
)


__all__ = [
    # Balance events
    "BalanceChange",
    "BalanceErrorEvent",
    "BalanceReconciledEvent",
    "BalanceSnapshot",
    "BalanceUpdatedEvent",
    # Core classes
    "BasePortfolioEvent",
    "ComponentInitializedEvent",
    "ComponentShutdownEvent",
    "ComponentStateData",
    "CompositeEventFilter",
    # Error/System events
    "ErrorData",
    "ErrorOccurredEvent",
    "ErrorRecoveredEvent",
    "EventDispatcher",
    "EventFilter",
    "EventHandler",
    "EventMetadata",
    "EventPriority",
    "EventType",
    "ExchangeEventFilter",
    "PositionClosedEvent",
    # Position events
    "PositionData",
    "PositionErrorEvent",
    "PositionOpenedEvent",
    "PositionUpdatedEvent",
    "PriorityEventFilter",
    "StateRestoredEvent",
    "StateSnapshotCreatedEvent",
    "StateSnapshotData",
    "TradeProcessedEvent",
    # Trade events
    "TradeReceivedEvent",
    "TradeRejectedEvent",
    "TradeValidatedEvent",
    # Filters
    "TypeEventFilter",
]
