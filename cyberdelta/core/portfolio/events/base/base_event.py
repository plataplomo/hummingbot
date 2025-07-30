"""Base event classes for the portfolio event system."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, NotRequired, TypedDict, TypeVar
from uuid import UUID, uuid4

from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions import MalformedTradeError


# Type-preserving factory function for tags dict
def _str_str_dict_factory() -> dict[str, str]:
    """Factory function that preserves dict[str, str] type information.

    Returns:
        dict[str, str]: Empty dictionary with preserved type information
    """
    return {}


logger = get_logger(__name__)

T = TypeVar("T", bound=object)  # Event data type


class EventType(Enum):
    """Portfolio event types."""

    # Trade events
    TRADE_RECEIVED = "trade.received"
    TRADE_VALIDATED = "trade.validated"
    TRADE_PROCESSED = "trade.processed"
    TRADE_REJECTED = "trade.rejected"

    # Balance events
    BALANCE_UPDATED = "balance.updated"
    BALANCE_RECONCILED = "balance.reconciled"
    BALANCE_ERROR = "balance.error"

    # Position events
    POSITION_OPENED = "position.opened"
    POSITION_UPDATED = "position.updated"
    POSITION_CLOSED = "position.closed"
    POSITION_ERROR = "position.error"

    # Order events
    ORDER_PLACED = "order.placed"
    ORDER_FILLED = "order.filled"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_REJECTED = "order.rejected"

    # P&L events
    PNL_REALIZED = "pnl.realized"
    PNL_UNREALIZED_UPDATED = "pnl.unrealized.updated"

    # Risk events
    EXPOSURE_CALCULATED = "exposure.calculated"
    RISK_LIMIT_WARNING = "risk.limit.warning"
    RISK_LIMIT_BREACH = "risk.limit.breach"

    # System events
    COMPONENT_INITIALIZED = "component.initialized"
    COMPONENT_SHUTDOWN = "component.shutdown"
    STATE_SNAPSHOT_CREATED = "state.snapshot.created"
    STATE_RESTORED = "state.restored"

    # Error events
    ERROR_OCCURRED = "error.occurred"
    ERROR_RECOVERED = "error.recovered"


class EventPriority(Enum):
    """Event priority levels."""

    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class EventMetadataKwargs(TypedDict, total=False):
    """Type definition for event metadata keyword arguments."""

    source_component: NotRequired[str]
    correlation_id: NotRequired[UUID | None]
    exchange_id: NotRequired[str | None]
    symbol: NotRequired[str | None]
    priority: NotRequired[EventPriority]
    retry_count: NotRequired[int]
    tags: NotRequired[dict[str, str]]


class EventMetadataKwargsWithoutExchange(TypedDict, total=False):
    """Type definition for event metadata keyword arguments without exchange_id."""

    source_component: NotRequired[str]
    correlation_id: NotRequired[UUID | None]
    symbol: NotRequired[str | None]
    priority: NotRequired[EventPriority]
    retry_count: NotRequired[int]
    tags: NotRequired[dict[str, str]]


class EventMetadataKwargsWithoutSymbol(TypedDict, total=False):
    """Type definition for event metadata keyword arguments without exchange_id and symbol."""

    source_component: NotRequired[str]
    correlation_id: NotRequired[UUID | None]
    priority: NotRequired[EventPriority]
    retry_count: NotRequired[int]
    tags: NotRequired[dict[str, str]]


@dataclass  # Mutable - supports progressive construction
class EventMetadata:
    """Metadata for portfolio events."""

    event_id: UUID = Field(default_factory=uuid4)
    timestamp: float = Field(default_factory=time.time)
    source_component: str = ""
    correlation_id: UUID | None = None
    exchange_id: str | None = None
    symbol: str | None = None
    priority: EventPriority = EventPriority.NORMAL
    retry_count: int = 0
    tags: dict[str, str] = Field(default_factory=_str_str_dict_factory)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: float) -> float:
        """Validate timestamp is positive.

        Returns:
            float: The validated timestamp

        Raises:
            MalformedTradeError: If timestamp is not positive
        """
        if v <= 0:
            raise MalformedTradeError(
                message="Timestamp must be positive", field_name="timestamp", field_value=str(v)
            )
        return v

    @field_validator("exchange_id", "symbol", mode="before")
    @classmethod
    def validate_optional_strings(cls, v: str | None) -> str | None:
        """Validate optional string fields, returning None for empty strings.

        Returns:
            str | None: The validated string, or None if empty/whitespace
        """
        if v is not None and not v.strip():
            return None
        return v

    @field_validator("retry_count", mode="before")
    @classmethod
    def validate_retry_count(cls, v: int) -> int:
        """Validate retry count is non-negative.

        Returns:
            int: The validated retry count

        Raises:
            MalformedTradeError: If retry count is negative
        """
        if v < 0:
            raise MalformedTradeError(
                message="Retry count cannot be negative",
                field_name="retry_count",
                field_value=str(v),
            )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert metadata to dictionary.

        Returns:
            dict[str, Any]: Dictionary representation of metadata
        """
        return {
            "event_id": str(self.event_id),
            "timestamp": self.timestamp,
            "source_component": self.source_component,
            "correlation_id": str(self.correlation_id) if self.correlation_id else None,
            "exchange_id": self.exchange_id,
            "symbol": self.symbol,
            "priority": self.priority.name,
            "retry_count": self.retry_count,
            "tags": self.tags,
        }


@dataclass
class BasePortfolioEvent[T](ABC):
    """Base class for all portfolio events."""

    event_type: EventType
    data: T
    metadata: EventMetadata = Field(default_factory=EventMetadata)

    def __post_init__(self) -> None:
        """Post-initialization setup."""
        if not self.metadata.source_component:
            self.metadata.source_component = self.__class__.__name__

    @property
    def event_id(self) -> UUID:
        """Get event ID."""
        return self.metadata.event_id

    @property
    def timestamp(self) -> float:
        """Get event timestamp."""
        return self.metadata.timestamp

    @property
    def age(self) -> float:
        """Get event age in seconds."""
        return time.time() - self.metadata.timestamp

    def is_expired(self, max_age_seconds: float) -> bool:
        """Check if event has expired.

        Args:
            max_age_seconds: Maximum age in seconds

        Returns:
            True if event is older than max age
        """
        return self.age > max_age_seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary representation.

        Returns:
            dict[str, Any]: Dictionary representation of the event
        """
        return {
            "event_type": self.event_type.value,
            "data": self._serialize_data(),
            "metadata": self.metadata.to_dict(),
        }

    @abstractmethod
    def _serialize_data(self) -> dict[str, Any]:
        """Serialize event data.

        Must be implemented by subclasses to handle specific data types.
        """

    def __str__(self) -> str:
        """String representation.

        Returns:
            str: Human-readable string representation of the event
        """
        return (
            f"{self.__class__.__name__}("
            f"type={self.event_type.value}, "
            f"id={self.event_id}, "
            f"source={self.metadata.source_component})"
        )


class EventHandler[T](ABC):
    """Abstract base class for event handlers."""

    @abstractmethod
    async def handle(self, event: BasePortfolioEvent[T]) -> None:
        """Handle an event.

        Args:
            event: Event to handle
        """

    @abstractmethod
    def can_handle(self, event: BasePortfolioEvent[Any]) -> bool:
        """Check if handler can handle the event.

        Args:
            event: Event to check

        Returns:
            True if handler can process this event
        """

    def get_handler_name(self) -> str:
        """Get handler name for logging.

        Returns:
            str: The class name of the handler
        """
        return self.__class__.__name__


class EventFilter(ABC):
    """Abstract base class for event filters."""

    @abstractmethod
    def should_process(self, event: BasePortfolioEvent[Any]) -> bool:
        """Check if event should be processed.

        Args:
            event: Event to check

        Returns:
            True if event should be processed
        """


class TypeEventFilter(EventFilter):
    """Filter events by type."""

    def __init__(self, allowed_types: set[EventType]) -> None:
        """Initialize filter with allowed event types.

        Args:
            allowed_types: Set of event types to allow
        """
        self.allowed_types = allowed_types

    def should_process(self, event: BasePortfolioEvent[Any]) -> bool:
        """Check if event type is allowed.

        Returns:
            bool: True if event type is in allowed types
        """
        return event.event_type in self.allowed_types


class PriorityEventFilter(EventFilter):
    """Filter events by priority."""

    def __init__(self, min_priority: EventPriority) -> None:
        """Initialize filter with minimum priority.

        Args:
            min_priority: Minimum priority level to allow
        """
        self.min_priority = min_priority

    def should_process(self, event: BasePortfolioEvent[Any]) -> bool:
        """Check if event priority meets minimum.

        Returns:
            bool: True if event priority meets or exceeds minimum priority
        """
        return event.metadata.priority.value >= self.min_priority.value


class ExchangeEventFilter(EventFilter):
    """Filter events by exchange."""

    def __init__(self, allowed_exchanges: set[str]) -> None:
        """Initialize filter with allowed exchanges.

        Args:
            allowed_exchanges: Set of exchange IDs to allow
        """
        self.allowed_exchanges = allowed_exchanges

    def should_process(self, event: BasePortfolioEvent[Any]) -> bool:
        """Check if event is from allowed exchange.

        Returns:
            bool: True if event exchange is allowed or None
        """
        if event.metadata.exchange_id is None:
            return True  # Allow events without exchange ID
        return event.metadata.exchange_id in self.allowed_exchanges


class CompositeEventFilter(EventFilter):
    """Combine multiple filters with AND logic."""

    def __init__(self, filters: list[EventFilter]) -> None:
        """Initialize with list of filters.

        Args:
            filters: List of filters to combine
        """
        self.filters = filters

    def should_process(self, event: BasePortfolioEvent[Any]) -> bool:
        """Check if all filters pass.

        Returns:
            bool: True if all filters allow the event to be processed
        """
        return all(f.should_process(event) for f in self.filters)

    def add_filter(self, event_filter: EventFilter) -> None:
        """Add a filter to the composite."""
        self.filters.append(event_filter)
