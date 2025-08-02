"""Generic portfolio events for untyped data.

This module provides generic event implementations for cases where
strongly-typed events are not available or needed.
"""

from __future__ import annotations

from typing import Any, Unpack
from dataclasses import dataclass

from cyberdelta.core.infrastructure.events import (
    BaseEvent,
    EventMetadata,
    EventMetadataKwargs,
    EventPriority,
    EventType,
)


@dataclass
class GenericPortfolioEvent(BaseEvent[dict[str, Any]]):
    """Generic portfolio event for untyped data.
    
    This event type is used for backward compatibility and cases where
    the data structure is not known at compile time.
    """

    @classmethod
    def create(
        cls,
        event_type: EventType,
        data: dict[str, Any],
        exchange_id: str | None = None,
        **kwargs: Any,
    ) -> GenericPortfolioEvent:
        """Create a generic portfolio event.

        Args:
            event_type: Type of the event
            data: Event data as dictionary
            exchange_id: Exchange identifier
            **kwargs: Additional metadata fields

        Returns:
            GenericPortfolioEvent: The created event
        """
        # Build metadata
        metadata = EventMetadata(
            exchange_id=exchange_id,
            **kwargs
        )

        return cls(
            event_type=event_type,
            data=data,
            metadata=metadata
        )

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize event data.
        
        Returns:
            dict[str, Any]: The data dictionary as-is
        """
        return self.data

    @property
    def exchange_id(self) -> str | None:
        """Get exchange ID from metadata."""
        return self.metadata.exchange_id

    @property
    def timestamp(self) -> float:
        """Get timestamp from metadata."""
        return self.metadata.timestamp