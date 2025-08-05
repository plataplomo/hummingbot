"""Base domain event class for all trading system events.

This module provides the foundational DomainEvent class that all
business events inherit from, ensuring consistent structure and
immutability across the event-driven architecture.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, Field


class DomainEvent(BaseModel):
    """Base class for all domain events.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO hardcoded values for timing or IDs
    - Proper UTC timestamp handling
    - Type-safe event identification
    - Immutable events for event sourcing consistency
    """

    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()), description="Unique identifier for this event"
    )

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when event was created",
    )

    version: int = Field(default=1, description="Event schema version for evolution tracking")

    class Config:
        """Pydantic configuration."""

        frozen = True  # Immutable events
        validate_assignment = True
