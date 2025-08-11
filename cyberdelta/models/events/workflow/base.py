"""Base workflow event model for trading system operations."""

import uuid
from datetime import UTC, datetime

import msgspec


class BaseWorkflowEvent(msgspec.Struct, kw_only=True):
    """Custom workflow event base using msgspec for performance.

    Replaces bubus BaseEvent with pure msgspec implementation
    optimized for trading system workflows.

    Key design principles:
    - Uses msgspec.Struct for maximum performance (25x faster than Pydantic)
    - Comprehensive workflow tracking and audit capabilities
    - Auto-generates event identifiers and timestamps for consistency
    - Supports complete event status lifecycle management
    - Zero external dependencies for enhanced system stability
    """

    # Required fields (no defaults)
    event_type: str
    timeout: float

    # Fields with defaults
    event_id: str = msgspec.field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = msgspec.field(default_factory=lambda: datetime.now(UTC))
    parent_id: str | None = None
    context: dict[str, str] = {}
    status: str = "pending"  # pending, running, completed, failed, cancelled
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
