"""Event bus health status model."""

from datetime import datetime

import msgspec

from cyberdelta.enums.component_state import ComponentState


class EventBusHealthStatus(msgspec.Struct):
    """Health status for the event bus.

    Uses msgspec.Struct for consistency with event system.
    """

    is_healthy: bool
    last_event_time: datetime | None
    event_count: int
    error_count: int
    handler_count: int
    pending_requests: int
    state: ComponentState
    message: str
