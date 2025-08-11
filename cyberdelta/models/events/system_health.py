"""System health report model."""

from datetime import datetime

import msgspec

from cyberdelta.enums.component_state import ComponentState
from cyberdelta.models.events.health_status import EventBusHealthStatus


class SystemHealthReport(msgspec.Struct):
    """Comprehensive system health report.

    Aggregates health status from all system components.
    """

    timestamp: datetime
    overall_health: bool
    event_bus_status: EventBusHealthStatus
    handler_statuses: dict[str, dict[str, object]]
    workflow_count: int
    active_workflows: list[dict[str, object]]
    system_state: ComponentState
    degraded_handlers: list[str]
    faulted_handlers: list[str]
    messages: list[str]
