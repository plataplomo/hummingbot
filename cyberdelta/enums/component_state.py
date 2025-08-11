"""Component lifecycle states."""

from enum import Enum


class ComponentState(Enum):
    """Component lifecycle states for event handlers and system components."""

    PRE_INITIALIZED = "PRE_INITIALIZED"
    READY = "READY"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPED = "STOPPED"
    FAULTED = "FAULTED"
