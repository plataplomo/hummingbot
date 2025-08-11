"""Handler health status model for event system monitoring."""

import msgspec

from cyberdelta.enums.component_state import ComponentState


class HandlerHealthModel(msgspec.Struct):
    """Model for handler health status using msgspec for performance."""

    state: ComponentState = ComponentState.PRE_INITIALIZED
    error_count: int = 0
    metrics: dict[str, int] = {}  # msgspec automatically handles empty collections
