"""Event bus related enums for CyberDelta Engine.

Contains enums for event handler priorities, routing, and other event system concepts.
"""

from enum import IntEnum


class HandlerPriority(IntEnum):
    """Priority levels for event handlers.

    Lower numeric values indicate higher priority for execution order.
    """

    CRITICAL = 0  # Risk checks, circuit breakers
    HIGH = 1  # Order validation
    NORMAL = 2  # Regular processing
    LOW = 3  # Logging, metrics
