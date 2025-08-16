"""API-level enumerations for CyberDeltaEngine.

This package contains all enums used throughout the API layer,
organized by domain for clear separation of concerns.
"""

from .websocket import (
    DataPresenceState,
    FieldPresenceState,
    HealthStatus,
    MetricUnit,
    WebSocketErrorCode,
    WSMetricType,
)


__all__ = [
    "DataPresenceState",
    "FieldPresenceState",
    "HealthStatus",
    "MetricUnit",
    "WSMetricType",
    "WebSocketErrorCode",
]
