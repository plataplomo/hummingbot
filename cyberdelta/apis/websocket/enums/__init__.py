"""WebSocket-related enumerations for type safety and structured data.

This package contains all enums used throughout the WebSocket system,
organized by domain for clear separation of concerns.
"""

from .error_codes import WebSocketErrorCode
from .health import HealthStatus
from .metrics import MetricUnit, WSMetricType


__all__ = [
    "HealthStatus",
    "MetricUnit",
    "WSMetricType",
    "WebSocketErrorCode",
]
