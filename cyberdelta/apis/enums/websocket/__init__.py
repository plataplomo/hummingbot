"""WebSocket-related enumerations for type safety and structured data.

This package contains all enums used throughout the WebSocket system,
organized by domain for clear separation of concerns.
"""

from .data_states import DataPresenceState, FieldPresenceState
from .error_codes import WebSocketErrorCode
from .health import HealthStatus
from .metrics import MetricUnit, WSMetricType
from .rate_limiting import RateLimitAlgorithm, RateLimitType
from .states import CancellationState, CircuitState, MessageProcessingResult, OperationResult


__all__ = [
    "CancellationState",
    "CircuitState",
    "DataPresenceState",
    "FieldPresenceState",
    "HealthStatus",
    "MessageProcessingResult",
    "MetricUnit",
    "OperationResult",
    "RateLimitAlgorithm",
    "RateLimitType",
    "WSMetricType",
    "WebSocketErrorCode",
]
