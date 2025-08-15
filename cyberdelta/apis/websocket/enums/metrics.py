"""WebSocket metrics-related enumerations.

This module contains enums for WebSocket metrics collection and monitoring.
"""

from enum import StrEnum


class WSMetricType(StrEnum):
    """WebSocket-specific metric types."""

    MESSAGE_COUNT = "message_count"
    ERROR_COUNT = "error_count"
    PROCESSING_TIME = "processing_time"
    MESSAGE_SIZE = "message_size"
    VALIDATION_ERROR = "validation_error"
    TRANSFORMATION_ERROR = "transformation_error"
    HANDLER_ERROR = "handler_error"
    CONNECTION_EVENT = "connection_event"


class MetricUnit(StrEnum):
    """Units for metric values."""

    COUNT = "count"
    MILLISECONDS = "ms"
    BYTES = "bytes"
    PERCENTAGE = "percent"
