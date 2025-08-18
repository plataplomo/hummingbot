"""Error event system for WebSocket operations.

Provides event publishing and handling for error tracking and monitoring.
"""

from cyberdelta.apis.models.websocket import (
    ConnectionHealthEvent,
    RecoveryAttemptEvent,
    SystemHealthEvent,
    WebSocketErrorEvent,
)

from .error_events import (
    LoggingEventHandler,
    RateLimitEventFilter,
    SeverityEventFilter,
    WebSocketErrorEventPublisher,
)


__all__ = [
    "ConnectionHealthEvent",
    "LoggingEventHandler",
    "RateLimitEventFilter",
    "RecoveryAttemptEvent",
    "SeverityEventFilter",
    "SystemHealthEvent",
    "WebSocketErrorEvent",
    "WebSocketErrorEventPublisher",
]
