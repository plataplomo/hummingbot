"""WebSocket error handling and recovery system.

This module provides comprehensive error handling, recovery strategies, and
monitoring for WebSocket operations. Includes error codes, event publishing,
recovery mechanisms, and strategy routing.

Modules:
- error_events: Event structures for error tracking and monitoring
- error_handler_factory: Factory for creating configured error handlers
- error_handler_registry: Registry pattern for managing error handlers
- error_recovery: Main error recovery logic with backoff/retry strategies
- error_validator: Validation logic for error handling configuration
- stream_error: Stream-specific error models and structures
- stream_error_handler: Typed error handler for stream processing
- stream_recovery: Stream recovery system with connection management
- recovery_strategy_router: Strategy pattern for recovery routing
"""

# Note: WebSocketErrorCode is in cyberdelta.apis.enums.websocket for proper organization
from cyberdelta.apis.websocket.validation import StreamErrorContextValidator

from .error_events import (
    LoggingEventHandler,
    SeverityEventFilter,
    WebSocketErrorEventPublisher,
)

# Import WebSocket error handler
from .error_handler import WebSocketErrorHandler
from .error_handler_factory import WebSocketErrorHandlerFactory
from .error_handler_registry import WebSocketErrorHandlerRegistry

# Import from recovery system
from .recovery.recovery_executor import RecoveryExecutor
from .recovery.recovery_policy import RecoveryPolicyManager
from .recovery_strategy_router import RecoveryStrategyRouter


__all__ = [
    "LoggingEventHandler",
    "RecoveryExecutor",
    "RecoveryPolicyManager",
    "RecoveryStrategyRouter",
    "SeverityEventFilter",
    "StreamErrorContextValidator",
    "WebSocketErrorEventPublisher",
    "WebSocketErrorHandler",
    "WebSocketErrorHandlerFactory",
    "WebSocketErrorHandlerRegistry",
]
