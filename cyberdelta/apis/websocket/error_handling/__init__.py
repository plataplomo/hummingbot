"""WebSocket error handling and recovery system.

This module provides comprehensive error handling, recovery strategies, and
monitoring for WebSocket operations. Includes error codes, event publishing,
recovery mechanisms, and strategy routing.

Modules:
- error_codes: WebSocket-specific error code definitions
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

# Note: WebSocketErrorCode moved to cyberdelta.apis.websocket.enums to avoid circular imports
from cyberdelta.apis.websocket.validation import StreamErrorContextValidator

from .error_events import (
    LoggingEventHandler,
    SeverityEventFilter,
    WebSocketErrorEventPublisher,
)
from .error_handler_factory import WebSocketErrorHandlerFactory
from .error_handler_registry import WebSocketErrorHandlerRegistry

# Import from unified recovery system
from .recovery.recovery_executor import RecoveryExecutor
from .recovery.recovery_policy import RecoveryPolicyManager
from .recovery_strategy_router import RecoveryStrategyRouter

# WebSocketStreamError moved to exceptions module
from .stream_error_handler import WebSocketStreamErrorHandler


__all__ = [
    # Error codes (moved to enums module)
    # "WebSocketErrorCode",
    # Event handling
    "LoggingEventHandler",
    # Unified recovery system
    "RecoveryExecutor",
    "RecoveryPolicyManager",
    # Recovery routing
    "RecoveryStrategyRouter",
    "SeverityEventFilter",
    # Validation
    "StreamErrorContextValidator",
    "WebSocketErrorEventPublisher",
    # Handler factory and registry
    "WebSocketErrorHandlerFactory",
    "WebSocketErrorHandlerRegistry",
    # Stream error handling
    # "WebSocketStreamError" moved to exceptions module
    "WebSocketStreamErrorHandler",
]
