"""WebSocket error context system.

This module provides comprehensive error handling for WebSocket operations,
including error context models, builders, handlers, events, and recovery mechanisms.
"""

# Import the model from models directory
from cyberdelta.apis.models.websocket import StreamErrorContext

# Import builders
from .builders.processor_builder import ProcessorErrorContextBuilder
from .builders.router_builder import RouterErrorContextBuilder

# Import handlers
from .error_handler import WebSocketErrorHandler
from .error_handler_factory import WebSocketErrorHandlerFactory

# Import events
from .events.error_events import (
    LoggingEventHandler,
    SeverityEventFilter,
    WebSocketErrorEventPublisher,
)

# Import recovery
from .recovery.recovery_executor import RecoveryExecutor
from .recovery.recovery_policy import RecoveryPolicyManager
from .recovery.recovery_strategy_router import RecoveryStrategyRouter

# Import validation
from .validation.error_validator import StreamErrorContextValidator


__all__ = [
    # Events
    "LoggingEventHandler",
    # Builders
    "ProcessorErrorContextBuilder",
    # Recovery
    "RecoveryExecutor",
    "RecoveryPolicyManager",
    "RecoveryStrategyRouter",
    "RouterErrorContextBuilder",
    "SeverityEventFilter",
    # Model
    "StreamErrorContext",
    # Validation
    "StreamErrorContextValidator",
    "WebSocketErrorEventPublisher",
    # Handlers
    "WebSocketErrorHandler",
    "WebSocketErrorHandlerFactory",
]
