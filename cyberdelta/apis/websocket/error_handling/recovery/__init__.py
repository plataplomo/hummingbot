"""WebSocket Recovery System.

This module provides a recovery system for WebSocket errors
using the Policy/Execution separation pattern.

Key Components:
    - RecoveryPolicyManager: Decides WHAT recovery actions to take and WHEN
    - RecoveryExecutor: Implements HOW to execute recovery actions
    - Uses existing WebSocketErrorConfig.recovery for configuration
"""

from cyberdelta.apis.websocket.error_handling.recovery.recovery_executor import (
    ConnectionManagerProtocol,
    MessageBufferProtocol,
    RecoveryExecutor,
    StateManagerProtocol,
    SubscriptionManagerProtocol,
)
from cyberdelta.apis.websocket.error_handling.recovery.recovery_policy import (
    CircuitBreakerState,
    CircuitState,
    RecoveryPolicyManager,
    RecoveryState,
    RetryState,
)


__all__ = [
    "CircuitBreakerState",
    "CircuitState",
    "ConnectionManagerProtocol",
    "MessageBufferProtocol",
    "RecoveryExecutor",
    "RecoveryPolicyManager",
    "RecoveryState",
    "RetryState",
    "StateManagerProtocol",
    "SubscriptionManagerProtocol",
]
