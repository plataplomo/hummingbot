"""Unified WebSocket Recovery System.

This module provides a unified recovery system for WebSocket errors,
replacing the previous dual recovery systems with a single, coherent
architecture using the Policy/Execution separation pattern.

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
    RetryState,
    UnifiedRecoveryState,
)


__all__ = [
    "CircuitBreakerState",
    "CircuitState",
    # Protocols
    "ConnectionManagerProtocol",
    "MessageBufferProtocol",
    # Executor
    "RecoveryExecutor",
    # Policy Manager
    "RecoveryPolicyManager",
    "RetryState",
    "StateManagerProtocol",
    "SubscriptionManagerProtocol",
    "UnifiedRecoveryState",
]
