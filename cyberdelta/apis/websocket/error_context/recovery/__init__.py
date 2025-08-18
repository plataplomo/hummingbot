"""Recovery system for WebSocket errors.

Provides recovery strategies, policies, and execution for error recovery.
"""

from cyberdelta.apis.models.websocket.recovery import (
    CircuitBreakerState,
    RecoveryAction,
    RecoveryResult,
    RecoveryState,
    RetryState,
)
from cyberdelta.apis.protocols.websocket.recovery import (
    ConnectionManagerProtocol,
    MessageBufferProtocol,
    StateManagerProtocol,
    SubscriptionManagerProtocol,
)

from .recovery_executor import RecoveryExecutor
from .recovery_policy import RecoveryPolicyManager
from .recovery_strategy_router import RecoveryStrategyRouter


__all__ = [
    # Policy
    "CircuitBreakerState",
    # Executor
    "ConnectionManagerProtocol",
    "MessageBufferProtocol",
    # Router
    "RecoveryAction",
    "RecoveryExecutor",
    "RecoveryPolicyManager",
    "RecoveryResult",
    "RecoveryState",
    "RecoveryStrategyRouter",
    "RetryState",
    "StateManagerProtocol",
    "SubscriptionManagerProtocol",
]
