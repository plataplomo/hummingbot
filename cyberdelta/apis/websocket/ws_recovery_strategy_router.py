"""WebSocket recovery strategy router.

This module routes different recovery strategies to appropriate handlers,
providing specialized recovery actions based on error type and context.
"""

from __future__ import annotations

import asyncio
from abc import abstractmethod
from typing import TYPE_CHECKING, Protocol

from pydantic import BaseModel, Field

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_error_recovery import WebSocketErrorRecovery

# Retry constants
ERROR_SEVERITY_THRESHOLD = 3  # ERROR or higher severity level
CRITICAL_MAX_RETRIES = 1  # Max retries for critical errors
HIGH_SEVERITY_MAX_RETRIES = 3  # Max retries for high severity errors
DEFAULT_MAX_RETRIES = 5  # Default max retries for normal errors


class RecoveryAction(BaseModel):
    """Represents a recovery action to be taken."""

    strategy: WebSocketRecoveryStrategy
    delay_ms: int = Field(default=0, ge=0)
    should_reconnect: bool = False
    should_resubscribe: bool = False
    should_clear_state: bool = False
    should_degrade_service: bool = False
    max_retries: int = Field(default=3, ge=0)
    metadata: dict[str, str] = Field(default_factory=dict)


class RecoveryResult(BaseModel):
    """Result of a recovery attempt."""

    success: bool
    strategy_used: WebSocketRecoveryStrategy
    action_taken: str
    time_elapsed_ms: int = Field(default=0, ge=0)
    error_message: str | None = None
    should_continue: bool = True
    metadata: dict[str, str] = Field(default_factory=dict)


class RecoveryHandler(Protocol):
    """Protocol for recovery handlers."""

    @abstractmethod
    async def can_handle(self, error: WebSocketStreamError) -> bool:
        """Check if this handler can handle the error.

        Args:
            error: The WebSocket stream error

        Returns:
            True if this handler can handle the error
        """
        ...

    @abstractmethod
    async def handle(
        self, error: WebSocketStreamError, recovery: WebSocketErrorRecovery | None = None
    ) -> RecoveryResult:
        """Handle the recovery for the error.

        Args:
            error: The WebSocket stream error
            recovery: Optional recovery system reference

        Returns:
            Result of the recovery attempt
        """
        ...


class ImmediateRetryHandler:
    """Handler for immediate retry strategy."""

    def __init__(self) -> None:
        """Initialize handler."""
        self.logger = get_logger("ImmediateRetryHandler")

    async def can_handle(self, error: WebSocketStreamError) -> bool:
        """Check if this handler can handle the error.

        Returns:
            True if this handler can handle immediate retry errors
        """
        return error.get_recovery_strategy() == WebSocketRecoveryStrategy.IMMEDIATE_RETRY

    async def handle(
        self, error: WebSocketStreamError, recovery: WebSocketErrorRecovery | None = None
    ) -> RecoveryResult:
        """Handle immediate retry recovery.

        Returns:
            RecoveryResult indicating immediate retry action
        """
        self.logger.info(
            "immediate_retry_recovery",
            error_code=error.code.name,
            message=error.message,
        )

        # No delay for immediate retry
        return RecoveryResult(
            success=True,
            strategy_used=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            action_taken="Immediate retry initiated",
            time_elapsed_ms=0,
            should_continue=True,
        )


class ExponentialBackoffHandler:
    """Handler for exponential backoff strategy."""

    def __init__(self) -> None:
        """Initialize handler."""
        self.logger = get_logger("ExponentialBackoffHandler")

    async def can_handle(self, error: WebSocketStreamError) -> bool:
        """Check if this handler can handle the error.

        Returns:
            True if this handler can handle exponential backoff errors
        """
        return error.get_recovery_strategy() == WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF

    async def handle(
        self, error: WebSocketStreamError, recovery: WebSocketErrorRecovery | None = None
    ) -> RecoveryResult:
        """Handle exponential backoff recovery.

        Returns:
            RecoveryResult indicating exponential backoff action with delay
        """
        delay_ms = error.get_retry_delay_ms()

        self.logger.info(
            "exponential_backoff_recovery",
            error_code=error.code.name,
            delay_ms=delay_ms,
            retry_count=error.context.metadata.retry_count,
        )

        # Apply the delay
        await asyncio.sleep(delay_ms / 1000.0)

        return RecoveryResult(
            success=True,
            strategy_used=WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF,
            action_taken=f"Applied exponential backoff delay of {delay_ms}ms",
            time_elapsed_ms=delay_ms,
            should_continue=True,
        )


class ReconnectionHandler:
    """Handler for reconnection strategies."""

    def __init__(self) -> None:
        """Initialize handler."""
        self.logger = get_logger("ReconnectionHandler")

    async def can_handle(self, error: WebSocketStreamError) -> bool:
        """Check if this handler can handle the error.

        Returns:
            True if this handler can handle reconnection strategy errors
        """
        return error.get_recovery_strategy() in {
            WebSocketRecoveryStrategy.RECONNECT_SAME,
            WebSocketRecoveryStrategy.RECONNECT_DIFFERENT,
            WebSocketRecoveryStrategy.FULL_RECONNECT,
        }

    async def handle(
        self, error: WebSocketStreamError, recovery: WebSocketErrorRecovery | None = None
    ) -> RecoveryResult:
        """Handle reconnection recovery.

        Returns:
            RecoveryResult indicating reconnection action taken
        """
        strategy = error.get_recovery_strategy()

        action = "Reconnecting to same endpoint"
        if strategy == WebSocketRecoveryStrategy.RECONNECT_DIFFERENT:
            action = "Reconnecting to different endpoint"
        elif strategy == WebSocketRecoveryStrategy.FULL_RECONNECT:
            action = "Performing full reconnection with state reset"

        self.logger.info(
            "reconnection_recovery",
            strategy=strategy.name,
            error_code=error.code.name,
            action=action,
        )

        # If we have a recovery system, trigger reconnection
        if recovery:
            # This would trigger actual reconnection logic
            pass

        return RecoveryResult(
            success=True,
            strategy_used=strategy,
            action_taken=action,
            should_continue=True,
            metadata={"reconnect_count": str(error.context.reconnect_count)},
        )


class ResubscriptionHandler:
    """Handler for resubscription strategies."""

    def __init__(self) -> None:
        """Initialize handler."""
        self.logger = get_logger("ResubscriptionHandler")

    async def can_handle(self, error: WebSocketStreamError) -> bool:
        """Check if this handler can handle the error.

        Returns:
            True if this handler can handle resubscription strategy errors
        """
        return error.get_recovery_strategy() in {
            WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
            WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
        }

    async def handle(
        self, error: WebSocketStreamError, recovery: WebSocketErrorRecovery | None = None
    ) -> RecoveryResult:
        """Handle resubscription recovery.

        Returns:
            RecoveryResult indicating resubscription action taken
        """
        strategy = error.get_recovery_strategy()

        if strategy == WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE:
            action = f"Resubscribing to channel: {error.context.channel}"
        else:
            action = "Resubscribing to all channels"

        self.logger.info(
            "resubscription_recovery",
            strategy=strategy.name,
            channel=error.context.channel,
            action=action,
        )

        return RecoveryResult(
            success=True,
            strategy_used=strategy,
            action_taken=action,
            should_continue=True,
            metadata={"channel": error.context.channel or "all"},
        )


class CircuitBreakerHandler:
    """Handler for circuit breaker strategy."""

    def __init__(self) -> None:
        """Initialize handler."""
        self.logger = get_logger("CircuitBreakerHandler")

    async def can_handle(self, error: WebSocketStreamError) -> bool:
        """Check if this handler can handle the error.

        Returns:
            True if this handler can handle circuit breaker strategy errors
        """
        return error.get_recovery_strategy() == WebSocketRecoveryStrategy.CIRCUIT_BREAKER

    async def handle(
        self, error: WebSocketStreamError, recovery: WebSocketErrorRecovery | None = None
    ) -> RecoveryResult:
        """Handle circuit breaker recovery.

        Returns:
            RecoveryResult indicating circuit breaker activation
        """
        self.logger.warning(
            "circuit_breaker_activated",
            error_code=error.code.name,
            reconnect_count=error.context.reconnect_count,
        )

        return RecoveryResult(
            success=False,
            strategy_used=WebSocketRecoveryStrategy.CIRCUIT_BREAKER,
            action_taken="Circuit breaker activated - stopping recovery attempts",
            should_continue=False,
            error_message="Too many failures - circuit breaker opened",
        )


class DegradeServiceHandler:
    """Handler for service degradation strategy."""

    def __init__(self) -> None:
        """Initialize handler."""
        self.logger = get_logger("DegradeServiceHandler")

    async def can_handle(self, error: WebSocketStreamError) -> bool:
        """Check if this handler can handle the error.

        Returns:
            True if this handler can handle service degradation strategy errors
        """
        return error.get_recovery_strategy() == WebSocketRecoveryStrategy.DEGRADE_SERVICE

    async def handle(
        self, error: WebSocketStreamError, recovery: WebSocketErrorRecovery | None = None
    ) -> RecoveryResult:
        """Handle service degradation recovery.

        Returns:
            RecoveryResult indicating service degradation action taken
        """
        self.logger.warning(
            "service_degradation",
            error_code=error.code.name,
            message="Degrading service due to error",
        )

        return RecoveryResult(
            success=True,
            strategy_used=WebSocketRecoveryStrategy.DEGRADE_SERVICE,
            action_taken="Service degraded to maintain partial functionality",
            should_continue=True,
            metadata={"degradation_reason": error.message},
        )


class NoRecoveryHandler:
    """Handler for no recovery strategy."""

    def __init__(self) -> None:
        """Initialize handler."""
        self.logger = get_logger("NoRecoveryHandler")

    async def can_handle(self, error: WebSocketStreamError) -> bool:
        """Check if this handler can handle the error.

        Returns:
            True if this handler can handle no recovery strategy errors
        """
        return error.get_recovery_strategy() == WebSocketRecoveryStrategy.NONE

    async def handle(
        self, error: WebSocketStreamError, recovery: WebSocketErrorRecovery | None = None
    ) -> RecoveryResult:
        """Handle no recovery strategy.

        Returns:
            RecoveryResult indicating no recovery action available
        """
        self.logger.error(
            "no_recovery_available",
            error_code=error.code.name,
            message=error.message,
        )

        return RecoveryResult(
            success=False,
            strategy_used=WebSocketRecoveryStrategy.NONE,
            action_taken="No recovery action taken - error is non-retryable",
            should_continue=False,
            error_message=error.message,
        )


class RecoveryStrategyRouter:
    """Routes recovery strategies to appropriate handlers."""

    def __init__(self) -> None:
        """Initialize the recovery strategy router."""
        self.logger = get_logger("RecoveryStrategyRouter")

        # Register all handlers
        self.handlers: list[RecoveryHandler] = [
            ImmediateRetryHandler(),
            ExponentialBackoffHandler(),
            ReconnectionHandler(),
            ResubscriptionHandler(),
            CircuitBreakerHandler(),
            DegradeServiceHandler(),
            NoRecoveryHandler(),
        ]

        # Track recovery attempts
        self.recovery_attempts: dict[str, int] = {}

    async def route_recovery(
        self, error: WebSocketStreamError, recovery: WebSocketErrorRecovery | None = None
    ) -> RecoveryResult:
        """Route error to appropriate recovery handler.

        Args:
            error: The WebSocket stream error
            recovery: Optional recovery system reference

        Returns:
            Result of the recovery attempt
        """
        # Track recovery attempts
        error_key = f"{error.code.name}:{error.context.connection_id}"
        self.recovery_attempts[error_key] = self.recovery_attempts.get(error_key, 0) + 1

        self.logger.info(
            "routing_recovery",
            error_code=error.code.name,
            strategy=error.get_recovery_strategy().name,
            attempt=self.recovery_attempts[error_key],
        )

        # Find appropriate handler
        for handler in self.handlers:
            if await handler.can_handle(error):
                try:
                    result = await handler.handle(error, recovery)

                    # Log result
                    self.logger.info(
                        "recovery_result",
                        success=result.success,
                        strategy=result.strategy_used.name,
                        action=result.action_taken,
                        should_continue=result.should_continue,
                    )

                except Exception as e:
                    self.logger.exception(
                        "recovery_handler_error",
                        handler=type(handler).__name__,
                        error=str(e),
                    )

                    return RecoveryResult(
                        success=False,
                        strategy_used=error.get_recovery_strategy(),
                        action_taken="Recovery handler failed",
                        should_continue=False,
                        error_message=str(e),
                    )
                else:
                    # Reset attempts on success
                    if result.success and not result.should_continue:
                        self.recovery_attempts.pop(error_key, None)

                    return result

        # No handler found (shouldn't happen)
        self.logger.error(
            "no_recovery_handler_found",
            error_code=error.code.name,
            strategy=error.get_recovery_strategy().name,
        )

        return RecoveryResult(
            success=False,
            strategy_used=error.get_recovery_strategy(),
            action_taken="No recovery handler available",
            should_continue=False,
            error_message="No recovery handler found for strategy",
        )

    def get_recovery_action(self, error: WebSocketStreamError) -> RecoveryAction:
        """Get the recovery action for an error.

        Args:
            error: The WebSocket stream error

        Returns:
            The recovery action to take
        """
        strategy = error.get_recovery_strategy()

        # Build recovery action based on strategy
        action = RecoveryAction(
            strategy=strategy,
            delay_ms=error.get_retry_delay_ms()
            if strategy != WebSocketRecoveryStrategy.IMMEDIATE_RETRY
            else 0,
        )

        # Set flags based on strategy
        if strategy in {
            WebSocketRecoveryStrategy.RECONNECT_SAME,
            WebSocketRecoveryStrategy.RECONNECT_DIFFERENT,
            WebSocketRecoveryStrategy.FULL_RECONNECT,
        }:
            action.should_reconnect = True
            if strategy == WebSocketRecoveryStrategy.FULL_RECONNECT:
                action.should_clear_state = True

        elif strategy in {
            WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
            WebSocketRecoveryStrategy.RESUBSCRIBE_ALL,
        }:
            action.should_resubscribe = True

        elif strategy == WebSocketRecoveryStrategy.DEGRADE_SERVICE:
            action.should_degrade_service = True

        # Set max retries based on error criticality
        if error.is_critical:
            action.max_retries = CRITICAL_MAX_RETRIES
        elif error.severity.value >= ERROR_SEVERITY_THRESHOLD:  # ERROR or higher
            action.max_retries = HIGH_SEVERITY_MAX_RETRIES
        else:
            action.max_retries = DEFAULT_MAX_RETRIES

        return action

    def reset_attempts(self, connection_id: str | None = None) -> None:
        """Reset recovery attempt counters.

        Args:
            connection_id: Optional connection ID to reset (resets all if None)
        """
        if connection_id:
            # Reset attempts for specific connection
            keys_to_remove = [key for key in self.recovery_attempts if connection_id in key]
            for key in keys_to_remove:
                self.recovery_attempts.pop(key, None)
        else:
            # Reset all attempts
            self.recovery_attempts.clear()

    def get_stats(self) -> dict[str, int]:
        """Get recovery statistics.

        Returns:
            Dictionary of recovery statistics
        """
        return {
            "total_handlers": len(self.handlers),
            "active_recovery_attempts": len(self.recovery_attempts),
            "total_attempts": sum(self.recovery_attempts.values()),
        }
