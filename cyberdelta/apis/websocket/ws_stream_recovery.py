"""WebSocket stream recovery system with type-safe strategies.

Implements various recovery strategies for WebSocket stream errors,
using typed enums and pattern matching for strategy selection.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

from cyberdelta.apis.common.error_foundation import (
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.config.models.websocket_error_config import (
    WebSocketErrorRecoveryConfig,
)


if TYPE_CHECKING:
    from logging import Logger

    from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext


# ============================================================================
# Protocols
# ============================================================================


class ConnectionManagerProtocol(Protocol):
    """Protocol for WebSocket connection managers."""

    async def reconnect(
        self,
        connection_id: str,
        exchange: str,
        force: bool = False,
    ) -> bool:
        """Reconnect a WebSocket connection.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name
            force: Force reconnection even if connected

        Returns:
            True if reconnection successful
        """
        ...

    async def reset_connection(
        self,
        connection_id: str,
        exchange: str,
    ) -> bool:
        """Reset a WebSocket connection completely.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name

        Returns:
            True if reset successful
        """
        ...

    async def get_connection_state(
        self,
        connection_id: str,
        exchange: str,
    ) -> str:
        """Get current connection state.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name

        Returns:
            Connection state (e.g., 'connected', 'disconnected')
        """
        ...


class SubscriptionManagerProtocol(Protocol):
    """Protocol for subscription managers."""

    async def resubscribe(
        self,
        connection_id: str,
        exchange: str,
        channel: str | None = None,
    ) -> bool:
        """Resubscribe to channels.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name
            channel: Specific channel or None for all

        Returns:
            True if resubscription successful
        """
        ...

    async def clear_subscriptions(
        self,
        connection_id: str,
        exchange: str,
    ) -> None:
        """Clear all subscriptions for a connection.

        Args:
            connection_id: Connection identifier
            exchange: Exchange name
        """
        ...


class StateManagerProtocol(Protocol):
    """Protocol for state managers."""

    async def request_snapshot(
        self,
        exchange: str,
        channel: str,
        symbol: str | None = None,
    ) -> bool:
        """Request a state snapshot.

        Args:
            exchange: Exchange name
            channel: Channel name
            symbol: Optional symbol

        Returns:
            True if snapshot requested successfully
        """
        ...

    async def clear_state(
        self,
        exchange: str,
        channel: str | None = None,
    ) -> None:
        """Clear cached state.

        Args:
            exchange: Exchange name
            channel: Specific channel or None for all
        """
        ...


# ============================================================================
# Recovery System Implementation
# ============================================================================


class StreamRecoverySystem:
    """WebSocket stream recovery system.

    Implements various recovery strategies for WebSocket errors,
    with type-safe strategy selection and execution.
    """

    def __init__(
        self,
        config: WebSocketErrorRecoveryConfig,
        connection_manager: ConnectionManagerProtocol | None = None,
        subscription_manager: SubscriptionManagerProtocol | None = None,
        state_manager: StateManagerProtocol | None = None,
        logger: Logger | None = None,
    ) -> None:
        """Initialize recovery system.

        Args:
            config: Recovery configuration
            connection_manager: Optional connection manager
            subscription_manager: Optional subscription manager
            state_manager: Optional state manager
            logger: Logger instance
        """
        self.config = config
        self._connection_mgr = connection_manager
        self._subscription_mgr = subscription_manager
        self._state_mgr = state_manager
        self._logger = logger or logging.getLogger(__name__)

        # Track recovery attempts
        self._recovery_attempts: dict[str, int] = {}
        self._last_recovery_times: dict[str, datetime] = {}

        # Track circuit breaker state
        self._circuit_breaker_active: dict[str, bool] = {}
        self._circuit_breaker_reset_times: dict[str, datetime] = {}

    # ========================================================================
    # Main Recovery Handler
    # ========================================================================

    async def handle_recovery(
        self,
        error: WebSocketStreamError,
        strategy: WebSocketRecoveryStrategy,
    ) -> bool:
        """Handle recovery for an error with a specific strategy.

        This method implements the RecoveryHandlerProtocol interface.

        Args:
            error: The WebSocket error to handle
            strategy: The recovery strategy to use

        Returns:
            True if recovery was successful
        """
        # Override the error's recovery strategy with the provided one
        old_strategy = error.recovery_strategy
        try:
            error.recovery_strategy = strategy
            return await self.handle_stream_error(error)
        finally:
            # Restore original strategy
            error.recovery_strategy = old_strategy

    async def handle_stream_error(
        self,
        error: WebSocketStreamError,
    ) -> bool:
        """Handle a WebSocket stream error with appropriate recovery.

        Args:
            error: The WebSocket error to handle

        Returns:
            True if recovery was successful
        """
        # Recovery is enabled unless max_recovery_attempts is set to 0
        if self.config.max_recovery_attempts == 0:
            self._logger.debug("Recovery disabled (max_attempts=0), skipping")
            return False

        # Check circuit breaker
        recovery_key = self._get_recovery_key(error.context)
        if self._is_circuit_breaker_active(recovery_key):
            self._logger.warning(f"Circuit breaker active for {recovery_key}, skipping recovery")
            return False

        # Check retry limits
        if not self._check_retry_limits(error, recovery_key):
            self._logger.error(
                f"Retry limit exceeded for {recovery_key}, activating circuit breaker"
            )
            self._activate_circuit_breaker(recovery_key)
            return False

        # Get recovery strategy
        strategy = error.get_recovery_strategy()

        # Execute recovery based on strategy using pattern matching
        success = await self._execute_recovery_strategy(error, strategy)

        # Update tracking
        self._update_recovery_tracking(recovery_key, success)

        return success

    # ========================================================================
    # Strategy Execution
    # ========================================================================

    async def _execute_recovery_strategy(
        self,
        error: WebSocketStreamError,
        strategy: WebSocketRecoveryStrategy,
    ) -> bool:
        """Execute recovery strategy using pattern matching.

        Args:
            error: The error to recover from
            strategy: Recovery strategy to apply

        Returns:
            True if recovery successful
        """
        self._logger.info(f"Executing recovery strategy {strategy.name} for {error.code.name}")

        # Pattern matching on recovery strategy
        match strategy:
            case WebSocketRecoveryStrategy.NONE:
                return False

            case WebSocketRecoveryStrategy.IMMEDIATE_RETRY:
                return await self._immediate_retry(error)

            case WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF:
                return await self._exponential_backoff_retry(error)

            case WebSocketRecoveryStrategy.LINEAR_BACKOFF:
                return await self._linear_backoff_retry(error)

            case WebSocketRecoveryStrategy.RECONNECT_SAME:
                return await self._reconnect(error)

            case WebSocketRecoveryStrategy.RECONNECT_DIFFERENT:
                return await self._reconnect_different(error)

            case WebSocketRecoveryStrategy.FULL_RECONNECT:
                return await self._full_reconnect(error)

            case WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE:
                return await self._resubscribe(error)

            case WebSocketRecoveryStrategy.RESUBSCRIBE_ALL:
                return await self._resubscribe_all(error)

            case WebSocketRecoveryStrategy.RESUBSCRIBE_SELECTIVE:
                return await self._resubscribe_selective(error)

            case WebSocketRecoveryStrategy.CIRCUIT_BREAKER:
                return await self._handle_circuit_breaker(error)

            case WebSocketRecoveryStrategy.FALLBACK_EXCHANGE:
                return await self._fallback_exchange(error)

            case WebSocketRecoveryStrategy.DEGRADE_SERVICE:
                return await self._degrade_service(error)

    # ========================================================================
    # Recovery Strategies
    # ========================================================================

    async def _immediate_retry(self, error: WebSocketStreamError) -> bool:
        """Immediate retry without delay.

        Args:
            error: The error to recover from

        Returns:
            True if recovery successful
        """
        self._logger.debug("Attempting immediate retry")

        # For connection errors, try reconnect
        if error.code.get_category() == "CONNECTION":
            return await self._reconnect(error)

        # For subscription errors, try resubscribe
        if error.code.get_category() == "SUBSCRIPTION":
            return await self._resubscribe(error)

        return False

    async def _exponential_backoff_retry(
        self,
        error: WebSocketStreamError,
    ) -> bool:
        """Retry with exponential backoff.

        Args:
            error: The error to recover from

        Returns:
            True if recovery successful
        """
        recovery_key = self._get_recovery_key(error.context)
        attempt = self._recovery_attempts.get(recovery_key, 0)

        # Calculate delay with exponential backoff
        delay_ms = min(
            self.config.initial_backoff_ms * (self.config.backoff_multiplier**attempt),
            self.config.max_backoff_ms,
        )

        self._logger.info(f"Exponential backoff retry, attempt {attempt + 1}, delay {delay_ms}ms")

        await asyncio.sleep(delay_ms / 1000.0)

        return await self._immediate_retry(error)

    async def _linear_backoff_retry(self, error: WebSocketStreamError) -> bool:
        """Retry with linear backoff.

        Args:
            error: The error to recover from

        Returns:
            True if recovery successful
        """
        recovery_key = self._get_recovery_key(error.context)
        attempt = self._recovery_attempts.get(recovery_key, 0)

        # Calculate delay with linear backoff
        delay_ms = min(
            self.config.initial_backoff_ms * (attempt + 1),
            self.config.max_backoff_ms,
        )

        self._logger.info(f"Linear backoff retry, attempt {attempt + 1}, delay {delay_ms}ms")

        await asyncio.sleep(delay_ms / 1000.0)

        return await self._immediate_retry(error)

    async def _reconnect(self, error: WebSocketStreamError) -> bool:
        """Reconnect WebSocket connection.

        Args:
            error: The error to recover from

        Returns:
            True if reconnection successful
        """
        if not self._connection_mgr:
            self._logger.warning("No connection manager available for reconnect")
            return False

        self._logger.info(f"Reconnecting {error.context.connection_id} to {error.context.exchange}")

        return await self._connection_mgr.reconnect(
            error.context.connection_id,
            error.context.exchange,
            force=False,
        )

    async def _full_reconnect(self, error: WebSocketStreamError) -> bool:
        """Full reconnect with connection reset.

        Args:
            error: The error to recover from

        Returns:
            True if reconnection successful
        """
        if not self._connection_mgr:
            self._logger.warning("No connection manager available for full reconnect")
            return False

        self._logger.info(
            f"Full reconnect for {error.context.connection_id} to {error.context.exchange}"
        )

        # Reset connection first
        reset_success = await self._connection_mgr.reset_connection(
            error.context.connection_id,
            error.context.exchange,
        )

        if not reset_success:
            self._logger.error("Failed to reset connection")
            return False

        # Then reconnect
        return await self._connection_mgr.reconnect(
            error.context.connection_id,
            error.context.exchange,
            force=True,
        )

    async def _resubscribe(self, error: WebSocketStreamError) -> bool:
        """Resubscribe to channels.

        Args:
            error: The error to recover from

        Returns:
            True if resubscription successful
        """
        if not self._subscription_mgr:
            self._logger.warning("No subscription manager available for resubscribe")
            return False

        self._logger.info(
            f"Resubscribing to {error.context.channel or 'all channels'} on "
            f"{error.context.exchange}"
        )

        return await self._subscription_mgr.resubscribe(
            error.context.connection_id,
            error.context.exchange,
            error.context.channel,
        )

    async def _reconnect_different(self, error: WebSocketStreamError) -> bool:
        """Reconnect to a different endpoint.

        Args:
            error: The error to recover from

        Returns:
            True if reconnection successful
        """
        if not self._connection_mgr:
            self._logger.warning("No connection manager available for reconnect")
            return False

        self._logger.info(f"Reconnecting to different endpoint for {error.context.connection_id}")

        # This would switch to a different endpoint
        # Implementation would depend on connection manager capabilities
        return await self._connection_mgr.reconnect(
            error.context.connection_id,
            error.context.exchange,
            force=True,  # Force different endpoint
        )

    async def _resubscribe_all(self, error: WebSocketStreamError) -> bool:
        """Resubscribe to all channels.

        Args:
            error: The error to recover from

        Returns:
            True if resubscription successful
        """
        if not self._subscription_mgr:
            self._logger.warning("No subscription manager available for resubscribe")
            return False

        self._logger.info(f"Resubscribing to all channels on {error.context.exchange}")

        return await self._subscription_mgr.resubscribe(
            error.context.connection_id,
            error.context.exchange,
            None,  # None means all channels
        )

    async def _resubscribe_selective(self, error: WebSocketStreamError) -> bool:
        """Selective resubscription based on error context.

        Args:
            error: The error to recover from

        Returns:
            True if successful
        """
        if not self._subscription_mgr:
            self._logger.warning("No subscription manager available for selective resubscribe")
            return False

        self._logger.info(f"Selective resubscribing for {error.context.exchange}")

        # Clear existing subscriptions
        await self._subscription_mgr.clear_subscriptions(
            error.context.connection_id,
            error.context.exchange,
        )

        # Selective resubscribe based on priority
        # This is a simplified version - real implementation would be more sophisticated
        return await self._subscription_mgr.resubscribe(
            error.context.connection_id,
            error.context.exchange,
            error.context.channel,  # Just resubscribe to the affected channel
        )

    async def _fallback_exchange(self, error: WebSocketStreamError) -> bool:
        """Fallback to alternative exchange.

        Args:
            error: The error to recover from

        Returns:
            False (not implemented - would require exchange fallback logic)
        """
        self._logger.warning(
            f"Exchange fallback requested for {error.context.exchange}, "
            "but fallback logic not implemented"
        )
        return False

    async def _degrade_service(self, error: WebSocketStreamError) -> bool:
        """Degrade service to essential functions only.

        Args:
            error: The error to recover from

        Returns:
            True if degradation successful
        """
        self._logger.warning(f"Service degradation triggered for {error.context.exchange}")

        # Clear non-essential subscriptions
        if self._subscription_mgr:
            # This would unsubscribe from non-essential channels
            # Implementation would depend on business logic
            await self._subscription_mgr.clear_subscriptions(
                error.context.connection_id,
                error.context.exchange,
            )

        return True

    async def _request_snapshot(self, error: WebSocketStreamError) -> bool:
        """Request state snapshot.

        Args:
            error: The error to recover from

        Returns:
            True if snapshot requested successfully
        """
        if not self._state_mgr:
            self._logger.warning("No state manager available for snapshot request")
            return False

        if not error.context.channel:
            self._logger.warning("No channel specified for snapshot request")
            return False

        self._logger.info(
            f"Requesting snapshot for {error.context.channel} on {error.context.exchange}"
        )

        return await self._state_mgr.request_snapshot(
            error.context.exchange,
            error.context.channel,
            error.context.topic,  # Topic often contains the symbol
        )

    async def _reset_state(self, error: WebSocketStreamError) -> bool:
        """Reset internal state.

        Args:
            error: The error to recover from

        Returns:
            True if state reset successfully
        """
        if not self._state_mgr:
            self._logger.warning("No state manager available for state reset")
            return False

        self._logger.info(
            f"Resetting state for {error.context.channel or 'all channels'} on "
            f"{error.context.exchange}"
        )

        await self._state_mgr.clear_state(
            error.context.exchange,
            error.context.channel,
        )

        # If we have a channel, request a snapshot
        if error.context.channel and self._state_mgr:
            return await self._state_mgr.request_snapshot(
                error.context.exchange,
                error.context.channel,
                error.context.topic,  # Topic often contains the symbol
            )

        return True

    async def _handle_circuit_breaker(self, error: WebSocketStreamError) -> bool:
        """Handle circuit breaker triggered.

        Args:
            error: The error that triggered circuit breaker

        Returns:
            False (circuit breaker doesn't recover immediately)
        """
        recovery_key = self._get_recovery_key(error.context)

        self._logger.critical(
            f"Circuit breaker triggered for {recovery_key} due to {error.code.name}"
        )

        # Activate circuit breaker
        self._activate_circuit_breaker(recovery_key)

        # Circuit breaker doesn't immediately recover
        return False

    # _request_manual_intervention removed - not in the actual recovery strategy enum

    async def _adaptive_recovery(self, error: WebSocketStreamError) -> bool:
        """Adaptive recovery based on error history.

        Args:
            error: The error to recover from

        Returns:
            True if recovery successful
        """
        recovery_key = self._get_recovery_key(error.context)
        attempts = self._recovery_attempts.get(recovery_key, 0)

        # Adapt strategy based on attempts
        if attempts == 0:
            # First attempt: immediate retry
            return await self._immediate_retry(error)
        if attempts < 3:
            # Early attempts: exponential backoff
            return await self._exponential_backoff_retry(error)
        if attempts < 5:
            # Mid attempts: full reconnect
            return await self._full_reconnect(error)
        # Many attempts: degrade service or give up
        return await self._degrade_service(error)

    # ========================================================================
    # Circuit Breaker Management
    # ========================================================================

    def _is_circuit_breaker_active(self, key: str) -> bool:
        """Check if circuit breaker is active.

        Args:
            key: Recovery key

        Returns:
            True if circuit breaker is active
        """
        if not self.config.circuit_breaker_enabled:
            return False

        if key not in self._circuit_breaker_active:
            return False

        if not self._circuit_breaker_active[key]:
            return False

        # Check if circuit breaker should reset
        reset_time = self._circuit_breaker_reset_times.get(key)
        if reset_time and datetime.now(UTC) >= reset_time:
            self._logger.info(f"Circuit breaker reset for {key}")
            self._circuit_breaker_active[key] = False
            return False

        return True

    def _activate_circuit_breaker(self, key: str) -> None:
        """Activate circuit breaker.

        Args:
            key: Recovery key
        """
        self._circuit_breaker_active[key] = True

        # Set reset time
        reset_time = datetime.now(UTC) + timedelta(
            milliseconds=self.config.circuit_breaker_timeout_ms
        )
        self._circuit_breaker_reset_times[key] = reset_time

        self._logger.warning(f"Circuit breaker activated for {key}, will reset at {reset_time}")

    # ========================================================================
    # Retry Management
    # ========================================================================

    def _check_retry_limits(self, error: WebSocketStreamError, key: str) -> bool:
        """Check if retry limits allow recovery attempt.

        Args:
            error: The error to check
            key: Recovery key

        Returns:
            True if retry is allowed
        """
        # Initialize if needed
        if key not in self._recovery_attempts:
            self._recovery_attempts[key] = 0
            self._last_recovery_times[key] = datetime.now(UTC)
            return True

        # Check time window for reset
        now = datetime.now(UTC)
        last_time = self._last_recovery_times[key]

        # Use a default recovery window of 5 minutes (300 seconds)
        recovery_window_seconds = 300
        if (now - last_time).total_seconds() > recovery_window_seconds:
            # Reset counter after time window
            self._recovery_attempts[key] = 0
            self._last_recovery_times[key] = now
            return True

        # Check attempt limit
        return self._recovery_attempts[key] < self.config.max_recovery_attempts

    def _update_recovery_tracking(self, key: str, success: bool) -> None:
        """Update recovery tracking.

        Args:
            key: Recovery key
            success: Whether recovery was successful
        """
        if success:
            # Reset on success
            self._recovery_attempts[key] = 0
            self._logger.info(f"Recovery successful for {key}, resetting counter")
        else:
            # Increment on failure
            self._recovery_attempts[key] = self._recovery_attempts.get(key, 0) + 1
            self._last_recovery_times[key] = datetime.now(UTC)
            self._logger.warning(
                f"Recovery failed for {key}, attempt "
                f"{self._recovery_attempts[key]}/{self.config.max_recovery_attempts}"
            )

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def _get_recovery_key(self, context: StreamErrorContext) -> str:
        """Get recovery key from error context.

        Args:
            context: Error context

        Returns:
            Recovery key string
        """
        return f"{context.exchange}:{context.channel or 'global'}:{context.connection_id}"

    def get_recovery_stats(self) -> dict[str, Any]:
        """Get recovery statistics.

        Returns:
            Dictionary of recovery stats
        """
        return {
            "recovery_attempts": self._recovery_attempts.copy(),
            "circuit_breakers_active": [
                key for key, active in self._circuit_breaker_active.items() if active
            ],
            "total_keys_tracked": len(self._recovery_attempts),
        }

    def reset_recovery_stats(self) -> None:
        """Reset all recovery statistics."""
        self._recovery_attempts.clear()
        self._last_recovery_times.clear()
        self._circuit_breaker_active.clear()
        self._circuit_breaker_reset_times.clear()
        self._logger.info("Recovery statistics reset")

    async def shutdown(self) -> None:
        """Shutdown recovery system gracefully."""
        self._logger.info(f"Recovery system shutting down with stats: {self.get_recovery_stats()}")
        self.reset_recovery_stats()
