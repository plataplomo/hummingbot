"""WebSocket Recovery Executor.

This module implements the execution layer of the recovery system,
responsible for HOW to execute recovery actions decided by the policy manager.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy
from cyberdelta.apis.exceptions.websocket import WebSocketStreamError
from cyberdelta.apis.protocols.websocket.recovery import (
    ConnectionManagerProtocol,
    MessageBufferProtocol,
    StateManagerProtocol,
    SubscriptionManagerProtocol,
)
from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.error_context.recovery.recovery_policy import (
        RecoveryPolicyManager,
    )


# ============================================================================
# Recovery Executor
# ============================================================================


class RecoveryExecutor:
    """Executes recovery strategies decided by the policy manager.

    This class is responsible for implementing HOW recovery actions are
    executed, using the various manager protocols to perform the actual
    recovery operations.
    """

    def __init__(
        self,
        policy: RecoveryPolicyManager,
        connection_manager: ConnectionManagerProtocol | None = None,
        subscription_manager: SubscriptionManagerProtocol | None = None,
        state_manager: StateManagerProtocol | None = None,
        message_buffer: MessageBufferProtocol | None = None,
    ) -> None:
        """Initialize the recovery executor.

        Args:
            policy: Recovery policy manager
            connection_manager: Connection management implementation
            subscription_manager: Subscription management implementation
            state_manager: State management implementation
            message_buffer: Message buffer implementation
        """
        self.policy = policy
        self.connection_manager = connection_manager
        self.subscription_manager = subscription_manager
        self.state_manager = state_manager
        self.message_buffer = message_buffer
        self.logger = get_logger("RecoveryExecutor")

        # Initialize strategy handlers
        self._strategy_handlers = self._init_strategy_handlers()

    def _init_strategy_handlers(
        self,
    ) -> dict[WebSocketRecoveryStrategy, Callable[[WebSocketStreamError], Awaitable[bool]]]:
        """Initialize mapping of strategies to handler methods.

        Returns:
            Dictionary mapping strategies to handler methods
        """
        return {
            WebSocketRecoveryStrategy.NONE: self._handle_none,
            WebSocketRecoveryStrategy.IMMEDIATE_RETRY: self._handle_immediate_retry,
            WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF: self._handle_exponential_backoff,
            WebSocketRecoveryStrategy.LINEAR_BACKOFF: self._handle_linear_backoff,
            WebSocketRecoveryStrategy.RECONNECT_SAME: self._handle_reconnect_same,
            WebSocketRecoveryStrategy.RECONNECT_DIFFERENT: self._handle_reconnect_different,
            WebSocketRecoveryStrategy.FULL_RECONNECT: self._handle_full_reconnect,
            WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE: self._handle_resubscribe_single,
            WebSocketRecoveryStrategy.RESUBSCRIBE_ALL: self._handle_resubscribe_all,
            WebSocketRecoveryStrategy.RESUBSCRIBE_SELECTIVE: self._handle_resubscribe_selective,
            WebSocketRecoveryStrategy.CIRCUIT_BREAKER: self._handle_circuit_breaker,
            WebSocketRecoveryStrategy.FALLBACK_EXCHANGE: self._handle_fallback_exchange,
            WebSocketRecoveryStrategy.DEGRADE_SERVICE: self._handle_degrade_service,
        }

    async def execute_recovery(
        self,
        error: WebSocketStreamError,
        strategy: WebSocketRecoveryStrategy | None = None,
    ) -> bool:
        """Execute recovery strategy for an error.

        Args:
            error: The WebSocket error to recover from
            strategy: Optional override strategy (uses policy decision if None)

        Returns:
            True if recovery was successful
        """
        # Get strategy from policy if not provided
        if strategy is None:
            strategy = self.policy.get_recovery_strategy(error)

        self.logger.info(
            "Executing recovery strategy",
            strategy=strategy.name,
            error_code=error.code.name,
            connection_id=error.context.connection_id,
            exchange=error.context.exchange,
        )

        # Get handler for strategy
        handler = self._strategy_handlers.get(strategy)
        if not handler:
            self.logger.error(
                "No handler for strategy",
                strategy=strategy.name,
            )
            return False

        try:
            # Execute the recovery strategy
            success = await handler(error)

            # Update policy state based on outcome
            self.policy.update_circuit_state(error.context, success)
            self.policy.update_retry_state(error.context, success)

            if success:
                self.logger.info(
                    "Recovery successful",
                    strategy=strategy.name,
                    connection_id=error.context.connection_id,
                )
            else:
                self.logger.warning(
                    "Recovery failed",
                    strategy=strategy.name,
                    connection_id=error.context.connection_id,
                )

        except Exception as e:
            self.logger.exception(
                "Recovery execution error",
                strategy=strategy.name,
                error=str(e),
            )
            # Record as failure
            self.policy.update_circuit_state(error.context, False)
            self.policy.update_retry_state(error.context, False)
            return False
        else:
            return success

    # ========================================================================
    # Strategy Handlers
    # ========================================================================

    async def _handle_none(self, error: WebSocketStreamError) -> bool:
        """Handle NONE strategy (no recovery).

        Args:
            error: The WebSocket error

        Returns:
            Always returns False
        """
        self.logger.debug(
            "No recovery strategy applied",
            error_code=error.code.name,
        )
        return False

    async def _handle_immediate_retry(self, error: WebSocketStreamError) -> bool:
        """Handle immediate retry strategy.

        Args:
            error: The WebSocket error

        Returns:
            True if retry successful
        """
        # No delay, just retry the connection
        if not self.connection_manager:
            self.logger.warning("No connection manager available for immediate retry")
            return False

        return await self.connection_manager.reconnect(
            error.context.connection_id,
            error.context.exchange,
        )

    async def _handle_exponential_backoff(self, error: WebSocketStreamError) -> bool:
        """Handle exponential backoff retry strategy.

        Args:
            error: The WebSocket error

        Returns:
            True if retry successful after backoff
        """
        if not self.connection_manager:
            self.logger.warning("No connection manager available for backoff retry")
            return False

        # Calculate delay
        attempt = self.policy.get_retry_count(error.context)
        delay = self.policy.calculate_backoff_delay(error, attempt)

        self.logger.debug(
            "Applying exponential backoff",
            delay=delay,
            attempt=attempt,
        )

        # Wait for backoff period
        await asyncio.sleep(delay)

        # Attempt reconnection
        return await self.connection_manager.reconnect(
            error.context.connection_id,
            error.context.exchange,
        )

    async def _handle_linear_backoff(self, error: WebSocketStreamError) -> bool:
        """Handle linear backoff retry strategy.

        Args:
            error: The WebSocket error

        Returns:
            True if retry successful after backoff
        """
        if not self.connection_manager:
            self.logger.warning("No connection manager available for linear backoff")
            return False

        # Simple linear delay (override policy calculation)
        attempt = self.policy.get_retry_count(error.context)
        delay = min(attempt * 2.0, 60.0)  # 2 seconds per attempt, max 60 seconds

        self.logger.debug(
            "Applying linear backoff",
            delay=delay,
            attempt=attempt,
        )

        await asyncio.sleep(delay)

        return await self.connection_manager.reconnect(
            error.context.connection_id,
            error.context.exchange,
        )

    async def _handle_reconnect_same(self, error: WebSocketStreamError) -> bool:
        """Handle reconnection to same endpoint.

        Args:
            error: The WebSocket error

        Returns:
            True if reconnection successful
        """
        if not self.connection_manager:
            self.logger.warning("No connection manager available for reconnection")
            return False

        return await self.connection_manager.reconnect(
            error.context.connection_id,
            error.context.exchange,
            force=False,
        )

    async def _handle_reconnect_different(self, error: WebSocketStreamError) -> bool:
        """Handle reconnection to different endpoint.

        Args:
            error: The WebSocket error

        Returns:
            True if reconnection successful
        """
        if not self.connection_manager:
            self.logger.warning("No connection manager available for different reconnection")
            return False

        # Reset connection completely before reconnecting
        await self.connection_manager.reset_connection(
            error.context.connection_id,
            error.context.exchange,
        )

        return await self.connection_manager.reconnect(
            error.context.connection_id,
            error.context.exchange,
            force=True,
        )

    async def _handle_full_reconnect(self, error: WebSocketStreamError) -> bool:
        """Handle full reconnection with state restoration.

        Args:
            error: The WebSocket error

        Returns:
            True if full reconnection successful
        """
        if not self.connection_manager:
            self.logger.warning("No connection manager available for full reconnect")
            return False

        # Save state if available
        if self.state_manager:
            await self.state_manager.save_state(
                error.context.connection_id,
                error.context.exchange,
            )

        # Reset and reconnect
        await self.connection_manager.reset_connection(
            error.context.connection_id,
            error.context.exchange,
        )

        success = await self.connection_manager.reconnect(
            error.context.connection_id,
            error.context.exchange,
            force=True,
        )

        if success:
            # Restore state if available
            if self.state_manager:
                await self.state_manager.restore_state(
                    error.context.connection_id,
                    error.context.exchange,
                )

            # Resubscribe if available
            if self.subscription_manager:
                await self.subscription_manager.resubscribe_all(
                    error.context.connection_id,
                    error.context.exchange,
                )

            # Replay messages if available
            if self.message_buffer and error.context.last_received_sequence:
                await self.message_buffer.replay_messages(
                    error.context.connection_id,
                    error.context.exchange,
                    since_sequence=error.context.last_received_sequence,
                )

        return success

    async def _handle_resubscribe_single(self, error: WebSocketStreamError) -> bool:
        """Handle single channel resubscription.

        Args:
            error: The WebSocket error

        Returns:
            True if resubscription successful
        """
        if not self.subscription_manager:
            self.logger.warning("No subscription manager available for resubscribe")
            return False

        return await self.subscription_manager.resubscribe(
            error.context.connection_id,
            error.context.exchange,
            channel=error.context.channel,
            topic=error.context.topic,
        )

    async def _handle_resubscribe_all(self, error: WebSocketStreamError) -> bool:
        """Handle all channels resubscription.

        Args:
            error: The WebSocket error

        Returns:
            True if all resubscriptions successful
        """
        if not self.subscription_manager:
            self.logger.warning("No subscription manager available for resubscribe all")
            return False

        return await self.subscription_manager.resubscribe_all(
            error.context.connection_id,
            error.context.exchange,
        )

    async def _handle_resubscribe_selective(self, error: WebSocketStreamError) -> bool:
        """Handle selective channel resubscription.

        Args:
            error: The WebSocket error

        Returns:
            True if selective resubscriptions successful
        """
        if not self.subscription_manager:
            self.logger.warning("No subscription manager available for selective resubscribe")
            return False

        # Get active subscriptions
        subscriptions = await self.subscription_manager.get_active_subscriptions(
            error.context.connection_id,
            error.context.exchange,
        )

        # Resubscribe to critical channels only
        critical_channels = ["orderbook", "trades", "userEvents"]
        success = True

        for channel, topic in subscriptions:
            if channel in critical_channels:
                result = await self.subscription_manager.resubscribe(
                    error.context.connection_id,
                    error.context.exchange,
                    channel=channel,
                    topic=topic,
                )
                success = success and result

        return success

    async def _handle_circuit_breaker(self, error: WebSocketStreamError) -> bool:
        """Handle circuit breaker strategy.

        Args:
            error: The WebSocket error

        Returns:
            False (circuit breaker doesn't attempt recovery)
        """
        self.logger.warning(
            "Circuit breaker activated",
            connection_id=error.context.connection_id,
            exchange=error.context.exchange,
        )

        # Clear any saved state
        if self.state_manager:
            await self.state_manager.clear_state(
                error.context.connection_id,
                error.context.exchange,
            )

        # Clear message buffer
        if self.message_buffer:
            await self.message_buffer.clear_buffer(
                error.context.connection_id,
                error.context.exchange,
            )

        return False

    async def _handle_fallback_exchange(self, error: WebSocketStreamError) -> bool:
        """Handle fallback to alternate exchange.

        Args:
            error: The WebSocket error

        Returns:
            False (fallback exchange not supported)
        """
        self.logger.error(
            "Fallback exchange strategy not implemented",
            connection_id=error.context.connection_id,
            exchange=error.context.exchange,
        )
        return False

    async def _handle_degrade_service(self, error: WebSocketStreamError) -> bool:
        """Handle service degradation strategy.

        Args:
            error: The WebSocket error

        Returns:
            True if degradation successful
        """
        self.logger.info(
            "Degrading service",
            connection_id=error.context.connection_id,
            exchange=error.context.exchange,
        )

        if not self.subscription_manager:
            return False

        # Unsubscribe from non-critical channels
        subscriptions = await self.subscription_manager.get_active_subscriptions(
            error.context.connection_id,
            error.context.exchange,
        )

        critical_channels = ["userEvents"]  # Only keep critical channels
        success = True

        for channel, topic in subscriptions:
            if channel not in critical_channels:
                # In a real implementation, would have unsubscribe method
                self.logger.debug("Would unsubscribe from channel", channel=channel, topic=topic)

        return success

    # ========================================================================
    # Utility Methods
    # ========================================================================

    async def shutdown(self) -> None:
        """Gracefully shutdown the executor."""
        self.logger.info("Recovery executor shutting down")
        # Clean up any resources if needed

    def get_statistics(self) -> dict[str, object]:
        """Get executor statistics.

        Returns:
            Dictionary of statistics
        """
        return {
            "has_connection_manager": self.connection_manager is not None,
            "has_subscription_manager": self.subscription_manager is not None,
            "has_state_manager": self.state_manager is not None,
            "has_message_buffer": self.message_buffer is not None,
            "available_strategies": len(self._strategy_handlers),
        }
