"""Handler Manager for lifecycle management of event handlers.

Manages the startup, shutdown, and health monitoring of all event handlers
in the system.
"""

import asyncio
from typing import TYPE_CHECKING

from cyberdelta.config.models.event_system_config import EventMonitoringConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.models.events.handler_health import HandlerHealthModel


if TYPE_CHECKING:
    from cyberdelta.domain.base_event_handler import EventHandlerActor


logger = get_logger(__name__)


class HandlerManager:
    """Manages lifecycle of all event handlers in the system.

    Responsibilities:
    - Start all handlers in proper sequence
    - Graceful shutdown in reverse order
    - Health monitoring and auto-degradation
    - State tracking for all components
    """

    def __init__(self, config: EventMonitoringConfig) -> None:
        """Initialize the handler manager.

        Args:
            config: Event monitoring configuration
        """
        self.handlers: list[EventHandlerActor] = []  # Will contain EventHandlerActor instances
        self.config = config
        # Maximum time for graceful shutdown from configuration
        self._shutdown_timeout = config.handler_shutdown_timeout_sec

    def register_handler(self, handler: "EventHandlerActor") -> None:
        """Register a handler to be managed.

        Args:
            handler: An EventHandlerActor instance
        """
        # Type-safe check - EventHandlerActor protocol ensures these properties exist
        try:
            handler_id = handler.handler_id
            # Verify handler has required methods
            _ = handler.start  # Check method exists without storing unused variable
            self.handlers.append(handler)
            logger.info("handler_registered", handler_id=handler_id)
        except AttributeError as e:
            logger.warning("invalid_handler_interface", handler=str(handler), error=str(e))

    async def start_all(self) -> None:
        """Start all handlers with proper initialization sequence.

        Handlers are started in registration order.
        Each handler transitions from PRE_INITIALIZED to RUNNING.
        """
        logger.info("starting_handlers", count=len(self.handlers))

        for handler in self.handlers:
            try:
                await handler.start()
                logger.info(
                    "handler_started", handler_id=handler.handler_id, state=handler.state.value
                )
            except Exception:
                logger.exception("handler_start_failed", handler_id=handler.handler_id)
                # Continue starting other handlers even if one fails

    async def stop_all(self) -> None:
        """Graceful shutdown of all handlers in reverse order.

        Handlers are stopped in reverse registration order to properly
        clean up dependencies. Each handler transitions to STOPPED state.
        """
        logger.info("stopping_handlers", count=len(self.handlers))

        # Stop in reverse order
        for handler in reversed(self.handlers):
            try:
                # Use asyncio.wait_for to prevent hanging on shutdown
                await asyncio.wait_for(
                    handler.stop(), timeout=self._shutdown_timeout / len(self.handlers)
                )
                logger.info(
                    "handler_stopped", handler_id=handler.handler_id, state=handler.state.value
                )
            except TimeoutError:
                # TimeoutError is expected during shutdown
                logger.exception("handler_stop_timeout", handler_id=handler.handler_id)
            except Exception:
                logger.exception("handler_stop_error", handler_id=handler.handler_id)

    async def check_health(self) -> dict[str, HandlerHealthModel]:
        """Check health of all registered handlers.

        Returns:
            Dictionary with handler health status including:
            - state: Current component state
            - error_count: Number of errors encountered
            - metrics: Handler-specific metrics
        """
        health_status: dict[str, HandlerHealthModel] = {}

        for handler in self.handlers:
            handler_health = HandlerHealthModel(
                state=ComponentState.PRE_INITIALIZED,  # Use enum instead of string
                error_count=0,
                metrics={},
            )

            # Use type-safe property access - EventHandlerActor protocol ensures these exist
            handler_health.state = handler.state
            handler_health.error_count = handler.error_count
            handler_health.metrics = handler.get_metrics()

            health_status[handler.handler_id] = handler_health

        return health_status

    async def monitor_and_degrade(self) -> None:
        """Monitor handlers and auto-degrade on error thresholds.

        This method should be called periodically to check handler health
        and automatically degrade handlers that exceed error thresholds.
        """
        for handler in self.handlers:
            # Type-safe property access - EventHandlerActor protocol ensures these exist
            # Check if handler should be degraded (use handler's own config thresholds)
            if (
                handler.error_count > handler.config.auto_degrade_after_errors
                and handler.state == ComponentState.RUNNING
            ):
                try:
                    await handler.degrade()
                    logger.warning(
                        "handler_auto_degraded",
                        handler_id=handler.handler_id,
                        error_count=handler.error_count,
                    )
                except Exception:
                    logger.exception("handler_degrade_failed", handler_id=handler.handler_id)

            # Check if handler should be faulted
            elif (
                handler.error_count > handler.config.auto_fault_after_errors
                and handler.state == ComponentState.DEGRADED
            ):
                try:
                    await handler.fault()
                    logger.error(
                        "handler_auto_faulted",
                        handler_id=handler.handler_id,
                        error_count=handler.error_count,
                    )
                except Exception:
                    logger.exception("handler_fault_failed", handler_id=handler.handler_id)

    def get_handler_by_id(self, handler_id: str) -> "EventHandlerActor | None":
        """Get a specific handler by its ID.

        Args:
            handler_id: The ID of the handler to retrieve

        Returns:
            The handler instance or None if not found
        """
        for handler in self.handlers:
            # Type-safe property access - EventHandlerActor protocol ensures handler_id exists
            if handler.handler_id == handler_id:
                return handler
        return None

    def clear_handlers(self) -> None:
        """Clear all registered handlers."""
        self.handlers.clear()
        logger.info("Cleared all registered handlers")
