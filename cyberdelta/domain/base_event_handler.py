"""Base Event Handler with lifecycle management.

This module provides the base class for all event handlers in the system,
implementing lifecycle management, error handling, auto-degradation, and
performance optimizations through caching.
"""

import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING

import msgspec

from cyberdelta.config.models.event_system_config import EventHandlerConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.enums.event_bus import HandlerPriority
from cyberdelta.utils.retry_utils import create_retryer


if TYPE_CHECKING:
    from cyberdelta.infrastructure.event_bus import EventBus

logger = get_logger(__name__)


class EventHandlerActor(ABC):
    """Base event handler with lifecycle management and tenacity retry logic.

    Features:
    - Lifecycle hooks (on_start, on_stop, on_degrade, on_fault)
    - Automatic error counting and degradation
    - Handler-level caching for performance
    - Metrics collection
    - Tenacity retry logic for resilience

    Subclasses should implement:
    - on_start(): Initialize resources, warm caches, subscribe to events
    - on_stop(): Cleanup resources, persist state
    - handle_event(): Main event processing logic
    """

    def __init__(self, handler_id: str, event_bus: "EventBus", config: EventHandlerConfig) -> None:
        """Initialize the event handler.

        Args:
            handler_id: Unique identifier for this handler
            event_bus: EventBus instance for event subscription
            config: Event handler configuration
        """
        self.handler_id = handler_id
        self.event_bus = event_bus
        self.config = config
        self._state = ComponentState.PRE_INITIALIZED
        self._error_count = 0
        self._consecutive_errors = 0
        self._metrics: defaultdict[str, int] = defaultdict(int)
        self._cache: dict[str, msgspec.Struct] = {}  # Handler-level cache
        self._start_time: float | None = None

    async def start(self) -> None:
        """Start the handler with configuration-driven retry.

        Uses create_retryer helper for clean configuration-driven retry with logging.
        """
        retryer = create_retryer(
            self.config.retry_config, logger_name=f"{__name__}.{self.handler_id}"
        )
        await retryer(self._start_impl)

    async def _start_impl(self) -> None:
        """Start the handler implementation.

        Transitions from PRE_INITIALIZED to RUNNING state.
        Subclasses should implement on_start() for specific initialization.

        Raises:
            ConnectionError: When connection issues occur during startup
        """
        if self._state not in {ComponentState.PRE_INITIALIZED, ComponentState.STOPPED}:
            logger.warning(
                "cannot_start_handler", handler_id=self.handler_id, current_state=self._state.value
            )
            return

        try:
            self._start_time = time.time()
            await self.on_start()
            self._state = ComponentState.RUNNING
            logger.info("handler_started", handler_id=self.handler_id)
        except ConnectionError:
            # Let tenacity retry these
            raise
        except Exception:
            logger.exception("handler_start_failed", handler_id=self.handler_id)
            self._state = ComponentState.FAULTED
            raise

    async def stop(self) -> None:
        """Stop the handler with cleanup.

        Transitions to STOPPED state.
        Subclasses should implement on_stop() for specific cleanup.
        """
        if self._state not in {ComponentState.RUNNING, ComponentState.DEGRADED}:
            logger.warning(
                "cannot_stop_handler", handler_id=self.handler_id, current_state=self._state.value
            )
            return

        try:
            await self.on_stop()
            self._state = ComponentState.STOPPED

            # Log final metrics
            uptime = time.time() - self._start_time if self._start_time else 0
            logger.info(
                "handler_stopped",
                handler_id=self.handler_id,
                uptime_seconds=int(uptime),
                events_processed=self._metrics["events_processed"],
                total_errors=self._error_count,
            )
        except Exception:
            logger.exception("handler_stop_error", handler_id=self.handler_id)
            self._state = ComponentState.FAULTED

    async def degrade(self) -> None:
        """Enter degraded mode (reduced functionality).

        Transitions from RUNNING to DEGRADED state.
        In degraded mode, handlers typically process only critical operations.
        """
        if self._state != ComponentState.RUNNING:
            return

        self._state = ComponentState.DEGRADED
        logger.warning("handler_degrading", handler_id=self.handler_id)

        try:
            await self.on_degrade()
        except Exception:
            logger.exception("handler_degrade_error", handler_id=self.handler_id)

    async def fault(self) -> None:
        """Enter faulted state (non-operational).

        Transitions to FAULTED state.
        Handler stops processing all events.
        """
        self._state = ComponentState.FAULTED
        logger.error("handler_faulted", handler_id=self.handler_id)

        try:
            await self.on_fault()
        except Exception:
            logger.exception("handler_fault_error", handler_id=self.handler_id)

    # Lifecycle hooks to override
    async def on_start(self) -> None:
        """Initialize handler resources.

        Called during start(). Subclasses should override to:
        - Initialize connections
        - Warm up caches
        - Subscribe to events

        Default implementation sets up subscriptions and logs readiness.
        """
        # Set up default event subscriptions based on handler type
        await self._setup_event_subscriptions()
        self._state = ComponentState.READY
        logger.debug("handler_initialized", handler_id=self.handler_id)

    async def on_stop(self) -> None:
        """Cleanup handler resources.

        Called during stop(). Subclasses should override to:
        - Close connections
        - Persist state
        - Unsubscribe from events

        Default implementation clears cache and logs shutdown.
        """
        self.cache_clear()
        logger.debug("handler_cleaned_up", handler_id=self.handler_id)

    async def on_degrade(self) -> None:
        """Handle degraded mode.

        Called when entering degraded mode. Subclasses can override to:
        - Reduce processing scope
        - Free up resources
        - Alert monitoring systems
        """
        # Default implementation does nothing
        logger.debug("handler_degraded_default", handler_id=self.handler_id)

    async def on_fault(self) -> None:
        """Handle fault state.

        Called when entering faulted state. Subclasses can override to:
        - Send critical alerts
        - Attempt recovery
        - Log diagnostic information
        """
        # Default implementation does nothing
        logger.debug("handler_faulted_default", handler_id=self.handler_id)

    async def handle_with_degradation(self, event: msgspec.Struct) -> None:
        """Handle event with configuration-driven retry.

        Uses create_retryer helper for clean configuration-driven retry.
        """
        retryer = create_retryer(
            self.config.retry_config,
            multiplier_factor=0.5,  # Faster retries for event handling
            attempts_factor=0.5,  # Half the attempts
            retry_on=(ConnectionError, TimeoutError),
        )
        await retryer(self._handle_with_degradation_impl, event)

    async def _handle_with_degradation_impl(self, event: msgspec.Struct) -> None:
        """Handle event with automatic degradation on errors.

        Args:
            event: The msgspec event to handle

        Note:
            Automatically degrades after configured errors, faults after configured errors.

        Raises:
            ConnectionError: When connection issues occur
            TimeoutError: When timeout occurs
        """
        if self._state == ComponentState.FAULTED:
            logger.debug("handler_faulted_skipping_event", handler_id=self.handler_id)
            return

        if self._state == ComponentState.STOPPED:
            logger.debug("handler_stopped_skipping_event", handler_id=self.handler_id)
            return

        try:
            await self.handle_event(event)
            self._consecutive_errors = 0  # Reset on success
            self._metrics["events_processed"] += 1

        except (ConnectionError, TimeoutError):
            # Let tenacity retry these
            self._consecutive_errors += 1
            self._error_count += 1
            self._metrics["retryable_errors"] += 1
            raise

        except Exception:
            self._consecutive_errors += 1
            self._error_count += 1
            self._metrics["errors"] += 1

            logger.exception("handler_event_error", handler_id=self.handler_id)

            # Auto-degradation based on consecutive errors from config
            if (
                self._consecutive_errors > self.config.auto_degrade_after_errors
                and self._state == ComponentState.RUNNING
            ):
                await self.degrade()
            elif (
                self._consecutive_errors > self.config.auto_fault_after_errors
                and self._state == ComponentState.DEGRADED
            ):
                await self.fault()

            raise

    @abstractmethod
    async def handle_event(self, event: msgspec.Struct) -> None:
        """Main event handling method.

        Args:
            event: The msgspec event to process

        Subclasses must implement this method with their specific
        event processing logic.
        """

    # Cache management utilities
    def cache_get(self, key: str) -> msgspec.Struct | None:
        """Get value from handler cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None
        """
        self._metrics["cache_requests"] += 1
        if key in self._cache:
            self._metrics["cache_hits"] += 1
            return self._cache[key]
        return None

    def cache_set(self, key: str, value: msgspec.Struct) -> None:
        """Set value in handler cache.

        Args:
            key: Cache key
            value: Value to cache
        """
        self._cache[key] = value

    def cache_clear(self) -> None:
        """Clear handler cache."""
        self._cache.clear()

    # Metrics utilities
    @property
    def state(self) -> ComponentState:
        """Get current component state."""
        return self._state

    @property
    def error_count(self) -> int:
        """Get total error count."""
        return self._error_count

    @property
    def consecutive_errors(self) -> int:
        """Get consecutive error count."""
        return self._consecutive_errors

    def get_metrics(self) -> dict[str, int]:
        """Get handler metrics.

        Returns:
            Dictionary of metric name to value
        """
        metrics = dict(self._metrics)
        metrics["error_count"] = self._error_count
        metrics["consecutive_errors"] = self._consecutive_errors
        metrics["cache_size"] = len(self._cache)

        if self._start_time:
            metrics["uptime_seconds"] = int(time.time() - self._start_time)

        return metrics

    def reset_metrics(self) -> None:
        """Reset handler metrics."""
        self._metrics.clear()
        self._error_count = 0
        self._consecutive_errors = 0

    # Event subscription and routing methods
    async def _setup_event_subscriptions(self) -> None:
        """Set up event subscriptions for this handler.

        Default implementation does nothing. Subclasses should override
        to subscribe to specific event types with appropriate priorities.

        Example:
            await self.subscribe_to_event(MarketData, HandlerPriority.NORMAL)
        """
        # Default implementation does nothing - subclasses should override
        logger.debug("default_subscriptions_setup", handler_id=self.handler_id)

    async def subscribe_to_event(
        self, event_type: type[msgspec.Struct], priority: HandlerPriority | None = None
    ) -> None:
        """Subscribe to an event type with optional priority.

        Args:
            event_type: The msgspec event type to subscribe to
            priority: Handler priority for routing order
        """
        if priority is None:
            priority = HandlerPriority.NORMAL

        self.event_bus.subscribe(event_type, self.handle_with_degradation, priority)
        logger.debug(
            "event_subscription_added",
            handler_id=self.handler_id,
            event_type=event_type.__name__,
            priority=priority.name,
        )

    async def unsubscribe_from_event(self, event_type: type[msgspec.Struct]) -> None:
        """Unsubscribe from an event type.

        Args:
            event_type: The msgspec event type to unsubscribe from
        """
        self.event_bus.unsubscribe(event_type, self.handle_with_degradation)
        logger.debug(
            "event_subscription_removed", handler_id=self.handler_id, event_type=event_type.__name__
        )

    def get_subscriptions(self) -> list[str]:
        """Get list of event types this handler is subscribed to.

        Returns:
            List of event type names
        """
        # This would need to be implemented in the event bus
        # For now, return empty list
        return []
