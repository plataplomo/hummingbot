"""Type-safe event publishing system for WebSocket errors.

Publishes typed error events for monitoring systems, alerting, and
external integrations with full type safety and structured event data.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from cyberdelta.apis.common.error_foundation import ErrorSeverity, WebSocketRecoveryStrategy
from cyberdelta.apis.enums.websocket import WebSocketErrorCode
from cyberdelta.apis.models.websocket import (
    ConnectionHealthEvent,
    RecoveryAttemptEvent,
    SystemHealthEvent,
    WebSocketErrorEvent,
)
from cyberdelta.apis.protocols.websocket import EventFilterProtocol, EventHandlerProtocol
from cyberdelta.apis.exceptions.websocket import WebSocketStreamError


if TYPE_CHECKING:
    from logging import Logger


# ============================================================================
# Event Publisher Implementation
# ============================================================================


class WebSocketErrorEventPublisher:
    """Type-safe event publisher for WebSocket errors.

    Publishes structured, typed events for monitoring systems and
    external integrations with configurable handlers and filters.
    """

    def __init__(
        self,
        logger: Logger | None = None,
        enable_async_publishing: bool = True,
        max_queue_size: int = 10000,
    ) -> None:
        """Initialize the event publisher.

        Args:
            logger: Optional logger for publisher operations
            enable_async_publishing: Whether to publish events asynchronously
            max_queue_size: Maximum size of event queue for async publishing
        """
        self._logger = logger
        self._enable_async = enable_async_publishing
        self._max_queue_size = max_queue_size

        # Event handlers by event type
        self._handlers: dict[str, list[EventHandlerProtocol]] = {}

        # Event filters
        self._filters: list[EventFilterProtocol] = []

        # Async publishing queue
        self._event_queue: asyncio.Queue[BaseModel] = asyncio.Queue(maxsize=max_queue_size)
        self._publisher_task: asyncio.Task[None] | None = None
        self._publishing = False

        # Statistics
        self._events_published = 0
        self._events_filtered = 0
        self._events_failed = 0

        if self._logger:
            self._logger.info(
                "WebSocket error event publisher initialized (async=%s, max_queue_size=%d)",
                enable_async_publishing,
                max_queue_size,
            )

    def add_handler(
        self,
        event_type: str,
        handler: EventHandlerProtocol,
    ) -> None:
        """Add an event handler for a specific event type.

        Args:
            event_type: Type of events to handle
            handler: Handler to add
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []

        self._handlers[event_type].append(handler)

        if self._logger:
            self._logger.debug(
                "Added event handler for type '%s' (total handlers: %d)",
                event_type,
                len(self._handlers[event_type]),
            )

    def remove_handler(
        self,
        event_type: str,
        handler: EventHandlerProtocol,
    ) -> bool:
        """Remove an event handler.

        Args:
            event_type: Event type
            handler: Handler to remove

        Returns:
            True if handler was removed
        """
        if event_type not in self._handlers:
            return False

        try:
            self._handlers[event_type].remove(handler)
            if not self._handlers[event_type]:
                del self._handlers[event_type]
        except ValueError:
            return False
        else:
            if self._logger:
                self._logger.debug("Removed event handler for type '%s'", event_type)
            return True

    def add_filter(self, filter_func: EventFilterProtocol) -> None:
        """Add an event filter.

        Args:
            filter_func: Filter to add
        """
        self._filters.append(filter_func)

        if self._logger:
            self._logger.debug("Added event filter (total filters: %d)", len(self._filters))

    async def start_async_publishing(self) -> None:
        """Start async event publishing."""
        if self._enable_async and not self._publishing:
            self._publishing = True
            self._publisher_task = asyncio.create_task(self._async_publisher_loop())

            if self._logger:
                self._logger.info("Started async event publishing")

    async def stop_async_publishing(self) -> None:
        """Stop async event publishing."""
        if self._publishing:
            self._publishing = False

            if self._publisher_task and not self._publisher_task.done():
                self._publisher_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._publisher_task

            if self._logger:
                self._logger.info("Stopped async event publishing")

    async def publish_error_event(
        self,
        error: WebSocketStreamError,
        recovery_attempted: bool = False,
        recovery_successful: bool | None = None,
        recovery_duration_ms: int | None = None,
        connection_duration_ms: int | None = None,
        error_count_in_window: int = 1,
    ) -> None:
        """Publish a WebSocket error event.

        Args:
            error: The WebSocket error
            recovery_attempted: Whether recovery was attempted
            recovery_successful: Whether recovery was successful
            recovery_duration_ms: Recovery duration in milliseconds
            connection_duration_ms: Connection duration in milliseconds
            error_count_in_window: Number of errors in current window
        """
        event = WebSocketErrorEvent(
            error_code=error.code,
            error_category=error.code.get_category(),
            severity=error.severity,
            error_message=error.message,
            exchange=error.context.exchange,
            connection_id=error.context.connection_id,
            channel=error.context.channel,
            topic=error.context.topic,
            user_id=error.context.user_id,
            session_id=error.context.session_id,
            sequence_number=error.context.sequence_number,
            recovery_strategy=error.recovery_strategy,
            recovery_attempted=recovery_attempted,
            recovery_successful=recovery_successful,
            recovery_duration_ms=recovery_duration_ms,
            raw_message_size=error.context.raw_message_size,
            connection_duration_ms=connection_duration_ms,
            error_count_in_window=error_count_in_window,
            additional_context={
                "environment": error.context.environment,
                "error_domain": "websocket_stream",
            },
        )

        await self._publish_event(event)

    async def publish_recovery_attempt_event(
        self,
        exchange: str,
        connection_id: str,
        strategy: WebSocketRecoveryStrategy,
        attempt_number: int,
        successful: bool,
        duration_ms: int,
        original_error_code: WebSocketErrorCode,
        error_count_before: int = 0,
        channel: str | None = None,
        failure_reason: str | None = None,
        next_strategy: WebSocketRecoveryStrategy | None = None,
    ) -> None:
        """Publish a recovery attempt event.

        Args:
            exchange: Exchange name
            connection_id: Connection identifier
            strategy: Recovery strategy used
            attempt_number: Attempt number
            successful: Whether recovery was successful
            duration_ms: Recovery duration
            original_error_code: Original error code
            error_count_before: Error count before recovery
            channel: Optional channel name
            failure_reason: Reason for failure if unsuccessful
            next_strategy: Next strategy to try if this failed
        """
        event = RecoveryAttemptEvent(
            exchange=exchange,
            connection_id=connection_id,
            strategy=strategy,
            attempt_number=attempt_number,
            successful=successful,
            duration_ms=duration_ms,
            original_error_code=original_error_code,
            error_count_before=error_count_before,
            channel=channel,
            failure_reason=failure_reason,
            next_strategy=next_strategy,
        )

        await self._publish_event(event)

    async def publish_connection_health_event(
        self,
        exchange: str,
        connection_id: str,
        health_status: str,
        error_rate: float,
        recovery_success_rate: float,
        connection_uptime_ms: int,
        total_errors: int,
        total_recoveries: int,
        successful_recoveries: int,
        recent_error_codes: list[str] | None = None,
        last_successful_message_ms: int | None = None,
        health_threshold_breached: str | None = None,
        recommended_action: str | None = None,
    ) -> None:
        """Publish a connection health event.

        Args:
            exchange: Exchange name
            connection_id: Connection identifier
            health_status: Health status string
            error_rate: Current error rate
            recovery_success_rate: Recovery success rate
            connection_uptime_ms: Connection uptime
            total_errors: Total error count
            total_recoveries: Total recovery attempts
            successful_recoveries: Successful recovery count
            recent_error_codes: Recent error codes
            last_successful_message_ms: Last successful message timestamp
            health_threshold_breached: Which threshold was breached
            recommended_action: Recommended action
        """
        event = ConnectionHealthEvent(
            exchange=exchange,
            connection_id=connection_id,
            health_status=health_status,
            error_rate=error_rate,
            recovery_success_rate=recovery_success_rate,
            connection_uptime_ms=connection_uptime_ms,
            total_errors=total_errors,
            total_recoveries=total_recoveries,
            successful_recoveries=successful_recoveries,
            recent_error_codes=recent_error_codes or [],
            last_successful_message_ms=last_successful_message_ms,
            health_threshold_breached=health_threshold_breached,
            recommended_action=recommended_action,
        )

        await self._publish_event(event)

    async def publish_system_health_event(
        self,
        overall_health: str,
        active_connections: int,
        total_connections: int,
        system_error_rate: float,
        system_recovery_rate: float,
        average_connection_uptime_ms: float,
        exchange_health: dict[str, str] | None = None,
        problematic_exchanges: list[str] | None = None,
        active_alerts: int = 0,
        critical_issues: list[str] | None = None,
    ) -> None:
        """Publish a system health event.

        Args:
            overall_health: Overall system health
            active_connections: Number of active connections
            total_connections: Total connections tracked
            system_error_rate: System-wide error rate
            system_recovery_rate: System-wide recovery rate
            average_connection_uptime_ms: Average connection uptime
            exchange_health: Health status by exchange
            problematic_exchanges: List of problematic exchanges
            active_alerts: Number of active alerts
            critical_issues: List of critical issues
        """
        event = SystemHealthEvent(
            overall_health=overall_health,
            active_connections=active_connections,
            total_connections=total_connections,
            system_error_rate=system_error_rate,
            system_recovery_rate=system_recovery_rate,
            average_connection_uptime_ms=average_connection_uptime_ms,
            exchange_health=exchange_health or {},
            problematic_exchanges=problematic_exchanges or [],
            active_alerts=active_alerts,
            critical_issues=critical_issues or [],
        )

        await self._publish_event(event)

    def get_statistics(self) -> dict[str, Any]:
        """Get publisher statistics.

        Returns:
            Dictionary with publisher statistics
        """
        return {
            "events_published": self._events_published,
            "events_filtered": self._events_filtered,
            "events_failed": self._events_failed,
            "handlers_count": sum(len(handlers) for handlers in self._handlers.values()),
            "filters_count": len(self._filters),
            "async_publishing": self._enable_async,
            "publishing_active": self._publishing,
            "queue_size": self._event_queue.qsize() if self._enable_async else 0,
            "max_queue_size": self._max_queue_size,
        }

    async def flush_events(self) -> None:
        """Flush any pending events in async mode."""
        if not self._enable_async or self._event_queue.empty():
            return

        # Create event for flush completion notification
        flush_complete = asyncio.Event()

        # Monitor queue size changes to detect when it's empty
        async def monitor_queue() -> None:
            try:
                for _ in range(50):  # Max 5 seconds at 0.1s intervals
                    if self._event_queue.empty():
                        flush_complete.set()
                        return
                    await asyncio.sleep(0.1)
            finally:
                flush_complete.set()  # Ensure event is always set

        # Start monitoring task
        monitor_task = asyncio.create_task(monitor_queue())

        try:
            # Wait for flush complete or timeout after 5 seconds
            await asyncio.wait_for(flush_complete.wait(), timeout=5.0)
        except TimeoutError:
            # Timeout is acceptable - queue may still have events
            pass
        finally:
            monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor_task

    # ========================================================================
    # Private Methods
    # ========================================================================

    async def _publish_event(self, event: BaseModel) -> None:
        """Publish an event through the appropriate channel.

        Args:
            event: Event to publish
        """
        # Apply filters
        for filter_func in self._filters:
            try:
                if not filter_func.should_publish(event):
                    self._events_filtered += 1
                    return
            except Exception:
                if self._logger:
                    self._logger.exception("Event filter failed")

        if self._enable_async:
            try:
                # Add to async queue
                self._event_queue.put_nowait(event)
            except asyncio.QueueFull:
                self._events_failed += 1
                if self._logger:
                    self._logger.warning("Event queue full, dropping event")
        else:
            # Publish synchronously
            await self._handle_event(event)

    async def _async_publisher_loop(self) -> None:
        """Async event publisher loop."""
        while self._publishing:
            try:
                # Get event with timeout to allow for shutdown
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)

                await self._handle_event(event)

            except TimeoutError:
                # Normal timeout, continue loop
                continue
            except asyncio.CancelledError:
                # Publisher stopped
                break
            except Exception:
                self._events_failed += 1
                if self._logger:
                    self._logger.exception("Error in async publisher loop")

    async def _handle_event(self, event: BaseModel) -> None:
        """Handle an event by sending it to appropriate handlers.

        Args:
            event: Event to handle
        """
        event_type = getattr(event, "event_type", event.__class__.__name__.lower())
        handlers = self._handlers.get(event_type, [])

        if not handlers:
            # Also try generic handlers
            handlers = self._handlers.get("*", [])

        if handlers:
            # Send to all handlers for this event type
            tasks = [handler.handle_event(event) for handler in handlers]

            try:
                await asyncio.gather(*tasks, return_exceptions=True)
                self._events_published += 1

                if self._logger:
                    self._logger.debug(
                        "Published event type '%s' to %d handlers", event_type, len(handlers)
                    )
            except Exception:
                self._events_failed += 1
                if self._logger:
                    self._logger.exception("Failed to publish event type '%s'", event_type)
        # No handlers for this event type
        elif self._logger:
            self._logger.debug("No handlers found for event type '%s'", event_type)


# ============================================================================
# Built-in Event Handlers
# ============================================================================


class LoggingEventHandler:
    """Event handler that logs events to the standard logger."""

    def __init__(self, logger: Logger, log_level: str = "INFO") -> None:
        """Initialize logging event handler.

        Args:
            logger: Logger to use
            log_level: Log level for events
        """
        self.logger = logger
        self.log_level = log_level.upper()

    async def handle_event(self, event: BaseModel) -> None:
        """Handle event by logging it.

        Args:
            event: Event to log
        """
        event_type = getattr(event, "event_type", event.__class__.__name__.lower())
        event_data = event.model_dump()

        # Create log message
        if event_type == "websocket_error":
            message = "WebSocket error on {}: {} (code: {}, severity: {})".format(
                event_data.get("exchange"),
                event_data.get("error_message"),
                event_data.get("error_code"),
                event_data.get("severity"),
            )
        elif event_type == "recovery_attempt":
            message = "Recovery attempt on {}: strategy={}, success={}, duration={}ms".format(
                event_data.get("exchange"),
                event_data.get("strategy"),
                event_data.get("successful"),
                event_data.get("duration_ms", 0),
            )
        elif event_type == "connection_health":
            message = "Connection health on {}: status={}, error_rate={:.2f}%".format(
                event_data.get("exchange"),
                event_data.get("health_status"),
                event_data.get("error_rate", 0.0) * 100,
            )
        elif event_type == "system_health":
            message = "System health: {} (active_connections={}, error_rate={:.2f}%)".format(
                event_data.get("overall_health"),
                event_data.get("active_connections", 0),
                event_data.get("system_error_rate", 0.0) * 100,
            )
        else:
            message = f"Event {event_type}: {event_data}"

        # Log at specified level
        getattr(self.logger, self.log_level.lower())(message)


# ============================================================================
# Event Filters
# ============================================================================


class SeverityEventFilter:
    """Filter events by severity level."""

    def __init__(self, min_severity: ErrorSeverity) -> None:
        """Initialize severity filter.

        Args:
            min_severity: Minimum severity to publish
        """
        self.min_severity = min_severity

    def should_publish(self, event: BaseModel) -> bool:
        """Check if event meets severity threshold.

        Args:
            event: Event to check

        Returns:
            True if event should be published
        """
        if hasattr(event, "severity"):
            severity = getattr(event, "severity", None)
            if isinstance(severity, ErrorSeverity):
                return severity >= self.min_severity

        # For non-error events, always publish
        return True


class RateLimitEventFilter:
    """Filter events based on rate limiting."""

    def __init__(self, max_events_per_second: int = 100) -> None:
        """Initialize rate limit filter.

        Args:
            max_events_per_second: Maximum events per second
        """
        self.max_events = max_events_per_second
        self.event_timestamps: list[float] = []

    def should_publish(self, event: BaseModel) -> bool:
        """Check if event is within rate limits.

        Args:
            event: Event to check

        Returns:
            True if event should be published
        """
        current_time = time.time()

        # Clean old timestamps (older than 1 second)
        cutoff_time = current_time - 1.0
        self.event_timestamps = [ts for ts in self.event_timestamps if ts > cutoff_time]

        # Check if we're within limits
        if len(self.event_timestamps) >= self.max_events:
            return False

        # Add current timestamp
        self.event_timestamps.append(current_time)
        return True
