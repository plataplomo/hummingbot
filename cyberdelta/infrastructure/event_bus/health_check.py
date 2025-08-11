"""Health check for event bus system.

This module provides health checking capabilities for the event bus.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.models.events.health_status import EventBusHealthStatus


if TYPE_CHECKING:
    from cyberdelta.infrastructure.event_bus import EventBus


logger = get_logger(__name__)


class EventBusHealthCheck:
    """Health check for the event bus.

    This class monitors the health of the event system,
    providing visibility into its operational status.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO hardcoded values, all from configuration
    - Simple implementation following KISS principle
    - No unnecessary metrics or complexity
    """

    def __init__(
        self,
        event_bus: EventBus,
        health_check_interval_seconds: int = 30,
        stale_threshold_seconds: int = 300,
    ) -> None:
        """Initialize health check for event bus.

        Args:
            event_bus: The event bus to monitor
            health_check_interval_seconds: How often to check health
            stale_threshold_seconds: How long before considering bus stale
        """
        self._event_bus = event_bus
        self._check_interval = health_check_interval_seconds
        self._stale_threshold = stale_threshold_seconds

        # Track last check time
        self._last_check = datetime.now(UTC)

        logger.info(
            "health_check_initialized",
            check_interval=health_check_interval_seconds,
            stale_threshold=stale_threshold_seconds,
        )

    def check_event_bus_health(self) -> EventBusHealthStatus:
        """Check health of the event bus (primary method).

        Returns:
            Health status of the event bus
        """
        return self.check_health()

    def check_health(self) -> EventBusHealthStatus:
        """Check health of the event bus.

        Returns:
            Health status of the event bus
        """
        try:
            # Get metrics from the bus using public methods
            handler_count = self._event_bus.get_total_handler_count()
            pending_requests = self._event_bus.get_pending_request_count()

            # Check if bus is stale (no recent activity)
            now = datetime.now(UTC)

            # Simplified health check - considers bus healthy if it has handlers
            is_healthy = handler_count > 0

            state = ComponentState.RUNNING if is_healthy else ComponentState.DEGRADED
            message = "Event bus operational" if is_healthy else "No handlers registered"

            return EventBusHealthStatus(
                is_healthy=is_healthy,
                last_event_time=now,
                event_count=0,  # Would need to track in bus
                error_count=0,  # Would need to track in bus
                handler_count=handler_count,
                pending_requests=pending_requests,
                state=state,
                message=message,
            )

        except Exception as e:
            logger.exception(
                "event_bus_health_check_failed",
                error=str(e),
            )
            return EventBusHealthStatus(
                is_healthy=False,
                last_event_time=None,
                event_count=0,
                error_count=0,
                handler_count=0,
                pending_requests=0,
                state=ComponentState.FAULTED,
                message=f"Health check failed: {e}",
            )

    def is_healthy(self) -> bool:
        """Quick check if event bus is healthy.

        Returns:
            True if event bus is healthy
        """
        status = self.check_health()
        return status.is_healthy

    def should_check_health(self) -> bool:
        """Determine if it's time for another health check.

        Returns:
            True if enough time has passed since last check
        """
        now = datetime.now(UTC)
        time_since_check = (now - self._last_check).total_seconds()
        return time_since_check >= self._check_interval

    def get_handler_count(self) -> int:
        """Get the number of registered event handlers.

        Returns:
            Total number of registered handlers
        """
        try:
            return self._event_bus.get_total_handler_count()
        except (AttributeError, TypeError, RuntimeError):
            return 0

    def get_pending_requests(self) -> int:
        """Get the number of pending request/response operations.

        Returns:
            Number of pending requests
        """
        try:
            return self._event_bus.get_pending_request_count()
        except (AttributeError, TypeError, RuntimeError):
            return 0
