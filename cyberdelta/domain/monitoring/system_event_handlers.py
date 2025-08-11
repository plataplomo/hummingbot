"""System Event Handlers for Monitoring and Alerting.

Handles system-level events for monitoring, alerting, and operational management.
Integrates with the msgspec event system for high-performance event processing.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import msgspec

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.base_event_handler import EventHandlerActor
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.enums.monitoring import HealthStatus, SystemEventType
from cyberdelta.models.events.core import SystemEvent


if TYPE_CHECKING:
    from cyberdelta.config.models.app_config import AppSettings
    from cyberdelta.infrastructure.event_bus import EventBus

logger = get_logger(__name__)


class SystemEventHandler(EventHandlerActor):
    """Handles system-level events for monitoring and alerting.

    Processes SystemEvent messages to track:
    - Component lifecycle (started/stopped)
    - Health checks
    - Errors and failures
    - Circuit breaker trips
    - Performance metrics

    Uses msgspec for high-performance event processing.
    No hardcoded values - all configuration from AppSettings.
    """

    def __init__(
        self,
        event_bus: EventBus,
        config: AppSettings,
    ) -> None:
        """Initialize system event handler.

        Args:
            event_bus: The event bus for publishing/subscribing
            config: Application configuration
        """
        # Initialize base handler with correct parameters
        super().__init__(
            handler_id="SystemEventHandler",
            event_bus=event_bus,
            config=config.event_system.handler,
        )

        self._config = config
        self._monitoring_config = config.monitoring

        # Track component states (no hardcoded values)
        self._component_states: dict[str, ComponentState] = {}
        self._error_counts: dict[str, int] = {}
        self._last_health_check: dict[str, datetime] = {}

        # Alert thresholds from config
        self._error_threshold = config.safety_systems.circuit_breakers.global_consecutive_failures
        self._health_check_interval_sec = config.event_system.monitoring.health_check_interval_sec

        logger.info(
            "system_event_handler_initialized",
            error_threshold=self._error_threshold,
            health_check_interval=self._health_check_interval_sec,
        )

    async def handle_event(self, event: msgspec.Struct) -> None:
        """Handle system events.

        Args:
            event: The msgspec event to handle
        """
        if not isinstance(event, SystemEvent):
            return

        try:
            # Route based on event type
            if event.event_type == SystemEventType.STARTED:
                await self._handle_component_started(event)
            elif event.event_type == SystemEventType.STOPPED:
                await self._handle_component_stopped(event)
            elif event.event_type == SystemEventType.HEALTH_CHECK:
                await self._handle_health_check(event)
            elif event.event_type == SystemEventType.ERROR:
                await self._handle_error_event(event)
            elif event.event_type == SystemEventType.WARNING:
                await self._handle_performance_event(event)

        except Exception as e:
            logger.exception(
                "system_event_handler_error",
                error=str(e),
                event_type=event.event_type,
                component=event.component,
            )
            # Log the handler error (no _handle_error method exists in base class)

    async def _handle_component_started(self, event: SystemEvent) -> None:
        """Handle component startup events.

        Args:
            event: Component started event
        """
        self._component_states[event.component] = ComponentState.RUNNING
        self._error_counts[event.component] = 0

        logger.info(
            "component_started",
            component=event.component,
            timestamp=event.timestamp,
        )

        # Reset error count for this component
        if event.component in self._error_counts:
            self._error_counts[event.component] = 0

    async def _handle_component_stopped(self, event: SystemEvent) -> None:
        """Handle component shutdown events.

        Args:
            event: Component stopped event
        """
        self._component_states[event.component] = ComponentState.STOPPED

        logger.info(
            "component_stopped",
            component=event.component,
            timestamp=event.timestamp,
            message=event.message,
        )

        # Alert if unexpected shutdown
        if event.status == HealthStatus.FAILED and self._monitoring_config.notifications_enabled:
            await self._send_alert(
                component=event.component,
                alert_type="unexpected_shutdown",
                details=event.message,
            )

    async def _handle_health_check(self, event: SystemEvent) -> None:
        """Handle health check events.

        Args:
            event: Health check event
        """
        self._last_health_check[event.component] = datetime.now(UTC)

        # Update component state based on health status
        if event.status == HealthStatus.HEALTHY:
            self._component_states[event.component] = ComponentState.RUNNING
            # Reset error count on healthy status
            self._error_counts[event.component] = 0
        elif event.status == HealthStatus.DEGRADED:
            self._component_states[event.component] = ComponentState.DEGRADED
            logger.warning(
                "component_degraded",
                component=event.component,
                message=event.message,
            )
        elif event.status == HealthStatus.FAILED:
            self._component_states[event.component] = ComponentState.FAULTED
            logger.error(
                "component_failed",
                component=event.component,
                message=event.message,
            )

            # Send alert for failed component
            if self._monitoring_config.notifications_enabled:
                await self._send_alert(
                    component=event.component,
                    alert_type="component_failed",
                    details=event.message or "Component health check failed",
                )

    async def _handle_error_event(self, event: SystemEvent) -> None:
        """Handle error events.

        Args:
            event: Error event
        """
        # Track error count
        if event.component not in self._error_counts:
            self._error_counts[event.component] = 0
        self._error_counts[event.component] += 1

        logger.error(
            "component_error",
            component=event.component,
            message=event.message,
            error_count=self._error_counts[event.component],
            threshold=self._error_threshold,
        )

        # Check if error threshold exceeded
        if self._error_counts[event.component] >= self._error_threshold:
            self._component_states[event.component] = ComponentState.FAULTED

            # Send critical alert
            if self._monitoring_config.notifications_enabled:
                error_count = self._error_counts[event.component]
                threshold = self._error_threshold
                details_msg = f"Component has {error_count} errors (threshold: {threshold})"
                await self._send_alert(
                    component=event.component,
                    alert_type="error_threshold_exceeded",
                    details=details_msg,
                )

            # Check for circuit breaker trip
            if event.component == "circuit_breaker":
                await self._handle_circuit_breaker_trip(event)

    async def _handle_circuit_breaker_trip(self, event: SystemEvent) -> None:
        """Handle circuit breaker trip events.

        Args:
            event: Circuit breaker event
        """
        logger.critical(
            "circuit_breaker_tripped",
            component=event.component,
            message=event.message,
        )

        # Send urgent alert
        if self._monitoring_config.notifications_enabled:
            await self._send_alert(
                component="circuit_breaker",
                alert_type="circuit_breaker_tripped",
                details=event.message or "Circuit breaker tripped",
                urgent=True,
            )

        # Check if trading should halt
        if self._config.should_halt_trading():
            logger.critical("trading_halted_by_safety_systems")
            # Publish trading halt event
            halt_event = SystemEvent(
                event_type=SystemEventType.STOPPED,
                component="trading",
                status=HealthStatus.FAILED,
                message="Trading halted by safety systems",
                timestamp=time.time(),
            )
            await self.event_bus.publish(halt_event)

    async def _handle_performance_event(self, event: SystemEvent) -> None:
        """Handle performance metric events.

        Args:
            event: Performance event
        """
        # Log performance/warning event (SystemEvent doesn't have metrics attribute)
        logger.info(
            "system_event",
            component=event.component,
            message=event.message,
            status=event.status,
            timestamp=event.timestamp,
        )

        # For warning events, we can parse message for performance info if needed
        # This is a simplified approach since SystemEvent doesn't have metrics structure

    async def _send_alert(
        self,
        component: str,
        alert_type: str,
        details: str,
        urgent: bool = False,
    ) -> None:
        """Send monitoring alert.

        Args:
            component: Component that triggered alert
            alert_type: Type of alert
            details: Alert details
            urgent: Whether alert is urgent
        """
        # Log the alert
        log_method = logger.critical if urgent else logger.warning
        log_method(
            "monitoring_alert",
            component=component,
            alert_type=alert_type,
            details=details,
            urgent=urgent,
        )

        # TODO: Integrate with actual alerting service (email, Slack, etc.)
        # This would use the monitoring configuration to determine channels

    async def get_component_states(self) -> dict[str, ComponentState]:
        """Get current states of all monitored components.

        Returns:
            Dictionary of component states
        """
        return self._component_states.copy()

    async def get_error_counts(self) -> dict[str, int]:
        """Get error counts for all components.

        Returns:
            Dictionary of error counts by component
        """
        return self._error_counts.copy()

    async def reset_error_count(self, component: str) -> None:
        """Reset error count for a specific component.

        Args:
            component: Component name to reset
        """
        if component in self._error_counts:
            self._error_counts[component] = 0
            logger.info(
                "error_count_reset",
                component=component,
            )

    def is_system_healthy(self) -> bool:
        """Check if the overall system is healthy.

        Returns:
            True if all components are healthy
        """
        # System is healthy if no components are faulted
        return not any(state == ComponentState.FAULTED for state in self._component_states.values())

    def get_unhealthy_components(self) -> list[str]:
        """Get list of unhealthy components.

        Returns:
            List of component names that are degraded or faulted
        """
        return [
            component
            for component, state in self._component_states.items()
            if state in {ComponentState.DEGRADED, ComponentState.FAULTED}
        ]
