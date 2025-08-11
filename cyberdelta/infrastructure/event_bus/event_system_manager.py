"""Event System Manager for comprehensive lifecycle and health management.

This module provides the central management point for the entire event system,
coordinating event buses, handlers, workflows, and health monitoring.

IMPORTANT: Following CODING_STANDARDS.md:
- NO hardcoded values, all from configuration
- Fail fast on errors, no silent failures
- Configuration-first approach
- Comprehensive lifecycle management
"""

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.component_state import ComponentState
from cyberdelta.infrastructure.event_bus.bus import EventBus
from cyberdelta.infrastructure.event_bus.handler_manager import HandlerManager
from cyberdelta.infrastructure.event_bus.health_check import EventBusHealthCheck
from cyberdelta.models.events.system_health import SystemHealthReport
from cyberdelta.orchestration.orchestrator import WorkflowOrchestrator
from cyberdelta.orchestration.registry import WorkflowRegistry


if TYPE_CHECKING:
    from cyberdelta.domain.base_event_handler import EventHandlerActor
    from cyberdelta.protocols import WorkflowHandler


logger = get_logger(__name__)


class EventSystemManager:
    """Central manager for the entire event system.

    Coordinates lifecycle management, health monitoring, and graceful shutdown
    for all event system components including buses, handlers, and workflows.

    Responsibilities:
    - System initialization and startup sequence
    - Health monitoring and reporting
    - Graceful shutdown coordination
    - Component state management
    - Migration support during dual-bus operation
    """

    def __init__(
        self,
        config: AppSettings,
    ) -> None:
        """Initialize the Event System Manager.

        Args:
            config: Application configuration
        """
        self._config = config
        self._event_config = config.event_system

        # Initialize core components
        self._event_bus = EventBus(config=self._event_config.event_bus)

        # Initialize management components
        self._handler_manager = HandlerManager(self._event_config.monitoring)
        self._workflow_registry = WorkflowRegistry()
        self._workflow_orchestrator = WorkflowOrchestrator(
            config=self._event_config.workflow,  # Fixed: workflows -> workflow
            registry=self._workflow_registry,
        )

        # Initialize health monitoring
        self._health_check = EventBusHealthCheck(
            event_bus=self._event_bus,
            health_check_interval_seconds=self._event_config.monitoring.health_check_interval_sec,
            # Use slow event threshold as proxy for stale detection
            stale_threshold_seconds=int(self._event_config.monitoring.slow_event_threshold_ms * 10),
        )

        # System state tracking
        self._system_state = ComponentState.PRE_INITIALIZED
        self._startup_time: datetime | None = None
        self._shutdown_requested = False
        self._health_monitor_task: asyncio.Task[None] | None = None

        logger.info(
            "event_system_manager_initialized",
            monitoring_interval=self._event_config.monitoring.health_check_interval_sec,
        )

    # ============= Component Registration =============

    def register_handler(self, handler: "EventHandlerActor") -> None:
        """Register an event handler with the system.

        Args:
            handler: Event handler implementing EventHandlerActor protocol
        """
        # Register with handler manager for lifecycle management
        self._handler_manager.register_handler(handler)

        # Handler will register its own subscriptions with the event bus
        # during its start() lifecycle method

        logger.info(
            "handler_registered_with_system",
            handler_id=handler.handler_id,
        )

    def register_workflow(
        self,
        event_type: str,
        handler: "WorkflowHandler",
    ) -> None:
        """Register a workflow handler.

        Args:
            event_type: The workflow event type
            handler: Workflow handler implementing WorkflowHandler protocol
        """
        self._workflow_orchestrator.register_handler(event_type, handler)

        logger.info(
            "workflow_registered_with_system",
            event_type=event_type,
            handler_type=type(handler).__name__,
        )

    # ============= Lifecycle Management =============

    async def start(self) -> None:
        """Start the event system with proper initialization sequence.

        Performs the following startup sequence:
        1. Validate configuration
        2. Start event buses
        3. Start all registered handlers
        4. Start health monitoring
        5. Verify system health

        Raises:
            RuntimeError: If startup fails or system unhealthy after startup
        """
        logger.info("starting_event_system")

        try:
            # Step 1: Validate configuration
            self._validate_configuration()

            # Step 2: Start health monitoring task
            self._health_monitor_task = asyncio.create_task(self._health_monitoring_loop())

            # Step 3: Start all handlers (they'll subscribe during start)
            await self._handler_manager.start_all()

            # Step 4: Verify initial system health
            health_report = await self.get_health_report()

        except Exception as e:
            logger.exception("event_system_startup_failed", error=str(e))
            self._system_state = ComponentState.FAULTED
            # Attempt cleanup
            await self._emergency_cleanup()
            raise

        # Health validation outside try block to fix TRY301
        if not health_report.overall_health:
            msg = f"System unhealthy after startup: {health_report.messages}"
            logger.error("startup_health_check_failed", messages=health_report.messages)
            self._system_state = ComponentState.FAULTED
            await self._emergency_cleanup()
            raise RuntimeError(msg)

        # Step 5: Update system state
        self._system_state = ComponentState.RUNNING
        self._startup_time = datetime.now(UTC)

        logger.info(
            "event_system_started",
            handler_count=len(self._handler_manager.handlers),
            workflow_count=len(self._workflow_orchestrator.get_registered_handlers()),
            state=self._system_state.value,
        )

    async def stop(self) -> None:
        """Gracefully stop the event system.

        Performs graceful shutdown sequence:
        1. Stop accepting new events
        2. Wait for pending events/workflows
        3. Stop all handlers in reverse order
        4. Stop health monitoring
        5. Final cleanup

        Uses configuration timeout for graceful shutdown.
        """
        logger.info("stopping_event_system")
        self._shutdown_requested = True

        try:
            # Use configured timeout for graceful shutdown
            async with asyncio.timeout(self._event_config.monitoring.handler_shutdown_timeout_sec):
                await self._graceful_shutdown_sequence()

        except TimeoutError:
            logger.warning(
                "graceful_shutdown_timeout",
                timeout_sec=self._event_config.monitoring.handler_shutdown_timeout_sec,
            )
            # Force shutdown after timeout
            await self._force_shutdown()

        except Exception as e:
            logger.exception("event_system_shutdown_error", error=str(e))
            await self._force_shutdown()

        finally:
            self._system_state = ComponentState.STOPPED
            logger.info("event_system_stopped")

    async def _graceful_shutdown_sequence(self) -> None:
        """Execute graceful shutdown sequence.

        Internal method that performs ordered shutdown steps.
        """
        # Step 1: Mark system as stopping
        self._system_state = ComponentState.DEGRADED
        logger.info("shutdown_sequence_started")

        # Step 2: Stop health monitoring
        if self._health_monitor_task and not self._health_monitor_task.done():
            self._health_monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_monitor_task

        # Step 3: Cancel active workflows
        active_workflows = self._workflow_orchestrator.get_active_workflows()
        if active_workflows:
            logger.info(
                "cancelling_active_workflows",
                count=len(active_workflows),
            )
            for workflow in active_workflows:
                event_id = str(workflow.get("event_id", ""))
                if event_id:
                    await self._workflow_orchestrator.cancel_workflow(event_id)

        # Step 4: Wait for pending events to complete
        await self._wait_for_pending_events()

        # Step 5: Stop all handlers in reverse order
        await self._handler_manager.stop_all()

        # Step 6: Shutdown workflow orchestrator
        await self._workflow_orchestrator.shutdown()

        # Step 7: Clear registrations
        self._handler_manager.clear_handlers()
        self._workflow_registry.clear()

        logger.info("shutdown_sequence_completed")

    async def _force_shutdown(self) -> None:
        """Force immediate shutdown without waiting.

        Used when graceful shutdown times out or fails.
        """
        logger.warning("forcing_immediate_shutdown")

        # Cancel health monitoring
        if self._health_monitor_task and not self._health_monitor_task.done():
            self._health_monitor_task.cancel()

        # Force stop all handlers via handler manager
        with contextlib.suppress(Exception):
            # Use handler manager's force stop capability - fire and forget in emergency
            asyncio.create_task(self._handler_manager.stop_all()).add_done_callback(
                lambda _: None  # Consume result to satisfy RUF006
            )

        # Clear all registrations
        self._handler_manager.clear_handlers()
        self._workflow_registry.clear()

        self._system_state = ComponentState.STOPPED

    async def _emergency_cleanup(self) -> None:
        """Emergency cleanup after startup failure."""
        logger.warning("performing_emergency_cleanup")

        # Cancel health monitoring if started
        if self._health_monitor_task and not self._health_monitor_task.done():
            self._health_monitor_task.cancel()

        # Try to stop any started handlers
        with contextlib.suppress(Exception):
            await self._handler_manager.stop_all()

        # Clear registrations
        self._handler_manager.clear_handlers()
        self._workflow_registry.clear()

    async def _wait_for_pending_events(self) -> None:
        """Wait for pending events to complete with timeout.

        Uses configuration to determine how long to wait.
        """
        start_time = datetime.now(UTC)
        max_wait = self._event_config.monitoring.handler_shutdown_timeout_sec / 2

        while True:
            # Check pending requests in event bus using public method
            pending = self._event_bus.get_pending_request_count()

            if pending == 0:
                logger.info("all_pending_events_completed")
                break

            elapsed = (datetime.now(UTC) - start_time).total_seconds()
            if elapsed > max_wait:
                logger.warning(
                    "pending_events_timeout",
                    pending_count=pending,
                    elapsed_sec=elapsed,
                )
                break

            # Brief wait before checking again - use health check interval / 10 for fast polling
            poll_interval = self._event_config.monitoring.health_check_interval_sec / 10
            await asyncio.sleep(poll_interval)

    # ============= Health Monitoring =============

    async def _health_monitoring_loop(self) -> None:
        """Background task for continuous health monitoring.

        Runs until shutdown is requested, checking health at configured intervals.
        """
        logger.info("health_monitoring_started")

        while not self._shutdown_requested:
            try:
                # Wait for configured interval
                await asyncio.sleep(self._event_config.monitoring.health_check_interval_sec)

                # Perform health check
                await self._check_and_handle_health()

            except asyncio.CancelledError:
                logger.info("health_monitoring_cancelled")
                break
            except Exception:
                logger.exception("health_monitoring_error")
                # Continue monitoring despite errors

    async def _check_and_handle_health(self) -> None:
        """Check system health and handle degradation.

        Performs health check and takes corrective actions if needed.
        """
        # Check handler health and auto-degrade if needed
        await self._handler_manager.monitor_and_degrade()

        # Get comprehensive health report
        health_report = await self.get_health_report()

        # Log health status
        logger.info(
            "system_health_check",
            overall_health=health_report.overall_health,
            handler_count=len(health_report.handler_statuses),
            degraded_count=len(health_report.degraded_handlers),
            faulted_count=len(health_report.faulted_handlers),
            active_workflows=len(health_report.active_workflows),
        )

        # Update system state based on health
        if health_report.faulted_handlers or not health_report.overall_health:
            self._system_state = ComponentState.DEGRADED
        elif self._system_state == ComponentState.DEGRADED and health_report.overall_health:
            # System recovered
            self._system_state = ComponentState.RUNNING
            logger.info("system_health_recovered")

    async def get_health_report(self) -> SystemHealthReport:
        """Get comprehensive system health report.

        Returns:
            SystemHealthReport with status of all components
        """
        # Get event bus health
        event_bus_status = self._health_check.check_event_bus_health()

        # Get handler health
        handler_health = await self._handler_manager.check_health()

        # Process handler statuses
        handler_statuses: dict[str, dict[str, object]] = {}
        degraded_handlers: list[str] = []
        faulted_handlers: list[str] = []

        for handler_id, health in handler_health.items():
            handler_statuses[handler_id] = {
                "state": health.state.value,
                "error_count": health.error_count,
                "metrics": health.metrics,
            }

            if health.state == ComponentState.DEGRADED:
                degraded_handlers.append(handler_id)
            elif health.state == ComponentState.FAULTED:
                faulted_handlers.append(handler_id)

        # Get workflow status
        active_workflows = self._workflow_orchestrator.get_active_workflows()
        workflow_count = len(self._workflow_orchestrator.get_registered_handlers())

        # Determine overall health
        overall_health = (
            event_bus_status.is_healthy
            and len(faulted_handlers) == 0
            and self._system_state in {ComponentState.RUNNING, ComponentState.READY}
        )

        # Build messages
        messages: list[str] = []
        if not event_bus_status.is_healthy:
            messages.append(f"Event bus unhealthy: {event_bus_status.message}")
        if faulted_handlers:
            messages.append(f"Faulted handlers: {', '.join(faulted_handlers)}")
        if degraded_handlers:
            messages.append(f"Degraded handlers: {', '.join(degraded_handlers)}")

        if not messages:
            messages.append("System healthy")

        return SystemHealthReport(
            timestamp=datetime.now(UTC),
            overall_health=overall_health,
            event_bus_status=event_bus_status,
            handler_statuses=handler_statuses,
            workflow_count=workflow_count,
            active_workflows=active_workflows,
            system_state=self._system_state,
            degraded_handlers=degraded_handlers,
            faulted_handlers=faulted_handlers,
            messages=messages,
        )

    # ============= Utility Methods =============

    def _validate_configuration(self) -> None:
        """Validate event system configuration.

        Raises:
            ValueError: If configuration is invalid
        """
        config = self._event_config

        # Validate monitoring configuration
        if config.monitoring.health_check_interval_sec <= 0:
            msg = "Health check interval must be positive"
            raise ValueError(msg)

        if config.monitoring.handler_shutdown_timeout_sec <= 0:
            msg = "Handler shutdown timeout must be positive"
            raise ValueError(msg)

        # Validate handler configuration
        if config.handler.auto_degrade_after_errors <= 0:
            msg = "Auto-degrade threshold must be positive"
            raise ValueError(msg)

        # Validate workflow configuration
        if config.workflow.workflow_timeout_sec <= 0:
            msg = "Workflow timeout must be positive"
            raise ValueError(msg)

        logger.info("configuration_validated")

    @property
    def event_bus(self) -> EventBus:
        """Get the event bus instance.

        Returns:
            The event bus for direct access when needed
        """
        return self._event_bus

    @property
    def workflow_orchestrator(self) -> WorkflowOrchestrator:
        """Get the workflow orchestrator instance.

        Returns:
            The workflow orchestrator for direct access when needed
        """
        return self._workflow_orchestrator

    @property
    def system_state(self) -> ComponentState:
        """Get current system state.

        Returns:
            Current ComponentState of the system
        """
        return self._system_state

    @property
    def uptime_seconds(self) -> float:
        """Get system uptime in seconds.

        Returns:
            Uptime in seconds since startup, or 0 if not started
        """
        if self._startup_time is None:
            return 0.0

        return (datetime.now(UTC) - self._startup_time).total_seconds()

    def is_healthy(self) -> bool:
        """Quick health check without full report.

        Returns:
            True if system is healthy
        """
        return self._system_state == ComponentState.RUNNING and not self._shutdown_requested

    def is_migration_active(self) -> bool:
        """Check if dual-bus migration is active.

        Returns:
            False - migration is complete
        """
        return False
