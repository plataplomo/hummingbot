"""Type-safe workflow orchestrator.

Implements workflow execution using type-safe handlers and dependency injection
without any dynamic attribute access or hardcoded logic.
"""

import asyncio
from datetime import UTC, datetime

from cyberdelta.config.models.event_system_config import EventWorkflowConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import WorkflowStatus
from cyberdelta.exceptions import ServiceValidationError
from cyberdelta.models.events.workflow.base import BaseWorkflowEvent
from cyberdelta.models.events.workflow_context import WorkflowContextModel
from cyberdelta.protocols import WorkflowHandler
from cyberdelta.utils.retry_utils import create_retryer

from .audit import WorkflowAuditLogger
from .registry import WorkflowRegistry


logger = get_logger(__name__)


class WorkflowOrchestrator:
    """Workflow orchestrator using protocol-based handlers.

    Executes workflows using protocol-based handlers with complete type safety
    and dependency injection. No dynamic attribute access or hardcoded logic.
    """

    def __init__(
        self,
        config: EventWorkflowConfig,
        registry: WorkflowRegistry | None = None,
        audit_logger: WorkflowAuditLogger | None = None,
    ) -> None:
        """Initialize orchestrator with dependencies.

        Args:
            config: Workflow configuration
            registry: Optional registry (creates default if None)
            audit_logger: Optional audit logger (creates default if None)
        """
        self._config = config
        self._registry = registry or WorkflowRegistry()
        self._audit_logger = audit_logger or WorkflowAuditLogger()
        self._active_workflows: dict[str, BaseWorkflowEvent] = {}

        logger.info(
            "workflow_orchestrator_initialized",
            max_timeout=config.workflow_timeout_sec,
        )

    def register_handler(
        self,
        event_type: str,
        handler: WorkflowHandler,
    ) -> None:
        """Register a workflow handler.

        Args:
            event_type: The event type string
            handler: Handler with execute method
        """
        self._registry.register_handler(event_type, handler)

    async def execute_workflow(self, event: BaseWorkflowEvent) -> WorkflowContextModel:
        """Execute workflow with type safety and error handling.

        Args:
            event: Workflow event

        Returns:
            WorkflowContextModel with execution results

        Raises:
            ServiceValidationError: If no handler registered for event type
        """
        # Get handler from registry
        handler = self._registry.get_handler(event.event_type)
        if handler is None:
            raise ServiceValidationError(
                "Handler", field_name="event_type", field_value=event.event_type
            )

        # Track active workflow
        self._active_workflows[event.event_id] = event

        # Update event status
        event.status = WorkflowStatus.RUNNING
        event.started_at = datetime.now(UTC)

        # Log workflow start
        self._audit_logger.log_workflow_start(event)

        try:
            # Configure retry mechanism
            retryer = create_retryer(
                self._config.retry_config,
                attempts_factor=self._config.workflow_retry_attempts_factor,
                retry_on=(ConnectionError, TimeoutError),
            )

            # Execute with timeout
            async with asyncio.timeout(event.timeout):
                result: WorkflowContextModel = await retryer(handler.execute, event)

            # Update success status
            event.status = WorkflowStatus.COMPLETED
            event.completed_at = datetime.now(UTC)

            # Log completion
            self._audit_logger.log_workflow_complete(event, result)

        except Exception as error:
            # Update error status
            event.status = WorkflowStatus.FAILED
            event.error = str(error)
            event.completed_at = datetime.now(UTC)

            # Log error
            self._audit_logger.log_workflow_error(event, error)

            raise

        else:
            return result

        finally:
            # Clean up tracking
            self._active_workflows.pop(event.event_id, None)

    def get_active_workflows(self) -> list[dict[str, object]]:
        """Get active workflow summaries.

        Returns:
            List of active workflow information
        """
        return [
            {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "status": event.status,
                "created_at": event.created_at,
                "started_at": event.started_at,
                "timeout": event.timeout,
            }
            for event in self._active_workflows.values()
        ]

    async def cancel_workflow(self, event_id: str) -> bool:
        """Cancel an active workflow.

        Args:
            event_id: The workflow event ID to cancel

        Returns:
            True if cancelled, False if not found
        """
        event = self._active_workflows.get(event_id)
        if event is None:
            return False

        event.status = WorkflowStatus.CANCELLED
        event.error = "Cancelled by request"
        event.completed_at = datetime.now(UTC)

        logger.info("workflow_cancelled", event_id=event_id, event_type=event.event_type)
        return True

    def get_registered_handlers(self) -> list[str]:
        """Get list of registered event types.

        Returns:
            List of registered event type strings
        """
        return self._registry.list_event_types()

    async def shutdown(self) -> None:
        """Gracefully shutdown orchestrator.

        Cancels active workflows and clears registry.
        """
        # Cancel active workflows
        if self._active_workflows:
            logger.info(
                "cancelling_active_workflows",
                active_count=len(self._active_workflows),
            )

            for event_id in list(self._active_workflows.keys()):
                await self.cancel_workflow(event_id)

        # Clear registry
        self._registry.clear()

        logger.info("workflow_orchestrator_shutdown_complete")
