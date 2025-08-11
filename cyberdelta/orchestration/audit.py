"""Audit logger for workflow execution tracking.

Implements audit logging using protocols without any dynamic attribute access.
"""

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.events.workflow import (
    BaseWorkflowEvent,
    EmergencyLiquidationEvent,
    GracefulShutdownEvent,
    PlaceOrderWorkflowEvent,
    RebalanceWorkflowEvent,
)
from cyberdelta.models.events.workflow_context import WorkflowContextModel


logger = get_logger(__name__)


class WorkflowAuditLogger:
    """Audit logger using protocols.

    Provides audit logging without any dynamic attribute access.
    Uses runtime protocol checks for type-safe field extraction.
    """

    def __init__(self) -> None:
        """Initialize audit logger."""
        self._logger = get_logger(__name__)

    def log_workflow_start(self, event: BaseWorkflowEvent) -> None:
        """Log workflow start with protocol-based field extraction.

        Args:
            event: Workflow event
        """
        base_data = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "timeout": event.timeout,
            "created_at": str(event.created_at),
            "started_at": str(event.started_at) if event.started_at else None,
            "parent_id": event.parent_id,
            "context": event.context,
        }

        # Add type-specific fields using protocol checks
        specific_data = self._extract_protocol_fields(event)

        self._logger.info(
            "workflow_started",
            **base_data,
            **specific_data,
        )

    def log_workflow_complete(self, event: BaseWorkflowEvent, result: WorkflowContextModel) -> None:
        """Log workflow completion.

        Args:
            event: Completed workflow event
            result: Workflow execution results
        """
        duration_ms = None
        if event.started_at and event.completed_at:
            # Calculate duration
            try:
                duration = event.completed_at - event.started_at
                duration_ms = duration.total_seconds() * 1000
            except (TypeError, AttributeError):
                duration_ms = None

        base_data = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "status": event.status,
            "duration_ms": duration_ms,
            "completed_at": str(event.completed_at) if event.completed_at else None,
            "workflow_id": result.workflow_id,
            "workflow_type": result.workflow_type,
            "audit_trail_length": len(result.audit_trail),
            "error_count": len(result.errors),
            "success": len(result.errors) == 0,
        }

        # Add type-specific fields
        specific_data = self._extract_protocol_fields(event)

        self._logger.info(
            "workflow_completed",
            **base_data,
            **specific_data,
        )

    def log_workflow_error(self, event: BaseWorkflowEvent, error: Exception) -> None:
        """Log workflow error.

        Args:
            event: Failed workflow event
            error: The exception that occurred
        """
        base_data = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "status": event.status,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_details": event.error,
            "completed_at": str(event.completed_at) if event.completed_at else None,
        }

        # Add type-specific fields
        specific_data = self._extract_protocol_fields(event)

        self._logger.error(
            "workflow_error",
            exc_info=error,
            **base_data,
            **specific_data,
        )

    def _extract_protocol_fields(self, event: BaseWorkflowEvent) -> dict[str, str | bool | None]:
        """Extract type-specific fields using attribute checks.

        Args:
            event: Workflow event to extract fields from

        Returns:
            Dictionary of type-specific fields
        """
        fields: dict[str, str | bool | None] = {}

        # Use isinstance checks with concrete event types
        if isinstance(event, PlaceOrderWorkflowEvent):
            fields.update({
                "symbol": event.symbol,
                "side": str(event.side),
                "quantity": str(event.quantity),
                "price": str(event.price) if event.price else None,
                "order_type": str(event.order_type),
                "strategy_id": event.strategy_id,
            })

        elif isinstance(event, RebalanceWorkflowEvent):
            fields.update({
                "max_slippage": str(event.max_slippage),
                "rebalance_mode": event.rebalance_mode,
                "dry_run": event.dry_run,
            })

        elif isinstance(event, EmergencyLiquidationEvent):
            fields.update({
                "reason": event.reason,
                "force": event.force,
                "max_loss": str(event.max_loss) if event.max_loss else None,
            })

        elif isinstance(event, GracefulShutdownEvent):
            fields.update({
                "close_positions": event.close_positions,
                "save_state": event.save_state,
                "notify_services": event.notify_services,
                "timeout_seconds": str(event.timeout_seconds) if event.timeout_seconds else None,
            })

        return fields
