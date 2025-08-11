"""Workflow context model for orchestration workflows."""

import time
from decimal import Decimal

import msgspec

from cyberdelta.enums.trading import OrderSide, OrderType


class WorkflowAuditEntry(msgspec.Struct):
    """Audit trail entry for workflow execution using msgspec."""

    step: str
    status: str
    details: str | None = None
    timestamp: float = msgspec.field(default_factory=time.time)


class WorkflowContextModel(msgspec.Struct):
    """Context model passed through workflow steps using msgspec.

    This high-performance context will integrate with bubus.Context
    when fully migrated.
    """

    # Workflow metadata (required fields first)
    workflow_id: str
    workflow_type: str
    timeout: float

    # Workflow data - enums work directly in msgspec (optional fields with defaults)
    # Symbol must be string as it's a Pydantic model
    symbol: str | None = None  # Symbol name as string (Pydantic models not supported)
    side: OrderSide | None = None  # Enum works directly
    order_type: OrderType | None = None  # Enum works directly
    quantity: Decimal | None = None
    price: Decimal | None = None

    # Execution tracking
    errors: list[str] = []  # msgspec automatically handles empty collections
    audit_trail: list[WorkflowAuditEntry] = []  # msgspec automatically handles empty collections

    # Additional context fields
    reason: str | None = None
    force: bool = False
    close_positions: bool = False
    start_time: float = msgspec.field(default_factory=time.time)

    def add_audit(self, step: str, status: str, details: str | None = None) -> None:
        """Add audit trail entry.

        Args:
            step: Name of the workflow step
            status: Status of the step execution
            details: Optional additional details
        """
        entry = WorkflowAuditEntry(step=step, status=status, details=details)
        self.audit_trail.append(entry)
